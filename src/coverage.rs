use anyhow::{Ok, Result, anyhow};
use ext_sort::{ExternalSorter, ExternalSorterBuilder, buffer::LimitedBufferBuilder};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use parquet::schema::types::Type;
use rayon::prelude::*;
use s2::{
    cap::Cap,
    cell::Cell,
    cellid::CellID,
    region::RegionCoverer,
    s1::{Angle, ChordAngle},
};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

/// Computes the spatial coverage of a set of places.
pub fn build_coverage(places: &Path, output: &Path) -> Result<()> {
    // To avoid deadlock, we must not use Rayon threads here.
    // https://dev.to/sgchris/scoped-threads-with-stdthreadscope-in-rust-163-48f9
    let (tx, rx) = sync_channel(50_000);
    std::thread::scope(|s| {
        let producer = s.spawn(|| read_places(places, tx));
        let consumer = s.spawn(|| build_spatial_coverage(rx, output));
        producer.join().unwrap().and(consumer.join().unwrap())
    })
}

/// The granularity of S2 cells we use to represent spatial coverage.
///
/// At level 19, An S2 cell is about 15 to 20 meters wide, see [S2 Cell
/// Statistics](https://s2geometry.io/resources/s2cell_statistics.html).
/// For an interactive visualization, see [S2 Region Coverer Online
/// Viewer](https://igorgatis.github.io/ws2/ for a visualization).
const S2_GRANULARITY_LEVEL: u8 = 19;

/// To store the coverage map as a bitvector in a more compact form,
/// we do not store the actual S2 cell IDs because this would lead
/// to a sparse bitvector without contiguous runs. Rather, we shift the
/// unsigned 64-bit integer ids to the right, so that neighboring
/// parent cells (at our finest granularity, S2_GRANULARITY_LEVEL)
/// become neighboring bits in the bitvector. This leads to a better
/// run-length encoding. S2 cell ids use the most significant three
/// bits to encode the cube face [0..5], and then two bits for each
/// level of granularity.
const S2_CELL_ID_SHIFT: u8 = 64 - (3 + 2 * S2_GRANULARITY_LEVEL);

fn read_places(places: &Path, covering: SyncSender<CellID>) -> Result<()> {
    let reader = SerializedFileReader::new(File::open(places)?)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema();
    let s2_cell_id_column = column_index("s2_cell_id", schema)?;
    // let source_column = column_index("source", schema)?;
    let tags_column = column_index("tags", schema)?;
    let num_row_groups = reader.num_row_groups();

    let large_radius = meters_to_chord_angle(100.0);
    let small_radius = meters_to_chord_angle(10.0);
    let coverer = RegionCoverer {
        max_cells: 8,
        min_level: S2_GRANULARITY_LEVEL,
        max_level: S2_GRANULARITY_LEVEL,
        level_mod: 1,
    };

    (0..num_row_groups)
        .into_par_iter()
        .try_for_each(|row_group_index| {
            // Because Apache’s implementation of Parquet is not
            // thread-safe, but the alternative implementation in
            // arrow2 is deprecated, we let each worker thread have
            // its own SerializedFileReader, each reading one row
            // group in the same Parquet file. This is a little
            // wasteful, but it’s actually not too bad.  In our
            // Parquet file for the full AllThePlace dump of
            // 2026-01-03, there were 24 row groups in total.
            // An earlier version of this code was using the
            // alternative implementation of the parquet2 crate,
            // but our application code got awfully complicated
            // when using that low-level library.
            let reader = SerializedFileReader::new(File::open(places)?)?;
            let row_group = reader.get_row_group(row_group_index)?;
            for row in row_group.get_row_iter(None)? {
                let row = row?;
                let s2_cell = Cell::from(CellID(row.get_ulong(s2_cell_id_column)?));
                // let source = row.get_string(source_column)?;
                let tags = row.get_map(tags_column)?.entries();
                let mut radius = small_radius;
                for (key, value) in tags.iter() {
                    use parquet::record::Field::Str;
                    if let (Str(key), Str(value)) = (key, value) {
                        radius = radius.max(match (key.as_ref(), value.as_ref()) {
                            ("shop", _) => large_radius,
                            ("tourism", _) => large_radius,
                            ("public_transport", "platform") => large_radius,
                            ("railway", "platform") => large_radius,
                            (_, _) => small_radius,
                        });
                    }
                }
                let cap = Cap::from_center_chordangle(&s2_cell.center(), &radius);
                for cell_id in coverer.covering(&cap).0.into_iter() {
                    covering.send(cell_id)?;
                }
            }
            Ok(())
        })?;

    Ok(())
}

fn column_index(name: &str, schema: &Type) -> Result<usize> {
    for (i, field) in schema.get_fields().iter().enumerate() {
        if field.name() == name {
            return Ok(i);
        }
    }
    Err(anyhow!("column \"{}\" not found", name))
}

fn meters_to_chord_angle(radius_meters: f64) -> ChordAngle {
    use s2::s1::angle::Rad;
    const EARTH_RADIUS_METERS: f64 = 6_371_000.0;
    ChordAngle::from(Angle::from(Rad(radius_meters / EARTH_RADIUS_METERS)))
}

/// Builds a spatial coverage file from a stream of s2::CellIDs.
fn build_spatial_coverage(cells: Receiver<CellID>, out: &Path) -> Result<()> {
    let mut writer = CoverageWriter::try_new(out)?;
    let sorter: ExternalSorter<CellID, std::io::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(Path::new("./"))
            .with_buffer(LimitedBufferBuilder::new(
                10_000_000, /* preallocate */ true,
            ))
            .build()?;
    let sorted = sorter.sort(cells.iter().map(std::io::Result::Ok))?;
    for cur in sorted {
        writer.write(cur?.0 >> S2_CELL_ID_SHIFT)?;
    }
    writer.close()?;
    Ok(())
}

struct CoverageWriter {
    run_start_writer: BufWriter<File>,
    run_length_writer: BufWriter<File>,

    num_values: u64,
    num_runs: u64,
    run_start: Option<u64>,
    run_length_minus_1: u8,
}

impl CoverageWriter {
    fn try_new(path: &Path) -> Result<CoverageWriter> {
        let file = File::create(path)?;
        let run_start_writer = BufWriter::with_capacity(32768, file);
        let run_length_path = path.with_extension("tmp_run_lengths");
        let run_length_file = File::create(run_length_path)?;
        let run_length_writer = BufWriter::with_capacity(32768, run_length_file);
        // todo: write file header, with zero offsets to run_start and run_length
        Ok(CoverageWriter {
            run_start_writer,
            run_length_writer,
            num_values: 0,
            num_runs: 0,
            run_start: None,
            run_length_minus_1: 0,
        })
    }

    fn write(&mut self, value: u64) -> Result<()> {
        let Some(run_start) = self.run_start else {
            self.num_values = 1;
            self.num_runs = 1;
            self.run_start = Some(value);
            self.run_length_minus_1 = 0;
            return Ok(());
        };

        let run_end: u64 = run_start + (self.run_length_minus_1 as u64);
        assert!(
            value >= run_end,
            "values not written in sort order: {} after {}",
            value,
            run_end
        );

        if value == run_end {
            // If we write the same value twice, we don’t need to do anything.
            return Ok(());
        } else if value == run_end + 1 && self.run_length_minus_1 < 0xff {
            // Extending the length of the current run, if there’s still
            // enough space to hold the new length in the available 8 bits.
            self.run_length_minus_1 += 1;
            self.num_values += 1;
            return Ok(());
        }

        // Start a new run with the current value.
        self.finish_run()?;
        self.run_start = Some(value);
        self.run_length_minus_1 = 0;
        self.num_values += 1;
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.finish_run()?;
        self.run_start_writer.flush()?;
        self.run_length_writer.flush()?;

        // todo: add runs at end of file

        // todo: seek to header position, fix up positions
        println!(
            "got num_values={} num_runs={}",
            self.num_values, self.num_runs
        );
        Ok(())
    }

    fn finish_run(&mut self) -> Result<()> {
        let Some(run_start) = self.run_start else {
            return Ok(());
        };
        self.run_start_writer.write_all(&run_start.to_le_bytes())?;
        self.run_length_writer
            .write_all(&[self.run_length_minus_1])?;
        self.num_runs += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{S2_CELL_ID_SHIFT, S2_GRANULARITY_LEVEL, build_coverage};
    use s2::cellid::CellID;
    use std::path::PathBuf;

    #[test]
    fn test_cell_id_shift() {
        let id = CellID::from_face_pos_level(3, 0x12345678, S2_GRANULARITY_LEVEL as u64);
        let range_len = id.range_max().0 - id.range_min().0 + 2;
        assert_eq!(id.level() as u8, S2_GRANULARITY_LEVEL);
        assert_eq!(range_len.ilog2() as u8, S2_CELL_ID_SHIFT);
    }

    #[test]
    fn test_build_coverage() {
        let mut atp = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        atp.push("tests/test_data/alltheplaces.parquet");
        let spatial_cov = PathBuf::from("test_build_coverage.spatial-coverage");
        build_coverage(&atp, &spatial_cov).unwrap();
    }
}
