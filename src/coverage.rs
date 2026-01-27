pub use reader::Coverage;
pub use writer::build_coverage;

/// The granularity of S2 cells we use to represent spatial coverage.
///
/// At level 19, An S2 cell is about 15 to 20 meters wide, see [S2 Cell
/// Statistics](https://s2geometry.io/resources/s2cell_statistics.html).
/// For an interactive visualization, see [S2 Region Coverer Online
// Viewer](https://igorgatis.github.io/ws2/ for a visualization).
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

mod reader {
    use super::S2_CELL_ID_SHIFT;
    use anyhow::{Ok, Result, anyhow};
    use memmap2::Mmap;
    use s2::cellid::CellID;
    use std::fs::File;
    use std::path::Path;

    #[allow(dead_code)] // TODO: Remove attribute once we use this struct.
    pub struct Coverage {
        file: File,
        mmap: Mmap,

        num_runs: usize,
        run_starts_offset: usize,
        run_lengths_offset: usize,
    }

    impl Coverage {
        #[allow(dead_code)] // TODO: Remove attribute once we use this function.
        pub fn is_covering(&self, cell: &CellID) -> bool {
            // SAFETY: Alignment and bounds already checked by get_offset_size().
            let run_starts = unsafe {
                let ptr = self.mmap.as_ptr().add(self.run_starts_offset) as *const u64;
                std::slice::from_raw_parts(ptr, self.num_runs)
            };

            let value: u64 = cell.0 >> S2_CELL_ID_SHIFT;
            let index = if cfg!(target_endian = "little") {
                run_starts.partition_point(|&start| start <= value)
            } else {
                run_starts.partition_point(|&start| start.swap_bytes() <= value)
            };
            if index > 0 {
                let start = run_starts[index - 1];
                let limit = start + self.mmap[self.run_lengths_offset + index - 1] as u64;
                start <= value && value <= limit
            } else {
                false
            }
        }

        pub fn load(path: &Path) -> Result<Coverage> {
            let file = File::open(path)?;

            // SAFETY: No other process writes to the file while we read it.
            let mmap = unsafe { Mmap::map(&file)? };

            Self::check_signature(&mmap)?;

            let (run_starts_offset, run_starts_size) = Self::get_offset_size(b"runstart", &mmap, 8)
                .ok_or(anyhow!("no \"runstart\" in coverage file {:?}", path))?;
            let (run_lengths_offset, run_lengths_size) =
                Self::get_offset_size(b"runlengt", &mmap, 1)
                    .ok_or(anyhow!("no \"runlengt\" in coverage file {:?}", path))?;
            if run_starts_size != run_lengths_size * 8 {
                return Err(anyhow!("inconsistent number of runs in {:?}", path));
            }
            let num_runs = run_lengths_size;

            Ok(Coverage {
                file,
                mmap,
                num_runs,
                run_starts_offset,
                run_lengths_offset,
            })
        }

        fn check_signature(data: &[u8]) -> Result<()> {
            if data.len() < 24 || &data[0..24] != b"diffed-places coverage\0\0" {
                return Err(anyhow!("malformed coverage file"));
            }
            Ok(())
        }

        // This implementation only works on 64-bit processors. Theoretically,
        // we could write a special implementation for 32-bit platforms, which
        // would either have to restrict its input to files smaller than 4 GiB.
        // However, since we’re not doing a retro-computing project, we’ll never
        // execute our code on such machines.
        #[cfg(target_pointer_width = "64")]
        fn get_offset_size(id: &[u8; 8], bytes: &[u8], alignment: usize) -> Option<(usize, usize)> {
            if bytes.len() < 32 {
                return None;
            }
            let num_headers = u64::from_le_bytes(bytes[24..32].try_into().ok()?) as usize;
            if bytes.len() < 32 + num_headers * 24 {
                return None;
            }
            for i in 0..num_headers {
                let pos = 32 + i * 24;
                if *id == bytes[pos..pos + 8] {
                    let offset =
                        u64::from_le_bytes(bytes[pos + 8..pos + 16].try_into().ok()?) as usize;
                    let size =
                        u64::from_le_bytes(bytes[pos + 16..pos + 24].try_into().ok()?) as usize;
                    if bytes.len() >= offset + size
                        && offset.is_multiple_of(alignment)
                        && size.is_multiple_of(alignment)
                    {
                        return Some((offset, size));
                    }
                }
            }
            None
        }
    }

    #[cfg(test)]
    mod tests {
        use super::Coverage;

        #[test]
        fn test_check_signature() {
            assert!(Coverage::check_signature(b"").is_err());
            assert!(Coverage::check_signature(b"foo").is_err());
            assert!(Coverage::check_signature(b"diffed-places coverage\0\x01").is_err());
            assert!(Coverage::check_signature(b"diffed-places coverage\0\0\0\0\0\0").is_ok());
        }

        #[test]
        fn test_get_offset_size() {
            // Construct a coverage file with two keys in its header.
            let mut bytes = Vec::new();
            bytes.extend_from_slice(b"diffed-places coverage\0\0");
            bytes.extend_from_slice(&2_u64.to_le_bytes());
            assert!(Coverage::get_offset_size(b"some_key", &bytes, 1) == None);
            for (key, offset, size) in [(b"some_key", 80, 16), (b"otherkey", 83, 2)] {
                bytes.extend_from_slice(key as &[u8; 8]);
                bytes.extend_from_slice(&(offset as u64).to_le_bytes());
                bytes.extend_from_slice(&(size as u64).to_le_bytes());
            }
            bytes.extend_from_slice(&0xdeadbeefcafefeed_u64.to_le_bytes());
            bytes.extend_from_slice(&42_u64.to_le_bytes());
            let bytes = bytes.as_ref();

            // The file header has no entry for key "in-exist".
            assert_eq!(Coverage::get_offset_size(b"in-exist", bytes, 1), None);

            // The data for "some_key" starts at offset 80 and is 16 bytes, which is
            // correctly aligned for single-byte, 8-byte and 16-byte access.
            // However, it would be unsafe (at least on RISC CPUs) to perform 32-byte
            // access, eg. loading a SIMD register, because 80 is not divisible by 32.
            // Also, the data size is too small to read 32-byte entitites anyway.
            assert_eq!(
                Coverage::get_offset_size(b"some_key", bytes, 1),
                Some((80, 16))
            );
            assert_eq!(
                Coverage::get_offset_size(b"some_key", bytes, 2),
                Some((80, 16))
            );
            assert_eq!(
                Coverage::get_offset_size(b"some_key", bytes, 8),
                Some((80, 16))
            );
            assert_eq!(Coverage::get_offset_size(b"some_key", bytes, 32), None);

            // The data for "otherkey" starts at offset 83 and is 2 bytes long.
            // Access is only safe with single-byte alignment.
            assert_eq!(
                Coverage::get_offset_size(b"otherkey", bytes, 1),
                Some((83, 2))
            );
            assert_eq!(Coverage::get_offset_size(b"otherkey", bytes, 2), None);
            assert_eq!(Coverage::get_offset_size(b"otherkey", bytes, 8), None);
        }
    }
}

mod writer {
    use super::{Coverage, S2_CELL_ID_SHIFT, S2_GRANULARITY_LEVEL};
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
    use std::fs::{File, remove_file};
    use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
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
        })?;

        // As a sanity check, let’s try to open the freshly generated file.
        // This verifies the presence of the correct file signature, and that
        // the run_starts and run_lengths array are correctly aligned and
        // within allowable bounds.
        _ = Coverage::load(output)?;

        Ok(())
    }

    fn read_places(places: &Path, covering: SyncSender<CellID>) -> Result<()> {
        use anyhow::Context;
        let file =
            File::open(places).with_context(|| format!("could not open file `{:?}`", places))?;
        let reader = SerializedFileReader::new(file)?;
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
                    1_000_000, /* preallocate */ true,
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
        writer: BufWriter<File>,
        run_starts_pos: u64, // offset of run_starts array relative to start of file

        run_lengths_path: PathBuf,
        run_lengths_writer: BufWriter<File>,

        num_values: u64,
        num_runs: u64,
        run_start: Option<u64>,
        run_length_minus_1: u8,
    }

    impl CoverageWriter {
        const NUM_HEADERS: usize = 2;

        fn try_new(path: &Path) -> Result<CoverageWriter> {
            let file = File::create(path)?;
            let mut writer = BufWriter::with_capacity(32768, file);

            // Write file header.
            writer.write_all(b"diffed-places coverage\0\0")?;
            writer.write_all(&(Self::NUM_HEADERS as u64).to_le_bytes())?;
            writer.write_all(&[0; 24 * Self::NUM_HEADERS])?; // leave space for headers

            let run_starts_pos = writer.stream_position()?;
            let run_lengths_path = path.with_extension("tmp_run_lengths");
            let run_lengths_file = File::create(&run_lengths_path)?;
            let run_lengths_writer = BufWriter::with_capacity(32768, run_lengths_file);

            Ok(CoverageWriter {
                writer,
                run_starts_pos,
                run_lengths_path,
                run_lengths_writer,
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
            self.num_runs += 1;
            Ok(())
        }

        fn close(mut self) -> Result<()> {
            self.finish_run()?;
            self.writer.flush()?;

            // Append the run lengths, which are at this point in time in a temporary file,
            // to end end of the main file.
            let run_lengths_pos = self.writer.stream_position()?;
            self.run_lengths_writer.flush()?;
            assert_eq!(self.run_lengths_writer.stream_position()?, self.num_runs);
            self.run_lengths_writer.seek(SeekFrom::Start(0))?;

            let run_lengths_path: &Path = self.run_lengths_path.as_path();
            let mut reader = BufReader::new(File::open(run_lengths_path)?);
            std::io::copy(&mut reader, &mut self.writer)?;
            remove_file(run_lengths_path)?;
            self.write_headers(&[
                ("runstart", self.run_starts_pos, self.num_runs * 8),
                ("runlengt", run_lengths_pos, self.num_runs),
            ])?;
            Ok(())
        }

        fn write_headers(&mut self, headers: &[(&str, u64, u64)]) -> Result<()> {
            self.writer.seek(SeekFrom::Start(32))?;
            assert_eq!(headers.len(), Self::NUM_HEADERS);
            for (id, pos, len) in headers {
                assert_eq!(
                    id.len(),
                    8,
                    "header id must be 8 chars long but \"{:?}\" is not",
                    id
                );
                self.writer.write_all(id.as_bytes())?;
                self.writer.write_all(&pos.to_le_bytes())?;
                self.writer.write_all(&len.to_le_bytes())?;
            }
            self.writer.flush()?;
            Ok(())
        }

        fn finish_run(&mut self) -> Result<()> {
            let Some(run_start) = self.run_start else {
                return Ok(());
            };
            self.writer.write_all(&run_start.to_le_bytes())?;
            self.run_lengths_writer
                .write_all(&[self.run_length_minus_1])?;
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::Coverage;
        use super::{CoverageWriter, S2_CELL_ID_SHIFT, S2_GRANULARITY_LEVEL, build_coverage};
        use anyhow::{Ok, Result};
        use s2::cellid::CellID;
        use std::path::PathBuf;
        use tempfile::NamedTempFile;

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

        #[test]
        fn test_coverage_writer() -> Result<()> {
            let temp_file = NamedTempFile::new()?;
            let mut writer = CoverageWriter::try_new(temp_file.path())?;
            writer.write(7)?;
            for i in 1000..=1258 {
                writer.write(i)?;
            }
            writer.close()?;

            let cov = Coverage::load(temp_file.path())?;
            for i in &[0, 5, 6, 8, 9, 999, 1259] {
                let cell = CellID(i << S2_CELL_ID_SHIFT);
                assert!(
                    !cov.is_covering(&cell),
                    "cell {:?} for i={} should not be covered, but is",
                    cell,
                    i,
                );
            }
            for i in &[7, 1000, 1001, 1002, 1111, 1254, 1255, 1256, 1257, 1258] {
                let cell = CellID(i << S2_CELL_ID_SHIFT);
                assert!(
                    cov.is_covering(&cell),
                    "cell {:?} for i={} should be covered, but is not",
                    cell,
                    i,
                );
            }

            Ok(())
        }
    }
}
