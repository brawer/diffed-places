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

pub fn is_wikidata_key(key: &str) -> bool {
    key == "wikidata" || (key.ends_with(":wikidata") && key != "species:wikidata")
}

pub fn parse_wikidata_ids(input: &str) -> impl Iterator<Item = u64> + '_ {
    input.split(';').filter_map(|part| {
        let trimmed = part.trim();
        let digits = trimmed
            .strip_prefix('Q')
            .or_else(|| trimmed.strip_prefix('q'))?;
        digits.parse::<u64>().ok()
    })
}

#[cfg(test)]
mod tests {
    use super::{is_wikidata_key, parse_wikidata_ids};

    #[test]
    fn test_is_wikidata_id() {
        assert_eq!(is_wikidata_key("highway"), false);
        assert_eq!(is_wikidata_key("wikidata"), true);
        assert_eq!(is_wikidata_key("brand:wikidata"), true);
        assert_eq!(is_wikidata_key("network:wikidata"), true);
        assert_eq!(is_wikidata_key("operator:wikidata"), true);
        assert_eq!(is_wikidata_key("species:wikidata"), false);
    }

    #[test]
    fn test_parse_wikidata_ids() {
        let ids: Vec<u64> = parse_wikidata_ids(" Q123;Q813 ; q21").collect();
        assert_eq!(ids, vec![123, 813, 21]);
    }
}

mod reader {
    use super::S2_CELL_ID_SHIFT;
    use anyhow::{Ok, Result, anyhow};
    use memmap2::Mmap;
    use s2::cellid::CellID;
    use std::fs::File;
    use std::path::Path;

    pub struct Coverage<'a> {
        /// Backing store for `mmap`.
        _file: File,

        /// Backing store for `run_starts`, `run_lengths` and `wikidata_ids`.
        _mmap: Mmap,

        /// Run-length encoded s2 cell ids, shifted by S2_CELL_ID_SHIFT.
        run_starts: &'a [u64],
        run_lengths: &'a [u8],

        /// Sorted array of covered wikidata ids.
        wikidata_ids: &'a [u64],
    }

    impl<'a> Coverage<'a> {
        pub fn contains_s2_cell(&self, cell: &CellID) -> bool {
            let val: u64 = cell.0 >> S2_CELL_ID_SHIFT;
            let index = if cfg!(target_endian = "little") {
                self.run_starts.partition_point(|&x| x <= val)
            } else {
                self.run_starts.partition_point(|&x| x.swap_bytes() <= val)
            };
            if index > 0 {
                let start = self.run_starts[index - 1];
                let limit = start + self.run_lengths[index - 1] as u64;
                start <= val && val <= limit
            } else {
                false
            }
        }

        #[allow(dead_code)] // TODO: Remove attribute once we use this function.
        pub fn contains_wikidata_item(&self, id: u64) -> bool {
            if cfg!(target_endian = "little") {
                self.wikidata_ids.binary_search(&id).is_ok()
            } else {
                self.wikidata_ids
                    .binary_search_by(|x| x.swap_bytes().cmp(&id))
                    .is_ok()
            }
        }

        pub fn load(path: &Path) -> Result<Coverage<'_>> {
            let file = File::open(path)?;

            // SAFETY: We don’t truncate the file while it’s mapped into memory.
            let mmap = unsafe { Mmap::map(&file)? };

            Self::check_signature(&mmap)?;

            let run_starts = {
                let (offset, size) = Self::get_offset_size(b"runstart", &mmap, 8)
                    .ok_or(anyhow!("no run starts in coverage file {:?}", path))?;
                // SAFETY: Alignment and bounds checked by get_offset_size().
                unsafe {
                    let ptr = mmap.as_ptr().add(offset) as *const u64;
                    std::slice::from_raw_parts(ptr, size / 8)
                }
            };

            let run_lengths = {
                let (offset, size) = Self::get_offset_size(b"runlengt", &mmap, 1)
                    .ok_or(anyhow!("no run lengths in coverage file {:?}", path))?;
                // SAFETY: Bounds checked by get_offset_size().
                unsafe { std::slice::from_raw_parts(mmap.as_ptr().add(offset), size) }
            };

            if run_starts.len() != run_lengths.len() {
                return Err(anyhow!("inconsistent number of runs in {:?}", path));
            }

            let wikidata_ids = {
                let (offset, size) = Self::get_offset_size(b"wikidata", &mmap, 8)
                    .ok_or(anyhow!("no wikidata ids in coverage file {:?}", path))?;
                // SAFETY: Alignment and bounds checked by get_offset_size().
                unsafe {
                    let ptr = mmap.as_ptr().add(offset) as *const u64;
                    std::slice::from_raw_parts(ptr, size / 8)
                }
            };

            Ok(Coverage {
                _file: file,
                _mmap: mmap,
                run_starts,
                run_lengths,
                wikidata_ids,
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
        // would have to restrict its input to files smaller than 4 GiB.
        // However, we’re not doing a retro-computing project, so we’ll never
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
    use super::{
        Coverage, S2_CELL_ID_SHIFT, S2_GRANULARITY_LEVEL, is_wikidata_key, parse_wikidata_ids,
    };
    use crate::PROGRESS_BAR_STYLE;
    use anyhow::{Ok, Result, anyhow};
    use ext_sort::{ExternalSorter, ExternalSorterBuilder, buffer::LimitedBufferBuilder};
    use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
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
    use std::collections::HashSet;
    use std::fs::{File, remove_file, rename};
    use std::hash::{BuildHasherDefault, Hasher};
    use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

    /// Computes the spatial coverage of a set of places.
    pub fn build_coverage(atp: &Path, progress: &MultiProgress, workdir: &Path) -> Result<PathBuf> {
        assert!(atp.exists());

        let out = workdir.join("coverage");
        if out.exists() {
            log::info!("skipping build_coverage, already found {:?}", out);
            return Ok(out);
        } else {
            log::info!("build_coverage is starting");
        }

        let mut tmp = PathBuf::from(&out);
        tmp.add_extension("tmp");
        let mut writer = CoverageWriter::try_new(&tmp)?;

        // To avoid deadlock, we must not use Rayon threads here.
        // https://dev.to/sgchris/scoped-threads-with-stdthreadscope-in-rust-163-48f9
        let (cell_tx, cell_rx) = sync_channel::<CellID>(50_000);
        let (wikidata_tx, wikidata_rx) = sync_channel::<u64>(1000);

        // The AllThePlaces dump of 2026-01-03 references 3791 unique wikidata ids.
        let mut wikidata_ids = Vec::<u64>::new();
        std::thread::scope(|s| {
            let producer = s.spawn(|| read_places(atp, progress, cell_tx, wikidata_tx));
            let cell_consumer = s.spawn(|| build_spatial_coverage(cell_rx, progress, &mut writer));
            let wikidata_consumer = s.spawn(|| {
                wikidata_ids = collect_wikidata_ids(wikidata_rx);
                Ok(())
            });
            producer
                .join()
                .expect("producer panic")
                .and(cell_consumer.join().expect("cell consumer panic"))
                .and(wikidata_consumer.join().expect("wikidata consumer panic"))
        })?;

        writer.set_wikidata_ids(wikidata_ids);
        writer.close()?;
        rename(&tmp, &out)?;

        // As a sanity check, let’s try to open the freshly generated file.
        // This verifies the presence of the correct file signature, and that
        // the run_starts and run_lengths array are correctly aligned and
        // within allowable bounds.
        _ = Coverage::load(&out)?;

        log::info!("build_spatial_coverage finished, built {:?}", out);
        Ok(out)
    }

    fn read_places(
        places: &Path,
        progress: &MultiProgress,
        out_cells: SyncSender<CellID>,
        out_wikidata_ids: SyncSender<u64>,
    ) -> Result<()> {
        use anyhow::Context;
        let file =
            File::open(places).with_context(|| format!("could not open file `{:?}`", places))?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let file_metadata = metadata.file_metadata();
        let schema = file_metadata.schema();
        let s2_cell_id_column = column_index("s2_cell_id", schema)?;
        // let source_column = column_index("source", schema)?;
        let tags_column = column_index("tags", schema)?;
        let num_row_groups = reader.num_row_groups();
        let num_rows: i64 = file_metadata.num_rows();
        let num_rows: u64 = if num_rows < 0 { 0 } else { num_rows as u64 };

        let large_radius = meters_to_chord_angle(100.0);
        let small_radius = meters_to_chord_angle(10.0);
        let coverer = RegionCoverer {
            max_cells: 8,
            min_level: S2_GRANULARITY_LEVEL,
            max_level: S2_GRANULARITY_LEVEL,
            level_mod: 1,
        };

        let bar = progress.add(ProgressBar::new(num_rows));
        bar.set_style(ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
        bar.set_prefix("cov.read ");
        bar.set_message("features");

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
                            if is_wikidata_key(key) {
                                for id in parse_wikidata_ids(value) {
                                    out_wikidata_ids.send(id)?;
                                }
                            }
                        }
                    }
                    let cap = Cap::from_center_chordangle(&s2_cell.center(), &radius);
                    for cell_id in coverer.covering(&cap).0.into_iter() {
                        out_cells.send(cell_id)?;
                    }
                    bar.inc(1);
                }

                Ok(())
            })?;

        bar.finish();
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
    fn build_spatial_coverage(
        cells: Receiver<CellID>,
        progress: &MultiProgress,
        writer: &mut CoverageWriter,
    ) -> Result<()> {
        let num_cells = AtomicU64::new(0);
        let sorter: ExternalSorter<CellID, std::io::Error, LimitedBufferBuilder> =
            ExternalSorterBuilder::new()
                .with_tmp_dir(Path::new("./"))
                .with_buffer(LimitedBufferBuilder::new(
                    1_000_000, /* preallocate */ true,
                ))
                .build()?;
        let sorted = sorter.sort(cells.iter().map(|x| {
            num_cells.fetch_add(1, Ordering::SeqCst);
            std::io::Result::Ok(x)
        }))?;

        let bar = progress.add(ProgressBar::new(num_cells.load(Ordering::SeqCst)));
        bar.set_style(ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
        bar.set_prefix("cov.write");
        bar.set_message("s2 cells");

        for cur in sorted {
            writer.write(cur?.0 >> S2_CELL_ID_SHIFT)?;
            bar.inc(1);
        }

        bar.finish();

        Ok(())
    }

    fn collect_wikidata_ids(stream: Receiver<u64>) -> Vec<u64> {
        // The AllThePlaces dump of 2026-01-03 references 3791 unique wikidata ids.
        let mut ids = HashSet::with_capacity_and_hasher(8192, NoOpBuildHasher::default());
        let mut last: u64 = 0; // not a valid wikidata id
        for id in stream {
            if id != last {
                ids.insert(id);
                last = id;
            }
        }

        let mut sorted_ids: Vec<u64> = ids.into_iter().collect();
        sorted_ids.sort();
        sorted_ids
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

        wikidata_ids: Vec<u64>,
    }

    impl CoverageWriter {
        const NUM_HEADERS: usize = 3;

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
                wikidata_ids: Vec::default(),
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

        fn set_wikidata_ids(&mut self, ids: Vec<u64>) {
            assert!(ids.is_sorted());
            self.wikidata_ids = ids;
        }

        fn write_wikidata_ids(&mut self) -> Result<()> {
            for id in &self.wikidata_ids {
                self.writer.write_all(&id.to_le_bytes())?;
            }
            Ok(())
        }

        fn close(mut self) -> Result<()> {
            self.finish_run()?;
            self.writer.flush()?;

            // Append the run lengths, which are at this point in time in a temporary file,
            // to the end of the main file.
            let run_lengths_pos = self.writer.stream_position()?;
            self.run_lengths_writer.flush()?;
            assert_eq!(self.run_lengths_writer.stream_position()?, self.num_runs);
            self.run_lengths_writer.seek(SeekFrom::Start(0))?;
            let run_lengths_path: &Path = self.run_lengths_path.as_path();
            let mut reader = BufReader::new(File::open(run_lengths_path)?);
            std::io::copy(&mut reader, &mut self.writer)?;
            remove_file(run_lengths_path)?;

            // Append wikidata ids.
            self.write_padding(8)?;
            let wikidata_ids_pos = self.writer.stream_position()?;
            self.write_wikidata_ids()?;
            let wikidata_ids_size = self.writer.stream_position()? - wikidata_ids_pos;

            self.write_headers(&[
                ("runstart", self.run_starts_pos, self.num_runs * 8),
                ("runlengt", run_lengths_pos, self.num_runs),
                ("wikidata", wikidata_ids_pos, wikidata_ids_size),
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

        fn write_padding(&mut self, alignment: usize) -> Result<()> {
            let pos = self.writer.stream_position()?;
            let alignment = alignment as u64;
            let num_bytes = ((alignment - (pos % alignment)) % alignment) as usize;
            if num_bytes > 0 {
                let padding = vec![0; num_bytes];
                self.writer.write_all(&padding)?;
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct NoOpHasher(u64);

    impl Hasher for NoOpHasher {
        fn write(&mut self, bytes: &[u8]) {
            if let Some(value) = bytes.last() {
                self.0 = *value as u64;
            }
        }

        fn write_u64(&mut self, value: u64) {
            self.0 = value;
        }

        fn finish(&self) -> u64 {
            self.0
        }
    }

    type NoOpBuildHasher = BuildHasherDefault<NoOpHasher>;

    #[cfg(test)]
    mod tests {
        use super::super::Coverage;
        use super::{CoverageWriter, S2_CELL_ID_SHIFT, S2_GRANULARITY_LEVEL, build_coverage};
        use anyhow::{Ok, Result};
        use indicatif::{MultiProgress, ProgressDrawTarget};
        use s2::cellid::CellID;
        use std::path::PathBuf;
        use tempfile::{NamedTempFile, TempDir};

        #[test]
        fn test_cell_id_shift() {
            let id = CellID::from_face_pos_level(3, 0x12345678, S2_GRANULARITY_LEVEL as u64);
            let range_len = id.range_max().0 - id.range_min().0 + 2;
            assert_eq!(id.level() as u8, S2_GRANULARITY_LEVEL);
            assert_eq!(range_len.ilog2() as u8, S2_CELL_ID_SHIFT);
        }

        #[test]
        fn test_build_coverage() -> Result<()> {
            use std::os::unix::fs::symlink;
            let mut atp = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            atp.push("tests/test_data/alltheplaces.parquet");
            let workdir = TempDir::new()?;
            symlink(&atp, workdir.path().join("alltheplaces.parquet"))?;
            let progress = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
            let coverage = build_coverage(&atp, &progress, workdir.path())?;
            assert!(coverage.exists());
            Ok(())
        }

        #[test]
        fn test_coverage_writer() -> Result<()> {
            let temp_file = NamedTempFile::new()?;
            let mut writer = CoverageWriter::try_new(temp_file.path())?;
            writer.write(7)?;
            for i in 1000..=1258 {
                writer.write(i)?;
            }
            writer.set_wikidata_ids(vec![23, 77, 88]);
            writer.close()?;

            let cov = Coverage::load(temp_file.path())?;
            for i in &[0, 5, 6, 8, 9, 999, 1259] {
                let cell = CellID(i << S2_CELL_ID_SHIFT);
                assert!(
                    !cov.contains_s2_cell(&cell),
                    "cell {:?} for i={} should not be covered, but is",
                    cell,
                    i,
                );
            }
            for i in &[7, 1000, 1001, 1002, 1111, 1254, 1255, 1256, 1257, 1258] {
                let cell = CellID(i << S2_CELL_ID_SHIFT);
                assert!(
                    cov.contains_s2_cell(&cell),
                    "cell {:?} for i={} should be covered, but is not",
                    cell,
                    i,
                );
            }

            assert_eq!(cov.contains_wikidata_item(1), false);
            assert_eq!(cov.contains_wikidata_item(23), true);
            assert_eq!(cov.contains_wikidata_item(51), false);
            assert_eq!(cov.contains_wikidata_item(77), true);
            assert_eq!(cov.contains_wikidata_item(88), true);
            assert_eq!(cov.contains_wikidata_item(89), false);

            Ok(())
        }
    }

    #[test]
    fn test_no_op_hasher() {
        let mut h = NoOpHasher(7);

        h.write_u64(2600);
        assert_eq!(h.finish(), 2600);

        h.write(&[31, 33, 7]);
        assert_eq!(h.finish(), 7);
    }
}
