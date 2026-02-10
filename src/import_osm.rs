use anyhow::{Context, Ok, Result, anyhow};
use ext_sort::{ExternalSorter, ExternalSorterBuilder, buffer::LimitedBufferBuilder};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use osm_pbf_iter::{Blob, Primitive, PrimitiveBlock, RelationMemberType};
use protobuf_iter::MessageIter;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{File, rename};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread;

use crate::PROGRESS_BAR_STYLE;
use crate::coverage::Coverage;
use crate::u64_table::U64Table;

pub fn import_osm(
    pbf: &Path,
    coverage: &Path,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<()> {
    assert!(workdir.exists());

    let pbf_error = || format!("could not open file `{:?}`", pbf);
    let mut file = File::open(pbf).with_context(pbf_error)?;
    let mut reader = BlobReader::try_new(&mut file).with_context(pbf_error)?;

    // Partition the PBF file into blobs with nodes, ways and relations.
    // Ranges may overlap by one blob, see BlobReader::partition().
    let (nodes_end, ways_end) = reader.partition()?;
    let node_blobs = (0, nodes_end);
    let way_blobs = (nodes_end.saturating_sub(1), ways_end);
    let rel_blobs = (ways_end.saturating_sub(1), reader.num_blobs());

    let coverage = Coverage::load(coverage)
        .with_context(|| format!("could not open coverage file `{:?}`", coverage))?;

    let relations_graph = build_relations_graph(&mut reader, rel_blobs, progress)?;

    // Find which nodes, ways and relations lie within the coverage.
    let covered_nodes = build_covered_nodes(&mut reader, node_blobs, &coverage, progress, workdir)?;
    let covered_ways =
        build_covered_ways(&mut reader, way_blobs, &covered_nodes, progress, workdir)?;
    let _covered_relations = build_covered_relations(
        &mut reader,
        rel_blobs,
        &covered_nodes,
        &covered_ways,
        &relations_graph,
        progress,
        workdir,
    )?;

    Ok(())
}

fn write_u64_table(stream: Receiver<u64>, workdir: &Path, out: &Path) -> Result<u64> {
    let sorter: ExternalSorter<u64, std::io::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(workdir)
            .with_buffer(LimitedBufferBuilder::new(
                5_000_000, /* preallocate */ true,
            ))
            .build()?;
    let sorted = sorter.sort(stream.into_iter().map(std::io::Result::Ok))?;
    let file = File::create(out)?;
    let mut writer = BufWriter::with_capacity(32768, file);
    let mut num_values = 0;
    for value in sorted {
        writer.write_all(&value?.to_le_bytes())?;
        num_values += 1;
    }
    writer.flush()?;
    Ok(num_values)
}

fn build_relations_graph<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    progress: &MultiProgress,
) -> Result<HashMap<u64, u64>> {
    let num_blobs = blobs.1 - blobs.0;
    let bar = Arc::new(progress.add(ProgressBar::new(num_blobs as u64)));
    bar.set_style(ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
    bar.set_prefix("osm.grf.r");
    bar.set_message("blobs");

    let mut result = HashMap::<u64, u64>::new();
    let num_workers = usize::from(thread::available_parallelism()?);
    thread::scope(|s| {
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);

        // Blob producer thread, I/O-bound.
        let bar_clone = Arc::clone(&bar);
        s.spawn(move || {
            for i in blobs.0..blobs.1 {
                let blob = reader.read_blob(i)?;
                blob_tx.send(blob)?;
                bar_clone.inc(1);
            }
            Ok(())
        });

        // Blob consumers, CPU-bound.
        s.spawn(|| {
            result = blob_rx
                .into_iter()
                .par_bridge()
                .try_fold(
                    || HashMap::with_capacity(1024),
                    |mut map, blob| {
                        let data = blob.into_data(); // decompress
                        let block = PrimitiveBlock::parse(&data);
                        for primitive in block.primitives() {
                            if let Primitive::Relation(rel) = primitive {
                                for (_, member_id, member_type) in rel.members() {
                                    if member_type == RelationMemberType::Relation {
                                        map.insert(member_id, rel.id);
                                    }
                                }
                            }
                        }
                        Ok(map)
                    },
                )
                .try_reduce(
                    || HashMap::with_capacity(16384),
                    |mut a, b| {
                        a.extend(b);
                        Ok(a)
                    },
                )?;
            Ok(())
        });

        Ok(())
    })?;

    bar.finish_with_message(format!("{} graph edges", result.len()));
    Ok(result)
}

fn build_covered_nodes<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<U64Table> {
    let num_blobs = blobs.1 - blobs.0;
    let bar = Arc::new(progress.add(ProgressBar::new(num_blobs as u64)));
    bar.set_style(ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
    bar.set_prefix("osm.cov.n");
    bar.set_message("blobs");

    let out = workdir.join("covered-nodes");
    if !out.exists() {
        log::info!("import_osm::build_covered_nodes is starting");
    } else {
        log::info!(
            "skipping import_osm::build_covered_nodes, already found {:?}",
            out
        );
        return Ok(U64Table::open(&out)?);
    }

    let mut tmp = out.clone();
    tmp.add_extension("tmp");

    let mut num_covered_nodes = 0;
    thread::scope(|s| {
        let num_workers = usize::from(thread::available_parallelism()?);
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (node_tx, node_rx) = sync_channel::<u64>(num_workers * 1024);

        // Blob producer thread, I/O-bound.
        let bar_clone = Arc::clone(&bar);
        s.spawn(move || {
            for i in blobs.0..blobs.1 {
                let blob = reader.read_blob(i)?;
                blob_tx.send(blob)?;
                bar_clone.inc(1);
            }
            Ok(())
        });

        // CPU-bound blob consumers (one for each CPU) concurrently
        // read and decompress blobs.  For each node within the
        // spatial coverage area, the node ID is sent to the node
        // consumer thread.
        s.spawn(move || {
            blob_rx.into_iter().par_bridge().try_for_each(|blob| {
                let node_tx = node_tx.clone();
                let data = blob.into_data(); // decompress
                let block = PrimitiveBlock::parse(&data);
                for primitive in block.primitives() {
                    if let Primitive::Node(node) = primitive {
                        let s2_lat_lng = s2::latlng::LatLng::from_degrees(node.lat, node.lon);
                        let cell_id = s2::cellid::CellID::from(s2_lat_lng);
                        if coverage.is_covering(&cell_id) {
                            node_tx.send(node.id)?;
                        }
                    }
                }
                Ok(())
            })?;
            Ok(())
        });

        // Consumer thread. Sorts a stream of node ids, which it
        // receives from the blob consumers over the channel `node_rx`,
        // into a temporary file.
        num_covered_nodes = write_u64_table(node_rx, workdir, &tmp)?;
        Ok(())
    })?;

    rename(&tmp, &out)?;
    bar.finish_with_message(format!("blobs → {} nodes", num_covered_nodes));

    Ok(U64Table::open(&out)?)
}

fn build_covered_ways<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    covered_nodes: &U64Table,
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<U64Table> {
    let out = workdir.join("covered-ways");
    if !out.exists() {
        log::info!("import_osm::build_covered_ways is starting");
    } else {
        log::info!(
            "skipping import_osm::build_covered_ways, already found {:?}",
            out
        );
        return Ok(U64Table::open(&out)?);
    }

    let num_blobs = blobs.1 - blobs.0;
    let bar = Arc::new(progress.add(ProgressBar::new(num_blobs as u64)));
    bar.set_style(ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
    bar.set_prefix("osm.cov.w");
    bar.set_message("blobs");

    let mut tmp = out.clone();
    tmp.add_extension("tmp");

    let num_workers = usize::from(thread::available_parallelism()?);
    let mut num_covered_ways = 0;
    thread::scope(|s| {
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (way_tx, way_rx) = sync_channel::<u64>(num_workers * 1024);

        // Blob producer thread, I/O-bound.
        let bar_clone = Arc::clone(&bar);
        s.spawn(move || {
            for i in blobs.0..blobs.1 {
                let blob = reader.read_blob(i)?;
                blob_tx.send(blob)?;
                bar_clone.inc(1);
            }
            Ok(())
        });

        // CPU-bound blob consumers (one for each CPU) concurrently
        // read and decompress blobs.  For each way that has at least one node
        // within the spatial coverage area, the way ID is sent to the way
        // consumer thread.
        s.spawn(move || {
            blob_rx.into_iter().par_bridge().try_for_each(|blob| {
                let data = blob.into_data(); // decompress
                let block = PrimitiveBlock::parse(&data);
                for primitive in block.primitives() {
                    if let Primitive::Way(way) = primitive {
                        let mut is_covered = false;
                        for node in way.refs() {
                            if node >= 0 && covered_nodes.contains(node as u64) {
                                is_covered = true;
                                break;
                            }
                        }
                        if is_covered {
                            way_tx.send(way.id)?;
                        }
                    }
                }
                Ok(())
            })?;
            Ok(())
        });

        // Consumer thread. Sorts a stream of way ids, which it
        // receives from the blob consumers over the channel `way_rx`,
        // into a temporary file.
        num_covered_ways = write_u64_table(way_rx, workdir, &tmp)?;
        Ok(())
    })?;

    rename(&tmp, &out)?;
    bar.finish_with_message(format!("blobs → {} ways", num_covered_ways));
    Ok(U64Table::open(&out)?)
}

fn build_covered_relations<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    covered_nodes: &U64Table,
    covered_ways: &U64Table,
    _relations_graph: &HashMap<u64, u64>, // TODO: Handle recursive relations.
    progress: &MultiProgress,
    workdir: &Path,
) -> Result<PathBuf> {
    let out = workdir.join("covered-relations");
    if !out.exists() {
        log::info!("import_osm::build_covered_relations is starting");
    } else {
        log::info!(
            "skipping import_osm::build_covered_relations, already found {:?}",
            out
        );
        return Ok(out);
    }

    let mut tmp = out.clone();
    tmp.add_extension("tmp");

    let num_blobs = blobs.1 - blobs.0;
    let bar = Arc::new(progress.add(ProgressBar::new(num_blobs as u64)));
    bar.set_style(ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
    bar.set_prefix("osm.cov.r");
    bar.set_message("blobs");

    let num_workers = usize::from(thread::available_parallelism()?);
    let mut num_relations = 0;
    thread::scope(|s| {
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (rel_tx, rel_rx) = sync_channel::<u64>(num_workers * 1024);

        // Blob producer thread, I/O-bound.
        let bar_clone = Arc::clone(&bar);
        s.spawn(move || {
            for i in blobs.0..blobs.1 {
                let blob = reader.read_blob(i)?;
                blob_tx.send(blob)?;
                bar_clone.inc(1);
            }
            Ok(())
        });

        // CPU-bound blob consumers (one for each CPU) concurrently
        // read and decompress blobs.  For each relation that has at
        // least one node or way within the spatial coverage area, the
        // relation ID is sent to the relation consumer thread.
        s.spawn(move || {
            blob_rx.into_iter().par_bridge().try_for_each(|blob| {
                let data = blob.into_data(); // decompress
                let block = PrimitiveBlock::parse(&data);
                for primitive in block.primitives() {
                    if let Primitive::Relation(rel) = primitive {
                        let mut is_covered = false;
                        for (_, member_id, member_type) in rel.members() {
                            match member_type {
                                RelationMemberType::Node => {
                                    if covered_nodes.contains(member_id) {
                                        is_covered = true;
                                        break;
                                    }
                                }
                                RelationMemberType::Way => {
                                    if covered_ways.contains(member_id) {
                                        is_covered = true;
                                        break;
                                    }
                                }
                                RelationMemberType::Relation => {}
                            }
                        }
                        if is_covered {
                            rel_tx.send(rel.id)?;
                        }
                    }
                }
                Ok(())
            })?;
            Ok(())
        });

        // Consumer thread. Sorts a stream of relation ids, which it
        // receives from the blob consumers over the channel `rel_rx`,
        // into a temporary file.
        num_relations = write_u64_table(rel_rx, workdir, &tmp)?;
        Ok(())
    })?;

    rename(&tmp, &out)?;
    bar.finish_with_message(format!("blobs → {} relations", num_relations));
    Ok(out)
}

/// Reads data blobs from OpenStreetMap PBF files.
struct BlobReader<'a, R: Read + Seek + Send> {
    reader: &'a mut R,

    /// Offset and size of each data blob.
    blobs: Vec<(u64, usize)>,
}

// SAFETY: Can be safely sent across threads, if the underlying reader
// implements the Send trait. With the type trait being declared as
// below (`+ Send`), this gets enforced by the Rust compiler.
unsafe impl<'a, R: Read + Seek + Send> Send for BlobReader<'a, R> {}

impl<'a, R: Read + Seek + Send> BlobReader<'a, R> {
    pub fn try_new(reader: &'a mut R) -> Result<BlobReader<'a, R>> {
        reader.seek(SeekFrom::End(0))?;
        let file_size = reader.stream_position()?;
        if file_size == 0 {
            return Err(anyhow!("empty file"));
        }
        let mut pos = 0_u64;
        let mut blobs = Vec::<(u64, usize)>::new();
        while pos < file_size {
            reader.seek(SeekFrom::Start(pos))?;
            let blob_header = Self::read_blob_header(reader)?;
            let Some((blob_type, data_size)) = Self::parse_blob_header(&blob_header) else {
                return Err(anyhow!("bad blob header at offset {}", pos));
            };
            match blob_type {
                b"OSMHeader" => {}
                b"OSMData" => {
                    blobs.push((pos + 4_u64 + (blob_header.len() as u64), data_size));
                }
                _ => {}
            }
            pos += 4_u64 + (blob_header.len() as u64) + (data_size as u64);
        }

        Ok(BlobReader { reader, blobs })
    }

    pub fn num_blobs(&self) -> usize {
        self.blobs.len()
    }

    pub fn read_blob(&mut self, blob: usize) -> Result<Blob> {
        let (offset, len) = self.blobs[blob];
        let mut buf = Vec::with_capacity(len);
        self.reader.seek(SeekFrom::Start(offset))?;

        // SAFETY: After read_exact(), all bytes in buffer have a defined value.
        unsafe {
            buf.set_len(len);
            self.reader.read_exact(&mut buf)?;
        }
        Self::decode_blob(&buf)
    }

    fn decode_blob(data: &[u8]) -> Result<Blob> {
        for m in MessageIter::new(data) {
            match m.tag {
                1 => return Ok(Blob::Raw(Vec::from(m.value.get_data()))),
                3 => return Ok(Blob::Zlib(Vec::from(m.value.get_data()))),
                _ => {}
            }
        }

        Err(anyhow!("cannot decode blob"))
    }

    /// Partitions the blogs into nodes, ways and relations.
    ///
    /// # Returns
    ///
    /// A tuple `(a, b)` where `a` is the first blob without any nodes,
    /// and `b` is the first blob without either nodes or ways.
    ///
    /// # Warnings
    ///
    /// In the
    /// [OpenStreetMap PBF format](https://wiki.openstreetmap.org/wiki/PBF_Format),
    /// a single blog may contain repeated PrimitiveGroups. While all primitives
    /// in the same must be of the same type (node, way or relation), the format
    /// makes no such guarantee on the blob level.
    pub fn partition(&mut self) -> Result<(usize, usize)> {
        let ways = {
            let mut left = 0;
            let mut right = self.blobs.len();
            while left < right {
                let mid = left + (right - left) / 2;
                let blob = self.read_blob(mid)?;
                if Self::classify(blob)? < 2 {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            left
        };

        let relations = {
            let mut left = ways;
            let mut right = self.blobs.len();
            while left < right {
                let mid = left + (right - left) / 2;
                let blob = self.read_blob(mid)?;
                if Self::classify(blob)? < 3 {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            left
        };

        Ok((ways, relations))
    }

    /// Internal helper for partition().
    fn classify(blob: Blob) -> Result<u8> {
        let data = blob.into_data();
        let block = PrimitiveBlock::parse(&data);
        match block.primitives().next() {
            Some(Primitive::Node(_)) => Ok(1),
            Some(Primitive::Way(_)) => Ok(2),
            Some(Primitive::Relation(_)) => Ok(3),
            None => Err(anyhow!("empty blob")),
        }
    }

    fn read_blob_header<T: Read>(reader: &mut T) -> Result<Vec<u8>> {
        let header_len = {
            let mut header_len_buf = [0; 4];
            reader.read_exact(&mut header_len_buf)?;
            u32::from_be_bytes(header_len_buf) as usize
        };
        let mut header = vec![0; header_len];
        reader.read_exact(&mut header)?;
        Ok(header)
    }

    fn parse_blob_header(data: &[u8]) -> Option<(&[u8], usize)> {
        let mut blob_type: Option<&[u8]> = None;
        let mut data_size: Option<usize> = None;
        for m in MessageIter::new(data) {
            match m.tag {
                1 => blob_type = Some(m.value.get_data()),
                3 => data_size = Some(u32::from(m.value) as usize),
                _ => {}
            }
        }
        Some((blob_type?, data_size?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;

    #[test]
    fn test_blob_reader() -> Result<()> {
        let mut file = File::open(test_data_path("zugerland.osm.pbf"))?;
        let mut reader = BlobReader::try_new(&mut file)?;
        assert_eq!(reader.blobs, &[(119, 16681), (16816, 15278), (32110, 8616)]);
        assert_eq!(reader.num_blobs(), 3);
        assert_eq!(reader.partition()?, (1, 2));
        if let Blob::Zlib(_) = reader.read_blob(2)? {
        } else {
            return Err(anyhow!("failed to read blob"));
        }
        Ok(())
    }

    #[test]
    fn test_blob_reader_decode() -> Result<()> {
        if let Blob::Raw(blob) = BlobReader::<File>::decode_blob(&[0x0a, 1, 77])? {
            assert_eq!(blob, &[77]);
        } else {
            panic!("unexpected blob type");
        }
        if let Blob::Zlib(blob) = BlobReader::<File>::decode_blob(&[0x1a, 1, 77])? {
            assert_eq!(blob, &[77]);
        } else {
            panic!("unexpected blob type");
        }
        assert!(BlobReader::<File>::decode_blob(&[0x2a, 1, 77]).is_err());
        Ok(())
    }

    #[test]
    fn test_blob_reader_bad_data() {
        assert!(BlobReader::try_new(&mut Cursor::new(b"")).is_err());
        assert!(BlobReader::try_new(&mut Cursor::new(b"\0\0\0")).is_err());
        assert!(BlobReader::try_new(&mut Cursor::new(b"\0\0\0\0")).is_err());
        assert!(BlobReader::try_new(&mut Cursor::new(b"test file with junk data")).is_err());
    }

    fn test_data_path(filename: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests");
        path.push("test_data");
        path.push(filename);
        path
    }
}
