use anyhow::{Context, Ok, Result, anyhow};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use osm_pbf_iter::{Blob, Primitive, PrimitiveBlock, RelationMemberType};
use protobuf_iter::MessageIter;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread;

use crate::PROGRESS_BAR_STYLE;
use crate::coverage::Coverage;

mod coords;
mod cover;
mod filter;

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

    let relation_parents = build_relation_parents(&mut reader, rel_blobs, progress)?;

    // Find which nodes, ways and relations lie within the coverage.
    let covered_nodes = cover::cover_nodes(&mut reader, node_blobs, &coverage, progress, workdir)?;
    let covered_ways =
        cover::cover_ways(&mut reader, way_blobs, &covered_nodes, progress, workdir)?;
    let covered_relations = cover::cover_relations(
        &mut reader,
        rel_blobs,
        &covered_nodes,
        &covered_ways,
        &relation_parents,
        progress,
        workdir,
    )?;

    let filtered_relations = filter::filter_relations(
        &mut reader,
        rel_blobs,
        &coverage,
        &covered_relations,
        progress,
        workdir,
    )?;

    let filtered_ways = filter::filter_ways(
        &mut reader,
        way_blobs,
        &coverage,
        &covered_ways,
        &filtered_relations,
        progress,
        workdir,
    )?;

    let _filtered_nodes = filter::filter_nodes(
        &mut reader,
        node_blobs,
        &coverage,
        &covered_nodes,
        &filtered_ways,
        &filtered_relations,
        progress,
        workdir,
    )?;

    Ok(())
}

fn make_progress_bar(
    progress: &MultiProgress,
    phase: &str,
    max_value: u64,
    message: &str,
) -> ProgressBar {
    let bar = progress.add(ProgressBar::new(max_value));
    bar.set_prefix(String::from(phase));
    bar.set_message(String::from(message));

    let style = ProgressStyle::with_template(PROGRESS_BAR_STYLE).expect("bad PROGRESS_BAR_STYLE");
    bar.set_style(style);

    bar
}

fn read_blobs<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    progress_bar: &ProgressBar,
    out: SyncSender<Blob>,
) -> Result<()> {
    for i in blobs.0..blobs.1 {
        let blob = reader.read_blob(i)?;
        out.send(blob)?;
        progress_bar.inc(1);
    }

    Ok(())
}

fn build_relation_parents<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    progress: &MultiProgress,
) -> Result<HashMap<u64, u64>> {
    let num_blobs = (blobs.1 - blobs.0) as u64;
    let progress_bar = make_progress_bar(progress, "osm.prt.r", num_blobs, "blobs");
    let mut result = HashMap::<u64, u64>::new();
    thread::scope(|s| {
        let num_workers = usize::from(thread::available_parallelism()?);
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let producer = s.spawn(|| read_blobs(reader, blobs, &progress_bar, blob_tx));
        let consumer = s.spawn(|| {
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
        producer
            .join()
            .expect("panic in producer thread")
            .and(consumer.join().expect("panic in consumer thread"))
    })?;

    progress_bar.finish_with_message(format!("blobs → {} relation parents", result.len()));
    Ok(result)
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

struct ParentChainIter<'a> {
    parents: &'a HashMap<u64, u64>,
    current: Option<u64>,
    visited: HashSet<u64>,
}

impl<'a> ParentChainIter<'a> {
    fn new(parents: &'a HashMap<u64, u64>, start: u64) -> Self {
        ParentChainIter {
            parents,
            current: Some(start),
            visited: HashSet::new(),
        }
    }
}

impl<'a> Iterator for ParentChainIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current?;

        // Check for cycles.
        if !self.visited.insert(current) {
            // Cycle detected; stop iteration.
            self.current = None;
            return None;
        }

        // Look up the parent for next iteration.
        self.current = self.parents.get(&current).copied();

        Some(current)
    }
}

trait ParentChainExt {
    fn parent_chain<'a>(&'a self, start: u64) -> ParentChainIter<'a>;
}

impl ParentChainExt for HashMap<u64, u64> {
    fn parent_chain<'a>(&'a self, start: u64) -> ParentChainIter<'a> {
        ParentChainIter::new(self, start)
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

    #[test]
    fn test_parent_chain() {
        let mut g = HashMap::new();
        g.insert(5, 23);
        g.insert(23, 42);
        g.insert(27, 42);
        g.insert(42, 100);

        let parent_chain = |i| g.parent_chain(i).collect::<Vec<u64>>();
        assert_eq!(parent_chain(5), &[5, 23, 42, 100]);
        assert_eq!(parent_chain(23), &[23, 42, 100]);
        assert_eq!(parent_chain(27), &[27, 42, 100]);
        assert_eq!(parent_chain(42), &[42, 100]);
        assert_eq!(parent_chain(100), &[100]);
        assert_eq!(parent_chain(9999), &[9999]);
    }

    #[test]
    fn test_parent_chain_cycle() {
        let mut g = HashMap::new();
        g.insert(5, 23);
        g.insert(23, 42);
        g.insert(42, 100);
        g.insert(100, 23);

        let parent_chain = |i| g.parent_chain(i).collect::<Vec<u64>>();
        assert_eq!(parent_chain(5), &[5, 23, 42, 100]);
        assert_eq!(parent_chain(23), &[23, 42, 100]);
        assert_eq!(parent_chain(42), &[42, 100, 23]);
        assert_eq!(parent_chain(100), &[100, 23, 42]);
    }
}
