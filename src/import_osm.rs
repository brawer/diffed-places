use anyhow::{Context, Ok, Result, anyhow};
use ext_sort::{ExternalSorter, ExternalSorterBuilder, buffer::LimitedBufferBuilder};
use osm_pbf_iter::{Blob, Primitive, PrimitiveBlock};
use protobuf_iter::MessageIter;
use rayon::prelude::*;
use std::fs::{File, rename};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread;

use crate::coverage::Coverage;

pub fn import_osm(pbf: &Path, coverage: &Path, workdir: &Path) -> Result<()> {
    assert!(workdir.exists());

    let pbf_error = || format!("could not open file `{:?}`", pbf);
    let mut file = File::open(pbf).with_context(pbf_error)?;
    let mut reader = BlobReader::try_new(&mut file).with_context(pbf_error)?;

    // Partition the PBF file into blobs with nodes, ways and relations.
    // Ranges may overlap by one blob, see BlobReader::partition().
    let (nodes_end, ways_end) = reader.partition()?;
    let node_blobs = (0, nodes_end);
    let _ways_start = nodes_end.saturating_sub(1);
    let _rels_start = ways_end.saturating_sub(1);
    let _rels_end = reader.num_blobs();

    let coverage = Coverage::load(coverage)
        .with_context(|| format!("could not open coverage file `{:?}`", coverage))?;

    // Find which nodes lie within our coverage area of interest.
    let _covered_nodes = build_covered_nodes(&mut reader, node_blobs, &coverage, workdir)?;

    // TODO: Find which ways lie within the coverage area, passing covered_nodes.
    // TODO: Find which relations lie within the coverage area, passing covered_nodes and covered_ways.

    Ok(())
}

fn write_sorted(stream: Receiver<u64>, workdir: &Path, out: &Path) -> Result<()> {
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
    for value in sorted {
        writer.write_all(&value?.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

fn build_covered_nodes<R: Read + Seek + Send>(
    reader: &mut BlobReader<R>,
    blobs: (usize, usize),
    coverage: &Coverage,
    workdir: &Path,
) -> Result<PathBuf> {
    let out = workdir.join("covered-nodes");
    if !out.exists() {
        log::info!("import_osm::build_covered_nodes is starting");
    } else {
        log::info!(
            "skipping import_osm::build_covered_nodes, already found {:?}",
            out
        );
        return Ok(out);
    }

    let mut tmp = out.clone();
    tmp.add_extension("tmp");

    let num_workers = usize::from(thread::available_parallelism()?);
    thread::scope(|s| {
        let (blob_tx, blob_rx) = sync_channel::<Blob>(num_workers);
        let (node_tx, node_rx) = sync_channel::<u64>(num_workers * 1024);

        // Blob producer thread, I/O-bound.
        s.spawn(move || {
            for i in blobs.0..blobs.1 {
                let blob = reader.read_blob(i)?;
                blob_tx.send(blob)?;
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
            })
        });

        // Node consumer thread. Sorts a stream of node ids, which it
        // receives from the blob consumers over the channel `node_rx`,
        // into a ´temporary file located at `tmp`.
        write_sorted(node_rx, workdir, &tmp)
    })?;

    rename(&tmp, &out)?;
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
