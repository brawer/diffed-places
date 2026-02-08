use anyhow::{Context, Ok, Result, anyhow};
use osm_pbf_iter::{Blob, Primitive, PrimitiveBlock};
use protobuf_iter::MessageIter;
use rayon::prelude::*;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::mpsc::sync_channel;
use std::thread;

use crate::coverage::Coverage;

pub fn import_osm(pbf: &Path, coverage: &Path, _output: &Path) -> Result<()> {
    let pbf_error = || format!("could not open file `{:?}`", pbf);
    let mut file = File::open(pbf).with_context(pbf_error)?;
    let mut reader = BlobReader::try_new(&mut file).with_context(pbf_error)?;

    // Partition the PBF file into blobs with nodes, ways and relations.
    // Ranges may overlap by one blob, see BlobReader::partition().
    let (nodes_end, ways_end) = reader.partition()?;
    let _ways_start = nodes_end.saturating_sub(1);
    let _rels_start = ways_end.saturating_sub(1);
    let _rels_end = reader.num_blobs();

    let _coverage = Coverage::load(coverage)
        .with_context(|| format!("could not open coverage file `{:?}`", coverage))?;

    // Pass 1: Determine which nodes are located in our coverage area.
    let num_workers = usize::from(thread::available_parallelism()?);
    thread::scope(|s| {
        let (tx, rx) = sync_channel::<Blob>(num_workers);
        let producer = s.spawn(move || {
            for i in 0..nodes_end {
                let blob = reader.read_blob(i)?;
                tx.send(blob)?;
            }
            Ok(())
        });
        rx.into_iter().par_bridge().try_for_each(|blob| {
            let data = blob.into_data(); // decompress
            let _block = PrimitiveBlock::parse(&data);
            Ok(())
        })?;
        producer.join().expect("panic in producer thread")
    })?;

    Ok(())
}

/// Reads data blobs from OpenStreetMap PBF files.
struct BlobReader<'a, R: Read + Seek> {
    reader: &'a mut R,

    /// Offset and size of each data blob.
    blobs: Vec<(u64, usize)>,
}

impl<'a, R: Read + Seek> BlobReader<'a, R> {
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
