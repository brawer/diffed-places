use anyhow::{Context, Ok, Result, anyhow};
use protobuf_iter::MessageIter;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::coverage::Coverage;

pub fn import_osm(pbf: &Path, coverage: &Path, _output: &Path) -> Result<()> {
    let pbf_error = || format!("could not open file `{:?}`", pbf);
    let mut pbf_file = File::open(pbf).with_context(pbf_error)?;
    let _index = BlobIndex::try_new(&mut pbf_file).with_context(pbf_error)?;
    let _coverage = Coverage::load(coverage)
        .with_context(|| format!("could not open coverage file `{:?}`", coverage))?;
    Ok(())
}

/// Keeps an index of blobs within an OpenStreetMap PBF file.
struct BlobIndex {
    /// Offset and size of each data blob.
    _blobs: Vec<(u64, usize)>,
}

impl BlobIndex {
    pub fn try_new<R>(reader: &mut R) -> Result<BlobIndex>
    where
        R: Read + Seek,
    {
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
        Ok(BlobIndex { _blobs: blobs })
    }

    fn read_blob_header<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
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
    fn test_blob_index() -> Result<()> {
        let mut file = File::open(test_data_path("zugerland.osm.pbf"))?;
        let index = BlobIndex::try_new(&mut file)?;
        assert_eq!(index._blobs, &[(119, 16681), (16816, 15278), (32110, 8616)]);
        Ok(())
    }

    #[test]
    fn test_blob_index_bad_data() {
        assert!(BlobIndex::try_new(&mut Cursor::new(b"")).is_err());
        assert!(BlobIndex::try_new(&mut Cursor::new(b"\0\0\0")).is_err());
        assert!(BlobIndex::try_new(&mut Cursor::new(b"\0\0\0\0")).is_err());
        assert!(BlobIndex::try_new(&mut Cursor::new(b"test file with junk data")).is_err());
    }

    fn test_data_path(filename: &str) -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests");
        path.push("test_data");
        path.push(filename);
        path
    }
}
