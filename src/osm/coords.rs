use anyhow::{Ok, Result};
use ext_sort::{ExternalSorter, ExternalSorterBuilder, buffer::LimitedBufferBuilder};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::mpsc::Receiver;

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Coords {
    pub key: u64,
    pub lon_e7: i32,
    pub lat_e7: i32,
}

pub fn build_tables(
    stream: Receiver<Coords>,
    workdir: &Path,
    keys_path: &Path,
    data_path: &Path,
) -> Result<u64> {
    let sorter: ExternalSorter<Coords, std::io::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(workdir)
            .with_buffer(LimitedBufferBuilder::new(
                /* buffer_size */ 2_000_000, /* preallocate */ true,
            ))
            .build()?;
    let mut keys_writer = BufWriter::with_capacity(32768, File::create(keys_path)?);
    let mut data_writer = BufWriter::with_capacity(32768, File::create(data_path)?);

    let sorted = sorter.sort(stream.into_iter().map(std::io::Result::Ok))?;
    let mut num_coords = 0_u64;
    for c in sorted {
        let c = c?;
        keys_writer.write_all(&c.key.to_le_bytes())?;
        data_writer.write_all(&c.lon_e7.to_le_bytes())?;
        data_writer.write_all(&c.lat_e7.to_le_bytes())?;
        num_coords += 1;
    }

    Ok(num_coords)
}

#[cfg(test)]
mod tests {
    use super::{Coords, build_tables};
    use anyhow::{Ok, Result};
    use std::fs::File;
    use std::io::Read;
    use std::sync::mpsc::sync_channel;
    use tempfile::{NamedTempFile, TempDir};

    #[test]
    fn test_build_tables() -> Result<()> {
        let (tx, rx) = sync_channel::<Coords>(5);
        tx.send(Coords {
            key: 23,
            lon_e7: 33,
            lat_e7: 44,
        })?;
        tx.send(Coords {
            key: 7,
            lon_e7: 11,
            lat_e7: 22,
        })?;
        drop(tx);

        let workdir = TempDir::new()?;
        let keys_file = NamedTempFile::new()?;
        let data_file = NamedTempFile::new()?;
        build_tables(rx, &workdir.path(), &keys_file.path(), &data_file.path())?;

        let keys: Vec<u64> = {
            let mut buf = Vec::new();
            File::open(keys_file)?.read_to_end(&mut buf)?;
            assert!(buf.len().is_multiple_of(8));
            buf.chunks_exact(8)
                .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
                .collect()
        };
        assert_eq!(keys, [7, 23]);

        let data: Vec<(i32, i32)> = {
            let mut buf = Vec::new();
            File::open(data_file)?.read_to_end(&mut buf)?;
            assert!(buf.len().is_multiple_of(8));
            buf.chunks_exact(8)
                .map(|chunk| {
                    let lon = i32::from_le_bytes(chunk[0..4].try_into().unwrap());
                    let lat = i32::from_le_bytes(chunk[4..8].try_into().unwrap());
                    (lon, lat)
                })
                .collect()
        };
        assert_eq!(data, [(11, 22), (33, 44)]);

        Ok(())
    }
}
