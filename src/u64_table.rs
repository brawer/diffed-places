use anyhow::{Ok, Result, anyhow};
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;

/// A memory-mapped file with sorted 64-bit integers in little-endian encoding.
///
/// Used in the pipeline to represent large sets of identifiers that may not
/// entirely fit into the available memory. For examle, the set of all OpenStreetMap
/// nodes that are geographically near an AllThePlaces feature.
///
/// Containment test is currently implemented as a regular binary search.
/// If performance ever becomes an issue, consider Cache-Sensitive Skip Lines,
/// but this would make the file format slightly more complicated.
pub struct U64Table {
    _file: File, // The file that backs mmap.
    mmap: Mmap,
}

impl U64Table {
    pub fn open(path: &Path) -> Result<U64Table> {
        let file_size: u64 = std::fs::metadata(path)?.len();
        if !file_size.is_multiple_of(8) {
            return Err(anyhow!("file size must be multiple of 8"));
        }

        let file = File::open(path)?;

        // SAFETY: We don’t truncate the file while it is mapped into memory.
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(U64Table { _file: file, mmap })
    }

    pub fn contains(&self, n: u64) -> bool {
        // SAFETY: We check in `open()` that the file size is a multiple of eight.
        // Alignment to page size, which is typically 4K or larger and
        // always than eight bytes, is guaranteed by the mmap system
        // call.
        let slice = unsafe {
            let ptr = self.mmap.as_ptr() as *const u64;
            std::slice::from_raw_parts(ptr, self.mmap.len() / 8)
        };

        if cfg!(target_endian = "little") {
            slice.binary_search(&n).is_ok()
        } else {
            slice.binary_search_by(|x| x.swap_bytes().cmp(&n)).is_ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_open() -> Result<()> {
        let mut file = NamedTempFile::new()?;

        // `open()` should accept an empty file.
        assert!(U64Table::open(file.path()).is_ok());

        // `open()` should accept a file with 16 bytes.
        file.write_all(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])?;
        assert!(U64Table::open(file.path()).is_ok());

        // `open()` should not accept a file with 17 bytes.
        file.write_all(&[0])?;
        assert!(U64Table::open(file.path()).is_err());

        Ok(())
    }

    #[test]
    fn test_open_file_does_not_exist() {
        let path = Path::new("file/does/not/exist");
        assert!(U64Table::open(&path).is_err());
    }

    #[test]
    fn test_contains() -> Result<()> {
        let mut file = NamedTempFile::new()?;
        for i in [7_u64, 23_u64, 42_u64] {
            file.write_all(&i.to_le_bytes())?;
        }

        let table = U64Table::open(file.path())?;
        assert_eq!(table.contains(7), true);
        assert_eq!(table.contains(23), true);
        assert_eq!(table.contains(42), true);

        assert_eq!(table.contains(u64::MIN), false);
        assert_eq!(table.contains(u64::MAX), false);
        assert_eq!(table.contains(19), false);

        Ok(())
    }
}
