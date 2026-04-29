use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{NotaDbError, Result};

/// Writes a single SSTable to disk from a sorted iterator of entries.
///
/// Call `write_entry` for each key in sorted order, then `finish` to
/// flush the index and footer. Do not reuse after `finish`.
///
/// On-disk layout:
///   [ data section  ] — sequential key/value/tombstone entries
///   [ index section ] — sparse index: sampled keys → byte offsets into data
///   [ footer        ] — 8-byte offset pointing to start of index section
pub struct SSTableWriter {
    writer: BufWriter<File>,
    // tracks current byte offset during write
    byte_offset: u64,
    // tracks index section during write
    indices: Vec<(Vec<u8>, u64)>,
}

impl SSTableWriter {
    const INDEX_BLOCK_SIZE: u64 = 4096;

    /// Create a new SSTable file at the given path.
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            byte_offset: 0,
            indices: Vec::new(),
        })
    }

    /// Write a live key-value pair.
    ///
    /// Keys must be provided in sorted order — the writer does not enforce this.
    pub fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry_start = self.byte_offset;

        self.writer.write_all(&[0x01])?;
        self.writer.write_all(&(key.len() as u32).to_be_bytes())?;
        self.writer.write_all(key)?;
        self.writer.write_all(&(value.len() as u32).to_be_bytes())?;
        self.writer.write_all(value)?;

        self.byte_offset += (1 + 4 + key.len() + 4 + value.len()) as u64;

        self.write_index(key, entry_start)?;

        Ok(())
    }

    /// Write a tombstone for a deleted key.
    pub fn write_tombstone(&mut self, key: &[u8]) -> Result<()> {
        let entry_start = self.byte_offset;

        self.writer.write_all(&[0x02])?;
        self.writer.write_all(&(key.len() as u32).to_be_bytes())?;
        self.writer.write_all(key)?;

        self.byte_offset += (1 + 4 + key.len()) as u64;

        self.write_index(key, entry_start)?;

        Ok(())
    }

    /// Finalize the file: flush the sparse index and write the footer.
    ///
    /// Must be called once after all entries have been written.
    pub fn finish(mut self) -> Result<()> {
        let index_start = self.byte_offset;

        for (key, offset) in &self.indices {
            self.writer.write_all(&(key.len() as u32).to_be_bytes())?;
            self.writer.write_all(key)?;
            self.writer.write_all(&offset.to_be_bytes())?;
        }

        self.writer.write_all(&index_start.to_be_bytes())?;

        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        Ok(())
    }

    fn write_index(&mut self, key: &[u8], entry_start: u64) -> Result<()> {
        let last_index_byte_offset = self.indices.last().map_or(0, |(_, offset)| *offset);
        let bytes_since_last_index = entry_start - last_index_byte_offset;

        if self.indices.is_empty() || bytes_since_last_index >= Self::INDEX_BLOCK_SIZE {
            self.indices.push((key.to_vec(), entry_start));
        }

        Ok(())
    }
}

/// A read-only view of an SSTable on disk.
///
/// Supports point lookups via a sparse index and full iteration.
pub struct SSTable {
    reader: BufReader<File>,
    indices: Vec<(Vec<u8>, u64)>,
    index_offset: u64,
}

impl SSTable {
    /// Open an existing SSTable file for reading.
    ///
    /// Reads the footer to locate the index, then loads the index into memory.
    /// index format: [key_len] [key] [offset]
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;

        file.seek(SeekFrom::End(-8))?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let index_offset = u64::from_be_bytes(buf);

        let file_size = file.metadata()?.len();

        if file_size < 8 || index_offset > file_size - 8 {
            // corrupt file
            return Err(NotaDbError::Corruption(String::from(
                "index offset exceeds its allocated size",
            )));
        }
        let index_section_size = file_size - 8 - index_offset;

        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_reader = BufReader::new(&file);

        let mut bytes_read: u64 = 0;
        let mut indices: Vec<(Vec<u8>, u64)> = Vec::new();
        while bytes_read < index_section_size {
            let mut key_len_buf = [0u8; 4];
            index_reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_be_bytes(key_len_buf) as usize;

            let mut key_buf = vec![0u8; key_len];
            index_reader.read_exact(&mut key_buf)?;

            let mut data_offset_buf = [0u8; 8];
            index_reader.read_exact(&mut data_offset_buf)?;
            let data_offset = u64::from_be_bytes(data_offset_buf);

            indices.push((key_buf, data_offset));

            let index_size = (4 + key_len + 8) as u64;
            bytes_read += index_size;
        }

        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(file);

        Ok(Self {
            reader,
            indices,
            index_offset,
        })
    }

    /// Look up a key in the SSTable.
    ///
    /// Returns `None` if the key is not present.
    /// Returns `Some(None)` if the key is present but is a tombstone.
    /// Returns `Some(Some(bytes))` for a live value.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        let idx = match self
            .indices
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(0) => return Ok(None), // key is before all indexed keys
            Err(i) => i - 1,           // key >= largest indexed key
        };
        let (_, data_offset) = self.indices[idx];

        self.reader.seek(SeekFrom::Start(data_offset))?;

        let mut bytes_read: u64 = data_offset;
        loop {
            if bytes_read >= self.index_offset {
                // reached end of data section
                break;
            }

            let mut tag_buf = [0u8; 1];
            match self.reader.read_exact(&mut tag_buf) {
                Ok(_) => {}
                // eof => key not found
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };

            let mut key_len_buf = [0u8; 4];
            self.reader.read_exact(&mut key_len_buf)?;
            let key_len = (u32::from_be_bytes(key_len_buf)) as usize;

            let mut key_buf = vec![0u8; key_len];
            self.reader.read_exact(&mut key_buf)?;

            bytes_read += (1 + 4 + key_len) as u64;

            if key_buf.as_slice() > key {
                // overshot target key, target key must not exist
                break;
            }

            let value: Option<Vec<u8>> = match tag_buf[0] {
                0x01 => {
                    // value
                    let mut value_len_buf = [0u8; 4];
                    self.reader.read_exact(&mut value_len_buf)?;
                    let value_len = (u32::from_be_bytes(value_len_buf)) as usize;

                    let mut value_buf = vec![0u8; value_len];
                    self.reader.read_exact(&mut value_buf)?;

                    bytes_read += (4 + value_len) as u64;

                    Some(value_buf)
                }
                0x02 => None,
                _ => {
                    return Err(NotaDbError::Corruption(format!(
                        "unknown tag: {}",
                        tag_buf[0]
                    )));
                }
            };

            if key_buf.as_slice() == key {
                // key hit
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Iterate over all entries in the SSTable in sorted key order.
    ///
    /// Yields both live values and tombstones — callers decide what to do with each.
    pub fn iter(&mut self) -> Result<impl Iterator<Item = Result<SSTableEntry>>> {
        let mut res_vec = Vec::<Result<SSTableEntry>>::new();

        self.reader.seek(SeekFrom::Start(0))?;

        let mut bytes_read: u64 = 0;

        while bytes_read < self.index_offset {
            let mut tag_buf = [0u8; 1];
            match self.reader.read_exact(&mut tag_buf) {
                Ok(_) => {}
                Err(e) => return Err(e.into()),
            };

            let mut key_len_buf = [0u8; 4];
            self.reader.read_exact(&mut key_len_buf)?;
            let key_len = (u32::from_be_bytes(key_len_buf)) as usize;

            let mut key_buf = vec![0u8; key_len];
            self.reader.read_exact(&mut key_buf)?;

            let mut data_size = (1 + 4 + key_len) as u64;

            match tag_buf[0] {
                0x01 => {
                    // value
                    let mut value_len_buf = [0u8; 4];
                    self.reader.read_exact(&mut value_len_buf)?;
                    let value_len = (u32::from_be_bytes(value_len_buf)) as usize;

                    let mut value_buf = vec![0u8; value_len];
                    self.reader.read_exact(&mut value_buf)?;

                    res_vec.push(Ok(SSTableEntry::Value {
                        key: key_buf,
                        value: value_buf,
                    }));
                    data_size += (4 + value_len) as u64;
                }
                0x02 => {
                    // tombstone
                    res_vec.push(Ok(SSTableEntry::Tombstone { key: key_buf }));
                }
                _ => {
                    return Err(NotaDbError::Corruption(format!(
                        "unknown tag: {}",
                        tag_buf[0]
                    )));
                }
            }

            bytes_read += data_size;
        }

        Ok(res_vec.into_iter())
    }
}

/// A single entry yielded by SSTable iteration.
pub enum SSTableEntry {
    Value { key: Vec<u8>, value: Vec<u8> },
    Tombstone { key: Vec<u8> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn tmp_path() -> std::path::PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("notadb_sstable_{}.sst", id))
    }

    #[test]
    fn test_single_entry_roundtrip() {
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_entry(b"hello", b"world").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        assert_eq!(table.get(b"hello").unwrap(), Some(Some(b"world".to_vec())));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_tombstone_roundtrip() {
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_tombstone(b"gone").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        assert_eq!(table.get(b"gone").unwrap(), Some(None));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_key_before_all_entries_returns_none() {
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_entry(b"mango", b"fruit").unwrap();
        writer.write_entry(b"orange", b"fruit").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        assert_eq!(table.get(b"apple").unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_key_not_found_within_range() {
        // "banana" is between "apple" and "cherry" — exercises the overshoot break path
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_entry(b"apple", b"1").unwrap();
        writer.write_entry(b"cherry", b"2").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        assert_eq!(table.get(b"banana").unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_multiple_entries_get_each() {
        let path = tmp_path();
        let entries: &[(&[u8], &[u8])] = &[
            (b"apple", b"fruit"),
            (b"banana", b"yellow"),
            (b"cherry", b"red"),
            (b"date", b"sweet"),
        ];

        let mut writer = SSTableWriter::new(&path).unwrap();
        for (k, v) in entries {
            writer.write_entry(k, v).unwrap();
        }
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        for (k, v) in entries {
            assert_eq!(table.get(k).unwrap(), Some(Some(v.to_vec())));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_mixed_values_and_tombstones_get() {
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_entry(b"alive", b"yes").unwrap();
        writer.write_tombstone(b"dead").unwrap();
        writer.write_entry(b"living", b"also yes").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        assert_eq!(table.get(b"alive").unwrap(), Some(Some(b"yes".to_vec())));
        assert_eq!(table.get(b"dead").unwrap(), Some(None));
        assert_eq!(
            table.get(b"living").unwrap(),
            Some(Some(b"also yes".to_vec()))
        );
        assert_eq!(table.get(b"between").unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_empty_value() {
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_entry(b"key", b"").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        assert_eq!(table.get(b"key").unwrap(), Some(Some(vec![])));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_iter_all_entries_in_order() {
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_entry(b"a", b"1").unwrap();
        writer.write_entry(b"b", b"2").unwrap();
        writer.write_tombstone(b"c").unwrap();
        writer.write_entry(b"d", b"4").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        let entries: Vec<_> = table.iter().unwrap().map(|e| e.unwrap()).collect();

        assert_eq!(entries.len(), 4);
        assert!(
            matches!(&entries[0], SSTableEntry::Value { key, value } if key == b"a" && value == b"1")
        );
        assert!(
            matches!(&entries[1], SSTableEntry::Value { key, value } if key == b"b" && value == b"2")
        );
        assert!(matches!(&entries[2], SSTableEntry::Tombstone { key } if key == b"c"));
        assert!(
            matches!(&entries[3], SSTableEntry::Value { key, value } if key == b"d" && value == b"4")
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_multiple_index_blocks() {
        // Each entry is ~1035 bytes; after ~4 entries the writer creates a new index block.
        // 20 entries spans ~5 index blocks, exercising the binary search across multiple blocks.
        let path = tmp_path();
        let value = vec![0u8; 1024];
        let keys: Vec<Vec<u8>> = (0..20u32)
            .map(|i| format!("key{:04}", i).into_bytes())
            .collect();

        let mut writer = SSTableWriter::new(&path).unwrap();
        for key in &keys {
            writer.write_entry(key, &value).unwrap();
        }
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        for key in &keys {
            assert_eq!(table.get(key).unwrap(), Some(Some(value.clone())));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_key_after_all_entries_returns_none() {
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_entry(b"apple", b"1").unwrap();
        writer.write_entry(b"banana", b"2").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        assert_eq!(table.get(b"zzz").unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_tombstone_as_last_entry() {
        // tombstone is the last entry — the boundary check must not fire before the equality check
        let path = tmp_path();
        let mut writer = SSTableWriter::new(&path).unwrap();
        writer.write_entry(b"apple", b"1").unwrap();
        writer.write_tombstone(b"banana").unwrap();
        writer.finish().unwrap();

        let mut table = SSTable::open(&path).unwrap();
        assert_eq!(table.get(b"banana").unwrap(), Some(None));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_corrupt_file_too_small() {
        let path = tmp_path();
        std::fs::write(&path, b"tiny").unwrap();
        assert!(SSTable::open(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_corrupt_file_bad_index_offset() {
        let path = tmp_path();
        // Write a valid-looking footer with an index_offset pointing past the file
        let mut data = vec![0u8; 16];
        let bad_offset: u64 = 9999;
        data[8..16].copy_from_slice(&bad_offset.to_be_bytes());
        std::fs::write(&path, &data).unwrap();
        assert!(SSTable::open(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
