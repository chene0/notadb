use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::error::{NotaDbError, Result};

/// A single record written to the WAL.
///
/// Each write to the memtable is first durably recorded here so it can
/// be replayed to reconstruct the memtable after a crash.
pub(crate) enum WalEntry {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// An append-only log file that records every write before it hits the memtable.
///
/// On startup, if the memtable was not cleanly flushed, the WAL is replayed
/// in order to reconstruct the lost memtable state.
pub struct Wal {
    writer: BufWriter<File>,
}

impl Wal {
    /// Open an existing WAL or create a new one at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Append a single entry to the log.
    ///
    /// Must be called before the corresponding write reaches the memtable.
    pub(crate) fn append(&mut self, entry: &WalEntry) -> Result<()> {
        match entry {
            WalEntry::Set { key, value } => self.append_entry(key, value),
            WalEntry::Delete { key } => self.append_tombstone(key),
        }?;

        Ok(())
    }

    /// Iterate over all entries in the WAL for crash recovery.
    ///
    /// Entries are returned in the order they were written.
    pub(crate) fn iter(path: &Path) -> Result<impl Iterator<Item = Result<WalEntry>>> {
        let mut res_vec = Vec::<Result<WalEntry>>::new();
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        loop {
            let mut tag = [0u8; 1];
            match reader.read_exact(&mut tag) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };

            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let key_len = u32::from_be_bytes(len_buf) as usize;

            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            match tag[0] {
                0x01 => {
                    len_buf = [0u8; 4];
                    reader.read_exact(&mut len_buf)?;
                    let val_len = u32::from_be_bytes(len_buf) as usize;

                    let mut val = vec![0u8; val_len];
                    reader.read_exact(&mut val)?;

                    res_vec.push(Ok(WalEntry::Set { key, value: val }));
                }
                0x02 => {
                    res_vec.push(Ok(WalEntry::Delete { key }));
                }
                _ => return Err(NotaDbError::Corruption(format!("unknown tag: {}", tag[0]))),
            }
        }

        Ok(res_vec.into_iter())
    }

    // [ 0x01 ][ key_len: 4 bytes ][ key: N bytes ][ value_len: 4 bytes ][ value: N bytes ]
    fn append_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.writer.write_all(&[0x01])?;
        self.writer.write_all(&(key.len() as u32).to_be_bytes())?;
        self.writer.write_all(key)?;
        self.writer.write_all(&(value.len() as u32).to_be_bytes())?;
        self.writer.write_all(value)?;

        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        Ok(())
    }

    // [ 0x02 ][ key_len: 4 bytes ][ key: N bytes ]
    fn append_tombstone(&mut self, key: &[u8]) -> Result<()> {
        self.writer.write_all(&[0x02])?;
        self.writer.write_all(&(key.len() as u32).to_be_bytes())?;
        self.writer.write_all(key)?;

        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn temp_wal() -> (NamedTempFile, Wal) {
        let file = NamedTempFile::new().unwrap();
        let wal = Wal::open(file.path()).unwrap();
        (file, wal)
    }

    #[test]
    fn append_and_replay_set() {
        let (file, mut wal) = temp_wal();
        wal.append(&WalEntry::Set {
            key: b"foo".to_vec(),
            value: b"bar".to_vec(),
        })
        .unwrap();

        let entries: Vec<_> = Wal::iter(file.path()).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let entry = entries.into_iter().next().unwrap().unwrap();
        match entry {
            WalEntry::Set { key, value } => {
                assert_eq!(key, b"foo");
                assert_eq!(value, b"bar");
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn append_and_replay_delete() {
        let (file, mut wal) = temp_wal();
        wal.append(&WalEntry::Delete {
            key: b"foo".to_vec(),
        })
        .unwrap();

        let entries: Vec<_> = Wal::iter(file.path()).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let entry = entries.into_iter().next().unwrap().unwrap();
        match entry {
            WalEntry::Delete { key } => assert_eq!(key, b"foo"),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn replay_preserves_order() {
        let (file, mut wal) = temp_wal();
        wal.append(&WalEntry::Set {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        })
        .unwrap();
        wal.append(&WalEntry::Set {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
        })
        .unwrap();
        wal.append(&WalEntry::Delete { key: b"a".to_vec() })
            .unwrap();

        let entries: Vec<_> = Wal::iter(file.path()).unwrap().collect();
        assert_eq!(entries.len(), 3);

        let keys: Vec<Vec<u8>> = entries
            .into_iter()
            .map(|e| match e.unwrap() {
                WalEntry::Set { key, .. } => key,
                WalEntry::Delete { key } => key,
            })
            .collect();

        assert_eq!(keys[0], b"a");
        assert_eq!(keys[1], b"b");
        assert_eq!(keys[2], b"a");
    }

    #[test]
    fn empty_wal_iter_returns_nothing() {
        let (file, _wal) = temp_wal();
        let entries: Vec<_> = Wal::iter(file.path()).unwrap().collect();
        assert_eq!(entries.len(), 0);
    }
}
