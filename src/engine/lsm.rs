use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::storage::memtable::MemTable;
use crate::storage::sstable::{SSTable, SSTableWriter};
use crate::storage::wal::{Wal, WalEntry};

/// An LSM-tree storage engine.
///
/// Writes land in the WAL and memtable. Once the memtable exceeds the size
/// threshold it is flushed to an immutable SSTable on disk. Reads check the
/// memtable first, then SSTables from newest to oldest.
pub struct LsmEngine {
    dir: PathBuf,
    memtable: MemTable,
    wal: Wal,
    /// SSTables ordered oldest → newest; reads scan in reverse.
    sstables: Vec<(PathBuf, SSTable)>,
    next_sstable_id: u64,
}

impl LsmEngine {
    const MEMTABLE_SIZE_THRESHOLD: usize = 4 * 1024 * 1024; // 4 MiB
    const WAL_FILENAME: &'static str = "wal.log";

    /// Open (or create) an LSM engine rooted at `dir`.
    ///
    /// On open: discovers existing SSTables, then replays the WAL into the memtable.
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut sstable_paths: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                let name = path.file_name()?.to_str()?;
                if name.starts_with("sstable_") && name.ends_with(".sst") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        sstable_paths.sort();

        let next_sstable_id = sstable_paths.len() as u64;

        let mut sstables = Vec::<(PathBuf, SSTable)>::new();
        for sstable_path in sstable_paths {
            let sstable = SSTable::open(&sstable_path)?;
            sstables.push((sstable_path, sstable));
        }

        let wal_path = Self::wal_path(dir);
        let wal = Wal::open(&wal_path)?;
        let wal_iter = Wal::iter(&wal_path)?;

        let mut memtable = MemTable::new();
        for wal_entry in wal_iter {
            match wal_entry? {
                WalEntry::Set { key, value } => memtable.set(&key, &value)?,
                WalEntry::Delete { key } => memtable.delete(&key)?,
            };
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            memtable,
            wal,
            sstables,
            next_sstable_id,
        })
    }

    /// Insert or update a key.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wal.append(&WalEntry::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        })?;
        self.memtable.set(key, value)?;

        if self.memtable.size_bytes() > Self::MEMTABLE_SIZE_THRESHOLD {
            self.flush()?;
        }

        Ok(())
    }

    /// Delete a key by writing a tombstone.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.wal.append(&WalEntry::Delete { key: key.to_vec() })?;
        self.memtable.delete(key)?;

        if self.memtable.size_bytes() > Self::MEMTABLE_SIZE_THRESHOLD {
            self.flush()?;
        }

        Ok(())
    }

    /// Look up a key.
    ///
    /// Returns `None` if the key does not exist or was deleted.
    /// Returns `Some(bytes)` for a live value.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get(key) {
            Some(Some(bytes)) => return Ok(Some(bytes.to_vec())),
            Some(None) => return Ok(None),
            None => {}
        };

        let sstables_inc = self.sstables.iter_mut().rev();

        for (_, sstable) in sstables_inc {
            match sstable.get(key)? {
                Some(Some(bytes)) => return Ok(Some(bytes.to_vec())),
                Some(None) => return Ok(None),
                None => {}
            }
        }

        Ok(None)
    }

    /// Flush the memtable to a new SSTable and rotate the WAL.
    ///
    /// Called automatically when the memtable exceeds the size threshold,
    /// or manually to force a flush.
    pub fn flush(&mut self) -> Result<()> {
        if self.memtable.size_bytes() == 0 {
            return Ok(());
        }

        let sstable_path = Self::sstable_path(&self.dir, self.next_sstable_id);
        let mut sstable_writer = SSTableWriter::new(&sstable_path)?;

        let memtable_iter = self.memtable.iter();

        for (key, value_option) in memtable_iter {
            match value_option {
                Some(value) => sstable_writer.write_entry(key, value)?,
                None => sstable_writer.write_tombstone(key)?,
            };
        }

        sstable_writer.finish()?;

        let wal_path = Self::wal_path(&self.dir);
        std::fs::remove_file(&wal_path)?;
        self.wal = Wal::open(&wal_path)?;

        let sstable = SSTable::open(&sstable_path)?;
        self.sstables.push((sstable_path, sstable));
        self.memtable = MemTable::new();
        self.next_sstable_id += 1;

        Ok(())
    }

    fn wal_path(dir: &Path) -> PathBuf {
        dir.join(Self::WAL_FILENAME)
    }

    fn sstable_path(dir: &Path, id: u64) -> PathBuf {
        dir.join(format!("sstable_{:08}.sst", id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn tmp_dir() -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("notadb_lsm_test_{}", id))
    }

    #[test]
    fn test_put_and_get() {
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        engine.put(b"hello", b"world").unwrap();
        assert_eq!(engine.get(b"hello").unwrap(), Some(b"world".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_get_missing_returns_none() {
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        assert_eq!(engine.get(b"missing").unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_overwrite_returns_latest() {
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        engine.put(b"key", b"first").unwrap();
        engine.put(b"key", b"second").unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"second".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_delete_returns_none() {
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine.delete(b"key").unwrap();
        assert_eq!(engine.get(b"key").unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_flush_and_get_from_sstable() {
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"value".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_replay_on_reopen() {
        let dir = tmp_dir();
        {
            let mut engine = LsmEngine::open(&dir).unwrap();
            engine.put(b"key", b"value").unwrap();
        }
        let mut engine = LsmEngine::open(&dir).unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"value".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_sstable_survives_reopen() {
        let dir = tmp_dir();
        {
            let mut engine = LsmEngine::open(&dir).unwrap();
            engine.put(b"key", b"value").unwrap();
            engine.flush().unwrap();
        }
        let mut engine = LsmEngine::open(&dir).unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"value".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_memtable_tombstone_shadows_sstable() {
        // put + flush → value in SSTable, then delete in memtable → should return None
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine.flush().unwrap();
        engine.delete(b"key").unwrap();
        assert_eq!(engine.get(b"key").unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_tombstone_in_sstable_shadows_older_sstable() {
        // value in SSTable 0, tombstone flushed to SSTable 1 → should return None
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine.flush().unwrap();
        engine.delete(b"key").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.get(b"key").unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_newest_sstable_wins() {
        // same key flushed twice — second value should win
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        engine.put(b"key", b"old").unwrap();
        engine.flush().unwrap();
        engine.put(b"key", b"new").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.get(b"key").unwrap(), Some(b"new".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_flush_empty_memtable_is_noop() {
        let dir = tmp_dir();
        let mut engine = LsmEngine::open(&dir).unwrap();
        engine.flush().unwrap();
        // no SSTable should have been created
        let count = std::fs::read_dir(&dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .ends_with(".sst")
            })
            .count();
        assert_eq!(count, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
