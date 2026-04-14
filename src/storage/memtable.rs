use std::collections::BTreeMap;

use crate::error::Result;

pub(crate) enum ValueEntry {
    Value(Vec<u8>),
    Tombstone,
}

/// An in-memory write buffer that maintains keys in sorted order.
///
/// In an LSM-tree, all writes land here first. Once the memtable
/// exceeds a size threshold, it is flushed to an immutable SSTable on disk.
pub struct MemTable {
    dict: BTreeMap<Vec<u8>, ValueEntry>,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            dict: BTreeMap::new(),
        }
    }

    /// Insert or update a key-value pair.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_vec = key.to_vec();
        let new_value = ValueEntry::Value(value.to_vec());

        self.dict.insert(key_vec, new_value);

        Ok(())
    }

    /// Retrieve the value for a key, if it exists.
    ///
    /// Returns `None` if the key is not present (including if it was deleted).
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.dict.get(key) {
            Some(ValueEntry::Value(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// Mark a key as deleted via a tombstone.
    ///
    /// Require tombstones to ensure that old sstable records
    /// of the same key are noops
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let key_vec = key.to_vec();

        self.dict.insert(key_vec, ValueEntry::Tombstone);

        Ok(())
    }

    /// Returns an iterator over key-value pairs in sorted key order.
    ///
    /// This is used when flushing the memtable to an SSTable.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.dict.iter().filter_map(|(key, val)| match val {
            ValueEntry::Value(bytes) => Some((key.as_slice(), bytes.as_slice())),
            ValueEntry::Tombstone => None,
        })
    }

    /// Approximate size of the memtable in bytes.
    pub fn size_bytes(&self) -> usize {
        self.dict.iter().fold(0, |acc, (key, val)| {
            let key_size = key.len();
            let value_size = match val {
                ValueEntry::Value(bytes) => bytes.len(),
                ValueEntry::Tombstone => 0,
            };

            acc + key_size + value_size
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let mut mt = MemTable::new();
        mt.set(b"foo", b"bar").unwrap();
        assert_eq!(mt.get(b"foo"), Some(b"bar".as_slice()));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let mt = MemTable::new();
        assert_eq!(mt.get(b"missing"), None);
    }

    #[test]
    fn overwrite_returns_latest_value() {
        let mut mt = MemTable::new();
        mt.set(b"foo", b"first").unwrap();
        mt.set(b"foo", b"second").unwrap();
        assert_eq!(mt.get(b"foo"), Some(b"second".as_slice()));
    }

    #[test]
    fn delete_makes_key_invisible() {
        let mut mt = MemTable::new();
        mt.set(b"foo", b"bar").unwrap();
        mt.delete(b"foo").unwrap();
        assert_eq!(mt.get(b"foo"), None);
    }

    #[test]
    fn delete_nonexistent_key_is_ok() {
        let mut mt = MemTable::new();
        assert!(mt.delete(b"ghost").is_ok());
        assert_eq!(mt.get(b"ghost"), None);
    }

    #[test]
    fn set_after_delete_restores_key() {
        let mut mt = MemTable::new();
        mt.set(b"foo", b"bar").unwrap();
        mt.delete(b"foo").unwrap();
        mt.set(b"foo", b"baz").unwrap();
        assert_eq!(mt.get(b"foo"), Some(b"baz".as_slice()));
    }

    #[test]
    fn iter_returns_sorted_order() {
        let mut mt = MemTable::new();
        mt.set(b"c", b"3").unwrap();
        mt.set(b"a", b"1").unwrap();
        mt.set(b"b", b"2").unwrap();

        let keys: Vec<&[u8]> = mt.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]);
    }

    #[test]
    fn iter_skips_tombstones() {
        let mut mt = MemTable::new();
        mt.set(b"a", b"1").unwrap();
        mt.set(b"b", b"2").unwrap();
        mt.delete(b"a").unwrap();

        let pairs: Vec<(&[u8], &[u8])> = mt.iter().collect();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0], (b"b".as_slice(), b"2".as_slice()));
    }

    #[test]
    fn size_bytes_empty() {
        let mt = MemTable::new();
        assert_eq!(mt.size_bytes(), 0);
    }

    #[test]
    fn size_bytes_counts_keys_and_values() {
        let mut mt = MemTable::new();
        mt.set(b"foo", b"bar").unwrap(); // 3 + 3 = 6
        mt.set(b"hi", b"there").unwrap(); // 2 + 5 = 7
        assert_eq!(mt.size_bytes(), 13);
    }

    #[test]
    fn size_bytes_tombstone_counts_only_key() {
        let mut mt = MemTable::new();
        mt.set(b"foo", b"bar").unwrap();
        mt.delete(b"foo").unwrap(); // tombstone: key=3, no value bytes
        assert_eq!(mt.size_bytes(), 3);
    }
}
