use rocksdb::DBPinnableSlice;
use std::ops::Deref;

/// Represents memory slice, returned by database (in a case of RocksDB), or vector, in a case of MemoryDb
pub enum DbSlice<'a> {
    RocksDb(DBPinnableSlice<'a>),
    Vector(Vec<u8>)
}

impl AsRef<[u8]> for DbSlice<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            DbSlice::RocksDb(slice) => slice.as_ref(),
            DbSlice::Vector(vector) => vector.as_slice(),
        }
    }
}

impl Deref for DbSlice<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<'a> From<DBPinnableSlice<'a>> for DbSlice<'a> {
    fn from(slice: DBPinnableSlice<'a>) -> Self {
        DbSlice::RocksDb(slice)
    }
}

impl<'a> From<&'a [u8]> for DbSlice<'a> {
    fn from(slice: &'a [u8]) -> Self {
        DbSlice::Vector(slice.to_vec())
    }
}

impl<'a> From<Vec<u8>> for DbSlice<'a> {
    fn from(vector: Vec<u8>) -> Self {
        DbSlice::Vector(vector)
    }
}
