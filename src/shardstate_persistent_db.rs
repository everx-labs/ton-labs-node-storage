use std::ops::{Deref, DerefMut};
use std::path::Path;

use crate::db::filedb::FileDb;
use crate::db::traits::KvcWriteableAsync;
use crate::types::BlockId;
use crate::db::async_adapter::KvcWriteableAsyncAdapter;

#[derive(Debug)]
pub struct ShardStatePersistentDb {
    db: Box<dyn KvcWriteableAsync<BlockId>>,
}

impl ShardStatePersistentDb {
    /// Constructs new instance using in-memory key-value collection
    pub fn in_memory() -> Self {
        Self {
            db: Box::new(KvcWriteableAsyncAdapter::new(crate::db::memorydb::MemoryDb::new()))
        }
    }

    /// Constructs new instance using FileDb with given path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Self {
        Self {
            db: Box::new(FileDb::with_path(path))
        }
    }
}

impl Deref for ShardStatePersistentDb {
    type Target = Box<dyn KvcWriteableAsync<BlockId>>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DerefMut for ShardStatePersistentDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}
