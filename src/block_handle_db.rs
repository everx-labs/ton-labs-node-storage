use std::path::Path;
use std::sync::Arc;

use ton_types::Result;

use crate::block_index_db::BlockIndexDb;
use crate::db::memorydb::MemoryDb;
use crate::db::rocksdb::RocksDb;
use crate::db::traits::KvcWriteable;
use crate::traits::Serializable;
use crate::types::{BlockId, BlockMeta};

#[derive(Debug)]
pub struct BlockHandleDb {
    db: Box<dyn KvcWriteable<BlockId>>,
    block_index_db: Arc<BlockIndexDb>,
}

impl BlockHandleDb {
    /// Constructs new instance using in-memory key-value collection
    pub fn in_memory(block_index_db: Arc<BlockIndexDb>) -> Self {
        Self {
            db: Box::new(MemoryDb::new()),
            block_index_db,
        }
    }

    /// Constructs new instance using RocksDB with given path
    pub fn with_path<P: AsRef<Path>>(path: P, block_index_db: Arc<BlockIndexDb>) -> Self {
        Self {
            db: Box::new(RocksDb::with_path(path)),
            block_index_db,
        }
    }

    pub fn block_index_db(&self) -> &Arc<BlockIndexDb> {
        &self.block_index_db
    }

    pub fn contains(&self, key: &BlockId) -> Result<bool> {
        self.db.contains(key)
    }

    pub fn try_get(&self, key: &BlockId) -> Result<Option<BlockMeta>> {
        match self.db.try_get(key)? {
            Some(slice) => {
                let block_meta = Self::deserialize_block_meta_stored(slice.as_ref())?;
                Ok(Some(block_meta))
            },
            _ => Ok(None),
        }
    }

    pub fn get(&self, key: &BlockId) -> Result<BlockMeta> {
        Self::deserialize_block_meta_stored(self.db.get(key)?.as_ref())
    }

    pub fn put(&self, key: &BlockId, meta: &BlockMeta) -> Result<()> {
        self.db.put(key, meta.to_vec()?.as_slice())?;
        if meta.set_handle_stored() {
            return Ok(());
        }
        self.block_index_db.add_handle(key, meta)
    }

    pub fn delete(&self, key: &BlockId) -> Result<()> {
        self.db.delete(key)
    }

    pub fn destroy(&mut self) -> Result<()> {
        self.db.destroy()
    }

    fn deserialize_block_meta_stored(slice: &[u8]) -> Result<BlockMeta> {
        let block_meta = BlockMeta::from_slice(slice)?;
        block_meta.set_handle_stored();

        Ok(block_meta)
    }
}

