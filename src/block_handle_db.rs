use std::sync::{Arc, Weak};

use ton_block::BlockIdExt;
use ton_types::{error, Result};

use crate::db::traits::KvcWriteable;
use crate::db_impl_serializable;
use crate::traits::Serializable;
use crate::types::{BlockHandle, BlockId, BlockMeta};


db_impl_serializable!(BlockHandleDb, KvcWriteable, BlockId, BlockMeta);

pub(crate) type BlockHandleCache = Arc<lockfree::map::Map<BlockIdExt, Weak<BlockHandle>>>;

pub struct BlockHandleStorage {
    block_handle_db: Arc<BlockHandleDb>,
    block_handle_cache: BlockHandleCache,
}

impl BlockHandleStorage {
    pub fn new(block_handle_db: Arc<BlockHandleDb>) -> Self {
        Self {
            block_handle_db,
            block_handle_cache: BlockHandleCache::default(),
        }
    }

    pub const fn block_handle_db(&self) -> &Arc<BlockHandleDb> {
        &self.block_handle_db
    }

    pub fn load_block_handle(&self, id: &BlockIdExt) -> Result<Arc<BlockHandle>> {
        log::trace!("load_block_handle {}", id);

        let mut handle = None;
        adnl::common::add_object_to_map_with_update(&self.block_handle_cache, id.clone(), |val| {
            if let Some(Some(strong)) = val.map(|weak| weak.upgrade()) {
                handle = Some(strong);
                return Ok(None)
            }
            let h = self.load_or_create_handle(id.clone())?;
            let r = Some(Arc::downgrade(&h));
            handle = Some(h);
            Ok(r)
        })?;

        Ok(handle.ok_or_else(|| error!("unexpected None value in load_block_handle_impl"))?)
    }

    pub fn store_block_handle(&self, handle: &BlockHandle) -> Result<()> {
        self.block_handle_db.put_value(&handle.id().into(), handle.meta())?;
        Ok(())
    }

    #[inline]
    pub(super) fn create_handle(&self, id: BlockIdExt, meta: BlockMeta) -> Arc<BlockHandle> {
        Arc::new(BlockHandle::with_values(id, meta, Arc::clone(&self.block_handle_cache)))
    }

    fn load_or_create_handle(&self, id: BlockIdExt) -> Result<Arc<BlockHandle>> {
        Ok(match self.block_handle_db.try_get_value(&(&id).into())? {
            None => self.create_handle(id, BlockMeta::default()),
            Some(block_meta) => self.create_handle(id, block_meta),
        })
    }
}

