use ton_types::Result;

use crate::base_impl;
use crate::db::traits::KvcWriteable;
use crate::types::{BlockId, BlockMeta};

base_impl!(BlockInfoDb, KvcWriteable, BlockId);

impl BlockInfoDb {
    pub fn get_block_meta(&self, key: &BlockId) -> Result<BlockMeta> {
        let data = self.get(key)?;
        Ok(BlockMeta::deserialize(data.as_ref())?)
    }

    pub fn put_block_meta(&self, key: &BlockId, meta: &BlockMeta) -> Result<()> {
        let data = meta.serialize();
        self.put(key, data.as_slice())
    }
}
