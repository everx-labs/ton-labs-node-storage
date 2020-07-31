use std::fmt::{Display, Formatter};  
use sha2::{Digest, Sha256};
use crate::db::traits::DbKey;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockId {
    key: Vec<u8>,
    block_id_ext: ton_block::BlockIdExt,
}

impl BlockId {
    pub fn block_id_ext(&self) -> &ton_block::BlockIdExt {
        &self.block_id_ext
    }
}

impl From<ton_block::BlockIdExt> for BlockId {
    fn from(block_id_ext: ton_block::BlockIdExt) -> Self {
        let mut hasher = Sha256::new();
        hasher.input(block_id_ext.shard_id.workchain_id().to_le_bytes());
        hasher.input(block_id_ext.shard_id.shard_prefix_with_tag().to_le_bytes());
        hasher.input(block_id_ext.seq_no.to_le_bytes());
        hasher.input(block_id_ext.root_hash.as_slice());
        hasher.input(block_id_ext.file_hash.as_slice());
        let key = hasher.result().to_vec();

        Self { key, block_id_ext }
    }
}

impl From<&ton_block::BlockIdExt> for BlockId {
    fn from(block_id_ext: &ton_block::BlockIdExt) -> Self {
        Self::from(block_id_ext.clone())
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}] {}", hex::encode(&self.key), self.block_id_ext))
    }
}

impl DbKey for BlockId {
    fn key(&self) -> &[u8] {
        &self.key
    }
}
