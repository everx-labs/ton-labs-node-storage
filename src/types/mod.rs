
mod block_id;
mod cell_id;
mod storage_cell;
mod reference;
mod db_slice;
mod block_meta;

pub use block_id::*;
pub use cell_id::*;
pub use storage_cell::*;
pub use reference::*;
pub use db_slice::*;
pub use block_meta::*;

/// Usually >= 1; 0 used to indicate the initial state, i.e. "zerostate"
pub type BlockSeqNo = i32;
pub type BlockVertSeqNo = u32;
pub type WorkchainId = i32;
pub type ShardId = i64;