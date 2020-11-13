
mod block_id;
mod cell_id;
mod storage_cell;
mod reference;
mod db_slice;
mod complex_id;
mod block_meta;
mod lt_desc;
mod lt_db_key;
mod lt_db_entry;
mod status_key;
mod shard_ident_key;
mod lt_db_status;

pub use block_id::*;
pub use cell_id::*;
pub use storage_cell::*;
pub use reference::*;
pub use db_slice::*;
pub use complex_id::*;
pub use block_meta::*;
pub use lt_desc::*;
pub use lt_db_key::*;
pub use lt_db_entry::*;
pub use status_key::*;
pub use shard_ident_key::*;
pub use lt_db_status::*;

/// Usually >= 1; 0 used to indicate the initial state, i.e. "zerostate"
pub type BlockSeqNo = i32;
pub type BlockVertSeqNo = u32;
pub type WorkchainId = i32;
pub type ShardId = i64;
