use ton_block::ShardIdent;

use crate::db::traits::{KvcWriteable, U32Key};
use crate::db_impl_serializable;
use crate::traits::Serializable;

db_impl_serializable!(LtShardDb, KvcWriteable, U32Key, ShardIdent);
