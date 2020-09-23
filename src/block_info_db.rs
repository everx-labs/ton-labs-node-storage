use crate::db_impl_base;
use crate::db::traits::KvcWriteable;
use crate::types::BlockId;

db_impl_base!(BlockInfoDb, KvcWriteable, BlockId);
