use crate::db_impl_base;
use crate::db::traits::KvcWriteable;

db_impl_base!(BlockDb, KvcWriteable, crate::types::BlockId);
