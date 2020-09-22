use crate::db_impl_cbor;
use crate::db::traits::KvcWriteable;
use crate::types::{LtDesc, ShardIdentKey};

db_impl_cbor!(LtDescDb, KvcWriteable, ShardIdentKey, LtDesc);
