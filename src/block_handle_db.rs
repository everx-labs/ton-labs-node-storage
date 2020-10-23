use crate::db::traits::KvcWriteable;
use crate::db_impl_serializable;
use crate::traits::Serializable;
use crate::types::{BlockId, BlockMeta};

db_impl_serializable!(BlockHandleDb, KvcWriteable, BlockId, BlockMeta);
