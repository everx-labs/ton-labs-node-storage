use crate::db_impl_cbor;
use crate::db::traits::KvcWriteable;
use crate::types::{LtDbEntry, LtDbKey};

db_impl_cbor!(LtDb, KvcWriteable, LtDbKey, LtDbEntry);
