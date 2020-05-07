use crate::base_impl;
use crate::db::traits::KvcWriteable;

base_impl!(CatchainPersistentDb, KvcWriteable, ton_types::types::UInt256);
