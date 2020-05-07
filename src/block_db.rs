use crate::base_impl;
use crate::db::traits::KvcWriteable;

base_impl!(BlockDb, KvcWriteable, crate::types::BlockId);
