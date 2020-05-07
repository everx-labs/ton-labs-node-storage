use crate::base_impl;
use crate::db::traits::KvcWriteable;

base_impl!(NodeStateDb, KvcWriteable, &'static str);
