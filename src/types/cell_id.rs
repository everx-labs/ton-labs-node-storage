use ton_types::types::UInt256;
use std::fmt::{Display, Formatter, Debug};
use crate::db::traits::DbKey;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CellId {
    hash: UInt256,
}

impl CellId {
    pub fn new(hash: UInt256) -> Self {
        Self { hash }
    }
}

impl Display for CellId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:#x}", self.hash))
    }
}

impl Debug for CellId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("CellId[{:#x}]", self.hash))
    }
}

impl DbKey for CellId {
    fn key(&self) -> &[u8] {
        self.hash.as_slice()
    }
}

impl From<UInt256> for CellId {
    fn from(value: UInt256) -> Self {
        CellId::new(value)
    }
}

impl Into<UInt256> for CellId {
    fn into(self) -> UInt256 {
        self.hash
    }
}