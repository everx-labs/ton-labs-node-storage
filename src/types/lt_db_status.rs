use std::io::{Read, Write};

use ton_types::{ByteOrderRead, Result};

use crate::traits::Serializable;

#[derive(Debug, PartialEq)]
pub struct LtDbStatusEntry {
    total_shards: u32,
}

impl LtDbStatusEntry {
    pub const fn new(total_shards: u32) -> Self {
        Self { total_shards }
    }

    pub const fn total_shards(&self) -> u32 {
        self.total_shards
    }
}

impl Serializable for LtDbStatusEntry {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.total_shards.to_le_bytes())?;

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let total_shards = reader.read_le_u32()?;

        Ok(Self { total_shards })
    }
}
