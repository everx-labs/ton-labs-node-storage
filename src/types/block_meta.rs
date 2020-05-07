use std::sync::atomic::{AtomicU32, Ordering};

use arrayref::array_ref;

use ton_types::{fail, Result};

#[derive(Default)]
pub struct BlockMeta {
    flags: AtomicU32,
    gen_utime: AtomicU32,
}

impl BlockMeta {
    pub fn new(flags: u32, gen_utime: u32) -> Self {
        Self {
            flags: AtomicU32::new(flags),
            gen_utime: AtomicU32::new(gen_utime)
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = self.flags.load(Ordering::SeqCst).to_le_bytes().to_vec();
        data.extend_from_slice(&self.gen_utime.load(Ordering::SeqCst).to_le_bytes());

        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let mut gen_utime = 0;
        let flags = match data.len() {
            1 => data[0] as u32,
            4 => u32::from_le_bytes(array_ref!(&data, 0, 4).clone()),
            8 => {
                gen_utime = u32::from_le_bytes(array_ref!(&data, 4, 4).clone());
                u32::from_le_bytes(array_ref!(&data, 0, 4).clone())
            }
            _ => fail!("Wrong data length")
        };

        Ok(Self::new(flags, gen_utime))
    }

    pub fn flags(&self) -> &AtomicU32 {
        &self.flags
    }

    pub fn gen_utime(&self) -> &AtomicU32 {
        &self.gen_utime
    }
}