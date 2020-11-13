use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use tokio::sync::RwLock;

use ton_types::{ByteOrderRead, Result};

use crate::traits::Serializable;

#[derive(Debug, Default)]
pub struct BlockMeta {
    flags: AtomicU32,
    gen_utime: AtomicU32,
    gen_lt: AtomicU64,
    masterchain_ref_seq_no: AtomicU32,
    fetched: AtomicBool,
    moving_to_archive_started: AtomicBool,
    temp_lock: RwLock<()>,
}

impl BlockMeta {
    pub fn with_data(flags: u32, gen_utime: u32, gen_lt: u64, masterchain_ref_seq_no: u32, fetched: bool) -> Self {
        Self {
            flags: AtomicU32::new(flags),
            gen_utime: AtomicU32::new(gen_utime),
            gen_lt: AtomicU64::new(gen_lt),
            masterchain_ref_seq_no: AtomicU32::new(masterchain_ref_seq_no),
            fetched: AtomicBool::new(fetched),
            moving_to_archive_started: AtomicBool::new(false),
            temp_lock: RwLock::new(()),
        }
    }

    pub const fn flags(&self) -> &AtomicU32 {
        &self.flags
    }

    pub const fn gen_utime(&self) -> &AtomicU32 {
        &self.gen_utime
    }

    pub const fn gen_lt(&self) -> &AtomicU64 {
        &self.gen_lt
    }

    pub const fn masterchain_ref_seq_no(&self) -> &AtomicU32 {
        &self.masterchain_ref_seq_no
    }

    pub fn fetched(&self) -> bool {
        self.fetched.load(Ordering::SeqCst)
    }

    pub fn set_fetched(&self) -> bool {
        self.fetched.swap(true, Ordering::SeqCst)
    }
}

impl Serializable for BlockMeta {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.flags.load(Ordering::SeqCst).to_le_bytes())?;
        writer.write_all(&self.gen_utime.load(Ordering::SeqCst).to_le_bytes())?;
        writer.write_all(&self.gen_lt.load(Ordering::SeqCst).to_le_bytes())?;
        writer.write_all(&self.masterchain_ref_seq_no.load(Ordering::SeqCst).to_le_bytes())?;
        writer.write_all(&[self.fetched() as u8])?;


        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
        let flags = reader.read_le_u32()?;
        let gen_utime = reader.read_le_u32()?;
        let gen_lt = reader.read_le_u64()?;
        let masterchain_ref_seq_no = reader.read_le_u32()?;
        let fetched = reader.read_byte()? != 0;
        let bm = Self::with_data(flags, gen_utime, gen_lt, masterchain_ref_seq_no, fetched);


        Ok(bm)
    }
}
