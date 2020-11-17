use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::RwLock;
use ton_block::{BlockIdExt, BlockInfo, Block};
use ton_types::{fail, Result};

use crate::block_handle_db::BlockHandleCache;
use crate::traits::Serializable;
use crate::types::BlockMeta;

const FLAG_DATA: u32 = 1;
const FLAG_PROOF: u32 = 1 << 1;
const FLAG_PROOF_LINK: u32 = 1 << 2;
const FLAG_EXT_DB: u32 = 1 << 3;
const FLAG_STATE: u32 = 1 << 4;
const FLAG_PERSISTENT_STATE: u32 = 1 << 5;
const FLAG_NEXT_1: u32 = 1 << 6;
const FLAG_NEXT_2: u32 = 1 << 7;
const FLAG_PREV_1: u32 = 1 << 8;
const FLAG_PREV_2: u32 = 1 << 9;
const FLAG_APPLIED: u32 = 1 << 10;
const FLAG_KEY_BLOCK: u32 = 1 << 11;
const FLAG_MOVED_TO_ARCHIVE: u32 = 1 << 13;
const FLAG_INDEXED: u32 = 1 << 14;

/// Meta information related to block
#[derive(Debug)]
pub struct BlockHandle {
    id: BlockIdExt,
    meta: BlockMeta,
    moving_to_archive_started: AtomicBool,
    temp_lock: RwLock<()>,
    block_handle_cache: BlockHandleCache,
}

impl BlockHandle {
    pub fn new(id: BlockIdExt, block_handle_cache: BlockHandleCache) -> Self {
        Self::with_values(id, BlockMeta::default(), block_handle_cache)
    }

    pub fn with_values(id: BlockIdExt, meta: BlockMeta, block_handle_cache: BlockHandleCache) -> Self {
        Self {
            id,
            meta,
            moving_to_archive_started: AtomicBool::new(false),
            temp_lock: RwLock::new(()),
            block_handle_cache
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.meta.serialize(writer)
    }

    // This flags might be set into true only. So flush only after transform false -> true.

    fn fetch_info(&self, info: &BlockInfo) -> Result<()> {
        self.meta.gen_utime().store(info.gen_utime().0, Ordering::SeqCst);
        if info.key_block() {
            self.set_flags(FLAG_KEY_BLOCK);
        }
        self.meta.set_fetched();
        Ok(())
    }

    pub fn fetch_block_info(&self, block: &Block) -> Result<()> {
        self.fetch_info(&block.read_info()?)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn set_data_inited(&self) -> bool {
        self.set_flags(FLAG_DATA)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn set_proof_inited(&self) -> bool {
        self.set_flags(FLAG_PROOF)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn set_proof_link_inited(&self) -> bool {
        self.set_flags(FLAG_PROOF_LINK)
    }

    pub fn set_processed_in_ext_db(&self) -> bool {
        self.set_flags(FLAG_EXT_DB)
    }

    pub fn set_state_inited(&self) -> bool {
        self.set_flags(FLAG_STATE)
    }

    pub fn set_persistent_state_inited(&self) -> bool {
        self.set_flags(FLAG_PERSISTENT_STATE)
    }

    pub fn set_next1_inited(&self) -> bool {
        self.set_flags(FLAG_NEXT_1)
    }

    pub fn set_next2_inited(&self) -> bool {
        self.set_flags(FLAG_NEXT_2)
    }

    pub fn set_prev1_inited(&self) -> bool {
        self.set_flags(FLAG_PREV_1)
    }

    pub fn set_prev2_inited(&self) -> bool {
        self.set_flags(FLAG_PREV_2)
    }

    pub fn set_applied(&self) -> bool {
        self.set_flags(FLAG_APPLIED)
    }

    pub fn id(&self) -> &BlockIdExt {
        &self.id
    }

    pub fn meta(&self) -> &BlockMeta {
        &self.meta
    }

    pub fn set_indexed(&self) -> bool {
        self.set_flags(FLAG_INDEXED)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn data_inited(&self) -> bool {
        self.flags_all(FLAG_DATA)
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn proof_inited(&self) -> bool {
        if cfg!(feature = "local_test") {
            true
        } else {
            self.flags_all(FLAG_PROOF)
        }
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn proof_link_inited(&self) -> bool {
        if cfg!(feature = "local_test") {
            true
        } else {
            self.flags_all(FLAG_PROOF_LINK)
        }
    }

    // TODO: Give correct name due to actual meaning (not "inited", but "saved" or "stored")
    pub fn proof_or_link_inited(&self, is_link: &mut bool) -> bool {
        *is_link = self.id.shard().is_masterchain();
        if *is_link {
            self.proof_link_inited()
        } else {
            self.proof_inited()
        }
    }

    pub fn processed_in_ext_db(&self) -> bool {
        self.flags_all(FLAG_EXT_DB)
    }

    pub fn state_inited(&self) -> bool {
        self.flags_all(FLAG_STATE)
    }

    pub fn persistent_state_inited(&self) -> bool {
        self.flags_all(FLAG_PERSISTENT_STATE)
    }

    pub fn next1_inited(&self) -> bool {
        self.flags_all(FLAG_NEXT_1)
    }

    pub fn next2_inited(&self) -> bool {
        self.flags_all(FLAG_NEXT_2)
    }

    pub fn prev1_inited(&self) -> bool {
        self.flags_all(FLAG_PREV_1)
    }

    pub fn prev2_inited(&self) -> bool {
        self.flags_all(FLAG_PREV_2)
    }

    pub fn applied(&self) -> bool {
        self.flags_all(FLAG_APPLIED)
    }

    pub fn indexed(&self) -> bool {
        self.flags_all(FLAG_INDEXED)
    }

    pub fn gen_lt(&self) -> u64 {
        self.meta.gen_lt().load(Ordering::Relaxed)
    }

    pub fn gen_utime(&self) -> Result<u32> {
        if self.fetched() || self.state_inited() {
            Ok(self.meta.gen_utime().load(Ordering::Relaxed))
        } else {
            fail!("Data is not inited yet")
        }
    }

    pub fn set_gen_utime(&self, time: u32) -> Result<()> {
        if self.fetched() || self.state_inited() {
            if time != self.meta.gen_utime().load(Ordering::Relaxed) {
                fail!("gen_utime was already set with another value")
            } else {
                Ok(())
            }
        } else {
            self.meta.gen_utime().store(time, Ordering::SeqCst);
            Ok(())
        }
    }

    pub fn masterchain_ref_seq_no(&self) -> u32 {
        if self.id.shard().is_masterchain() {
            return self.id.seq_no();
        }

        self.meta.masterchain_ref_seq_no().load(Ordering::SeqCst)
    }

    pub fn set_masterchain_ref_seq_no(&self, masterchain_ref_seq_no: u32) -> u32 {
        self.meta.masterchain_ref_seq_no().swap(masterchain_ref_seq_no, Ordering::SeqCst)
    }

    pub fn moved_to_archive(&self) -> bool {
        self.flags_all(FLAG_MOVED_TO_ARCHIVE)
    }

    pub fn set_moved_to_archive(&self) -> bool {
        self.set_flags(FLAG_MOVED_TO_ARCHIVE)
    }

    pub fn fetched(&self) -> bool {
        self.meta().fetched()
    }

    pub fn is_key_block(&self) -> Result<bool> {
        if self.fetched() {
            Ok(self.flags_all(FLAG_KEY_BLOCK))
        } else {
            fail!("Data is not inited yet")
        }
    }

    pub fn start_moving_to_archive(&self) -> bool {
        self.moving_to_archive_started.swap(true, Ordering::SeqCst)
    }

    pub(crate) fn temp_lock(&self) -> &RwLock<()>  {
        &self.temp_lock
    }

    #[inline]
    fn flags(&self) -> u32 {
        self.meta.flags().load(Ordering::SeqCst)
    }

    #[inline]
    fn flags_all(&self, flags: u32) -> bool {
        self.flags() & flags == flags
    }

    #[inline]
    fn set_flags(&self, flags: u32) -> bool {
        self.meta.flags().fetch_or(flags, Ordering::SeqCst) & flags == flags
    }
}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        self.block_handle_cache.remove_with(&self.id, |(_id, weak)| {
            weak.strong_count() == 0
        });
    }
}


