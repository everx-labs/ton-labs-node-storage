use std::cmp::Ordering::{Greater, Less};
use std::convert::TryInto;
use std::path::Path;
use std::sync::RwLock;

use ton_block::{AccountIdPrefixFull, BlockIdExt, MAX_SPLIT_DEPTH, ShardIdent, UnixTime32};
use ton_types::{fail, Result};

use crate::lt_db::LtDb;
use crate::lt_desc_db::LtDescDb;
use crate::types::{BlockHandle, LtDbEntry, LtDbKey, LtDesc, ShardIdentKey};

#[derive(Debug)]
pub struct BlockIndexDb {
    lt_desc_db: RwLock<LtDescDb>,
    lt_db: LtDb,
}

impl BlockIndexDb {
    pub fn with_dbs(lt_desc_db: LtDescDb, lt_db: LtDb) -> Self {
        Self { lt_desc_db: RwLock::new(lt_desc_db), lt_db }
    }

    pub fn in_memory() -> Self {
        Self::with_dbs(
            LtDescDb::in_memory(),
            LtDb::in_memory(),
        )
    }

    pub fn with_paths(
        lt_desc_db_path: impl AsRef<Path>,
        lt_db_path: impl AsRef<Path>,
    ) -> Self {
        Self::with_dbs(
            LtDescDb::with_path(lt_desc_db_path),
            LtDb::with_path(lt_db_path),
        )
    }

    pub const fn lt_desc_db(&self) -> &RwLock<LtDescDb> {
        &self.lt_desc_db
    }

    pub const fn lt_db(&self) -> &LtDb {
        &self.lt_db
    }

    pub fn get_block_by_lt(&self, account_id: &AccountIdPrefixFull, lt: u64) -> Result<BlockIdExt> {
        self.get_block(
            account_id,
            |desc| lt.cmp(&desc.last_lt()),
            |entry| lt.cmp(&entry.lt()),
            false
        )
    }

    pub fn get_block_by_ut(&self, account_id: &AccountIdPrefixFull, unix_time: UnixTime32) -> Result<BlockIdExt> {
        self.get_block(
            account_id,
            |desc| unix_time.0.cmp(&desc.last_unix_time()),
            |entry| unix_time.0.cmp(&entry.unix_time()),
            false
        )
    }

    pub fn get_block_by_seq_no(&self, account_id: &AccountIdPrefixFull, seq_no: u32) -> Result<BlockIdExt> {
        self.get_block(
            account_id,
            |desc| seq_no.cmp(&desc.last_seq_no()),
            |entry| seq_no.cmp(&(entry.block_id_ext().seqno as u32)),
            true
        )
    }

    pub fn get_block<FDesc, FLtDb>(
        &self,
        account_id: &AccountIdPrefixFull,
        compare_desc: FDesc,
        compare_lt_db: FLtDb,
        exact: bool,
    ) -> Result<BlockIdExt>
    where
        FDesc: Fn(&LtDesc) -> std::cmp::Ordering,
        FLtDb: Fn(&LtDbEntry) -> std::cmp::Ordering
    {
        let mut found = false;
        let mut block_id_opt: Option<BlockIdExt> = None;
        let mut max_left_seq_no = 0;

        for len in 0..=MAX_SPLIT_DEPTH {
            let shard = ShardIdent::with_prefix_len(
                len,
                account_id.workchain_id,
                account_id.prefix)?;

            let shard_key = ShardIdentKey::new(&shard)?;
            let lt_desc = match self.lt_desc_db.read()
                .expect("Poisoned RwLock")
                .try_get_value(&shard_key)?
            {
                Some(lt_desc) => lt_desc,
                _ if found => break,
                _ => continue,
            };

            found = true;

            if compare_desc(&lt_desc) == Greater {
                continue;
            }

            let mut lb = lt_desc.first_index();
            let mut left_seq_no_opt = None;
            let mut rb = lt_desc.last_index() + 1;
            let mut right_seq_no_opt = None;
            let mut last_index = rb + 1;
            while rb > lb {
                let index = lb + (rb - lb) / 2;

                // In order to prevent infinite loops in cases of gaps:
                if last_index == index {
                    break;
                }
                last_index = index;

                let lt_db_key = LtDbKey::with_values(&shard, index)?;
                let entry = self.lt_db.get_value(&lt_db_key)?;
                let result: BlockIdExt = entry.block_id_ext().try_into()?;
                match compare_lt_db(&entry) {
                    Less => {
                        right_seq_no_opt = Some(result);
                        rb = index;
                    },
                    Greater => {
                        left_seq_no_opt = Some(result);
                        lb = index;
                    },
                    _ => return Ok(result),
                }
            }

            if let Some(ref right_seq_no) = right_seq_no_opt {
                if let Some(ref block_id) = block_id_opt {
                    if block_id.seq_no() > right_seq_no.seq_no() as u32 {
                        block_id_opt = right_seq_no_opt;
                    }
                } else {
                    block_id_opt = right_seq_no_opt;
                }
            }

            if let Some(left_seq_no) = left_seq_no_opt {
                if max_left_seq_no < left_seq_no.seq_no() {
                    max_left_seq_no = left_seq_no.seq_no();
                }
            }

            if let Some(ref block_id) = block_id_opt {
                if block_id.seq_no() == max_left_seq_no + 1 {
                    if !exact {
                        return Ok(block_id.clone());
                    } else {
                        fail!("Block not found");
                    }
                }
            }
        }

        if !exact && block_id_opt.is_some() {
            return Ok(block_id_opt.unwrap());
        }

        fail!("Block not found")
    }

    pub fn add_handle(&self, handle: &BlockHandle) -> Result<()> {
        log::trace!(target: "storage", "BlockIndexDb::add_handle {}", handle.id());
        let desc_key = ShardIdentKey::new(handle.id().shard())?;
        let lt_desc_db_locked = self.lt_desc_db.write()
            .expect("Poisoned RwLock");
        let index = if let Some(lt_desc) = lt_desc_db_locked.try_get_value(&desc_key)? {
            match handle.id().seq_no().cmp(&lt_desc.last_seq_no()) {
                std::cmp::Ordering::Equal => return Ok(()),
                std::cmp::Ordering::Less => fail!("Block handles seq_no must be written in the ascending order!"),
                _ => lt_desc.last_index() + 1,
            }
        } else {
            1
        };

        let lt_key = LtDbKey::with_values(handle.id().shard(), index)?;

        let lt_entry = LtDbEntry::with_values(
            handle.id().into(),
            handle.gen_lt(),
            handle.gen_utime()?
        );

        self.lt_db.put_value(&lt_key, &lt_entry)?;

        let lt_desc = LtDesc::with_values(
            1,
            index,
            handle.id().seq_no(),
            handle.gen_lt(),
            handle.gen_utime()?,
        );

        lt_desc_db_locked.put_value(&desc_key, &lt_desc)?;

        Ok(())
    }
}
