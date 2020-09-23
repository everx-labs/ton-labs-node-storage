use std::cmp::Ordering::{Greater, Less};
use std::convert::TryInto;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering;

use ton_block::{AccountIdPrefixFull, BlockIdExt, MAX_SPLIT_DEPTH, ShardIdent, UnixTime32};
use ton_types::{fail, Result};

use crate::lt_db::LtDb;
use crate::lt_desc_db::LtDescDb;
use crate::lt_shard_db::LtShardDb;
use crate::status_db::StatusDb;
use crate::types::{BlockId, BlockMeta, LtDbEntry, LtDbKey, LtDbStatusEntry, LtDesc, ShardIdentKey, StatusKey};

#[derive(Debug)]
pub struct BlockIndexDb {
    lt_desc_db: RwLock<LtDescDb>,
    lt_db: LtDb,
    lt_shard_db: LtShardDb,
    status_db: Arc<StatusDb>,
}

impl BlockIndexDb {
    pub fn with_dbs(lt_desc_db: LtDescDb, lt_db: LtDb, lt_shard_db: LtShardDb, status_db: Arc<StatusDb>) -> Self {
        Self { lt_desc_db: RwLock::new(lt_desc_db), lt_db, lt_shard_db, status_db }
    }

    pub fn in_memory(status_db: Arc<StatusDb>) -> Self {
        Self::with_dbs(
            LtDescDb::in_memory(),
            LtDb::in_memory(),
            LtShardDb::in_memory(),
            status_db,
        )
    }

    pub fn with_paths(
        lt_desc_db_path: impl AsRef<Path>,
        lt_db_path: impl AsRef<Path>,
        lt_shard_db_path: impl AsRef<Path>,
        status_db: Arc<StatusDb>
    ) -> Self {
        Self::with_dbs(
            LtDescDb::with_path(lt_desc_db_path),
            LtDb::with_path(lt_db_path),
            LtShardDb::with_path(lt_shard_db_path),
            status_db,
        )
    }

    pub const fn lt_desc_db(&self) -> &RwLock<LtDescDb> {
        &self.lt_desc_db
    }

    pub const fn lt_db(&self) -> &LtDb {
        &self.lt_db
    }

    pub const fn lt_shard_db(&self) -> &LtShardDb {
        &self.lt_shard_db
    }

    pub const fn status_db(&self) -> &Arc<StatusDb> {
        &self.status_db
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

    pub(crate) fn add_handle(&self, block_id: &BlockId, block_meta: &BlockMeta) -> Result<()> {
        let desc_key = ShardIdentKey::new(block_id.block_id_ext().shard())?;
        let mut shard_index = 0;
        let lt_desc_db_locked = self.lt_desc_db.write()
            .expect("Poisoned RwLock");
        let (mut lt_desc, add_shard) = if let Some(lt_desc) = lt_desc_db_locked.try_get_value(&desc_key)? {
            (lt_desc, false)
        } else {
            if let Some(status) = self.status_db.try_get_value::<LtDbStatusEntry>(&StatusKey::LtDbStatus)? {
                shard_index = status.total_shards();
            }
            (LtDesc::with_values(1, 0, 0, 0, 0), true)
        };

        let index = lt_desc.last_index() + 1;
        let lt_key = LtDbKey::with_values(block_id.block_id_ext().shard(), index)?;

        let lt_entry = LtDbEntry::with_values(
            block_id.block_id_ext().into(),
            block_meta.gen_lt().load(Ordering::SeqCst),
            block_meta.gen_utime().load(Ordering::SeqCst)
        );

        self.lt_db.put_value(&lt_key, &lt_entry)?;

        lt_desc = LtDesc::with_values(
            lt_desc.first_index(),
            index,
            block_id.block_id_ext().seq_no(),
            block_meta.gen_lt().load(Ordering::SeqCst),
            block_meta.gen_utime().load(Ordering::SeqCst),
        );

        lt_desc_db_locked.put_value(&desc_key, &lt_desc)?;

        if add_shard {
            self.lt_shard_db.put_value(&shard_index.into(), block_id.block_id_ext().shard())?;
            self.status_db.put_value(&StatusKey::LtDbStatus, LtDbStatusEntry::new(shard_index + 1))?;
        }

        Ok(())
    }
}