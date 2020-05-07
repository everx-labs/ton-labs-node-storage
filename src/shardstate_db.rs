use std::io::{Cursor, Read, Write};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use ton_block::{BlockIdExt, ShardIdent, UnixTime32};
use ton_types::{Cell, CellImpl, Result};
use ton_types::types::{ByteOrderRead, UInt256};

use crate::block_info_db::BlockInfoDb;
use crate::cell_db::CellDb;
use crate::db::memorydb::MemoryDb;
use crate::db::rocksdb::RocksDb;
use crate::db::traits::{DbKey, KvcTransaction, KvcWriteable};
use crate::dynamic_boc_db::DynamicBocDb;
use crate::types::{BlockId, CellId, StorageCell};

pub struct ShardStateDb {
    shardstate_db: Arc<dyn KvcWriteable<BlockId>>,
    dynamic_boc_db: Arc<DynamicBocDb>,
}

pub(crate) struct DbEntry {
    pub cell_id: CellId,
    pub block_id_ext: BlockIdExt,
}

impl DbEntry {
    pub fn with_params(cell_id: CellId, block_id_ext: BlockIdExt) -> Self {
        Self { cell_id, block_id_ext }
    }

    pub fn serialize<T: Write>(&self, writer: &mut T) -> Result<()> {
        writer.write_all(self.cell_id.key())?;

        // TODO: Implement (de)serialization into BlockIdExt:
        writer.write_i32::<LittleEndian>(self.block_id_ext.shard_id.workchain_id())?;
        writer.write_u64::<LittleEndian>(self.block_id_ext.shard_id.shard_prefix_with_tag())?;
        writer.write_u32::<LittleEndian>(self.block_id_ext.seq_no())?;
        writer.write_all(self.block_id_ext.root_hash().as_ref())?;
        writer.write_all(self.block_id_ext.file_hash().as_ref())?;

        Ok(())
    }

    pub fn deserialize<T: Read>(reader: &mut T) -> Result<Self> {
        let mut buf = [0; 32];
        reader.read_exact(&mut buf)?;
        let cell_id = CellId::new(buf.into());

        // TODO: Implement (de)serialization into BlockIdExt:
        let workchain_id = reader.read_i32::<LittleEndian>()?;
        let shard_prefix_tagged = reader.read_u64::<LittleEndian>()?;
        let seq_no = reader.read_u32::<LittleEndian>()?;
        let root_hash = UInt256::from(reader.read_u256()?);
        let file_hash = UInt256::from(reader.read_u256()?);

        let shard_id = ShardIdent::with_tagged_prefix(workchain_id, shard_prefix_tagged)?;
        let block_id_ext = BlockIdExt::with_params(shard_id, seq_no, root_hash, file_hash);

        Ok(Self { cell_id, block_id_ext })
    }
}

impl ShardStateDb {
    /// Constructs new instance using in-memory key-value collections
    pub fn in_memory() -> Self {
        Self::with_dbs(Arc::new(MemoryDb::new()), CellDb::in_memory())
    }

    /// Constructs new instance using RocksDB with given paths
    pub fn with_paths(shardstate_db_path: &str, cell_db_path: &str) -> Self {
        Self::with_dbs(
            Arc::new(RocksDb::with_path(shardstate_db_path)),
            CellDb::with_path(cell_db_path),
        )
    }

    /// Constructs new instance using given key-value collection implementations
    fn with_dbs(shardstate_db: Arc<dyn KvcWriteable<BlockId>>, cell_db: CellDb) -> Self {
        Self {
            shardstate_db,
            dynamic_boc_db: Arc::new(DynamicBocDb::with_db(cell_db)),
        }
    }

    /// Returns reference to shardstates database
    pub fn shardstate_db(&self) -> Arc<dyn KvcWriteable<BlockId>> {
        Arc::clone(&self.shardstate_db)
    }

    /// Returns reference to dynamic BOC database
    pub fn dynamic_boc_db(&self) -> Arc<DynamicBocDb> {
        Arc::clone(&self.dynamic_boc_db)
    }

    /// Returns reference to cell_db database
    pub fn cell_db(&self) -> &Arc<CellDb> {
        self.dynamic_boc_db.deref()
    }

    /// Stores cells from given tree which don't exist in the storage.
    /// Returns root cell which is implemented as StorageCell.
    /// So after store() origin shard state's cells might be dropped.
    pub fn put(&self, id: &BlockId, state_root: Cell) -> Result<Cell> {
        let cell_id = CellId::from(state_root.repr_hash());
        let (result_cell, _written_count) = self.dynamic_boc_db.save_as_dynamic_boc(state_root)?;

        let block_id_ext = id.block_id_ext().clone();
        let db_entry = DbEntry::with_params(cell_id, block_id_ext);

        let mut buf = Vec::new();
        db_entry.serialize(&mut Cursor::new(&mut buf))?;

        self.shardstate_db.put(id, buf.as_slice())?;

        Ok(result_cell)
    }

    /// Loads previously stored root cell
    pub fn get(&self, id: &BlockId) -> Result<Cell> {
        let entry = self.shardstate_db.get(id)?;
        let db_entry = DbEntry::deserialize(&mut Cursor::new(entry.as_ref()))?;
        let root_cell = self.dynamic_boc_db.load_dynamic_boc(&db_entry.cell_id)?;

        Ok(root_cell)
    }
}

pub(crate) trait AllowStateGcResolver: Send + Sync {
    fn allow_state_gc(&self, block_id_ext: &BlockIdExt, gc_utime: u32) -> Result<bool>;
}

struct AllowStateGcResolverImpl {
    // dynamic_boc_db: Arc<DynamicBocDb>,
    block_handle_db: Arc<BlockInfoDb>,
    shard_state_ttl: AtomicU32,
}

impl AllowStateGcResolverImpl {
    pub fn with_data(/*dynamic_boc_db: Arc<DynamicBocDb>,*/ block_handle_db: Arc<BlockInfoDb>) -> Self {
        Self {
            // dynamic_boc_db,
            block_handle_db,
            shard_state_ttl: AtomicU32::new(3600 * 24),
        }
    }

    #[allow(dead_code)]
    pub fn shard_state_ttl(&self) -> u32 {
        self.shard_state_ttl.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn set_shard_state_ttl(&self, value: u32) {
        self.shard_state_ttl.store(value, Ordering::SeqCst)
    }
}

impl AllowStateGcResolver for AllowStateGcResolverImpl {
    fn allow_state_gc(&self, block_id_ext: &BlockIdExt, gc_utime: u32) -> Result<bool> {
        let block_id = BlockId::from(block_id_ext);
        let block_meta = self.block_handle_db.get_block_meta(&block_id)?;

        // TODO: Implement more sophisticated logic of decision shard state garbage collecting

        Ok(block_meta.gen_utime().load(Ordering::SeqCst) + self.shard_state_ttl() < gc_utime)
    }
}

pub struct GC {
    shardstate_db: Arc<dyn KvcWriteable<BlockId>>,
    dynamic_boc_db: Arc<DynamicBocDb>,
    allow_state_gc_resolver: Arc<dyn AllowStateGcResolver>,
}

impl GC {
    pub fn new(db: &ShardStateDb, block_handle_db: Arc<BlockInfoDb>) -> Self {
        Self::with_data(
            db.shardstate_db(),
            db.dynamic_boc_db(),
            Arc::new(
                AllowStateGcResolverImpl::with_data(
                    /*db.dynamic_boc_db(),*/
                    block_handle_db
                )
            )
        )
    }

    pub(crate) fn with_data(
        shardstate_db: Arc<dyn KvcWriteable<BlockId>>,
        dynamic_boc_db: Arc<DynamicBocDb>,
        allow_state_gc_resolver: Arc<dyn AllowStateGcResolver>
    ) -> Self {
        Self {
            shardstate_db,
            dynamic_boc_db,
            allow_state_gc_resolver,
        }
    }

    pub fn collect(&self) -> Result<usize> {
        let gc_gen = self.dynamic_boc_db.new_gc_generation();
        let gc_utime = UnixTime32::now();

        let (marked, to_sweep) = self.mark(gc_gen, gc_utime.0)?;
        let result = self.sweep(to_sweep, gc_gen);

        // We're handling and dropping marked trees only after sweep operation in order to prevent
        // dropping of marked cells which will make them removable by the sweeper
        drop(marked);

        result
    }

    fn mark(&self, gc_gen: u32, gc_utime: u32) -> Result<(Vec<Arc<StorageCell>>, Vec<(BlockId, CellId)>)> {
        let mut to_mark = Vec::new();
        let mut to_sweep = Vec::new();
        self.shardstate_db.for_each(&mut |_key, value| {
            let mut cursor = Cursor::new(value);
            let db_entry = DbEntry::deserialize(&mut cursor)?;
            let cell_id = db_entry.cell_id;
            let block_id_ext = db_entry.block_id_ext;
            if (!self.dynamic_boc_db.cells_map().lock().unwrap().contains_key(&cell_id))
                && self.allow_state_gc_resolver.allow_state_gc(&block_id_ext, gc_utime)?
            {
                let block_id = BlockId::from(block_id_ext);
                to_sweep.push((block_id, cell_id));
            } else {
                to_mark.push(cell_id);
            }

            Ok(true)
        })?;

        let mut marked = Vec::new();
        if to_sweep.len() > 0 {
            for cell_id in to_mark {
                let root_cell = self.dynamic_boc_db.load_cell(&cell_id)?;
                self.mark_subtree_recursive(Arc::clone(&root_cell), gc_gen)?;
                marked.push(root_cell);
            }
        }

        Ok((marked, to_sweep))
    }

    fn mark_subtree_recursive(&self, root: Arc<StorageCell>, gc_gen: u32) -> Result<()> {
        if root.gc_gen().load(Ordering::SeqCst) >= gc_gen {
            return Ok(());
        }

        root.gc_gen().store(gc_gen, Ordering::SeqCst);

        for i in 0..root.references_count() {
            self.mark_subtree_recursive(root.reference(i)?, gc_gen)?;
        }

        Ok(())
    }

    fn sweep(&self, to_sweep: Vec<(BlockId, CellId)>, gc_gen: u32) -> Result<usize> {
        if to_sweep.len() < 1 {
            return Ok(0);
        }

        let transaction = self.dynamic_boc_db.begin_transaction()?;
        for (block_id, cell_id) in to_sweep {
            let root = self.dynamic_boc_db.load_cell(&cell_id)?;
            self.sweep_cells_recursive(&transaction, root, gc_gen)?;
            self.shardstate_db.delete(&block_id)?;
        }
        let delete_count = transaction.len();
        transaction.commit()?;

        Ok(delete_count)
    }

    fn sweep_cells_recursive(
        &self,
        transaction: &Box<dyn KvcTransaction<CellId>>,
        root: Arc<StorageCell>,
        gc_gen: u32
    ) -> Result<()> {
        if root.gc_gen().load(Ordering::SeqCst) >= gc_gen {
            return Ok(());
        }

        for i in 0..root.references_count() {
            self.sweep_cells_recursive(transaction, root.reference(i)?, gc_gen)?;
        }

        if root.gc_gen().load(Ordering::SeqCst) < gc_gen {
            transaction.delete(&CellId::new(root.repr_hash()))?;
        }

        Ok(())
    }
}
