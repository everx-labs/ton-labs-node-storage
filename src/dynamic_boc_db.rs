use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicU32, Ordering};

use fnv::FnvHashMap;

use ton_types::{Cell, Result};

use crate::cell_db::CellDb;
use crate::types::{CellId, Reference, StorageCell};
use crate::db::traits::KvcTransaction;

#[derive(Debug)]
pub struct DynamicBocDb {
    db: Arc<CellDb>,
    cells: Arc<Mutex<FnvHashMap<CellId, Weak<StorageCell>>>>,
    gc_gen: AtomicU32,
}

impl DynamicBocDb {
    /// Constructs new instance using in-memory key-value collection
    pub fn in_memory() -> Self {
        Self::with_db(CellDb::in_memory())
    }

    /// Constructs new instance using RocksDB with given path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Self {
        Self::with_db(CellDb::with_path(path))
    }

    /// Constructs new instance using given key-value collection implementation
    pub(crate) fn with_db(db: CellDb) -> Self {
        Self {
            db: Arc::new(db),
            cells: Arc::new(Mutex::new(FnvHashMap::default())),
            gc_gen: AtomicU32::new(0),
        }
    }

    pub fn cells_map(&self) -> Arc<Mutex<FnvHashMap<CellId, Weak<StorageCell>>>> {
        Arc::clone(&self.cells)
    }

    /// Converts tree of cells into DynamicBoc
    pub fn save_as_dynamic_boc(self: &Arc<Self>, root_cell: Cell) -> Result<usize> {
        let transaction = self.db.begin_transaction()?;

        self.save_tree_of_cells_recursive(
            root_cell.clone(),
            Arc::clone(&self.db),
            &transaction)?;

        let written_count = transaction.len();
        transaction.commit()?;

        Ok(written_count)
    }

    /// Gets root cell from key-value storage
    pub fn load_dynamic_boc(self: &Arc<Self>, root_cell_id: &CellId) -> Result<Cell> {
        let storage_cell = self.load_cell(root_cell_id)?;

        storage_cell.gc_gen().compare_and_swap(0, self.gc_generation(), Ordering::SeqCst);

        Ok(Cell::with_cell_impl_arc(storage_cell))
    }

    pub(crate) fn load_cell(self: &Arc<Self>, cell_id: &CellId) -> Result<Arc<StorageCell>> {
        if let Some(cell) = self.cells.lock().unwrap().get(&cell_id) {
            if let Some(ref cell) = Weak::upgrade(&cell) {
                return Ok(Arc::clone(cell));
            }
            // Even if the cell is disposed, we will load and store it later,
            // so we don't need to remove garbage here.
        }
        let storage_cell = Arc::new(
            CellDb::get_cell(&*self.db, &cell_id, Arc::clone(self))?
        );
        self.cells.lock().unwrap()
            .insert(cell_id.clone(), Arc::downgrade(&storage_cell));

        Ok(storage_cell)
    }

    fn gc_generation(&self) -> u32 {
        self.gc_gen.load(Ordering::SeqCst)
    }

    pub(crate) fn new_gc_generation(&self) -> u32 {
        let result = self.gc_gen.fetch_add(1, Ordering::SeqCst) + 1;

        // Fail, if overflowed:
        assert_ne!(result, 0);

        result
    }

    fn save_tree_of_cells_recursive(
        self: &Arc<Self>,
        cell: Cell,
        cell_db: Arc<CellDb>,
        transaction: &Box<dyn KvcTransaction<CellId>>
    ) -> Result<()> {
        let cell_id = CellId::new(cell.repr_hash());
        if cell_db.contains(&cell_id)? {
            return Ok(());
        }

        let mut references = Vec::with_capacity(cell.references_count());
        for i in 0..cell.references_count() {
            references.push(Reference::NeedToLoad(cell.reference(i)?.repr_hash()));
        }

        CellDb::put_cell(transaction.as_ref(), &cell_id, cell.clone())?;

        for i in 0..cell.references_count() {
            self.save_tree_of_cells_recursive(
                cell.reference(i)?,
                Arc::clone(&cell_db),
                transaction
            )?;
        }

        Ok(())
    }
}

impl Deref for DynamicBocDb {
    type Target = Arc<CellDb>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DerefMut for DynamicBocDb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}