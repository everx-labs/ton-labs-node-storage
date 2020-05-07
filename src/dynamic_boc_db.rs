use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, Weak};
use std::sync::atomic::{AtomicU32, Ordering};

use fnv::FnvHashMap;

use ton_types::{Cell, CellImpl, Result};

use crate::cell_db::CellDb;
use crate::types::{CellId, Reference, StorageCell};

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
    pub fn with_path(path: &str) -> Self {
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
    pub fn save_as_dynamic_boc(self: &Arc<Self>, root_cell: Cell) -> Result<(Cell, usize)> {
        let mut added = Vec::new();
        let gc_gen = self.gc_gen.load(Ordering::SeqCst);
        let storage_cell = match self.load_tree_of_cells_recursive(
            root_cell.clone(),
            Arc::clone(&self.cells),
            Arc::clone(&self.db),
            gc_gen,
            &mut added)?
        {
            Reference::Loaded(storage_cell) => storage_cell,
            Reference::NeedToLoad(_) => Arc::new(
                StorageCell::from_single_cell(
                    root_cell,
                    Arc::clone(self),
                    gc_gen
                )?
            ),
        };

        let transaction = self.db.begin_transaction()?;
        for cell in added {
            CellDb::put_cell(&*transaction, &cell.id(), &*cell)?;
        }
        let written_count = transaction.len();
        transaction.commit()?;

        Ok((Cell::with_cell_impl_arc(storage_cell as Arc<dyn CellImpl>), written_count))
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

    fn load_tree_of_cells_recursive(
        self: &Arc<Self>,
        cell: Cell,
        cells: Arc<Mutex<FnvHashMap<CellId, Weak<StorageCell>>>>,
        cell_db: Arc<CellDb>,
        gc_gen: u32,
        added: &mut Vec<Arc<StorageCell>>
    ) -> Result<Reference> {
        let cell_id = CellId::new(cell.repr_hash());
        let cell_opt = cells.lock().unwrap()
            .get(&cell_id)
            .and_then(|cell| {
                Weak::upgrade(&cell)
            });

        if let Some(cell) = cell_opt {
            cell.mark_gc_gen(self.gc_generation())?;
            return Ok(Reference::Loaded(cell));
        }

        if cell_db.contains(&cell_id)? {
            return Ok(Reference::NeedToLoad(cell_id.into()));
        }

        let mut references = Vec::with_capacity(cell.references_count());
        for i in 0..cell.references_count() {
            references.push(
                self.load_tree_of_cells_recursive(
                    cell.reference(i)?,
                    Arc::clone(&cells),
                    Arc::clone(&cell_db),
                    gc_gen,
                    added
                )?
            );
        }

        let storage_cell = Arc::new(
            StorageCell::with_params(
                cell.cell_data().clone(),
                references,
                Arc::clone(self),
                gc_gen
            )
        );

        cells.lock().unwrap()
            .insert(cell_id.clone(), Arc::downgrade(&storage_cell));

        added.push(Arc::clone(&storage_cell));

        Ok(Reference::Loaded(storage_cell))
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