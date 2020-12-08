use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, RwLock, Weak, atomic::{AtomicU64, Ordering}};
use std::time::SystemTime;
use std::cmp::max;

use fnv::FnvHashMap;

use ton_types::{Cell, Result};

use crate::cell_db::CellDb;
use crate::dynamic_boc_diff_writer::{DynamicBocDiffFactory, DynamicBocDiffWriter};
use crate::types::{CellId, StorageCell};

#[derive(Debug)]
pub struct DynamicBocDb {
    db: Arc<CellDb>,
    cells: Arc<RwLock<FnvHashMap<CellId, Weak<StorageCell>>>>,
    diff_factory: DynamicBocDiffFactory,

    total_gets: AtomicU64,
    total_success_gets: AtomicU64,
    total_db_gets: AtomicU64,
    total_get_read_lock_time: AtomicU64,
    total_get_write_lock_time: AtomicU64,
    total_get_time: AtomicU64,
    total_db_get_time: AtomicU64,

    max_get_time: AtomicU64,
    min_get_time: AtomicU64,
    max_db_get_time: AtomicU64,
    min_db_get_time: AtomicU64,
    max_get_read_lock_time: AtomicU64,
    min_get_read_lock_time: AtomicU64,
    max_get_write_lock_time: AtomicU64,
    min_get_write_lock_time: AtomicU64,
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
        let db = Arc::new(db);
        Self {
            db: Arc::clone(&db),
            cells: Arc::new(RwLock::new(FnvHashMap::default())),
            diff_factory: DynamicBocDiffFactory::new(db),

            total_gets: AtomicU64::new(0),
            total_success_gets: AtomicU64::new(0),
            total_db_gets: AtomicU64::new(0),
            total_get_read_lock_time: AtomicU64::new(0),
            total_get_write_lock_time: AtomicU64::new(0),
            total_get_time: AtomicU64::new(0),
            total_db_get_time: AtomicU64::new(0),

            max_get_time: AtomicU64::new(0),
            min_get_time: AtomicU64::new(u64::MAX),
            max_db_get_time: AtomicU64::new(0),
            min_db_get_time: AtomicU64::new(u64::MAX),
            max_get_read_lock_time: AtomicU64::new(0),
            min_get_read_lock_time: AtomicU64::new(u64::MAX),
            max_get_write_lock_time: AtomicU64::new(0),
            min_get_write_lock_time: AtomicU64::new(u64::MAX),
        }
    }

    pub fn cell_db(&self) -> &Arc<CellDb> {
        &self.db
    }

    pub fn cells_map(&self) -> Arc<RwLock<FnvHashMap<CellId, Weak<StorageCell>>>> {
        Arc::clone(&self.cells)
    }

    /// Converts tree of cells into DynamicBoc
    pub fn save_as_dynamic_boc(self: &Arc<Self>, root_cell: Cell) -> Result<usize> {
        let diff_writer = self.diff_factory.construct();

        let written_count = self.save_tree_of_cells_recursive(
            root_cell.clone(),
            Arc::clone(&self.db),
            &diff_writer)?;

        diff_writer.apply()?;

        Ok(written_count)
    }

    /// Gets root cell from key-value storage
    pub fn load_dynamic_boc(self: &Arc<Self>, root_cell_id: &CellId) -> Result<Cell> {
        let storage_cell = self.load_cell(root_cell_id)?;

        Ok(Cell::with_cell_impl_arc(storage_cell))
    }

    pub(crate) fn diff_factory(&self) -> &DynamicBocDiffFactory {
        &self.diff_factory
    }

    pub(crate) fn load_cell(self: &Arc<Self>, cell_id: &CellId) -> Result<Arc<StorageCell>> {

        if SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs() % 10 == 0 {

            let total_gets = self.total_gets.load(Ordering::Relaxed);
            let total_get_time  = self.total_get_time.load(Ordering::Relaxed);
            let total_get_read_lock_time = self.total_get_read_lock_time.load(Ordering::Relaxed);
            let total_get_write_lock_time = self.total_get_write_lock_time.load(Ordering::Relaxed);

            log::warn!("DYNAMIC BOC STAT (micros)
                        total_gets: {}\n\
                total_success_gets: {}\n\
                     total_db_gets: {}\n\
          total_get_read_lock_time: {}\n\
         total_get_write_lock_time: {}\n\
                    total_get_time: {}\n\
                 total_db_get_time: {}\n\
                      max_get_time: {}\n\
                      min_get_time: {}\n\
                   max_db_get_time: {}\n\
                   min_db_get_time: {}\n\
            max_get_read_lock_time: {}\n\
            min_get_read_lock_time: {}\n\
           max_get_write_lock_time: {}\n\
           min_get_write_lock_time: {}\n\
                      AVG get time: {}\n\
                AVG read lock time: {}\n\
               AVG write lock time: {}",
                total_gets,
                self.total_success_gets.load(Ordering::Relaxed),
                self.total_db_gets.load(Ordering::Relaxed),
                total_get_read_lock_time,
                total_get_write_lock_time,
                total_get_time,
                self.total_db_get_time.load(Ordering::Relaxed),
                self.max_get_time.load(Ordering::Relaxed),
                self.min_get_time.load(Ordering::Relaxed),
                self.max_db_get_time.load(Ordering::Relaxed),
                self.min_db_get_time.load(Ordering::Relaxed),
                self.max_get_read_lock_time.load(Ordering::Relaxed),
                self.min_get_read_lock_time.load(Ordering::Relaxed),
                self.max_get_write_lock_time.load(Ordering::Relaxed),
                self.min_get_write_lock_time.load(Ordering::Relaxed),
                total_get_time / max(1, total_gets),
                total_get_read_lock_time / max(1, total_gets),
                total_get_write_lock_time / max(1, total_gets),
            );
        }

        self.total_gets.fetch_add(1, Ordering::Relaxed);

        let get_srart = std::time::Instant::now();

        // check cache
        {
            let read_lock_start = std::time::Instant::now();
            let guard = self.cells.read().expect("Poisoned RwLock");
            let read_lock_time = read_lock_start.elapsed().as_micros() as u64;

            self.total_get_read_lock_time.fetch_add(read_lock_time, Ordering::Relaxed);
            self.max_get_read_lock_time.fetch_max(read_lock_time, Ordering::Relaxed);
            self.min_get_read_lock_time.fetch_min(read_lock_time, Ordering::Relaxed);
            if read_lock_time > 500 {
                log::warn!("load_cell {} SLOW read_lock_time = {}micros", cell_id, read_lock_time);
            }

            if let Some(cell) = guard.get(&cell_id)
            {
                if let Some(ref cell) = Weak::upgrade(&cell) {
                    self.total_success_gets.fetch_add(1, Ordering::Relaxed);

                    let get_time = get_srart.elapsed().as_micros() as u64;

                    self.total_get_time.fetch_add(get_time, Ordering::Relaxed);
                    self.max_get_time.fetch_max(get_time, Ordering::Relaxed);
                    self.min_get_time.fetch_min(get_time, Ordering::Relaxed);

                    if get_time > 500 {
                        log::warn!("load_cell {} SLOW get_time (cache) = {}micros", cell_id, get_time);
                    }

                    return Ok(Arc::clone(cell));
                }
                // Even if the cell is disposed, we will load and store it later,
                // so we don't need to remove garbage here.
            }
        }

        // read DB

        self.total_db_gets.fetch_add(1, Ordering::Relaxed);
        let get_db_srart = std::time::Instant::now();

        let storage_cell = Arc::new(CellDb::get_cell(&*self.db, &cell_id, Arc::clone(self))?);

        let read_db_time = get_db_srart.elapsed().as_micros() as u64;
        self.total_db_get_time.fetch_add(read_db_time, Ordering::Relaxed);
        self.max_db_get_time.fetch_max(read_db_time, Ordering::Relaxed);
        self.min_db_get_time.fetch_min(read_db_time, Ordering::Relaxed);
        if read_db_time > 500 {
            log::warn!("load_cell {} SLOW read_db_time = {}micros", cell_id, read_db_time);
        }

        // update cache

        let write_lock_start = std::time::Instant::now();
        
        let mut guard = self.cells.write().expect("Poisoned RwLock");
        
        let write_lock_time = write_lock_start.elapsed().as_micros() as u64;
        self.total_get_write_lock_time.fetch_add(write_lock_time, Ordering::Relaxed);
        self.max_get_write_lock_time.fetch_max(write_lock_time, Ordering::Relaxed);
        self.min_get_write_lock_time.fetch_min(write_lock_time, Ordering::Relaxed);
        if write_lock_time > 500 {
            log::warn!("load_cell {} SLOW write_lock_time = {}micros", cell_id, write_lock_time);
        }

        guard.insert(cell_id.clone(), Arc::downgrade(&storage_cell));


        self.total_success_gets.fetch_add(1, Ordering::Relaxed);

        let get_time = get_srart.elapsed().as_micros() as u64;

        self.total_get_time.fetch_add(get_time, Ordering::Relaxed);
        self.max_get_time.fetch_max(get_time, Ordering::Relaxed);
        self.min_get_time.fetch_min(get_time, Ordering::Relaxed);

        if get_time > 500 {
            log::warn!("load_cell {} SLOW get_time (DB) = {}micros", cell_id, get_time);
        }
        
        Ok(storage_cell)
        
        
        /*if let Some(cell) = self.cells.read()
            .expect("Poisoned RwLock")
            .get(&cell_id)
        {
            if let Some(ref cell) = Weak::upgrade(&cell) {
                return Ok(Arc::clone(cell));
            }
            // Even if the cell is disposed, we will load and store it later,
            // so we don't need to remove garbage here.
        }
        let storage_cell = Arc::new(
            CellDb::get_cell(&*self.db, &cell_id, Arc::clone(self))?
        );
        self.cells.write()
            .expect("Poisoned RwLock")
            .insert(cell_id.clone(), Arc::downgrade(&storage_cell));

        Ok(storage_cell)*/
    }

    fn save_tree_of_cells_recursive(
        self: &Arc<Self>,
        cell: Cell,
        cell_db: Arc<CellDb>,
        diff_writer: &DynamicBocDiffWriter
    ) -> Result<usize> {
        let cell_id = CellId::new(cell.repr_hash());
        if cell_db.contains(&cell_id)? {
            return Ok(0);
        }

        diff_writer.add_cell(cell_id, cell.clone());

        let mut count = 1;
        for i in 0..cell.references_count() {
            count += self.save_tree_of_cells_recursive(
                cell.reference(i)?,
                Arc::clone(&cell_db),
                diff_writer
            )?;
        }

        Ok(count)
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
