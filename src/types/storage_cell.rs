use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};

use ton_types::{Cell, CellData, CellImpl, CellType, LevelMask, MAX_LEVEL, Result};
use ton_types::types::{ByteOrderRead, UInt256};

use crate::{
    dynamic_boc_db::DynamicBocDb, types::{CellId, Reference}
};

#[derive(Debug)]
pub struct StorageCell {
    cell_data: CellData,
    references: Mutex<Vec<Reference>>,
    boc_db: Arc<DynamicBocDb>,
    gc_gen: AtomicU32,
}

/// Represents Cell for storing in persistent storage
impl StorageCell {
    /// Constructs StorageCell with given parameters
    pub fn with_params(
        cell_data: CellData,
        references: Vec<Reference>,
        boc_db: Arc<DynamicBocDb>,
        gc_gen: u32,
    ) -> Self {
        Self {
            cell_data,
            references: Mutex::new(references),
            boc_db,
            gc_gen: AtomicU32::new(gc_gen),
        }
    }

    /// Constructs StorageCell from Cell. All references are created in NeedToLoad state.
    pub fn from_single_cell(cell: Cell, boc_db: Arc<DynamicBocDb>, gc_gen: u32,) -> Result<Self> {
        let mut references = Vec::with_capacity(cell.references_count());
        for i in 0..cell.references_count() {
            references.push(
                Reference::NeedToLoad(
                    cell.reference(i)?.repr_hash()
                )
            )
        }

        Ok(Self::with_params(cell.cell_data().clone(), references, boc_db, gc_gen))
    }

    /// Gets cell's id
    pub fn id(&self) -> CellId {
        CellId::new(self.repr_hash())
    }

    /// Gets representation hash
    pub fn repr_hash(&self) -> UInt256 {
        self.hash(MAX_LEVEL as usize)
    }

    /// Gets cell's garbage collection generation
    pub(crate) fn gc_gen(&self) -> &AtomicU32 {
        &self.gc_gen
    }

    pub(crate) fn reference(&self, index: usize) -> Result<Arc<StorageCell>> {
        let hash = match &self.references.lock().unwrap()[index] {
            Reference::Loaded(cell) => return Ok(Arc::clone(cell)),
            Reference::NeedToLoad(hash) => hash.clone()
        };

        let cell_id = CellId::from(hash.clone());
        let storage_cell = self.boc_db.load_cell(&cell_id)?;
        self.references.lock().unwrap()[index] = Reference::Loaded(Arc::clone(&storage_cell));

        Ok(storage_cell)
    }

    pub(crate) fn mark_gc_gen(self: &Arc<Self>, gc_gen: u32) -> Result<()> {
        if self.gc_gen().load(Ordering::SeqCst) >= gc_gen {
            return Ok(());
        }

        self.gc_gen().store(gc_gen, Ordering::SeqCst);

        for i in 0..self.references_count() {
            if let Some(Reference::Loaded(ref cell)) = self.references.lock().unwrap().get(i) {
                cell.mark_gc_gen(gc_gen)?;
            }
        }

        Ok(())
    }
    
    /// Binary serialization of cell data
    pub fn serialize<T: Write>(&self, writer: &mut T) -> std::io::Result<()> {
        assert!(self.references.lock().unwrap().len() < 5);

        self.cell_data.serialize(writer)?;
        writer.write(&[self.references.lock().unwrap().len() as u8])?;

        for reference in &*self.references.lock().unwrap() {
            writer.write(reference.hash().as_slice())?;
        }
        Ok(())
    }

    /// Binary deserialization of cell data
    pub fn deserialize<T: Read>(reader: &mut T, boc_db: Arc<DynamicBocDb>) -> std::io::Result<Self> {
        let cell_data = CellData::deserialize(reader)?;
        let reference_count = reader.read_byte()?;
        let mut references = Vec::with_capacity(reference_count as usize);
        for _ in 0..reference_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(Reference::NeedToLoad(hash));
        }

        Ok(Self::with_params(cell_data, references, boc_db, 0))
    }
}

impl CellImpl for StorageCell {
    fn data(&self) -> &[u8] {
        self.cell_data.data()
    }

    fn cell_data(&self) -> &CellData {
        &self.cell_data
    }

    fn bit_length(&self) -> usize {
        self.cell_data.bit_length() as usize
    }

    fn references_count(&self) -> usize {
        self.references.lock().unwrap().len()
    }

    fn reference(&self, index: usize) -> Result<Cell> {
        Ok(Cell::with_cell_impl_arc(self.reference(index)?))
    }

    fn cell_type(&self) -> CellType {
        self.cell_data.cell_type()
    }

    fn level_mask(&self) -> LevelMask {
        self.cell_data.level_mask()
    }

    fn hash(&self, index: usize) -> UInt256 {
        self.cell_data.hash(index)
    }

    fn depth(&self, index: usize) -> u16 {
        self.cell_data.depth(index)
    }

    fn store_hashes(&self) -> bool {
        self.cell_data.store_hashes()
    }
}

fn references_hashes_equal(left: &Vec<Reference>, right: &Vec<Reference>) -> bool {
    for i in 0..left.len() {
        if left[i].hash() != right[i].hash() {
            return false;
        }
    }
    true
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        self.boc_db.cells_map().lock().unwrap().remove(&self.id());
    }
}

impl PartialEq for StorageCell {
    fn eq(&self, other: &Self) -> bool {
        self.cell_data == other.cell_data
            && self.references.lock().unwrap().len() == other.references.lock().unwrap().len()
        && references_hashes_equal(&self.references.lock().unwrap(), &other.references.lock().unwrap())
    }
}
