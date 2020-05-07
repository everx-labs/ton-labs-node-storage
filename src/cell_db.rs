use std::io::Cursor;
use std::sync::Arc;

use ton_types::Result;

use crate::base_impl;
use crate::db::traits::{KvcTransaction, KvcTransactional};
use crate::dynamic_boc_db::DynamicBocDb;
use crate::types::{CellId, StorageCell};

base_impl!(CellDb, KvcTransactional, CellId);

impl CellDb {
    /// Gets cell from key-value storage by cell id
    pub fn get_cell(&self, cell_id: &CellId, boc_db: Arc<DynamicBocDb>) -> Result<StorageCell> {
        Self::deserialize_cell(self.db.get(&cell_id)?.as_ref(), boc_db)
    }

    /// Puts cell into transaction
    pub fn put_cell<T: KvcTransaction<CellId> + ?Sized>(transaction: &T, cell_id: &CellId, cell: &StorageCell) -> Result<()> {
        transaction.put(cell_id, &Self::serialize_cell(cell)?)
    }

    fn serialize_cell(cell: &StorageCell) -> Result<Vec<u8>> {
        let mut data: Vec<u8> = Vec::new();
        cell.serialize(&mut data)?;
        assert!(data.len() > 0);
        Ok(data)
    }

    fn deserialize_cell(data: &[u8], boc_db: Arc<DynamicBocDb>) -> Result<StorageCell> {
        assert!(data.len() > 0);
        Ok(StorageCell::deserialize(&mut Cursor::new(data), boc_db)?)
    }
}
