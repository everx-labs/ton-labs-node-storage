use std::fmt::Debug;
use std::sync::Arc;

pub use db_key::*;
use ton_types::Result;

use crate::types::DbSlice;

mod db_key;

/// Trait for key-value collections
pub trait Kvc: Debug + Send + Sync {
    /// Element count of collection
    fn len(&self) -> Result<usize>;

    /// Returns true, if collection is empty; false otherwise
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Destroys this key-value collection and underlying database
    fn destroy(&mut self) -> Result<()>;
}

/// Trait for readable key-value collections
pub trait KvcReadable<K: DbKey>: Kvc {
    /// Gets value from collection by the key
    fn get(&self, key: &K) -> Result<DbSlice>;

    /// Determines, is key exists in key-value collection
    fn contains(&self, key: &K) -> Result<bool>;

    /// Iterates over items in key-value collection, running predicate for each key-value pair
    fn for_each(&self, predicate: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>) -> Result<bool>;
}

/// Trait for writable key-value collections
pub trait KvcWriteable<K: DbKey>: KvcReadable<K> {
    /// Puts value into collection by the key
    fn put(&self, key: &K, value: &[u8]) -> Result<()>;

    /// Deletes value from collection by the key
    fn delete(&self, key: &K) -> Result<()>;
}

/// Trait for key-value collections with the ability of take snapshots
pub trait KvcSnapshotable<K: DbKey>: KvcWriteable<K> {
    /// Takes snapshot from key-value collection
    fn snapshot<'db>(&'db self) -> Result<Arc<dyn KvcReadable<K> + 'db>>;
}

/// Trait for transactional key-value collections
pub trait KvcTransactional<K: DbKey>: KvcSnapshotable<K> {
    /// Creates new transaction (batch)
    fn begin_transaction(&self) -> Result<Box<dyn KvcTransaction<K>>>;
}

/// Trait for transaction on key-value collection. The transaction must be committed before the
/// data actually being written into the collection. The transaction is automatically being aborted
/// on destroy, if not committed.
pub trait KvcTransaction<K: DbKey> {
    /// Adds put operation into transaction (batch)
    fn put(&self, key: &K, value: &[u8]) -> Result<()>;

    /// Adds delete operation into transaction (batch)
    fn delete(&self, key: &K) -> Result<()>;

    /// Removes all pending operations from transaction (batch)
    fn clear(&self) -> Result<()>;

    /// Commits the transaction (batch)
    fn commit(self: Box<Self>) -> Result<()>;

    /// Gets pending operations count
    fn len(&self) -> usize;

    /// Returns true if pending operation count is zero; otherwise false
    fn is_empty(&self) -> bool;
}