#[derive(Debug, failure::Fail)]
pub enum StorageError {
    /// Key not found  
    #[fail(display = "Key not found: {}", 0)]
    KeyNotFound(String),

    /// Reference not loaded
    #[fail(display = "Reference not loaded. Need to load reference.")]
    ReferenceNotLoaded,

    /// Database is dropped
    #[fail(display = "Database is dropped")]
    DbIsDropped,

    /// One or more active transactions exist
    #[fail(display = "Operation is not permitted while one or more active transactions exist")]
    HasActiveTransactions,
}
