#[macro_export]
macro_rules! base_impl {
    ($type: ident, $trait: ident, $key_type: ty) => {
        #[derive(Debug)]
        pub struct $type {
            db: Box<dyn crate::db::traits::$trait<$key_type>>,
        }

        impl $type{
            /// Constructs new instance using in-memory key-value collection
            pub fn in_memory() -> Self {
                Self {
                    db: Box::new(crate::db::memorydb::MemoryDb::new())
                }
            }

            /// Constructs new instance using RocksDB with given path
            pub fn with_path(path: &str) -> Self {
                Self {
                    db: Box::new(crate::db::rocksdb::RocksDb::with_path(path))
                }
            }
        }

        impl std::ops::Deref for $type {
            type Target = dyn $trait<$key_type>;

            fn deref(&self) -> &Self::Target {
                self.db.deref()
            }
        }

        impl std::ops::DerefMut for $type {
            fn deref_mut(&mut self) -> &mut Self::Target {
                self.db.deref_mut()
            }
        }
    }
}