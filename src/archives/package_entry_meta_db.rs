use crate::archives::package_entry_meta::PackageEntryMeta;
use crate::db::traits::{KvcWriteable, U32Key};
use crate::db_impl_cbor;

db_impl_cbor!(PackageEntryMetaDb, KvcWriteable, U32Key, PackageEntryMeta);
