use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::archives::package::Package;
use crate::archives::package_id::PackageId;

#[derive(Debug)]
pub struct PackageInfo {
    package_id: PackageId,
    package: Arc<RwLock<Package>>,
    path: PathBuf,
    idx: u32,
    version: u32,
}

impl PackageInfo {
    pub const fn with_data(package_id: PackageId, package: Arc<RwLock<Package>>, path: PathBuf, idx: u32, version: u32) -> Self {
        Self { package_id, package, path, idx, version }
    }

    pub const fn package_id(&self) -> &PackageId {
        &self.package_id
    }

    pub const fn package(&self) -> &Arc<RwLock<Package>> {
        &self.package
    }

    pub const fn path(&self) -> &PathBuf {
        &self.path
    }

    pub const fn idx(&self) -> u32 {
        self.idx
    }

    pub const fn version(&self) -> u32 {
        self.version
    }
}
