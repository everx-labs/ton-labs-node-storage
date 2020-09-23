use std::io::SeekFrom;
use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use ton_types::{fail, Result};

use crate::archives::package_entry::{PackageEntry, PKG_ENTRY_HEADER_SIZE};
use futures::Future;
use std::sync::Arc;


#[derive(Debug)]
pub struct Package {
    file: File,
    size: u64,
}

pub(crate) const PKG_HEADER_SIZE: usize = 4;
const PKG_HEADER_MAGIC: u32 = 0xAE8F_DD01;

impl Package {
    pub async fn open(path: impl AsRef<Path>, read_only: bool, create: bool) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(!read_only || create)
            .create(create)
            .open(&path).await?;
        let mut size = file.metadata().await?.len();

        file.seek(SeekFrom::Start(0)).await?;
        if size < PKG_HEADER_SIZE as u64 {
            if !create {
                fail!("Package file is too short")
            }
            file.write(&PKG_HEADER_MAGIC.to_le_bytes()).await?;
            size = PKG_HEADER_SIZE as u64;
        } else {
            let mut buf = [0; PKG_HEADER_SIZE];
            if file.read(&mut buf).await? != PKG_HEADER_SIZE {
                fail!("Package file read failed")
            }
            if u32::from_le_bytes(buf) != PKG_HEADER_MAGIC {
                fail!("Package file header mismatch")
            }
        }

        Ok(Self { file, size })
    }

    pub fn size(&self) -> u64 {
        self.size - PKG_HEADER_SIZE as u64
    }

    pub async fn truncate(&mut self, size: u64) -> Result<()> {
        let new_size = PKG_HEADER_SIZE as u64 + size;
        log::debug!(target: "storage", "Truncating package, new size: {} bytes", new_size);
        self.size = new_size;
        Ok(self.file.set_len(new_size).await?)
    }

    pub async fn read_entry(&mut self, offset: u64) -> Result<PackageEntry> {
        if self.size() <= offset + PKG_ENTRY_HEADER_SIZE as u64 {
            fail!("Unexpected end of file while reading archives entry with offset: {}", offset)
        }
        self.file.seek(SeekFrom::Start(PKG_HEADER_SIZE as u64 + offset)).await?;

        PackageEntry::read_from_file(&mut self.file).await
    }

    pub async fn append_entry(&mut self, entry: &PackageEntry) -> Result<u64> {
        assert!(entry.filename().as_bytes().len() <= u16::max_value() as usize);
        assert!(entry.data().len() <= u32::max_value() as usize);

        let entry_offset = self.size();

        self.file.seek(SeekFrom::End(0)).await?;
        self.size += entry.write_to_file(&mut self.file).await?;

        Ok(entry_offset)
    }

    pub async fn for_each<F, P>(&mut self, mut predicate: impl FnMut(PackageEntry, Arc<P>) -> F, payload: Arc<P>) -> Result<bool>
    where
        F: Future<Output = Result<bool>>
    {
        self.file.seek(SeekFrom::Start(PKG_HEADER_SIZE as u64)).await?;
        let mut remaining = self.size();
        while remaining > 0 {
            let entry = PackageEntry::read_from_file(&mut self.file).await?;
            remaining -= (PKG_ENTRY_HEADER_SIZE + entry.filename().as_bytes().len() + entry.data().len()) as u64;
            if !predicate(entry, Arc::clone(&payload)).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
