use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::Future;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use ton_types::{fail, Result};

use crate::archives::package_entry::{PackageEntry, PKG_ENTRY_HEADER_SIZE};


#[derive(Debug)]
pub struct Package {
    path: Arc<PathBuf>,
    read_only: bool,
    size: AtomicU64,
    write_mutex: Mutex<()>
}

pub(crate) const PKG_HEADER_SIZE: usize = 4;
const PKG_HEADER_MAGIC: u32 = 0xAE8F_DD01;

impl Package {
    pub async fn open(path: Arc<PathBuf>, read_only: bool, create: bool) -> Result<Self> {
        let mut file = Self::open_file_ext(read_only, create, &*path).await?;
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

        Ok(
            Self {
                path,
                read_only, size:
                AtomicU64::new(size),
                write_mutex: Mutex::new(()),
            }
        )
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::SeqCst) - PKG_HEADER_SIZE as u64
    }

    pub const fn path(&self) -> &Arc<PathBuf> {
        &self.path
    }

    pub async fn truncate(&self, size: u64) -> Result<()> {
        let new_size = PKG_HEADER_SIZE as u64 + size;
        log::debug!(target: "storage", "Truncating package, new size: {} bytes", new_size);
        self.size.store(new_size, Ordering::SeqCst);

        {
            let mut file = self.open_file().await?;
            let _write_guard = self.write_mutex.lock().await;
            file.set_len(new_size).await?;
        }

        Ok(())
    }

    pub async fn read_entry(&self, offset: u64) -> Result<PackageEntry> {
        if self.size() <= offset + PKG_ENTRY_HEADER_SIZE as u64 {
            fail!("Unexpected end of file while reading archives entry with offset: {}", offset)
        }

        let mut file = self.open_file().await?;
        file.seek(SeekFrom::Start(PKG_HEADER_SIZE as u64 + offset)).await?;

        PackageEntry::read_from_file(&mut file).await
    }

    pub async fn append_entry(&self, entry: &PackageEntry) -> Result<(u64, u64)> {
        assert!(entry.filename().as_bytes().len() <= u16::max_value() as usize);
        assert!(entry.data().len() <= u32::max_value() as usize);

        let mut file = self.open_file().await?;
        {
            let _write_guard = self.write_mutex.lock().await;
            file.seek(SeekFrom::End(0)).await?;
            let entry_offset = self.size();
            let entry_size = entry.write_to_file(&mut file).await?;
            self.size.fetch_add(entry_size, Ordering::SeqCst);

            Ok((entry_offset, entry_offset + entry_size))
        }
    }

    pub async fn for_each<F, P>(&self, mut predicate: impl FnMut(PackageEntry, Arc<P>) -> F, payload: Arc<P>) -> Result<bool>
    where
        F: Future<Output = Result<bool>>
    {
        let mut file = self.open_file().await?;
        file.seek(SeekFrom::Start(PKG_HEADER_SIZE as u64)).await?;
        let mut remaining = self.size();
        while remaining > 0 {
            let entry = PackageEntry::read_from_file(&mut file).await?;
            remaining -= (PKG_ENTRY_HEADER_SIZE + entry.filename().as_bytes().len() + entry.data().len()) as u64;
            if !predicate(entry, Arc::clone(&payload)).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn open_file_ext(read_only: bool, create: bool, path: impl AsRef<Path>) -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(!read_only || create)
            .create(create)
            .open(&path).await?)
    }

    async fn open_file(&self) -> Result<File> {
        Self::open_file_ext(self.read_only, false, &*self.path).await
    }
}
