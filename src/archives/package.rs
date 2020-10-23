use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use ton_types::{error, fail, Result};

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

async fn read_header<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<()> {
    let mut buf = [0; PKG_HEADER_SIZE];
    if reader.read_exact(&mut buf).await? != PKG_HEADER_SIZE {
        fail!("Package file read failed")
    }
    if u32::from_le_bytes(buf) != PKG_HEADER_MAGIC {
        fail!("Package file header mismatch")
    }

    Ok(())
}

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
            read_header(&mut file).await?;
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

        PackageEntry::read_from(&mut file).await?
            .ok_or_else(|| error!("Package::read_entry: Unexpected end of file"))
    }

    pub async fn append_entry(
        &self,
        entry: &PackageEntry,
        after_append: impl FnOnce(u64, u64) -> Result<()>
    ) -> Result<()> {
        assert!(entry.filename().as_bytes().len() <= u16::max_value() as usize);
        assert!(entry.data().len() <= u32::max_value() as usize);

        let mut file = self.open_file().await?;
        {
            let _write_guard = self.write_mutex.lock().await;
            file.seek(SeekFrom::End(0)).await?;
            let entry_offset = self.size();
            let entry_size = entry.write_to(&mut file).await?;
            self.size.fetch_add(entry_size, Ordering::SeqCst);

            after_append(entry_offset, entry_offset + entry_size)
        }
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

pub struct PackageReader<R: AsyncReadExt + Unpin> {
    reader: BufReader<R>,
}

impl<R: AsyncReadExt + Unpin> PackageReader<R> {
    pub async fn next(&mut self) -> Result<Option<PackageEntry>> {
        PackageEntry::read_from(&mut self.reader).await
    }
}

pub async fn read_package_from_file(path: impl AsRef<Path>) -> Result<PackageReader<File>> {
    read_package_from(
        OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path).await?
    ).await
}

pub async fn read_package_from<R: AsyncReadExt + Unpin>(reader: R) -> Result<PackageReader<R>> {
    let mut reader = BufReader::with_capacity(1 << 19, reader);
    read_header(&mut reader).await?;

    Ok(PackageReader::<R> { reader })
}
