use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};

use tokio::io::AsyncReadExt;

use async_trait::async_trait;
use ton_types::{error, fail, Result};

use crate::db::traits::{DbKey, KvcAsync, KvcReadableAsync, KvcWriteableAsync};
use crate::error::StorageError;
use crate::types::DbSlice;

#[derive(Debug)]
pub struct FileDb {
    path: PathBuf,
}

static PATH_CHUNK_MAX_LEN: usize = 4;
static PATH_MAX_DEPTH: usize = 2;

impl FileDb {
    /// Creates new instance with given path
    pub fn with_path<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf()
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub(crate) fn make_path(&self, key: &[u8]) -> PathBuf {
        let mut key_str = hex::encode(key);
        let mut result = self.path.clone();
        let mut depth = 1;
        while depth < PATH_MAX_DEPTH && key_str.len() > 0 {
            let remaining = key_str.split_off(std::cmp::min(key_str.len(), PATH_CHUNK_MAX_LEN));
            result = result.join(key_str);
            key_str = remaining;
            depth += 1;
        }
        if key_str.len() > 0 {
            return result.join(key_str);
        }
        result
    }

    fn transform_io_error(err: std::io::Error, key: &[u8]) -> failure::Error {
        match err.kind() {
            ErrorKind::NotFound => StorageError::KeyNotFound(hex::encode(key)).into(),
            ErrorKind::UnexpectedEof => StorageError::OutOfRange.into(),
            _ => err.into()
        }
    }

    async fn is_dir_empty<P: AsRef<Path>>(path: P) -> bool {
        if let Ok(mut read_dir) = tokio::fs::read_dir(path).await {
            if let Ok(val) = read_dir.next_entry().await {
                return val.is_none();
            }
        }
        false
    }
}

#[async_trait]
impl KvcAsync for FileDb {
    async fn len(&self) -> Result<usize> {
        fail!("len() is not supported for FileDb")
    }

    async fn destroy(&mut self) -> Result<()> {
        match tokio::fs::metadata(&self.path).await {
            Ok(meta) if meta.is_dir() => Ok(tokio::fs::remove_dir_all(&self.path).await?),
            _ => Ok(())
        }
    }
}

#[async_trait]
impl<K: DbKey + Send + Sync> KvcReadableAsync<K> for FileDb {
    async fn get<'a>(&'a self, key: &K) -> Result<DbSlice<'a>> {
        let path = self.make_path(key.key());
        Ok(DbSlice::Vector(tokio::fs::read(path).await
            .map_err(|err| Self::transform_io_error(err, key.key()))?))
    }

    async fn get_slice<'a>(&'a self, key: &K, offset: u64, size: u64) -> Result<DbSlice<'a>> {
        let path = self.make_path(key.key());
        let mut file = tokio::fs::File::open(path).await
            .map_err(|err| Self::transform_io_error(err, key.key()))?;
        file.seek(SeekFrom::Start(offset)).await?;
        let mut result = vec![0u8; size as usize];
        file.read_exact(&mut result).await
            .map_err(|err| Self::transform_io_error(err, key.key()))?;

        Ok(DbSlice::Vector(result))
    }

    async fn get_size(&self, key: &K) -> Result<u64> {
        let path = self.make_path(key.key());
        let metadata = tokio::fs::metadata(path).await
            .map_err(|err| Self::transform_io_error(err, key.key()))?;

        Ok(metadata.len())
    }

    async fn contains(&self, key: &K) -> Result<bool> {
        let path = self.make_path(key.key());
        Ok(path.is_file() && path.exists())
    }
}

#[async_trait]
impl<K: DbKey + Send + Sync> KvcWriteableAsync<K> for FileDb {
    async fn put(&self, key: &K, value: &[u8]) -> Result<()> {
        let path = self.make_path(key.key());
        let dir = path.parent()
            .ok_or_else(|| error!("Unable to get parent path"))?;
        tokio::fs::create_dir_all(dir).await?;
        tokio::fs::write(path, value).await?;

        Ok(())
    }

    async fn delete(&self, key: &K) -> Result<()> {
        let path = self.make_path(key.key());
        if let Err(err) = tokio::fs::remove_file(&path).await {
            if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        // Cleanup upper-level empty directories
        let mut dir = path.as_path();
        loop {
            dir = dir.parent()
                .ok_or_else(|| error!("Unable to get parent path"))?;
            if self.path().starts_with(dir) || !Self::is_dir_empty(&dir).await {
                break;
            } else {
                tokio::fs::remove_dir(&dir).await
                    .unwrap_or_else(|_error| {
                        // If can't remove empty dir, do nothing.
                    });
            }
        }

        Ok(())
    }
}