use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;

use async_trait::async_trait;
use ton_types::Result;

use crate::db::traits::{DbKey, KvcAsync, KvcReadableAsync, KvcWriteable, KvcWriteableAsync};
use crate::types::DbSlice;

/// This facade wraps key-value collections implementing sync traits into async traits
#[derive(Debug)]
pub struct KvcWriteableAsyncAdapter<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> {
    kvc: T,
    phantom: PhantomData<K>,
}

impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> KvcWriteableAsyncAdapter<K, T> {
    pub fn new(kvc: T) -> Self {
        Self { kvc, phantom: PhantomData::default() }
    }

    pub fn kvc(&self) -> &T {
        &self.kvc
    }
}

impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> Deref for KvcWriteableAsyncAdapter<K, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.kvc()
    }
}

#[async_trait]
impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> KvcAsync for KvcWriteableAsyncAdapter<K, T> {
    async fn len(&self) -> Result<usize> {
        self.kvc.len()
    }

    async fn is_empty(&self) -> Result<bool> {
        self.kvc.is_empty()
    }

    async fn destroy(&mut self) -> Result<()> {
        self.kvc.destroy()
    }
}

#[async_trait]
impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> KvcReadableAsync<K> for KvcWriteableAsyncAdapter<K, T> {
    async fn try_get<'a>(&'a self, key: &K) -> Result<Option<DbSlice<'a>>> {
        self.kvc.try_get(key)
    }

    async fn get<'a>(&'a self, key: &K) -> Result<DbSlice<'a>> {
        self.kvc.get(key)
    }

    async fn get_slice<'a>(&'a self, key: &K, offset: u64, size: u64) -> Result<DbSlice<'a>> {
        self.kvc.get_slice(key, offset, size)
    }

    async fn get_size(&self, key: &K) -> Result<u64> {
        self.kvc.get_size(key)
    }

    async fn contains(&self, key: &K) -> Result<bool> {
        self.kvc.contains(key)
    }
}

#[async_trait]
impl<K: DbKey + Debug + Send + Sync, T: KvcWriteable<K>> KvcWriteableAsync<K> for KvcWriteableAsyncAdapter<K, T> {
    async fn put(&self, key: &K, value: &[u8]) -> Result<()> {
        self.kvc.put(key, value)
    }

    async fn delete(&self, key: &K) -> Result<()> {
        self.kvc.delete(key)
    }
}
