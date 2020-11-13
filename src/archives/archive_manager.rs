use std::borrow::Borrow;
use std::hash::Hash;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use ton_api::ton::PublicKey;
use ton_block::BlockIdExt;
use ton_types::{error, Result, UInt256};

use crate::archives::{get_mc_seq_no, is_data_inited, is_key_block, is_moved_to_archive, is_proof_inited, is_prooflink_inited};
use crate::archives::archive_slice::ArchiveSlice;
use crate::archives::file_maps::{FileDescription, FileMaps};
use crate::archives::package_entry_id::{GetFileNameShort, PackageEntryId};
use crate::archives::package_id::PackageId;
use crate::types::BlockMeta;


pub const ARCHIVE_SIZE: usize = 20_000;
pub const KEY_ARCHIVE_SIZE: usize = 200_000;
pub const SLICE_SIZE: u32 = 100;

pub struct ArchiveManager {
    db_root_path: Arc<PathBuf>,
    unapplied_dir: Arc<PathBuf>,
    file_maps: FileMaps,
}

impl ArchiveManager {
    pub async fn with_data(
        db_root_path: Arc<PathBuf>,
    ) -> Result<Self> {
        let file_maps = FileMaps::new(&db_root_path).await?;
        let unapplied_dir = Arc::new(db_root_path.join("archive").join("unapplied"));
        tokio::fs::create_dir_all(&*unapplied_dir).await?;

        Ok(Self {
            db_root_path,
            unapplied_dir,
            file_maps,
        })
    }

    pub const fn db_root_path(&self) -> &Arc<PathBuf> {
        &self.db_root_path
    }

    pub const fn unapplied_dir(&self) -> &Arc<PathBuf> {
        &self.unapplied_dir
    }

    pub async fn add_file<B, U256, PK>(&self, entry_id: &PackageEntryId<B, U256, PK>, data: Vec<u8>) -> Result<()>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
    {
        log::debug!(target: "storage", "Saving unapplied file: {}", entry_id);

        let filename = self.unapplied_dir.join(entry_id.filename_short());
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&filename).await?;
        file.write_all(&data).await?;
        file.flush().await?;

        Ok(())
    }

    pub async fn get_file<B, U256, PK>(
        &self,
        block_id: &BlockIdExt,
        block_meta: &BlockMeta,
        entry_id: &PackageEntryId<B, U256, PK>
    ) -> Result<Vec<u8>>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
    {
        block_meta.temp_lock().read().await;

        if is_moved_to_archive(block_meta) {
            let package_id = self.get_package_id(get_mc_seq_no(block_id, block_meta)).await?;
            if let Some(ref fd) = self.get_file_desc(package_id, false).await? {
                return Ok(fd.archive_slice()
                    .get_file(Some((block_id, block_meta)), entry_id).await?
                    .take_data());
            }
        }

        self.read_temp_file(entry_id).await
            .map(|(_filename, data)| data)
    }

    pub async fn move_to_archive(
        &self,
        block_id: &BlockIdExt,
        block_meta: &BlockMeta,
        mut on_success: impl FnMut() -> Result<()>,
    ) -> Result<()> {
        if block_meta.start_moving_to_archive() {
            return Ok(());
        }

        let proof_inited = is_proof_inited(block_meta);
        let prooflink_inited = is_prooflink_inited(block_meta);
        let data_inited = is_data_inited(block_meta);

        if !data_inited || !(proof_inited || prooflink_inited) {
            log::error!(
                target: "storage",
                "Block {} is not moved to archive: data are not stored (data = {}, proof = {}, prooflink = {})",
                block_id,
                data_inited,
                proof_inited,
                prooflink_inited
            );
        }

        let proof_filename = if proof_inited {
            Some(self.move_file_to_archive(block_id, block_meta, &PackageEntryId::<&BlockIdExt, &UInt256, &PublicKey>::Proof(block_id)).await?)
        } else if prooflink_inited {
            Some(self.move_file_to_archive(block_id, block_meta, &PackageEntryId::<&BlockIdExt, &UInt256, &PublicKey>::ProofLink(block_id)).await?)
        } else {
            None
        };
        let block_filename = if data_inited {
            Some(self.move_file_to_archive(block_id, block_meta, &PackageEntryId::<&BlockIdExt, &UInt256, &PublicKey>::Block(block_id)).await?)
        } else {
            None
        };

        on_success()?;

        {
            block_meta.temp_lock().write().await;
            if let Some(filename) = proof_filename {
                tokio::fs::remove_file(filename).await?;
            }
            if let Some(filename) = block_filename {
                tokio::fs::remove_file(filename).await?;
            }
        }

        Ok(())
    }

    pub async fn get_archive_id(&self, mc_seq_no: u32) -> Result<Option<u64>> {
        if let Some(fd) = self.get_file_desc(PackageId::for_block(mc_seq_no), false).await? {
            fd.archive_slice().get_archive_id(mc_seq_no).await
                .map(|id| Some(id))
        } else {
            Ok(None)
        }
    }

    pub async fn get_archive_slice(&self, archive_id: u64, offset: u64, limit: u32) -> Result<Vec<u8>> {
        let fd = self.get_file_desc(PackageId::for_block(archive_id as u32), false).await?
            .ok_or_else(|| error!("Archive not found"))?;

        fd.archive_slice().get_slice(archive_id, offset, limit).await
    }

    async fn move_file_to_archive<B, U256, PK>(&self, block_id: &BlockIdExt, block_meta: &BlockMeta, entry_id: &PackageEntryId<B, U256, PK>) -> Result<PathBuf>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
    {
        log::debug!(target: "storage", "Moving entry to archive: {}", entry_id.filename_short());
        let (filename, data) = {
            block_meta.temp_lock().read().await;
            self.read_temp_file(entry_id).await?
        };

        // TODO: Copy proofs and prooflinks into a corresponding keyblocks archive?

        let mc_seq_no = get_mc_seq_no(block_id, block_meta);

        let is_key = is_key_block(block_meta);
        let package_id = self.get_package_id_force(mc_seq_no, is_key).await;
        log::debug!(target: "storage", "PackageId for ({},{},{}) (mc_seq_no = {}, key block = {:?}) is {:?}, path: {:?}",
            block_id.shard().workchain_id(),
            block_id.shard().shard_prefix_as_str_with_tag(),
            block_id.seq_no(),
            mc_seq_no,
            is_key,
            package_id,
            package_id.full_path(self.db_root_path.as_ref(), "pack"),
        );

        let fd = self.get_file_desc(package_id,true).await?
            .ok_or_else(|| error!("Expected some value"))?;

        fd.archive_slice().add_file(Some((block_id, block_meta)), entry_id, data).await?;

        Ok(filename)
    }

    async fn read_temp_file<B, U256, PK>(&self, entry_id: &PackageEntryId<B, U256, PK>) -> Result<(PathBuf, Vec<u8>)>
    where
        B: Borrow<BlockIdExt> + Hash,
        U256: Borrow<UInt256> + Hash,
        PK: Borrow<PublicKey> + Hash
    {
        let temp_filename = self.unapplied_dir.join(entry_id.filename_short());
        let data = tokio::fs::read(&temp_filename).await
            .map_err(|error| {
                if error.kind() == ErrorKind::NotFound {
                    error!("File not found in archive: {:?}, {}", temp_filename, error)
                } else {
                    error!("Error reading file: {:?}, {}", temp_filename, error)
                }
            })?;

        Ok((temp_filename, data))
    }

    async fn get_file_desc(&self, id: PackageId, force: bool) -> Result<Option<Arc<FileDescription>>> {
        // TODO: Rewrite logics in order to handle multithreaded adding of packages
        if let Some(fd) = self.file_maps.get(id.package_type()).get(id.id()).await {
            if fd.deleted() {
                return Ok(None);
            }

            return Ok(Some(fd));
        }

        if force {
            Ok(Some(self.add_file_desc(id).await?))
        } else {
            Ok(None)
        }
    }

    async fn add_file_desc(&self, id: PackageId) -> Result<Arc<FileDescription>> {
        // TODO: Rewrite logics in order to handle multithreaded adding of packages
        let file_map = self.file_maps.get(id.package_type());
        assert!(file_map.get(id.id()).await.is_none());

        let dir = self.db_root_path.join(id.path());
        tokio::fs::create_dir_all(&dir).await?;

        let archive_slice = Arc::new(
            ArchiveSlice::with_data(
                Arc::clone(&self.db_root_path),
                id.id(),
                id.package_type(),
                false,
            ).await?
        );

        let fd = Arc::new(FileDescription::with_data(
            id.clone(),
            archive_slice,
            false
        ));

        file_map.put(id.id(), Arc::clone(&fd)).await?;

        Ok(fd)
    }

    async fn get_package_id(&self, seq_no: u32) -> Result<PackageId> {
        Ok(self.file_maps.files().get_closest(seq_no).await
            .ok_or_else(|| {
                log::error!(target: "storage", "Package not found for seq_no: {}", seq_no);
                error!("Package not found for seq_no: {}", seq_no)
            })?
            .id()
            .clone())
    }

    async fn get_package_id_force(&self, mc_seq_no: u32, is_key: bool) -> PackageId {
        if is_key {
            PackageId::for_block(mc_seq_no)
        } else {
            let mut package_id = PackageId::for_block(mc_seq_no - (mc_seq_no % ARCHIVE_SIZE as u32));
            if let Some(fd) = self.file_maps.files().get_closest(mc_seq_no).await {
                let found_package_id = fd.id();
                if package_id < *found_package_id {
                    package_id = found_package_id.clone();
                }
            }
            package_id
        }
    }
}
