//! Manifest file handling for persistence following C++ implementation
//!
//! The manifest format matches the C++ version:
//! - Header: checksum(8B) | root(4B) | ttl_root(4B) | log_size(4B)
//! - Log entries: varint encoded page mappings

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use bytes::{Bytes, BytesMut};

use crate::types::{PageId, FilePageId, TableIdent, MAX_PAGE_ID, MAX_FILE_PAGE_ID};
use crate::page::MappingSnapshot;
use crate::index::CowRootMeta;
use crate::codec::encoding::{encode_varint, decode_varint};
use crate::Result;
use crate::error::Error;

/// Manifest header size (following C++ ManifestBuilder)
/// checksum(8B) | root(4B) | ttl_root(4B) | log_size(4B)
const HEADER_SIZE: usize = 20;
const OFFSET_CHECKSUM: usize = 0;
const OFFSET_ROOT: usize = 8;
const OFFSET_TTL_ROOT: usize = 12;
const OFFSET_LOG_SIZE: usize = 16;

/// Invalid value marker for deleted mappings
const INVALID_VALUE: u64 = u64::MAX;

/// Manifest builder for creating manifest entries (following C++ ManifestBuilder)
pub struct ManifestBuilder {
    /// Internal buffer
    buffer: BytesMut,
}

impl ManifestBuilder {
    /// Create a new manifest builder
    pub fn new() -> Self {
        let mut buffer = BytesMut::with_capacity(1024);
        buffer.resize(HEADER_SIZE, 0);
        Self { buffer }
    }

    /// Update a page mapping (following C++ UpdateMapping)
    pub fn update_mapping(&mut self, page_id: PageId, file_page_id: FilePageId) {
        let page_bytes = encode_varint(page_id as u64);
        self.buffer.extend_from_slice(&page_bytes);

        // Encode file page ID as varint (C++ uses EncodeFilePageId)
        let fp_bytes = encode_varint(encode_file_page_id(file_page_id));
        self.buffer.extend_from_slice(&fp_bytes);
    }

    /// Delete a page mapping (following C++ DeleteMapping)
    pub fn delete_mapping(&mut self, page_id: PageId) {
        let page_bytes = encode_varint(page_id as u64);
        self.buffer.extend_from_slice(&page_bytes);

        let invalid_bytes = encode_varint(INVALID_VALUE);
        self.buffer.extend_from_slice(&invalid_bytes);
    }

    /// Create a snapshot of the entire mapping state (following C++ Snapshot)
    pub fn snapshot(
        &mut self,
        root_id: PageId,
        ttl_root_id: PageId,
        mapping: &MappingSnapshot,
        max_fp_id: FilePageId,
    ) -> Bytes {
        self.reset();

        // Write max file page ID
        let max_bytes = encode_varint(max_fp_id.raw());
        self.buffer.extend_from_slice(&max_bytes);

        // Serialize the mapping
        self.serialize_mapping(mapping);

        self.finalize(root_id, ttl_root_id)
    }

    /// Serialize mapping snapshot
    fn serialize_mapping(&mut self, mapping: &MappingSnapshot) {
        // Write all mappings
        for page_id in 0..mapping.max_page_id() {
            if let Ok(file_page_id) = mapping.to_file_page(page_id) {
                if file_page_id != MAX_FILE_PAGE_ID {
                    self.update_mapping(page_id, file_page_id);
                }
            }
        }
    }

    /// Finalize the manifest entry (following C++ Finalize)
    pub fn finalize(&mut self, root_id: PageId, ttl_root_id: PageId) -> Bytes {
        // Write header fields
        let mut header = [0u8; HEADER_SIZE];
        header[OFFSET_ROOT..OFFSET_ROOT + 4].copy_from_slice(&root_id.to_le_bytes());
        header[OFFSET_TTL_ROOT..OFFSET_TTL_ROOT + 4].copy_from_slice(&ttl_root_id.to_le_bytes());

        let log_size = (self.buffer.len() - HEADER_SIZE) as u32;
        header[OFFSET_LOG_SIZE..OFFSET_LOG_SIZE + 4].copy_from_slice(&log_size.to_le_bytes());

        // Copy header to buffer
        self.buffer[..HEADER_SIZE].copy_from_slice(&header);

        // Calculate and set checksum
        self.set_checksum();

        self.buffer.clone().freeze()
    }

    /// Reset the builder (following C++ Reset)
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.buffer.resize(HEADER_SIZE, 0);
    }

    /// Check if builder is empty (following C++ Empty)
    pub fn is_empty(&self) -> bool {
        self.buffer.len() <= HEADER_SIZE
    }

    /// Get current size (following C++ CurrentSize)
    pub fn current_size(&self) -> usize {
        self.buffer.len()
    }

    /// Get buffer view (following C++ BuffView)
    pub fn buffer_view(&self) -> &[u8] {
        &self.buffer
    }

    /// Build the manifest and return data with checksum
    pub fn build(&mut self) -> (Vec<u8>, u64) {
        // Set log size in header before calculating checksum
        let log_size = (self.buffer.len() - HEADER_SIZE) as u32;
        self.buffer[OFFSET_LOG_SIZE..OFFSET_LOG_SIZE + 4].copy_from_slice(&log_size.to_le_bytes());

        self.set_checksum();

        // Extract checksum
        let checksum_bytes = &self.buffer[OFFSET_CHECKSUM..OFFSET_CHECKSUM + 8];
        let checksum = u64::from_le_bytes(checksum_bytes.try_into().unwrap());

        (self.buffer.to_vec(), checksum)
    }

    /// Calculate and set checksum (following C++ SetChecksum)
    fn set_checksum(&mut self) {
        // Zero out checksum field
        for i in OFFSET_CHECKSUM..OFFSET_CHECKSUM + 8 {
            self.buffer[i] = 0;
        }

        // Calculate CRC32C over everything except checksum
        let checksum = crc32c::crc32c(&self.buffer[OFFSET_CHECKSUM + 8..]) as u64;
        self.buffer[OFFSET_CHECKSUM..OFFSET_CHECKSUM + 8].copy_from_slice(&checksum.to_le_bytes());
    }
}

/// Encode file page ID (following C++ MappingSnapshot::EncodeFilePageId)
fn encode_file_page_id(fp_id: FilePageId) -> u64 {
    // Set high bit to indicate this is a file page ID
    fp_id.raw() | (1u64 << 63)
}

/// Decode file page ID
fn decode_file_page_id(val: u64) -> Option<FilePageId> {
    if val == INVALID_VALUE {
        None
    } else if val & (1u64 << 63) != 0 {
        // Clear the high bit to get the actual file page ID
        Some(FilePageId::from_raw(val & !(1u64 << 63)))
    } else {
        None
    }
}

/// Check if value is a file page ID
fn is_file_page_id(val: u64) -> bool {
    val != INVALID_VALUE && (val & (1u64 << 63)) != 0
}

/// Manifest data structure (what gets saved/loaded)
pub struct ManifestData {
    /// Version
    pub version: u32,
    /// Timestamp
    pub timestamp: u64,
    /// Page mappings (logical -> physical)
    pub mappings: HashMap<PageId, FilePageId>,
    /// Root metadata for each table
    pub roots: HashMap<TableIdent, CowRootMeta>,
}

impl ManifestData {
    /// Create a new empty manifest
    pub fn new() -> Self {
        Self {
            version: 1,
            timestamp: 0,
            mappings: HashMap::new(),
            roots: HashMap::new(),
        }
    }
}

/// Manifest file for reading/writing manifest data
pub struct ManifestFile {
    /// File path
    path: PathBuf,
    /// Current content
    content: Vec<u8>,
    /// Read position
    read_pos: usize,
}

impl ManifestFile {
    /// Create a new manifest file
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            content: Vec::new(),
            read_pos: 0,
        }
    }

    /// Load manifest data from file
    pub async fn load_from_file(path: impl AsRef<Path>) -> Result<ManifestData> {
        let content = tokio::fs::read(path.as_ref()).await?;
        Self::deserialize_manifest(&content)
    }

    /// Save manifest data to file
    pub async fn save_to_file(manifest: &ManifestData, path: impl AsRef<Path>) -> Result<()> {
        let data = Self::serialize_manifest(manifest)?;

        // Write to temporary file first
        let tmp_path = path.as_ref().with_extension("tmp");
        tokio::fs::write(&tmp_path, &data).await?;

        // Rename atomically
        tokio::fs::rename(&tmp_path, path.as_ref()).await?;
        Ok(())
    }

    /// Serialize manifest to bytes
    fn serialize_manifest(manifest: &ManifestData) -> Result<Vec<u8>> {
        let mut builder = ManifestBuilder::new();

        // Add all mappings
        for (page_id, file_page_id) in &manifest.mappings {
            builder.update_mapping(*page_id, *file_page_id);
        }

        // Get root ID from roots if available
        let root_id = manifest.roots.values()
            .next()
            .map(|meta| meta.root_id)
            .unwrap_or(MAX_PAGE_ID);

        // Set root IDs in header
        let ttl_root_id = MAX_PAGE_ID; // TTL not implemented yet
        builder.buffer[OFFSET_ROOT..OFFSET_ROOT + 4].copy_from_slice(&root_id.to_le_bytes());
        builder.buffer[OFFSET_TTL_ROOT..OFFSET_TTL_ROOT + 4].copy_from_slice(&ttl_root_id.to_le_bytes());

        // Build and return
        let (data, _checksum) = builder.build();
        Ok(data)
    }

    /// Deserialize manifest from bytes
    fn deserialize_manifest(data: &[u8]) -> Result<ManifestData> {
        let mut replayer = ManifestReplayer::new();
        replayer.replay_buffer(data)?;

        let mut manifest = ManifestData::new();
        manifest.mappings = replayer.mappings;

        // If we have a root ID, create a root entry
        if replayer.root_id != MAX_PAGE_ID {
            // Create a default table ident for now
            let table_id = TableIdent::new("test_table", 0);
            let root_meta = CowRootMeta {
                root_id: replayer.root_id,
                ttl_root_id: replayer.ttl_root_id,
                mapper: Some(Box::new(crate::page::PageMapper::new())),
                manifest_size: 0,
                old_mapping: None,
                next_expire_ts: u64::MAX,
            };
            manifest.roots.insert(table_id, root_meta);
        }

        Ok(manifest)
    }

    /// Create from content (for testing and replaying)
    pub fn from_content(content: Vec<u8>) -> Self {
        Self {
            path: PathBuf::new(),
            content,
            read_pos: 0,
        }
    }

    /// Load manifest from disk
    pub async fn load(&mut self) -> Result<()> {
        match tokio::fs::read(&self.path).await {
            Ok(content) => {
                self.content = content;
                self.read_pos = 0;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File doesn't exist yet, that's ok
                self.content.clear();
                self.read_pos = 0;
                Ok(())
            }
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// Save manifest to disk (atomic write)
    pub async fn save(&self, data: &[u8]) -> Result<()> {
        // Write to temporary file first
        let tmp_path = self.path.with_extension("tmp");
        tokio::fs::write(&tmp_path, data).await?;

        // Sync to ensure durability
        let file = tokio::fs::File::open(&tmp_path).await?;
        file.sync_all().await?;

        // Atomic rename
        tokio::fs::rename(tmp_path, &self.path).await?;

        Ok(())
    }

    /// Append to manifest
    pub async fn append(&mut self, data: &[u8]) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        file.write_all(data).await?;
        file.sync_all().await?;

        // Update in-memory content
        self.content.extend_from_slice(data);

        Ok(())
    }

    /// Switch manifest (atomic replacement)
    pub async fn switch_manifest(&self, snapshot_data: &[u8]) -> Result<()> {
        self.save(snapshot_data).await
    }

    /// Read bytes from manifest (following C++ ManifestFile::Read)
    pub fn read(&mut self, dst: &mut [u8]) -> Result<usize> {
        let available = self.content.len() - self.read_pos;
        let to_read = dst.len().min(available);

        if to_read == 0 {
            return Ok(0);
        }

        dst[..to_read].copy_from_slice(&self.content[self.read_pos..self.read_pos + to_read]);
        self.read_pos += to_read;

        Ok(to_read)
    }

    /// Skip bytes (following C++ Manifest::Skip)
    pub fn skip(&mut self, n: usize) {
        self.read_pos = (self.read_pos + n).min(self.content.len());
    }

    /// Check if at end of file
    pub fn is_eof(&self) -> bool {
        self.read_pos >= self.content.len()
    }
}

/// Manifest replayer for loading manifest data (following C++ Replayer)
pub struct ManifestReplayer {
    /// Log buffer for reading records
    log_buf: Vec<u8>,
    /// Root page ID
    pub root_id: PageId,
    /// TTL root page ID
    pub ttl_root_id: PageId,
    /// Maximum file page ID
    pub max_file_page_id: FilePageId,
    /// Mapping table (raw values like C++)
    pub mapping_table: Vec<u64>,
    /// File size processed
    pub file_size: usize,
    /// Page mappings (reconstructed from log)
    pub mappings: HashMap<PageId, FilePageId>,
    /// Root metadata for each table
    pub roots: HashMap<TableIdent, CowRootMeta>,
}

impl ManifestReplayer {
    /// Create a new replayer (following C++ Replayer constructor)
    pub fn new() -> Self {
        let mut log_buf = Vec::with_capacity(1024);
        log_buf.resize(HEADER_SIZE, 0);

        Self {
            log_buf,
            root_id: MAX_PAGE_ID,
            ttl_root_id: MAX_PAGE_ID,
            max_file_page_id: FilePageId::from_raw(0),
            mapping_table: Vec::new(),
            file_size: 0,
            mappings: HashMap::new(),
            roots: HashMap::new(),
        }
    }

    /// Replay a manifest buffer
    pub fn replay_buffer(&mut self, buffer: &[u8]) -> Result<()> {
        // Parse the manifest data directly from buffer
        if buffer.len() < HEADER_SIZE {
            return Err(Error::InvalidArgument("Manifest too small".into()));
        }

        // Read header
        self.root_id = u32::from_le_bytes(buffer[OFFSET_ROOT..OFFSET_ROOT + 4].try_into().unwrap());
        self.ttl_root_id = u32::from_le_bytes(buffer[OFFSET_TTL_ROOT..OFFSET_TTL_ROOT + 4].try_into().unwrap());

        let log_size = u32::from_le_bytes(buffer[OFFSET_LOG_SIZE..OFFSET_LOG_SIZE + 4].try_into().unwrap()) as usize;

        // Process log entries
        let mut pos = HEADER_SIZE;
        while pos < HEADER_SIZE + log_size && pos < buffer.len() {
            // Decode page ID
            let (page_id, bytes_read) = decode_varint(&buffer[pos..])
                .ok_or_else(|| Error::InvalidArgument("Failed to decode page ID".into()))?;
            pos += bytes_read;

            // Decode file page ID or invalid marker
            let (value, bytes_read) = decode_varint(&buffer[pos..])
                .ok_or_else(|| Error::InvalidArgument("Failed to decode value".into()))?;
            pos += bytes_read;

            if value != INVALID_VALUE {
                // Decode the file page ID
                if let Some(file_page_id) = decode_file_page_id(value) {
                    self.mappings.insert(page_id as PageId, file_page_id);
                }
            }
        }

        Ok(())
    }

    /// Replay a manifest file (following C++ Replay)
    pub fn replay(&mut self, manifest: &mut ManifestFile) -> Result<()> {
        self.reset();

        while !manifest.is_eof() {
            match self.parse_next_record(manifest) {
                Ok(_) => {},
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// Parse the next record from manifest (following C++ ParseNextRecord)
    fn parse_next_record(&mut self, manifest: &mut ManifestFile) -> Result<()> {
        // Read header
        self.log_buf.resize(HEADER_SIZE, 0);
        let read = manifest.read(&mut self.log_buf)?;
        if read < HEADER_SIZE {
            return Err(Error::Eof);
        }

        let log_size = u32::from_le_bytes(
            self.log_buf[OFFSET_LOG_SIZE..OFFSET_LOG_SIZE + 4].try_into().unwrap()
        ) as usize;

        // Resize buffer for full record
        self.log_buf.resize(HEADER_SIZE + log_size, 0);

        // Read log data
        let read = manifest.read(&mut self.log_buf[HEADER_SIZE..])?;
        if read < log_size {
            return Err(Error::Eof);
        }

        // Verify checksum
        let stored_checksum = u64::from_le_bytes(
            self.log_buf[OFFSET_CHECKSUM..OFFSET_CHECKSUM + 8].try_into().unwrap()
        );

        // Zero out checksum field for calculation
        let mut check_buf = self.log_buf.clone();
        for i in OFFSET_CHECKSUM..OFFSET_CHECKSUM + 8 {
            check_buf[i] = 0;
        }

        let calculated_checksum = crc32c::crc32c(&check_buf[OFFSET_CHECKSUM + 8..]) as u64;
        if stored_checksum != calculated_checksum {
            tracing::error!("Manifest file corrupted, checksum mismatch");
            return Err(Error::Corruption("Manifest checksum mismatch".into()));
        }

        // Parse header
        self.root_id = u32::from_le_bytes(
            self.log_buf[OFFSET_ROOT..OFFSET_ROOT + 4].try_into().unwrap()
        );
        self.ttl_root_id = u32::from_le_bytes(
            self.log_buf[OFFSET_TTL_ROOT..OFFSET_TTL_ROOT + 4].try_into().unwrap()
        );

        // Parse log entries (following C++ Replayer::ReplayLog)
        let log_data = self.log_buf[HEADER_SIZE..HEADER_SIZE + log_size].to_vec();
        self.replay_log(&log_data)?;

        self.file_size += HEADER_SIZE + log_size;

        Ok(())
    }

    /// Replay log entries (following C++ Replayer::ReplayLog)
    fn replay_log(&mut self, data: &[u8]) -> Result<()> {
        let mut pos = 0;

        // First entry might be max_file_page_id for snapshots
        if pos < data.len() {
            if let Some((val, n)) = decode_varint(&data[pos..]) {
                if !is_file_page_id(val) && val != INVALID_VALUE {
                    // This is max_file_page_id
                    self.max_file_page_id = FilePageId::from_raw(val);
                    pos += n;
                }
            }
        }

        // Parse mappings
        while pos < data.len() {
            let page_id = match decode_varint(&data[pos..]) {
                Some((val, n)) => {
                    pos += n;
                    val as u32
                }
                None => break,
            };

            let file_page_val = match decode_varint(&data[pos..]) {
                Some((val, n)) => {
                    pos += n;
                    val
                }
                None => break,
            };

            // Ensure mapping table is large enough
            if page_id as usize >= self.mapping_table.len() {
                self.mapping_table.resize(page_id as usize + 1, 0);
            }

            // Store the raw value (like C++ does)
            self.mapping_table[page_id as usize] = file_page_val;
        }

        Ok(())
    }

    /// Reset the replayer (following C++ Reset)
    pub fn reset(&mut self) {
        self.root_id = MAX_PAGE_ID;
        self.ttl_root_id = MAX_PAGE_ID;
        self.max_file_page_id = FilePageId::from_raw(0);
        self.mapping_table.clear();
        self.file_size = 0;
    }
}

/// Create an archive of the manifest
pub async fn create_archive(
    table_id: &TableIdent,
    base_path: &Path,
    snapshot_data: &[u8],
    timestamp: u64,
) -> Result<()> {
    let archive_dir = base_path.join(format!("table_{}", table_id.table_name));
    tokio::fs::create_dir_all(&archive_dir).await?;

    let archive_path = archive_dir.join(format!("manifest_{}", timestamp));

    let mut file = tokio::fs::File::create(&archive_path).await?;
    use tokio::io::AsyncWriteExt;
    file.write_all(snapshot_data).await?;
    file.sync_all().await?;

    tracing::info!("Created archive: {:?}", archive_path);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_builder() {
        let mut builder = ManifestBuilder::new();

        // Add some mappings
        builder.update_mapping(1, FilePageId::from_raw(100));
        builder.update_mapping(2, FilePageId::from_raw(200));
        builder.delete_mapping(1);

        // Finalize
        let data = builder.finalize(10, 20);

        // Check header
        assert_eq!(u32::from_le_bytes(data[OFFSET_ROOT..OFFSET_ROOT + 4].try_into().unwrap()), 10);
        assert_eq!(u32::from_le_bytes(data[OFFSET_TTL_ROOT..OFFSET_TTL_ROOT + 4].try_into().unwrap()), 20);
    }

    #[test]
    fn test_manifest_replay() {
        let mut builder = ManifestBuilder::new();
        builder.update_mapping(1, FilePageId::from_raw(100));
        builder.update_mapping(2, FilePageId::from_raw(200));
        let data = builder.finalize(10, 20);

        let mut manifest = ManifestFile::from_content(data.to_vec());

        let mut replayer = ManifestReplayer::new();
        replayer.replay(&mut manifest).unwrap();

        assert_eq!(replayer.root_id, 10);
        assert_eq!(replayer.ttl_root_id, 20);
        assert!(replayer.mapping_table.len() >= 2);
    }

    #[tokio::test]
    async fn test_manifest_file_save_load() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("manifest");

        let mut manifest = ManifestFile::new(&manifest_path);

        // Create some data
        let mut builder = ManifestBuilder::new();
        builder.update_mapping(1, FilePageId::from_raw(100));
        let data = builder.finalize(5, 6);

        // Save and load
        manifest.save(&data).await.unwrap();
        manifest.load().await.unwrap();

        // Replay
        let mut replayer = ManifestReplayer::new();
        replayer.replay(&mut manifest).unwrap();

        assert_eq!(replayer.root_id, 5);
        assert_eq!(replayer.ttl_root_id, 6);
    }
}