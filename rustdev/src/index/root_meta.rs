//! Root metadata for B-tree index management
//! Following the C++ root_meta.h implementation

use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::oneshot;

use crate::types::{PageId, FilePageId, MAX_PAGE_ID};
use crate::page::PageMapper;
use crate::page::MappingSnapshot;
use crate::Result;

/// Calculate CRC32 checksum
fn crc32(data: &[u8]) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() as u64
}

/// COW (Copy-on-Write) root metadata for write transactions
#[derive(Debug, Clone)]
pub struct CowRootMeta {
    /// Root page ID of the index tree
    pub root_id: PageId,
    /// TTL root page ID for expiration management
    pub ttl_root_id: PageId,
    /// Page mapper for this COW transaction
    pub mapper: Option<Box<PageMapper>>,
    /// Manifest size
    pub manifest_size: u64,
    /// Old mapping snapshot to replace
    pub old_mapping: Option<Arc<MappingSnapshot>>,
    /// Next expiration timestamp
    pub next_expire_ts: u64,
}

impl Default for CowRootMeta {
    fn default() -> Self {
        Self {
            root_id: MAX_PAGE_ID,
            ttl_root_id: MAX_PAGE_ID,
            mapper: None,
            manifest_size: 0,
            old_mapping: None,
            next_expire_ts: 0,
        }
    }
}

/// Waiting zone for coordinating access to locked resources
#[derive(Debug, Default)]
pub struct WaitingZone {
    waiters: Mutex<Vec<oneshot::Sender<()>>>,
}

impl WaitingZone {
    /// Add a waiter
    pub fn wait(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().unwrap().push(tx);
        rx
    }

    /// Wake up all waiters
    pub fn wake_all(&self) {
        let mut waiters = self.waiters.lock().unwrap();
        for waiter in waiters.drain(..) {
            let _ = waiter.send(());
        }
    }
}

/// Root metadata for a table's index tree
#[derive(Debug)]
pub struct RootMeta {
    /// Root page ID of the index tree
    pub root_id: PageId,
    /// TTL root page ID for expiration management
    pub ttl_root_id: PageId,
    /// Page mapper for logical to physical page mapping
    pub mapper: Option<Box<PageMapper>>,
    /// Active mapping snapshots
    pub mapping_snapshots: RwLock<HashSet<*const MappingSnapshot>>,
    /// Manifest size
    pub manifest_size: u64,
    /// Next expiration timestamp
    pub next_expire_ts: u64,
    /// Reference count for pinning
    pub ref_cnt: Mutex<u32>,
    /// Whether this root is locked for exclusive access
    pub locked: Mutex<bool>,
    /// Waiting zone for blocked operations
    pub waiting: WaitingZone,
}

impl Default for RootMeta {
    fn default() -> Self {
        Self {
            root_id: MAX_PAGE_ID,
            ttl_root_id: MAX_PAGE_ID,
            mapper: None,
            mapping_snapshots: RwLock::new(HashSet::new()),
            manifest_size: 0,
            next_expire_ts: 0,
            ref_cnt: Mutex::new(0),
            locked: Mutex::new(false),
            waiting: WaitingZone::default(),
        }
    }
}

impl RootMeta {
    /// Create new root metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin the root metadata (increment reference count)
    pub fn pin(&self) {
        let mut ref_cnt = self.ref_cnt.lock().unwrap();
        *ref_cnt += 1;
    }

    /// Unpin the root metadata (decrement reference count)
    pub fn unpin(&self) {
        let mut ref_cnt = self.ref_cnt.lock().unwrap();
        if *ref_cnt > 0 {
            *ref_cnt -= 1;
        }
    }

    /// Check if the root is pinned
    pub fn is_pinned(&self) -> bool {
        *self.ref_cnt.lock().unwrap() > 0
    }

    /// Lock the root for exclusive access
    pub async fn lock(&self) -> Result<()> {
        loop {
            {
                let mut locked = self.locked.lock().unwrap();
                if !*locked {
                    *locked = true;
                    return Ok(());
                }
            }
            // Wait for unlock
            let rx = self.waiting.wait();
            let _ = rx.await;
        }
    }

    /// Unlock the root
    pub fn unlock(&self) {
        {
            let mut locked = self.locked.lock().unwrap();
            *locked = false;
        }
        self.waiting.wake_all();
    }

    /// Add a mapping snapshot
    pub fn add_mapping_snapshot(&self, snapshot: *const MappingSnapshot) {
        let mut snapshots = self.mapping_snapshots.write().unwrap();
        snapshots.insert(snapshot);
    }

    /// Remove a mapping snapshot
    pub fn remove_mapping_snapshot(&self, snapshot: *const MappingSnapshot) {
        let mut snapshots = self.mapping_snapshots.write().unwrap();
        snapshots.remove(&snapshot);
    }

    /// Check if there are active mapping snapshots
    pub fn has_active_snapshots(&self) -> bool {
        !self.mapping_snapshots.read().unwrap().is_empty()
    }
}

/// Manifest builder for creating manifest entries
/// Following the C++ ManifestBuilder
#[derive(Debug, Default)]
pub struct ManifestBuilder {
    buffer: Vec<u8>,
}

impl ManifestBuilder {
    /// Header size: checksum(8B) + root(4B) + ttl_root(4B) + log_size(4B)
    pub const HEADER_SIZE: usize = 8 + 4 + 4 + 4;
    pub const OFFSET_ROOT: usize = 8;
    pub const OFFSET_TTL_ROOT: usize = 12;
    pub const OFFSET_LEN: usize = 16;

    /// Create a new manifest builder
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(4096),
        }
    }

    /// Update a page mapping
    pub fn update_mapping(&mut self, page_id: PageId, file_page_id: FilePageId) {
        // Encode as: operation(1B) + page_id(4B) + file_page_id(4B)
        self.buffer.push(1); // UPDATE operation
        self.buffer.extend_from_slice(&page_id.to_le_bytes());
        self.buffer.extend_from_slice(&file_page_id.to_le_bytes());
    }

    /// Delete a page mapping
    pub fn delete_mapping(&mut self, page_id: PageId) {
        // Encode as: operation(1B) + page_id(4B)
        self.buffer.push(2); // DELETE operation
        self.buffer.extend_from_slice(&page_id.to_le_bytes());
    }

    /// Create a snapshot entry
    pub fn snapshot(
        &mut self,
        root_id: PageId,
        ttl_root: PageId,
        mapping: &MappingSnapshot,
        max_fp_id: FilePageId,
    ) -> &[u8] {
        self.reset();

        // Write header placeholder
        self.buffer.resize(Self::HEADER_SIZE, 0);

        // Write root IDs
        self.buffer[Self::OFFSET_ROOT..Self::OFFSET_ROOT + 4]
            .copy_from_slice(&root_id.to_le_bytes());
        self.buffer[Self::OFFSET_TTL_ROOT..Self::OFFSET_TTL_ROOT + 4]
            .copy_from_slice(&ttl_root.to_le_bytes());

        // Serialize mapping snapshot
        // TODO: Implement mapping serialization following C++ format

        // Update length
        let log_size = (self.buffer.len() - Self::HEADER_SIZE) as u32;
        self.buffer[Self::OFFSET_LEN..Self::OFFSET_LEN + 4]
            .copy_from_slice(&log_size.to_le_bytes());

        // Calculate and write checksum
        let checksum = crc32(&self.buffer[8..]);
        self.buffer[0..8].copy_from_slice(&checksum.to_le_bytes());

        &self.buffer
    }

    /// Finalize the manifest with new root
    pub fn finalize(&mut self, new_root: PageId, ttl_root: PageId) -> &[u8] {
        // Write final root IDs at the beginning
        if self.buffer.len() < Self::HEADER_SIZE {
            self.buffer.resize(Self::HEADER_SIZE, 0);
        }

        self.buffer[Self::OFFSET_ROOT..Self::OFFSET_ROOT + 4]
            .copy_from_slice(&new_root.to_le_bytes());
        self.buffer[Self::OFFSET_TTL_ROOT..Self::OFFSET_TTL_ROOT + 4]
            .copy_from_slice(&ttl_root.to_le_bytes());

        let log_size = (self.buffer.len() - Self::HEADER_SIZE) as u32;
        self.buffer[Self::OFFSET_LEN..Self::OFFSET_LEN + 4]
            .copy_from_slice(&log_size.to_le_bytes());

        // Calculate checksum
        let checksum = crc32(&self.buffer[8..]);
        self.buffer[0..8].copy_from_slice(&checksum.to_le_bytes());

        &self.buffer
    }

    /// Get buffer view
    pub fn buffer_view(&self) -> &[u8] {
        &self.buffer
    }

    /// Reset the builder
    pub fn reset(&mut self) {
        self.buffer.clear();
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.buffer.len() <= Self::HEADER_SIZE
    }

    /// Get current size
    pub fn current_size(&self) -> usize {
        self.buffer.len()
    }
}