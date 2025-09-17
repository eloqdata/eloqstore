//! Index page manager implementation
//! Following the C++ index_page_manager.h/cpp

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::ptr::NonNull;

use crate::config::KvOptions;
use crate::codec::Comparator;
use crate::error::{Error, KvError};
use crate::types::{PageId, TableIdent, MAX_PAGE_ID};
use crate::page::MappingSnapshot;
use crate::storage::AsyncFileManager;
use crate::Result;

use super::index_page::{MemIndexPage, IndexPageIter};
use super::root_meta::{RootMeta, CowRootMeta};

/// Index page manager for managing in-memory index pages
#[derive(Debug)]
pub struct IndexPageManager {
    /// KV options
    options: Arc<KvOptions>,
    /// File manager for async I/O
    io_manager: Arc<AsyncFileManager>,
    /// Root metadata for each table
    roots: RwLock<HashMap<TableIdent, RootMeta>>,
    /// Free list of index pages
    free_list: Mutex<Vec<Box<MemIndexPage>>>,
    /// Active list head (most recently used)
    active_head: Mutex<Option<NonNull<MemIndexPage>>>,
    /// Active list tail (least recently used)
    active_tail: Mutex<Option<NonNull<MemIndexPage>>>,
    /// Total number of pages in memory
    page_count: Mutex<usize>,
    /// Maximum pages in memory
    max_pages: usize,
}

// Safe because we carefully manage the raw pointers
unsafe impl Send for IndexPageManager {}
unsafe impl Sync for IndexPageManager {}

impl IndexPageManager {
    /// Create a new index page manager
    pub fn new(io_manager: Arc<AsyncFileManager>, options: Arc<KvOptions>) -> Self {
        let max_pages = options.max_index_pages();

        Self {
            options,
            io_manager,
            roots: RwLock::new(HashMap::new()),
            free_list: Mutex::new(Vec::new()),
            active_head: Mutex::new(None),
            active_tail: Mutex::new(None),
            page_count: Mutex::new(0),
            max_pages,
        }
    }

    /// Get comparator
    pub fn get_comparator(&self) -> Arc<dyn Comparator> {
        self.options.comparator()
    }

    /// Allocate a new index page from the pool
    pub fn alloc_index_page(&self) -> Box<MemIndexPage> {
        // Try to get from free list first
        {
            let mut free_list = self.free_list.lock().unwrap();
            if let Some(page) = free_list.pop() {
                return page;
            }
        }

        // Allocate new page
        Box::new(MemIndexPage::new(true))
    }

    /// Free an index page back to the pool
    pub fn free_index_page(&self, mut page: Box<MemIndexPage>) {
        // Reset the page
        page.unpin();
        page.deque();

        // Add to free list
        let mut free_list = self.free_list.lock().unwrap();
        free_list.push(page);
    }

    /// Enqueue an index page into the active list for cache replacement
    pub fn enqueue_index_page(&self, page: &mut MemIndexPage) {
        let mut head = self.active_head.lock().unwrap();
        let mut tail = self.active_tail.lock().unwrap();

        let page_ptr = NonNull::new(page as *mut _).unwrap();

        if head.is_none() {
            // First page in the list
            *head = Some(page_ptr);
            *tail = Some(page_ptr);
        } else {
            // Add to head (MRU position)
            unsafe {
                let head_page = head.unwrap().as_mut();
                page.enque_next(head_page);
            }
            *head = Some(page_ptr);
        }

        let mut page_count = self.page_count.lock().unwrap();
        *page_count += 1;
    }

    /// Find root metadata for a table
    /// Returns a simplified view since RootMeta can't be cloned
    pub fn find_root(&self, table_ident: &TableIdent) -> Result<RootMeta> {
        let roots = self.roots.read().unwrap();

        if let Some(root) = roots.get(table_ident) {
            // Create a simplified RootMeta copy
            // Note: This is a workaround - in production we'd need proper Arc wrapping
            let meta = RootMeta {
                root_id: root.root_id,
                ttl_root_id: root.ttl_root_id,
                mapper: None,  // Can't clone the mapper easily
                mapping_snapshots: RwLock::new(Default::default()),
                manifest_size: root.manifest_size,
                next_expire_ts: root.next_expire_ts,
                ref_cnt: Mutex::new(0),
                locked: Mutex::new(false),
                waiting: Default::default(),
            };
            return Ok(meta);
        }

        Err(Error::from(KvError::NotFound))
    }

    /// Create a COW root for write transaction
    pub fn make_cow_root(&self, table_ident: &TableIdent) -> Result<CowRootMeta> {
        let roots = self.roots.read().unwrap();

        if let Some(root) = roots.get(table_ident) {
            // Lock the root for exclusive access
            // Create a COW copy of the mapper
            let cow_meta = CowRootMeta {
                root_id: root.root_id,
                ttl_root_id: root.ttl_root_id,
                mapper: root.mapper.as_ref().map(|m| Box::new((**m).clone())),
                manifest_size: root.manifest_size,
                old_mapping: None,
                next_expire_ts: root.next_expire_ts,
            };

            return Ok(cow_meta);
        }

        // Create new COW root for new table
        Ok(CowRootMeta::default())
    }

    /// Update root metadata after commit
    pub fn update_root(&self, table_ident: &TableIdent, new_meta: CowRootMeta) {
        let mut roots = self.roots.write().unwrap();

        let root = roots.entry(table_ident.clone()).or_insert_with(RootMeta::default);

        root.root_id = new_meta.root_id;
        root.ttl_root_id = new_meta.ttl_root_id;
        root.mapper = new_meta.mapper;
        root.manifest_size = new_meta.manifest_size;
        root.next_expire_ts = new_meta.next_expire_ts;
    }

    /// Export all root metadata for manifest
    pub fn export_roots(&self) -> HashMap<TableIdent, CowRootMeta> {
        let roots = self.roots.read().unwrap();
        roots.iter()
            .map(|(table_id, root)| {
                let cow_meta = CowRootMeta {
                    root_id: root.root_id,
                    ttl_root_id: root.ttl_root_id,
                    mapper: root.mapper.clone(),
                    manifest_size: root.manifest_size,
                    next_expire_ts: root.next_expire_ts,
                    old_mapping: None, // Not needed for checkpoint
                };
                (table_id.clone(), cow_meta)
            })
            .collect()
    }

    /// Restore root metadata from manifest
    pub fn restore_roots(&self, roots_data: HashMap<TableIdent, CowRootMeta>) {
        let mut roots = self.roots.write().unwrap();
        for (table_id, cow_meta) in roots_data {
            let root = roots.entry(table_id).or_insert_with(RootMeta::default);
            root.root_id = cow_meta.root_id;
            root.ttl_root_id = cow_meta.ttl_root_id;
            root.mapper = cow_meta.mapper;
            root.manifest_size = cow_meta.manifest_size;
            root.next_expire_ts = cow_meta.next_expire_ts;
        }
    }

    /// Find a page by ID, loading from disk if necessary
    pub async fn find_page(
        &self,
        mapping: &MappingSnapshot,
        page_id: PageId,
    ) -> Result<Box<MemIndexPage>> {
        // Check if page is already in memory (swizzled pointer)
        // For now, always load from disk

        // Map logical page to file page
        let file_page_id = mapping.to_file_page(page_id)?;

        // Load page from disk
        let page_data = self.io_manager
            .read_page(file_page_id.file_id() as u64, file_page_id.page_offset())
            .await?;

        let mut index_page = self.alloc_index_page();
        index_page.set_page_id(page_id);
        index_page.set_file_page_id(file_page_id.into());
        // TODO: Copy page_data into index_page

        Ok(index_page)
    }

    /// Free a mapping snapshot
    pub fn free_mapping_snapshot(&self, mapping: *const MappingSnapshot) {
        // Find which root this mapping belongs to and remove it
        let roots = self.roots.read().unwrap();
        for (_, root) in roots.iter() {
            root.remove_mapping_snapshot(mapping);
        }
    }

    /// Unswizzle a page (convert swizzled pointer back to page ID)
    pub fn unswizzling(&self, page: &mut MemIndexPage) {
        // Convert in-memory pointer references back to page IDs
        // This is called before evicting a page
        // TODO: Implement swizzling/unswizzling logic
    }

    /// Finish I/O for a page
    pub fn finish_io(&self, mapping: &MappingSnapshot, idx_page: &mut MemIndexPage) {
        // Wake up any waiters on this page
        // TODO: Implement waiting zone notification
    }

    /// Seek in the index tree to find the data page containing a key
    pub async fn seek_index(
        &self,
        mapping: &MappingSnapshot,
        page_id: PageId,
        key: &[u8],
    ) -> Result<PageId> {
        if page_id == MAX_PAGE_ID {
            return Ok(MAX_PAGE_ID);
        }

        let mut current_page_id = page_id;

        loop {
            // Load the index page
            let index_page = self.find_page(mapping, current_page_id).await?;

            // Create iterator for the page
            let mut iter = IndexPageIter::new(&index_page, &self.options);

            // Seek to the key
            iter.seek(key);

            // Get the page ID that might contain the key
            let next_page_id = iter.get_page_id();

            // If this index page points to leaf (data pages), we're done
            if index_page.is_pointing_to_leaf() {
                return Ok(next_page_id);
            }

            // Otherwise, continue traversing down the tree
            current_page_id = next_page_id;
        }
    }

    /// Get KV options
    pub fn options(&self) -> &Arc<KvOptions> {
        &self.options
    }

    /// Get I/O manager
    pub fn io_manager(&self) -> &Arc<AsyncFileManager> {
        &self.io_manager
    }

    /// Evict root if empty
    pub fn evict_root_if_empty(&self, table_id: &TableIdent) {
        let mut roots = self.roots.write().unwrap();

        if let Some(root) = roots.get(table_id) {
            // Check if root can be evicted:
            // 1. Root page has been evicted
            // 2. No mapping snapshots active
            // 3. All pages of the tree have been evicted
            if root.root_id == MAX_PAGE_ID && !root.has_active_snapshots() && !root.is_pinned() {
                roots.remove(table_id);
            }
        }
    }

    /// Check if memory is full
    fn is_full(&self) -> bool {
        let count = self.page_count.lock().unwrap();
        *count >= self.max_pages
    }

    /// Evict a page from memory
    fn evict(&self) -> bool {
        // Find the LRU page that is not pinned
        let tail = self.active_tail.lock().unwrap();

        if let Some(tail_ptr) = *tail {
            unsafe {
                let page = tail_ptr.as_ref();
                if !page.is_pinned() {
                    // Unswizzle and evict
                    // TODO: Implement eviction
                    return true;
                }
            }
        }

        false
    }

    /// Recycle a page
    fn recycle_page(&self, page: &mut MemIndexPage) -> bool {
        if page.is_pinned() {
            return false;
        }

        // Remove from active list
        page.deque();

        // Add to free list
        self.free_index_page(Box::new(MemIndexPage::new(false)));

        let mut page_count = self.page_count.lock().unwrap();
        *page_count -= 1;

        true
    }
}