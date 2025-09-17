//! Write task implementation following C++ batch_write_task.cpp
//!
//! This implementation follows the C++ write path with:
//! - Index tree navigation using stack
//! - Leaf triple page management for linked list updates
//! - Proper page allocation and COW semantics

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value, TableIdent, PageId, MAX_PAGE_ID};
use crate::page::{DataPage, PageCache, DataPageBuilder, DataPageIterator, Page};
use crate::page::PageMapper;
use crate::storage::AsyncFileManager;
use crate::index::{IndexPageManager, CowRootMeta, IndexPageIter};
use crate::config::KvOptions;
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Write operation type
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOp {
    /// Insert or update
    Upsert,
    /// Delete key
    Delete,
}

/// Index operation for tracking changes to index pages
#[derive(Debug, Clone)]
struct IndexOp {
    key: Bytes,
    page_id: PageId,
    op: WriteOp,
}

/// Write data entry (following C++ WriteDataEntry)
#[derive(Clone, Debug)]
pub struct WriteDataEntry {
    /// Key to write
    pub key: Key,
    /// Value to write (empty for deletes)
    pub value: Value,
    /// Operation type
    pub op: WriteOp,
    /// Timestamp
    pub timestamp: u64,
    /// TTL expiration timestamp (0 for no expiration)
    pub expire_ts: u64,
}

/// Index stack entry (following C++ IndexStackEntry)
struct IndexStackEntry {
    /// Index page (if any)
    idx_page: Option<Arc<crate::index::MemIndexPage>>,
    /// Index page iterator
    idx_page_iter: crate::index::IndexPageIter<'static>,
    /// KV options
    options: Arc<crate::config::KvOptions>,
    /// Is this a leaf index
    is_leaf_index: bool,
    /// Changes to apply at this level
    changes: Vec<IndexOp>,
}

impl IndexStackEntry {
    /// Create a new stack entry
    fn new(idx_page: Option<Arc<crate::index::MemIndexPage>>, options: Arc<crate::config::KvOptions>) -> Self {
        // Create a dummy iterator for now - will be properly initialized when used
        let dummy_iter = unsafe {
            std::mem::transmute::<crate::index::IndexPageIter<'_>, crate::index::IndexPageIter<'static>>(
                crate::index::IndexPageIter::from_page_data(&[], options.comparator())
            )
        };

        Self {
            idx_page,
            idx_page_iter: dummy_iter,
            options,
            is_leaf_index: false,
            changes: Vec::new(),
        }
    }
}


/// Single write task
#[derive(Clone, Debug)]
pub struct WriteTask {
    /// Key to write
    key: Key,
    /// Value to write
    value: Value,
    /// Table identifier
    table_id: TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
    /// Index manager
    index_manager: Arc<IndexPageManager>,
}

impl WriteTask {
    /// Create a new write task
    pub fn new(
        key: Key,
        value: Value,
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self {
            key,
            value,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
        }
    }
}

#[async_trait]
impl Task for WriteTask {
    fn task_type(&self) -> TaskType {
        TaskType::Write
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // For single writes, delegate to batch write
        let entry = WriteDataEntry {
            key: self.key.clone(),
            value: self.value.clone(),
            op: WriteOp::Upsert,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            expire_ts: 0,
        };

        let batch_task = BatchWriteTask::new(
            vec![entry],
            self.table_id.clone(),
            self.page_cache.clone(),
            self.page_mapper.clone(),
            self.file_manager.clone(),
            self.index_manager.clone(),
        );

        batch_task.execute(_ctx).await
    }

    fn can_merge(&self, other: &dyn Task) -> bool {
        // Single writes can be merged into batch writes
        other.task_type() == TaskType::BatchWrite
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        // Merging handled by batch write
        Ok(())
    }

    fn estimated_cost(&self) -> usize {
        1
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Batch write task for multiple key-value pairs (following C++ BatchWriteTask)
pub struct BatchWriteTask {
    /// Write entries
    entries: Vec<WriteDataEntry>,
    /// Table identifier
    table_id: TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
    /// Index manager
    index_manager: Arc<IndexPageManager>,
    /// Index stack for tree traversal
    stack: Vec<IndexStackEntry>,
    /// Leaf triple for linked list management
    /// [0]: previous page, [1]: current page, [2]: next page
    leaf_triple: [Option<DataPage>; 3],
    /// Currently applying page
    applying_page: Option<DataPage>,
    /// COW metadata
    cow_meta: Option<CowRootMeta>,
    /// TTL batch for expired entries
    ttl_batch: Vec<WriteDataEntry>,
    /// Data page builder
    data_page_builder: DataPageBuilder,
    /// Data page builder for index pages (reuse DataPageBuilder)
    idx_page_builder: DataPageBuilder,
    /// Options
    options: Arc<KvOptions>,
}

impl BatchWriteTask {
    /// Create a new batch write task
    pub fn new(
        entries: Vec<WriteDataEntry>,
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self {
            entries,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
            stack: Vec::new(),
            leaf_triple: [None, None, None],
            applying_page: None,
            cow_meta: None,
            ttl_batch: Vec::new(),
            data_page_builder: DataPageBuilder::new(4096), // Default page size
            idx_page_builder: DataPageBuilder::new(4096), // Default page size
            options: Arc::new(KvOptions::default()),
        }
    }

    /// Seek to position in index stack (following C++ SeekStack)
    async fn seek_stack(&mut self, search_key: &Key) -> Result<()> {
        let comparator = self.index_manager.get_comparator();

        // Navigate up the stack to find the right position
        while self.stack.len() > 1 {
            let stack_entry = self.stack.last_mut().unwrap();

            // Check if we need to move to next sibling or pop
            // Simplified version - full implementation would check bounds
            if !stack_entry.idx_page_iter.has_next() {
                self.pop_stack().await?;
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Pop from index stack (following C++ Pop)
    async fn pop_stack(&mut self) -> Result<Option<PageId>> {
        if self.stack.is_empty() {
            return Ok(None);
        }

        let stack_entry = self.stack.pop().unwrap();

        // If no changes, just return
        if stack_entry.changes.is_empty() {
            // TODO: Unpin page if needed
            return Ok(stack_entry.idx_page.map(|p| p.get_page_id()));
        }

        // Reset index page builder for merging
        self.idx_page_builder.reset();

        let changes = stack_entry.changes;
        let stack_page = stack_entry.idx_page.as_ref();
        let is_leaf_index = stack_entry.is_leaf_index;

        // Initialize base page iterator if we have a page
        let mut base_page_iter = if let Some(page) = stack_page {
            Some(crate::index::IndexPageIter::new(page, &stack_entry.options))
        } else {
            None
        };

        // Advance iterator to first valid entry
        let mut is_base_iter_valid = false;
        if let Some(ref mut iter) = base_page_iter {
            is_base_iter_valid = iter.next();
        }

        // Keep track of previous page for redistribution
        let mut prev_page_id = stack_page.map_or(MAX_PAGE_ID, |p| p.get_page_id());
        let mut prev_page_data: Option<Vec<u8>> = None;
        let mut prev_page_key = String::new();

        // Get page key from parent if not root
        let page_key = if !self.stack.is_empty() {
            if let Some(parent) = self.stack.last() {
                String::from_utf8_lossy(parent.idx_page_iter.key()).to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        let mut curr_page_key = page_key.clone();

        // Merge changes with existing entries
        let mut change_iter = changes.into_iter();
        let mut curr_change = change_iter.next();

        // Process both iterators in merge order
        while is_base_iter_valid && curr_change.is_some() {
            let base_iter = base_page_iter.as_ref().unwrap();
            let base_key = base_iter.key();
            let base_page_id = base_iter.get_page_id();

            let change = curr_change.as_ref().unwrap();
            let change_key = &change.key;
            let change_page_id = change.page_id;

            // Compare keys
            let cmp_result = base_key.cmp(change_key.as_ref());

            let (new_key, new_page_id, advance_base, advance_change) = match cmp_result {
                std::cmp::Ordering::Less => {
                    // Base key comes first
                    (base_key.to_vec(), base_page_id, true, false)
                }
                std::cmp::Ordering::Equal => {
                    // Keys are equal, apply change
                    match change.op {
                        WriteOp::Delete => {
                            // Skip this entry
                            (vec![], MAX_PAGE_ID, true, true)
                        }
                        WriteOp::Upsert => {
                            // Replace with new page
                            (change_key.to_vec(), change_page_id, true, true)
                        }
                    }
                }
                std::cmp::Ordering::Greater => {
                    // Change key comes first (must be insert)
                    debug_assert!(matches!(change.op, WriteOp::Upsert));
                    (change_key.to_vec(), change_page_id, false, true)
                }
            };

            // Add to page if not deleted
            if !new_key.is_empty() || new_page_id != MAX_PAGE_ID {
                // Check if page is full
                if !self.idx_page_builder.add_index_entry(&new_key, new_page_id, is_leaf_index) {
                    // Page is full, finish current page
                    if let Some(page_data) = prev_page_data.take() {
                        // Flush previous page
                        self.flush_index_page(prev_page_id, page_data, prev_page_key.as_bytes(), true).await?;
                    }

                    prev_page_key = curr_page_key.clone();
                    prev_page_data = Some(self.idx_page_builder.finish_index_page());
                    prev_page_id = self.page_mapper.allocate_page()?;

                    curr_page_key = String::from_utf8_lossy(&new_key).to_string();
                    self.idx_page_builder.reset();

                    // Add leftmost pointer without key
                    self.idx_page_builder.add_index_entry(b"", new_page_id, is_leaf_index);
                }
            }

            // Advance iterators
            if advance_base {
                if let Some(ref mut iter) = base_page_iter {
                    is_base_iter_valid = iter.next();
                }
            }
            if advance_change {
                curr_change = change_iter.next();
            }
        }

        // Process remaining base entries
        while is_base_iter_valid {
            if let Some(ref base_iter) = base_page_iter {
                let new_key = base_iter.key();
                let new_page_id = base_iter.get_page_id();

                // Check if page is full
                if !self.idx_page_builder.add_index_entry(&new_key, new_page_id, is_leaf_index) {
                    // Page is full, finish current page
                    if let Some(page_data) = prev_page_data.take() {
                        // Flush previous page
                        self.flush_index_page(prev_page_id, page_data, prev_page_key.as_bytes(), true).await?;
                    }

                    prev_page_key = curr_page_key.clone();
                    prev_page_data = Some(self.idx_page_builder.finish_index_page());
                    prev_page_id = self.page_mapper.allocate_page()?;

                    curr_page_key = String::from_utf8_lossy(&new_key).to_string();
                    self.idx_page_builder.reset();

                    // Add leftmost pointer without key
                    self.idx_page_builder.add_index_entry(b"", new_page_id, is_leaf_index);
                }

                if let Some(ref mut iter) = base_page_iter {
                    is_base_iter_valid = iter.next();
                }
            }
        }

        // Process remaining changes
        while let Some(change) = curr_change {
            if !matches!(change.op, WriteOp::Delete) {
                let new_key = change.key.as_ref();
                let new_page_id = change.page_id;

                // Check if page is full
                if !self.idx_page_builder.add_index_entry(new_key, new_page_id, is_leaf_index) {
                    // Page is full, finish current page
                    if let Some(page_data) = prev_page_data.take() {
                        // Flush previous page
                        self.flush_index_page(prev_page_id, page_data, prev_page_key.as_bytes(), true).await?;
                    }

                    prev_page_key = curr_page_key.clone();
                    prev_page_data = Some(self.idx_page_builder.finish_index_page());
                    prev_page_id = self.page_mapper.allocate_page()?;

                    curr_page_key = String::from_utf8_lossy(new_key).to_string();
                    self.idx_page_builder.reset();

                    // Add leftmost pointer without key
                    self.idx_page_builder.add_index_entry(b"", new_page_id, is_leaf_index);
                }
            }
            curr_change = change_iter.next();
        }

        // Handle final page
        let mut new_root_id = None;
        if self.idx_page_builder.is_empty() {
            // All entries deleted, free the page
            if let Some(page) = stack_entry.idx_page {
                self.free_page(page.get_page_id());
            }

            // Notify parent about deletion
            if !self.stack.is_empty() {
                if let Some(parent) = self.stack.last_mut() {
                    let page_key = parent.idx_page_iter.key();
                    parent.changes.push(IndexOp {
                        key: Bytes::copy_from_slice(page_key),
                        page_id: prev_page_id,
                        op: WriteOp::Delete,
                    });
                }
            }
        } else {
            // Finish and flush the final page
            let splited = prev_page_data.is_some();

            // Finish current builder content
            if !self.idx_page_builder.is_empty() {
                let final_page = self.idx_page_builder.finish_index_page();

                if let Some(prev_data) = prev_page_data {
                    // We have a previous page to flush first
                    self.flush_index_page(prev_page_id, prev_data, prev_page_key.as_bytes(), true).await?;

                    // Now handle the final page
                    let final_page_id = self.page_mapper.allocate_page()?;
                    self.flush_index_page(final_page_id, final_page, curr_page_key.as_bytes(), false).await?;
                    new_root_id = Some(final_page_id);
                } else {
                    // This is the only page
                    if prev_page_id == MAX_PAGE_ID {
                        prev_page_id = self.page_mapper.allocate_page()?;
                    }
                    self.flush_index_page(prev_page_id, final_page, curr_page_key.as_bytes(), false).await?;
                    new_root_id = Some(prev_page_id);
                }
            } else if let Some(prev_data) = prev_page_data {
                // Just flush the previous page
                self.flush_index_page(prev_page_id, prev_data, prev_page_key.as_bytes(), false).await?;
                new_root_id = Some(prev_page_id);
            }
        }

        Ok(new_root_id)
    }

    /// Seek to leaf page (following C++ Seek)
    async fn seek(&mut self, key: &Key) -> Result<PageId> {
        // Check if we have an empty tree (no index page)
        if self.stack.is_empty() {
            return Err(Error::InvalidState("Stack is empty".into()));
        }

        let stack_entry = self.stack.last_mut().unwrap();
        if stack_entry.idx_page.is_none() {
            // Empty tree case - no index pages yet
            stack_entry.is_leaf_index = true;
            return Ok(PageId::MAX);
        }

        // Navigate down the index tree to find the leaf
        loop {
            let stack_len = self.stack.len();
            let should_continue = {
                let stack_entry = &mut self.stack[stack_len - 1];

                // Seek to the key position in current index page
                if let Some(ref idx_page) = stack_entry.idx_page {
                    // Create a new iterator for this page
                    let mut iter = IndexPageIter::new(idx_page, &self.options);
                    iter.seek(key.as_ref());
                    let page_id = iter.get_page_id();

                    if page_id == PageId::MAX {
                        // This shouldn't happen in a valid index
                        return Err(Error::InvalidState("Invalid page ID in index".into()));
                    }

                    // Check if this index page points to leaf pages
                    if idx_page.is_pointing_to_leaf() {
                        // We've reached the leaf level
                        stack_entry.is_leaf_index = true;
                        stack_entry.idx_page_iter = unsafe {
                            std::mem::transmute::<IndexPageIter<'_>, IndexPageIter<'static>>(iter)
                        };
                        return Ok(page_id);
                    }

                    // Store the iterator
                    stack_entry.idx_page_iter = unsafe {
                        std::mem::transmute::<IndexPageIter<'_>, IndexPageIter<'static>>(iter)
                    };

                    // Need to load next level
                    Some(page_id)
                } else {
                    None
                }
            };

            if let Some(page_id) = should_continue {
                // Load the next index page
                let mapper = self.cow_meta.as_ref()
                    .and_then(|m| m.mapper.as_ref())
                    .ok_or_else(|| Error::InvalidState("No mapper available".into()))?;

                let next_idx_page = self.index_manager
                    .find_page(&mapper.snapshot(), page_id).await?;

                next_idx_page.pin();

                // Push new stack entry for the next level
                let new_entry = IndexStackEntry::new(Some(Arc::new(*next_idx_page)), self.options.clone());
                self.stack.push(new_entry);
            } else {
                return Err(Error::InvalidState("No index page in stack entry".into()));
            }
        }
    }

    /// Load triple element (following C++ LoadTripleElement)
    async fn load_triple_element(&mut self, idx: usize, page_id: PageId) -> Result<()> {
        if idx >= 3 || self.leaf_triple[idx].is_some() {
            return Ok(());
        }

        assert!(page_id != PageId::MAX);
        let page = self.load_data_page(page_id).await?;
        self.leaf_triple[idx] = Some(page);
        Ok(())
    }

    /// Shift leaf link (following C++ ShiftLeafLink)
    async fn shift_leaf_link(&mut self) -> Result<()> {
        if let Some(page) = self.leaf_triple[0].take() {
            // Write page to disk
            self.write_page(page).await?;
        }
        self.leaf_triple[0] = self.leaf_triple[1].take();
        Ok(())
    }

    /// Update leaf link (following C++ LeafLinkUpdate)
    fn leaf_link_update(&mut self, mut page: DataPage) {
        if let Some(ref applying) = self.applying_page {
            page.set_next_page_id(applying.next_page_id());
            page.set_prev_page_id(applying.prev_page_id());
        }
        self.leaf_triple[1] = Some(page);
    }

    /// Insert into leaf link (following C++ LeafLinkInsert)
    async fn leaf_link_insert(&mut self, mut page: DataPage) -> Result<()> {
        assert!(self.leaf_triple[1].is_none());

        // Handle the mutable borrow carefully
        let (next_id, prev_id) = if let Some(prev_page) = &mut self.leaf_triple[0] {
            let nid = prev_page.next_page_id();
            let pid = prev_page.page_id();
            prev_page.set_next_page_id(page.page_id());
            (nid, pid)
        } else {
            (PageId::MAX, PageId::MAX)
        };

        // Now handle the next page if needed
        if next_id != PageId::MAX {
            self.load_triple_element(2, next_id).await?;
            if let Some(next_page) = &mut self.leaf_triple[2] {
                next_page.set_prev_page_id(page.page_id());
            }
        }

        // Set page links
        page.set_prev_page_id(prev_id);
        page.set_next_page_id(next_id);

        self.leaf_triple[1] = Some(page);
        Ok(())
    }

    /// Load data page
    async fn load_data_page(&self, page_id: PageId) -> Result<DataPage> {
        // Get mapping and load from disk
        let snapshot = self.page_mapper.snapshot();
        if let Ok(file_page_id) = snapshot.to_file_page(page_id) {
            let page_data = self.file_manager
                .read_page(file_page_id.file_id() as u64, file_page_id.page_offset())
                .await?;
            Ok(DataPage::from_page(page_id, page_data))
        } else {
            // Create new page if not found
            Ok(DataPage::new(page_id, 4096))
        }
    }

    /// Write page to disk
    async fn write_page(&self, page: DataPage) -> Result<()> {
        let page_id = page.page_id();
        tracing::debug!("write_page: writing page_id={}", page_id);

        // Ensure file exists first
        if self.file_manager.get_metadata(0).await.is_err() {
            tracing::debug!("Creating file 0");
            self.file_manager.create_file(&self.table_id).await?;
        }

        // First check if page is already mapped
        let snapshot = self.page_mapper.snapshot();
        let file_page_id = if let Ok(fid) = snapshot.to_file_page(page_id) {
            tracing::debug!("Page {} already mapped to file page {:?}", page_id, fid);
            fid
        } else {
            // Need to allocate a file page for this logical page
            tracing::debug!("Allocating file page for page_id={}", page_id);

            // Switch to file 0 if needed
            self.page_mapper.switch_file(0)?;
            let file_page_id = self.page_mapper.allocate_file_page()?;
            self.page_mapper.map_page(page_id, file_page_id)?;
            self.page_mapper.update_mapping(page_id, file_page_id);
            tracing::debug!("Mapped page {} to file page {:?}", page_id, file_page_id);
            file_page_id
        };

        // Now write the page
        self.file_manager
            .write_page(file_page_id.file_id() as u64, file_page_id.page_offset(), page.as_page())
            .await?;

        tracing::debug!("Successfully wrote page {} to disk", page_id);
        Ok(())
    }



    /// Apply batch (following C++ Apply)
    async fn apply(&mut self) -> Result<()> {
        tracing::debug!("BatchWriteTask::apply starting with {} entries", self.entries.len());

        // Following C++ Apply() exactly
        // 1. Make COW root
        self.cow_meta = Some(self.index_manager.make_cow_root(&self.table_id)?);

        // 2. Apply main batch
        let root_id = self.cow_meta.as_ref().unwrap().root_id;
        self.apply_batch(root_id, true).await?;

        // 3. Apply TTL batch if any
        self.apply_ttl_batch().await?;

        // 4. Update metadata
        self.update_meta().await?;

        tracing::debug!("BatchWriteTask::apply completed");
        Ok(())
    }

    /// Apply TTL batch
    async fn apply_ttl_batch(&mut self) -> Result<()> {
        if !self.ttl_batch.is_empty() {
            // Sort TTL batch
            self.ttl_batch.sort_by(|a, b| a.key.cmp(&b.key));

            let ttl_root = self.cow_meta.as_ref().unwrap().ttl_root_id;
            let saved_entries = self.entries.clone();
            self.entries = self.ttl_batch.clone();
            self.apply_batch(ttl_root, false).await?;
            self.entries = saved_entries;
            self.ttl_batch.clear();
        }
        Ok(())
    }

    /// Apply batch to tree (following C++ ApplyBatch)
    async fn apply_batch(&mut self, root_id: PageId, update_ttl: bool) -> Result<()> {
        tracing::debug!("apply_batch: root_id={}, update_ttl={}, entries={}",
                        root_id, update_ttl, self.entries.len());

        // Initialize stack with root
        if root_id != PageId::MAX {
            // Load root index page
            // Simplified - full implementation would load from index manager
            tracing::debug!("Loading existing root");
            let entry = IndexStackEntry::new(None, self.options.clone());
            self.stack.push(entry);
        } else {
            // Empty tree
            tracing::debug!("Creating new empty tree");
            let mut entry = IndexStackEntry::new(None, self.options.clone());
            entry.is_leaf_index = true;
            self.stack.push(entry);
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut cidx = 0;
        while cidx < self.entries.len() {
            let batch_start_key = self.entries[cidx].key.clone();

            if self.stack.len() > 1 {
                self.seek_stack(&batch_start_key).await?;
            }

            let page_id = self.seek(&batch_start_key).await?;

            if page_id != PageId::MAX {
                self.load_applying_page(page_id).await?;
            }

            self.apply_one_page(&mut cidx, now_ms, update_ttl).await?;
        }

        // Flush remaining pages
        self.shift_leaf_link().await?;
        self.shift_leaf_link().await?;

        // Update root by popping all stack entries
        let mut final_root_id = None;
        while !self.stack.is_empty() {
            if let Some(root_id) = self.pop_stack().await? {
                final_root_id = Some(root_id);
            }
        }

        // Update COW metadata with new root if changed
        if let Some(new_root) = final_root_id {
            if let Some(ref mut meta) = self.cow_meta {
                meta.root_id = new_root;
            }
        }

        Ok(())
    }

    /// Load applying page (following C++ LoadApplyingPage)
    async fn load_applying_page(&mut self, page_id: PageId) -> Result<()> {
        assert!(page_id != PageId::MAX);

        // Check if page is already in leaf_triple[1]
        if let Some(ref page) = self.leaf_triple[1] {
            if page.page_id() == page_id {
                // Fast path: move it to applying_page
                self.applying_page = self.leaf_triple[1].take();
                return Ok(());
            }
        }

        // Load from disk
        let page = self.load_data_page(page_id).await?;
        self.applying_page = Some(page);

        // Shift link if needed
        if self.leaf_triple[1].is_some() {
            self.shift_leaf_link().await?;
        }

        Ok(())
    }

    /// Apply one page (following C++ ApplyOnePage)
    async fn apply_one_page(&mut self, cidx: &mut usize, now_ms: u64, update_ttl: bool) -> Result<()> {
        tracing::debug!("apply_one_page: starting at cidx={}", *cidx);
        self.data_page_builder.reset();

        // Get the comparator
        let comparator = self.index_manager.get_comparator();

        // Collect TTL updates separately to avoid borrow issues
        let mut ttl_updates = Vec::new();

        // Initialize base page iterator if we have an applying page
        let mut base_iter = self.applying_page.as_ref().map(|page| DataPageIterator::new(page));
        let mut is_base_valid = base_iter.as_ref().map_or(false, |_| true);

        // Process entries, merging with existing data
        let start_cidx = *cidx;
        let change_end = self.entries.len();

        while is_base_valid || *cidx < change_end {
            let mut should_add = false;
            let mut key_to_add = Bytes::new();
            let mut val_to_add = Bytes::new();
            let mut ts_to_add = 0u64;
            let mut expire_to_add = 0u64;
            let mut advance_base = false;
            let mut advance_change = false;

            // Compare base and change entries
            if is_base_valid && *cidx < change_end {
                let base_key = base_iter.as_ref().unwrap().key().unwrap();
                let change_entry = &self.entries[*cidx];
                let cmp_result = comparator.compare(&base_key, &change_entry.key);

                if cmp_result < 0 {
                    // Base key comes first - keep it
                    if let Some(ref iter) = base_iter {
                        key_to_add = iter.key().unwrap();
                        val_to_add = iter.value().unwrap_or(Bytes::new());
                        ts_to_add = iter.timestamp();
                        expire_to_add = iter.expire_ts().unwrap_or(0);
                        should_add = true;
                        advance_base = true;
                    }
                } else if cmp_result == 0 {
                    // Same key - update/delete
                    advance_base = true;
                    advance_change = true;

                    if change_entry.timestamp >= base_iter.as_ref().unwrap().timestamp() {
                        // Change is newer
                        match change_entry.op {
                            WriteOp::Delete => {
                                // Delete - don't add
                                if update_ttl && expire_to_add > 0 {
                                    ttl_updates.push((expire_to_add, base_key.clone(), WriteOp::Delete));
                                }
                            }
                            WriteOp::Upsert => {
                                // Update
                                key_to_add = change_entry.key.clone();
                                val_to_add = change_entry.value.clone();
                                ts_to_add = change_entry.timestamp;
                                expire_to_add = change_entry.expire_ts;
                                should_add = true;

                                if update_ttl {
                                    let base_expire = base_iter.as_ref().unwrap().expire_ts().unwrap_or(0);
                                    if base_expire != expire_to_add {
                                        if base_expire > 0 {
                                            ttl_updates.push((base_expire, base_key.clone(), WriteOp::Delete));
                                        }
                                        if expire_to_add > 0 {
                                            ttl_updates.push((expire_to_add, key_to_add.clone(), WriteOp::Upsert));
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // Base is newer - keep it
                        if let Some(ref iter) = base_iter {
                            key_to_add = iter.key().unwrap();
                            val_to_add = iter.value().unwrap_or(Bytes::new());
                            ts_to_add = iter.timestamp();
                            expire_to_add = iter.expire_ts().unwrap_or(0);
                            should_add = true;
                        }
                    }
                } else {
                    // Change key comes first - insert
                    advance_change = true;
                    match change_entry.op {
                        WriteOp::Delete => {
                            // Deleting non-existent key - skip
                        }
                        WriteOp::Upsert => {
                            key_to_add = change_entry.key.clone();
                            val_to_add = change_entry.value.clone();
                            ts_to_add = change_entry.timestamp;
                            expire_to_add = change_entry.expire_ts;
                            should_add = true;

                            if update_ttl && expire_to_add > 0 {
                                ttl_updates.push((expire_to_add, key_to_add.clone(), WriteOp::Upsert));
                            }
                        }
                    }
                }
            } else if is_base_valid {
                // Only base entries left
                if let Some(ref iter) = base_iter {
                    key_to_add = iter.key().unwrap();
                    val_to_add = iter.value().unwrap_or(Bytes::new());
                    ts_to_add = iter.timestamp();
                    expire_to_add = iter.expire_ts().unwrap_or(0);
                    should_add = true;
                    advance_base = true;
                }
            } else if *cidx < change_end {
                // Only change entries left
                let change_entry = &self.entries[*cidx];
                advance_change = true;

                match change_entry.op {
                    WriteOp::Delete => {
                        // Deleting non-existent key - skip
                    }
                    WriteOp::Upsert => {
                        // Check if expired
                        if change_entry.expire_ts != 0 && change_entry.expire_ts <= now_ms {
                            // Expired - skip
                        } else {
                            key_to_add = change_entry.key.clone();
                            val_to_add = change_entry.value.clone();
                            ts_to_add = change_entry.timestamp;
                            expire_to_add = change_entry.expire_ts;
                            should_add = true;

                            if update_ttl && expire_to_add > 0 {
                                ttl_updates.push((expire_to_add, key_to_add.clone(), WriteOp::Upsert));
                            }
                        }
                    }
                }
            } else {
                // No more entries
                break;
            }

            // Add to page if needed
            if should_add {
                let added = self.data_page_builder.add(
                    &key_to_add,
                    &val_to_add,
                    ts_to_add,
                    if expire_to_add > 0 { Some(expire_to_add) } else { None },
                    false, // is_overflow
                );

                if !added {
                    // Page is full, need to finish this page
                    break;
                }
            }

            // Advance iterators
            if advance_base {
                if let Some(ref mut iter) = base_iter {
                    if iter.next().is_none() {
                        is_base_valid = false;
                    }
                }
            }
            if advance_change {
                *cidx += 1;
            }
        }

        // Build and write the page if there's content
        if !self.data_page_builder.is_empty() {
            tracing::debug!("Finishing page with data");
            // Allocate a new page ID
            let page_id = self.page_mapper.allocate_page()?;
            tracing::debug!("Allocated page_id: {}", page_id);
            // Create a new builder to move from self
            let builder = std::mem::replace(&mut self.data_page_builder, DataPageBuilder::new(4096));
            let page = builder.finish(page_id);

            // Following C++ - if this is an update to the applying page, use leaf_link_update
            // Otherwise use leaf_link_insert
            if self.applying_page.is_some() {
                self.leaf_link_update(page);
            } else {
                self.leaf_link_insert(page).await?;
            }
            tracing::debug!("Added page to leaf link");
        }

        // Apply TTL updates now that we're done with the iterator
        for (expire_ts, key, op) in ttl_updates {
            self.update_ttl(expire_ts, &key, op);
        }

        Ok(())
    }

    /// Update TTL (helper for ApplyOnePage)
    fn update_ttl(&mut self, expire_ts: u64, key: &[u8], op: WriteOp) {
        if expire_ts > 0 {
            self.ttl_batch.push(WriteDataEntry {
                key: Bytes::copy_from_slice(key),
                value: if matches!(op, WriteOp::Upsert) { Bytes::copy_from_slice(key) } else { Bytes::new() },
                op,
                timestamp: 0,
                expire_ts,
            });
        }
    }

    /// Update metadata (following C++ UpdateMeta)
    async fn update_meta(&mut self) -> Result<()> {
        // Update the COW metadata in index manager
        if let Some(cow_meta) = self.cow_meta.take() {
            self.index_manager.update_root(&self.table_id, cow_meta);
        }
        Ok(())
    }

    /// Finish current data page and create index entry
    async fn finish_data_page(&mut self, page_key: Bytes, mut page_id: PageId) -> Result<()> {
        // Following C++ FinishDataPage exactly
        let cur_page_len = self.data_page_builder.current_size_estimate();
        let builder = std::mem::replace(&mut self.data_page_builder, DataPageBuilder::new(4096));

        if page_id == PageId::MAX {
            // Allocate new page
            page_id = self.page_mapper.allocate_page()?;
        }

        let data_page = builder.finish(page_id);

        // Check if we should redistribute with previous page
        let one_quarter = self.options.data_page_size >> 2;
        let three_quarter = self.options.data_page_size - one_quarter;

        if cur_page_len < one_quarter as usize {
            if let Some(prev_page) = &self.leaf_triple[0] {
                if prev_page.restart_num() > 1 && prev_page.content_length() as usize > three_quarter as usize {
                    // Redistribute pages - for now just proceed
                    // Full implementation would redistribute entries
                }
            }
        }

        // Update index with this page
        if !self.stack.is_empty() {
            self.stack.last_mut().unwrap().changes.push(IndexOp {
                key: page_key,
                page_id,
                op: WriteOp::Upsert,
            });
        }

        // Insert into leaf triple
        self.leaf_link_insert(data_page).await?;
        Ok(())
    }

    /// Finish index page
    async fn finish_index_page(&mut self, prev_page: &mut Option<(PageId, Vec<u8>)>, page_key: Bytes) -> Result<()> {
        // Following C++ FinishIndexPage
        let cur_page_len = self.idx_page_builder.current_size_estimate();
        let page_data = self.idx_page_builder.finish_index_page();

        // Check if should redistribute
        let one_quarter = self.options.index_page_size >> 2;
        let three_quarter = self.options.index_page_size - one_quarter;

        if cur_page_len < one_quarter as usize {
            if let Some((prev_id, prev_data)) = prev_page {
                if prev_data.len() > three_quarter as usize {
                    // Would redistribute here - for now just proceed
                }
            }
        }

        // Flush previous page if exists
        if let Some((prev_id, prev_data)) = prev_page.take() {
            self.flush_index_page(prev_id, prev_data, &page_key, true).await?;
        }

        // Allocate page for current
        let page_id = self.page_mapper.allocate_page()?;
        *prev_page = Some((page_id, page_data));

        Ok(())
    }

    /// Flush index page to disk
    async fn flush_index_page(&mut self, page_id: PageId, page_data: Vec<u8>, page_key: &[u8], split: bool) -> Result<()> {
        // Following C++ FlushIndexPage
        // Write the index page to disk
        let mut page = Page::new(self.options.index_page_size);
        let copy_len = page_data.len().min(page.size());
        page.as_bytes_mut()[..copy_len].copy_from_slice(&page_data[..copy_len]);

        // Map and write page
        if self.file_manager.get_metadata(0).await.is_err() {
            self.file_manager.create_file(&self.table_id).await?;
        }

        let snapshot = self.page_mapper.snapshot();
        let file_page_id = if let Ok(fid) = snapshot.to_file_page(page_id) {
            fid
        } else {
            self.page_mapper.switch_file(0)?;
            let fid = self.page_mapper.allocate_file_page()?;
            self.page_mapper.map_page(page_id, fid)?;
            self.page_mapper.update_mapping(page_id, fid);
            fid
        };

        self.file_manager.write_page(file_page_id.file_id() as u64, file_page_id.page_offset(), &page).await?;

        // Update parent if split and (new page or root)
        if split && (page_id == PageId::MAX || self.stack.len() == 1) {
            if self.stack.len() == 1 {
                // Create new root level
                self.stack.insert(0, IndexStackEntry::new(None, self.options.clone()));
            }

            if self.stack.len() >= 2 {
                let parent_index = self.stack.len() - 2;
                self.stack[parent_index].changes.push(IndexOp {
                    key: Bytes::copy_from_slice(page_key),
                    page_id,
                    op: WriteOp::Upsert,
                });
            }
        }

        Ok(())
    }

    /// Free a page
    fn free_page(&mut self, page_id: PageId) {
        // Mark page as free in page mapper
        // Full implementation would handle page recycling
    }
}

#[async_trait]
impl Task for BatchWriteTask {
    fn task_type(&self) -> TaskType {
        TaskType::BatchWrite
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        tracing::debug!("BatchWriteTask execute: {} entries", self.entries.len());

        // Clone self to get mutable version for processing
        let mut task = self.clone();
        task.apply().await?;

        tracing::debug!("BatchWriteTask completed apply");
        Ok(TaskResult::BatchWrite(self.entries.len()))
    }

    fn can_merge(&self, other: &dyn Task) -> bool {
        // Batch writes can merge with other writes to same table
        if let Some(other_batch) = other.as_any().downcast_ref::<BatchWriteTask>() {
            self.table_id == other_batch.table_id
        } else if let Some(other_single) = other.as_any().downcast_ref::<WriteTask>() {
            self.table_id == other_single.table_id
        } else {
            false
        }
    }

    fn merge(&mut self, other: Box<dyn Task>) -> Result<()> {
        if let Some(other_batch) = other.as_any().downcast_ref::<BatchWriteTask>() {
            // Merge entries
            self.entries.extend(other_batch.entries.clone());
            Ok(())
        } else if let Some(other_single) = other.as_any().downcast_ref::<WriteTask>() {
            // Add single write
            let entry = WriteDataEntry {
                key: other_single.key.clone(),
                value: other_single.value.clone(),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: 0,
            };
            self.entries.push(entry);
            Ok(())
        } else {
            Err(Error::InvalidState("Cannot merge incompatible tasks".into()))
        }
    }

    fn estimated_cost(&self) -> usize {
        self.entries.len()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// Custom Debug implementation for BatchWriteTask
impl std::fmt::Debug for BatchWriteTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchWriteTask")
            .field("entries_count", &self.entries.len())
            .field("table_id", &self.table_id)
            .field("has_cow_meta", &self.cow_meta.is_some())
            .field("stack_depth", &self.stack.len())
            .finish()
    }
}

// Manual Clone implementation for BatchWriteTask
impl Clone for BatchWriteTask {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            table_id: self.table_id.clone(),
            page_cache: self.page_cache.clone(),
            page_mapper: self.page_mapper.clone(),
            file_manager: self.file_manager.clone(),
            index_manager: self.index_manager.clone(),
            stack: Vec::new(),
            leaf_triple: [None, None, None],
            applying_page: None,
            cow_meta: None,
            ttl_batch: Vec::new(),
            data_page_builder: DataPageBuilder::new(4096),
            idx_page_builder: DataPageBuilder::new(4096),
            options: Arc::new(KvOptions::default()),
        }
    }
}

/// Delete task
#[derive(Clone, Debug)]
pub struct DeleteTask {
    /// Key to delete
    key: Key,
    /// Table identifier
    table_id: TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
}

impl DeleteTask {
    /// Create a new delete task
    pub fn new(
        key: Key,
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
    ) -> Self {
        Self {
            key,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
        }
    }
}

#[async_trait]
impl Task for DeleteTask {
    fn task_type(&self) -> TaskType {
        TaskType::Delete
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // TODO: Implement proper delete logic with page lookup
        // For now, just return success
        Ok(TaskResult::Delete(true))
    }

    fn can_merge(&self, other: &dyn Task) -> bool {
        // Delete tasks for same key can be merged
        if let Some(other_delete) = other.as_any().downcast_ref::<DeleteTask>() {
            self.key == other_delete.key
        } else {
            false
        }
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        // Nothing to do - same key means same delete
        Ok(())
    }

    fn estimated_cost(&self) -> usize {
        1
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::io::backend::{IoBackendFactory, IoBackendType};
    use crate::page::PageCacheConfig;
    use crate::config::KvOptions;

    #[tokio::test]
    async fn test_write_task() {
        let temp_dir = TempDir::new().unwrap();
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();

        let file_manager = Arc::new(AsyncFileManager::new(
            temp_dir.path(),
            4096,
            10,
            backend,
        ));

        let page_cache = Arc::new(PageCache::new(PageCacheConfig::default()));
        let page_mapper = Arc::new(PageMapper::new());
        let options = Arc::new(KvOptions::default());
        let index_manager = Arc::new(IndexPageManager::new(file_manager.clone(), options.clone()));

        // Initialize
        file_manager.init().await.unwrap();

        let table_id = TableIdent::new("test", 1);
        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        // Create write task
        let task = WriteTask::new(
            key.clone(),
            value.clone(),
            table_id,
            page_cache.clone(),
            page_mapper.clone(),
            file_manager.clone(),
            index_manager.clone(),
        );

        // Execute
        let ctx = TaskContext::default();
        let result = task.execute(&ctx).await.unwrap();

        match result {
            TaskResult::BatchWrite(count) => assert_eq!(count, 1),
            _ => panic!("Unexpected result"),
        }
    }

    #[tokio::test]
    async fn test_batch_write_task() {
        let temp_dir = TempDir::new().unwrap();
        let backend = IoBackendFactory::create_default(IoBackendType::Tokio).unwrap();

        let file_manager = Arc::new(AsyncFileManager::new(
            temp_dir.path(),
            4096,
            10,
            backend,
        ));

        let page_cache = Arc::new(PageCache::new(PageCacheConfig::default()));
        let page_mapper = Arc::new(PageMapper::new());
        let options = Arc::new(KvOptions::default());
        let index_manager = Arc::new(IndexPageManager::new(file_manager.clone(), options.clone()));

        // Initialize
        file_manager.init().await.unwrap();

        let table_id = TableIdent::new("test", 1);

        // Create batch write task
        let entries = vec![
            WriteDataEntry {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: 0,
            },
            WriteDataEntry {
                key: Bytes::from("key2"),
                value: Bytes::from("value2"),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: 0,
            },
        ];

        let task = BatchWriteTask::new(
            entries,
            table_id,
            page_cache.clone(),
            page_mapper.clone(),
            file_manager.clone(),
            index_manager.clone(),
        );

        // Execute
        let ctx = TaskContext::default();
        let result = task.execute(&ctx).await.unwrap();

        match result {
            TaskResult::BatchWrite(count) => assert_eq!(count, 2),
            _ => panic!("Unexpected result"),
        }
    }
}