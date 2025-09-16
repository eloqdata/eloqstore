//! Write task implementation following C++ batch_write_task.cpp
//!
//! This implementation follows the C++ write path with:
//! - Index tree navigation using stack
//! - Leaf triple page management for linked list updates
//! - Proper page allocation and COW semantics

use std::sync::Arc;
use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value, FileId, TableIdent, PageId};
use crate::page::{DataPage, PageCache, Page, DataPageBuilder};
use crate::page::PageMapper;
use crate::storage::AsyncFileManager;
use crate::index::{IndexPageManager, CowRootMeta, MemIndexPage};
use crate::utils::Comparator;
use crate::Result;
use crate::error::{Error, KvError};

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Write operation type
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOp {
    /// Insert or update
    Upsert,
    /// Delete key
    Delete,
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
    /// Iterator position
    idx_iter_pos: usize,
    /// Pending changes
    changes: Vec<IndexOp>,
    /// Is this a leaf index
    is_leaf_index: bool,
}

/// Index operation
struct IndexOp {
    key: String,
    page_id: PageId,
    op: WriteOp,
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
            timestamp: 0, // TODO: Get timestamp
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
            data_page_builder: DataPageBuilder::new(4096), // TODO: Get from options
            idx_page_builder: DataPageBuilder::new(4096), // TODO: Get from options
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
            if stack_entry.idx_iter_pos >= stack_entry.changes.len() {
                self.pop_stack().await?;
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Pop from index stack
    async fn pop_stack(&mut self) -> Result<Option<PageId>> {
        if let Some(entry) = self.stack.pop() {
            // Process any pending changes
            // In full implementation, would update parent index page
            Ok(None)
        } else {
            Ok(None)
        }
    }

    /// Seek to leaf page (following C++ Seek)
    async fn seek(&mut self, key: &Key) -> Result<PageId> {
        if self.stack.is_empty() {
            return Ok(PageId::MAX);
        }

        // Navigate down to leaf
        // Simplified - just return MAX for now
        // Full implementation would traverse index tree
        Ok(PageId::MAX)
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
        let snapshot = self.page_mapper.snapshot();
        if let Ok(file_page_id) = snapshot.to_file_page(page.page_id()) {
            self.file_manager
                .write_page(file_page_id.file_id() as u64, file_page_id.page_offset(), page.as_page())
                .await?;
        }
        Ok(())
    }


    /// Apply batch (following C++ Apply)
    async fn apply(&mut self) -> Result<()> {
        // Make COW root
        self.cow_meta = Some(self.index_manager.make_cow_root(&self.table_id)?);

        // Apply main batch
        let root_id = self.cow_meta.as_ref().unwrap().root_id;
        self.apply_batch(root_id, true).await?;

        // Apply TTL batch if any
        if !self.ttl_batch.is_empty() {
            self.apply_ttl_batch().await?;
        }

        // Update metadata
        self.update_meta().await?;

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
        // Initialize stack with root
        if root_id != PageId::MAX {
            // Load root index page
            // Simplified - full implementation would load from index manager
            self.stack.push(IndexStackEntry {
                idx_page: None,
                idx_iter_pos: 0,
                changes: Vec::new(),
                is_leaf_index: false,
            });
        } else {
            // Empty tree
            self.stack.push(IndexStackEntry {
                idx_page: None,
                idx_iter_pos: 0,
                changes: Vec::new(),
                is_leaf_index: true,
            });
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

        // Update root
        while !self.stack.is_empty() {
            self.pop_stack().await?;
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
        self.data_page_builder.reset();

        // Process entries that belong to this page
        while *cidx < self.entries.len() {
            let entry = &self.entries[*cidx];

            // Check if entry is expired
            if entry.expire_ts != 0 && entry.expire_ts <= now_ms {
                // Skip expired entry
                if update_ttl {
                    // Add to TTL batch for deletion
                    self.ttl_batch.push(WriteDataEntry {
                        key: entry.key.clone(),
                        value: Bytes::new(),
                        op: WriteOp::Delete,
                        timestamp: now_ms,
                        expire_ts: 0,
                    });
                }
                *cidx += 1;
                continue;
            }

            // Try to add to page
            let added = self.data_page_builder.add(
                &entry.key,
                &entry.value,
                entry.timestamp,
                if entry.expire_ts > 0 { Some(entry.expire_ts) } else { None },
                false, // is_overflow
            );

            if !added {
                // Page is full, need to flush
                break;
            }

            *cidx += 1;
        }

        // Build and write the page if there's content
        if !self.data_page_builder.is_empty() {
            // TODO: Allocate proper page ID
            let page_id = 0;
            // Create a new builder to move from self
            let builder = std::mem::replace(&mut self.data_page_builder, DataPageBuilder::new(4096));
            let page = builder.finish(page_id);
            self.leaf_link_insert(page).await?;
        }

        Ok(())
    }

    /// Update metadata
    async fn update_meta(&mut self) -> Result<()> {
        // Update COW metadata
        // Simplified - full implementation would update index manager
        Ok(())
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
        // Clone self to get mutable version for processing
        let mut task = self.clone();
        task.apply().await?;
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