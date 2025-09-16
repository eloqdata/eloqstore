//! Write task implementations

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::sync::RwLock;

use crate::types::{Key, Value, PageId};
use crate::page::{DataPage, PageBuilder, PageCache};
use crate::page::PageMapper;
use crate::io::{UringManager, IoOperation, BatchIoRequest};
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Single write task
#[derive(Debug)]
pub struct WriteTask {
    /// Key to write
    key: Key,
    /// Value to write
    value: Value,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// I/O manager
    io_manager: Arc<UringManager>,
}

impl WriteTask {
    /// Create a new write task
    pub fn new(
        key: Key,
        value: Value,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        Self {
            key,
            value,
            page_cache,
            page_mapper,
            io_manager,
        }
    }
}

#[async_trait]
impl Task for WriteTask {
    fn task_type(&self) -> TaskType {
        TaskType::Write
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::High
    }

    async fn execute(&mut self, _context: TaskContext) -> Result<TaskResult> {
        // Find the page for this key
        let page_id = self.page_mapper.find_page_for_key(&self.key).await?;

        if let Some(page_id) = page_id {
            // Update existing page
            let mapping = self.page_mapper.get_mapping(page_id).await?;
            let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;

            let mut data_page = DataPage::from_page(page)?;

            // Try to update the page
            if data_page.can_fit(&self.key, &self.value) {
                data_page.insert(self.key.clone(), self.value.clone())?;

                // Write back
                self.io_manager.write_page(
                    mapping.file_id,
                    mapping.offset,
                    data_page.as_page(),
                ).await?;

                // Update cache
                self.page_cache.invalidate(&self.key).await;
            } else {
                // Page is full, need to split or use overflow
                return Err(Error::PageFull);
            }
        } else {
            // Create new page
            let mut builder = PageBuilder::new(4096);
            builder.add_entry(self.key.clone(), self.value.clone())?;
            let page = builder.build()?;

            // Allocate new page
            let page_id = self.page_mapper.allocate_page().await?;
            let mapping = self.page_mapper.get_mapping(page_id).await?;

            // Write page
            self.io_manager.write_page(
                mapping.file_id,
                mapping.offset,
                &page,
            ).await?;

            // Update mapper
            self.page_mapper.update_key_range(page_id, self.key.clone(), self.key.clone()).await?;
        }

        Ok(TaskResult::Write(()))
    }

    fn cancel(&mut self) {
        // Write operations are not cancelable once started
    }

    fn is_cancelable(&self) -> bool {
        false
    }
}
/// Batch write task for writing multiple key-value pairs
#[derive(Debug)]
pub struct BatchWriteTask {
    /// Items to write
    items: Vec<(Key, Value)>,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// I/O manager
    io_manager: Arc<UringManager>,
    /// Write buffer
    write_buffer: Arc<RwLock<WriteBuffer>>,
}

impl BatchWriteTask {
    /// Create a new batch write task
    pub fn new(
        items: Vec<(Key, Value)>,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        Self {
            items,
            page_cache,
            page_mapper,
            io_manager,
            write_buffer: Arc::new(RwLock::new(WriteBuffer::new())),
        }
    }

    /// Sort items by key for sequential writes
    fn sort_items(&mut self) {
        self.items.sort_by(|a, b| a.0.cmp(&b.0));
    }

    /// Group items by target page
    async fn group_by_page(&self) -> Result<HashMap<Option<PageId>, Vec<(Key, Value)>>> {
        let mut groups: HashMap<Option<PageId>, Vec<(Key, Value)>> = HashMap::new();

        for (key, value) in &self.items {
            let page_id = self.page_mapper.find_page_for_key(key).await?;
            groups.entry(page_id)
                .or_insert_with(Vec::new)
                .push((key.clone(), value.clone()));
        }

        Ok(groups)
    }

    /// Write a group of items to a page
    async fn write_group(&self, page_id: Option<PageId>, items: Vec<(Key, Value)>) -> Result<usize> {
        if let Some(page_id) = page_id {
            // Update existing page
            let mapping = self.page_mapper.get_mapping(page_id).await?;
            let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;

            let mut data_page = DataPage::from_page(page)?;
            let mut written = 0;

            for (key, value) in items {
                if data_page.can_fit(&key, &value) {
                    data_page.insert(key.clone(), value)?;
                    written += 1;
                    
                    // Invalidate cache
                    self.page_cache.invalidate(&key).await;
                } else {
                    // Page is full, need new page
                    break;
                }
            }

            // Write back
            self.io_manager.write_page(
                mapping.file_id,
                mapping.offset,
                data_page.as_page(),
            ).await?;

            Ok(written)
        } else {
            // Create new pages as needed
            let mut written = 0;
            let mut builder = PageBuilder::new(4096);

            for (key, value) in items {
                if builder.can_add(&key, &value) {
                    builder.add_entry(key.clone(), value)?;
                    written += 1;
                } else {
                    // Flush current page
                    if builder.entry_count() > 0 {
                        let page = builder.build()?;
                        let page_id = self.page_mapper.allocate_page().await?;
                        let mapping = self.page_mapper.get_mapping(page_id).await?;

                        self.io_manager.write_page(
                            mapping.file_id,
                            mapping.offset,
                            &page,
                        ).await?;

                        // Update key range
                        if let (Some(first), Some(last)) = (builder.first_key(), builder.last_key()) {
                            self.page_mapper.update_key_range(page_id, first, last).await?;
                        }
                    }

                    // Start new page
                    builder = PageBuilder::new(4096);
                    builder.add_entry(key.clone(), value)?;
                    written += 1;
                }
            }

            // Flush final page
            if builder.entry_count() > 0 {
                let page = builder.build()?;
                let page_id = self.page_mapper.allocate_page().await?;
                let mapping = self.page_mapper.get_mapping(page_id).await?;

                self.io_manager.write_page(
                    mapping.file_id,
                    mapping.offset,
                    &page,
                ).await?;

                if let (Some(first), Some(last)) = (builder.first_key(), builder.last_key()) {
                    self.page_mapper.update_key_range(page_id, first, last).await?;
                }
            }

            Ok(written)
        }
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

    async fn execute(&mut self, context: TaskContext) -> Result<TaskResult> {
        // Sort items for better locality
        self.sort_items();

        // Group items by target page
        let groups = self.group_by_page().await?;

        let mut total_written = 0;

        // Write each group
        for (page_id, items) in groups {
            // Check for cancellation
            if context.cancel_token.is_cancelled() {
                break;
            }

            let written = self.write_group(page_id, items).await?;
            total_written += written;
        }

        Ok(TaskResult::BatchWrite(total_written))
    }

    fn cancel(&mut self) {
        // Batch writes can be cancelled between groups
    }

    fn estimate_cost(&self) -> usize {
        // Estimate based on number of items and potential page writes
        (self.items.len() / 100).max(1)
    }
}

/// Write buffer for batching writes
#[derive(Debug)]
struct WriteBuffer {
    /// Buffered writes by page
    pages: HashMap<PageId, Vec<(Key, Value)>>,
    /// Total buffered size
    total_size: usize,
    /// Maximum buffer size
    max_size: usize,
}

impl WriteBuffer {
    /// Create a new write buffer
    fn new() -> Self {
        Self {
            pages: HashMap::new(),
            total_size: 0,
            max_size: 16 * 1024 * 1024, // 16MB
        }
    }

    /// Add an item to the buffer
    fn add(&mut self, page_id: PageId, key: Key, value: Value) -> bool {
        let item_size = key.len() + value.len();

        if self.total_size + item_size > self.max_size {
            return false;
        }

        self.pages.entry(page_id)
            .or_insert_with(Vec::new)
            .push((key, value));

        self.total_size += item_size;
        true
    }

    /// Check if buffer should be flushed
    fn should_flush(&self) -> bool {
        self.total_size >= self.max_size * 80 / 100  // 80% full
    }

    /// Clear the buffer
    fn clear(&mut self) {
        self.pages.clear();
        self.total_size = 0;
    }

    /// Take all buffered items
    fn take(&mut self) -> HashMap<PageId, Vec<(Key, Value)>> {
        let pages = std::mem::take(&mut self.pages);
        self.total_size = 0;
        pages
    }
}
