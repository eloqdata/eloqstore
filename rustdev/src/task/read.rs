//! Read task implementation

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value};
use crate::page::{DataPage, PageCache};
use crate::page::PageMapper;
use crate::io::UringManager;
use crate::Result;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Read task for fetching a single key
#[derive(Debug)]
pub struct ReadTask {
    /// Key to read
    key: Key,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// I/O manager
    io_manager: Arc<UringManager>,
}

impl ReadTask {
    /// Create a new read task
    pub fn new(
        key: Key,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        Self {
            key,
            page_cache,
            page_mapper,
            io_manager,
        }
    }

    /// Find the page containing the key
    async fn find_page(&self) -> Result<Option<Arc<DataPage>>> {
        // First check cache
        if let Some(page) = self.page_cache.get(&self.key).await {
            return Ok(Some(page));
        }

        // Find page ID from mapper
        let page_id = self.page_mapper.find_page_for_key(&self.key).await?;

        if let Some(page_id) = page_id {
            // Load page from disk
            let mapping = self.page_mapper.get_mapping(page_id).await?;
            let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;

            // Parse as data page
            let data_page = Arc::new(DataPage::from_page(page)?);

            // Add to cache
            self.page_cache.insert(self.key.clone(), data_page.clone()).await;

            Ok(Some(data_page))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl Task for ReadTask {
    fn task_type(&self) -> TaskType {
        TaskType::Read
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::High
    }

    async fn execute(&mut self, _context: TaskContext) -> Result<TaskResult> {
        // Find the page containing the key
        let page = self.find_page().await?;

        if let Some(page) = page {
            // Search within the page
            let value = page.get(&self.key)?;
            Ok(TaskResult::Read(value.map(|v| Bytes::from(v))))
        } else {
            Ok(TaskResult::Read(None))
        }
    }

    fn cancel(&mut self) {
        // Read operations are not typically cancelable mid-execution
    }

    fn is_cancelable(&self) -> bool {
        false
    }

    fn estimate_cost(&self) -> usize {
        1 // Single page read
    }
}

/// Floor read task - finds largest key <= target
#[derive(Debug)]
pub struct FloorReadTask {
    /// Target key
    key: Key,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// I/O manager
    io_manager: Arc<UringManager>,
}

impl FloorReadTask {
    /// Create a new floor read task
    pub fn new(
        key: Key,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        Self {
            key,
            page_cache,
            page_mapper,
            io_manager,
        }
    }
}

#[async_trait]
impl Task for FloorReadTask {
    fn task_type(&self) -> TaskType {
        TaskType::Read
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::High
    }

    async fn execute(&mut self, _context: TaskContext) -> Result<TaskResult> {
        // Find the page that might contain the floor key
        let page_id = self.page_mapper.find_floor_page(&self.key).await?;

        if let Some(page_id) = page_id {
            // Load page
            let mapping = self.page_mapper.get_mapping(page_id).await?;
            let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;
            let data_page = DataPage::from_page(page)?;

            // Find floor entry in page
            let floor_entry = data_page.floor(&self.key)?;

            if let Some((key, value)) = floor_entry {
                // Check if we need to look at previous page
                if key < self.key {
                    // This is a valid floor
                    return Ok(TaskResult::Read(Some(Bytes::from(value))));
                }
            }

            // Check previous page if exists
            if let Some(prev_page_id) = self.page_mapper.get_prev_page(page_id).await? {
                let mapping = self.page_mapper.get_mapping(prev_page_id).await?;
                let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;
                let data_page = DataPage::from_page(page)?;

                // Get last entry of previous page
                if let Some((_, value)) = data_page.last_entry()? {
                    return Ok(TaskResult::Read(Some(Bytes::from(value))));
                }
            }
        }

        Ok(TaskResult::Read(None))
    }

    fn cancel(&mut self) {
        // Not cancelable
    }

    fn is_cancelable(&self) -> bool {
        false
    }
}

/// Multi-get read task for reading multiple keys
#[derive(Debug)]
pub struct MultiGetTask {
    /// Keys to read
    keys: Vec<Key>,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// I/O manager
    io_manager: Arc<UringManager>,
}

impl MultiGetTask {
    /// Create a new multi-get task
    pub fn new(
        keys: Vec<Key>,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        Self {
            keys,
            page_cache,
            page_mapper,
            io_manager,
        }
    }
}

#[async_trait]
impl Task for MultiGetTask {
    fn task_type(&self) -> TaskType {
        TaskType::Read
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::High
    }

    async fn execute(&mut self, context: TaskContext) -> Result<TaskResult> {
        let mut results = Vec::with_capacity(self.keys.len());

        for key in &self.keys {
            // Check for cancellation
            if context.cancel_token.is_cancelled() {
                break;
            }

            // Create individual read task
            let mut read_task = ReadTask::new(
                key.clone(),
                self.page_cache.clone(),
                self.page_mapper.clone(),
                self.io_manager.clone(),
            );

            // Execute read
            match read_task.execute(context.clone()).await? {
                TaskResult::Read(value) => {
                    if let Some(value) = value {
                        results.push((Bytes::from(key.as_bytes()), value));
                    }
                }
                _ => unreachable!(),
            }
        }

        Ok(TaskResult::Scan(results))
    }

    fn cancel(&mut self) {
        // Cancellation handled via context token
    }

    fn estimate_cost(&self) -> usize {
        self.keys.len()
    }
}