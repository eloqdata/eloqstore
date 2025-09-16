//! Read task implementation using the new I/O abstraction
//!
//! This is a refactored version that uses the pluggable I/O backend
//! instead of directly using UringManager.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value};
use crate::page::{DataPage, PageCache, Page};
use crate::page::PageMapper;
use crate::storage::AsyncFileManager;
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Read task for fetching a single key
#[derive(Clone, Debug)]
pub struct ReadTask {
    /// Key to read
    key: Key,
    /// Table identifier
    table_id: crate::types::TableIdent,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager using I/O abstraction
    file_manager: Arc<AsyncFileManager>,
}

impl ReadTask {
    /// Create a new read task
    pub fn new(
        key: Key,
        table_id: crate::types::TableIdent,
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

    /// Find the page containing the key
    async fn find_page(&self) -> Result<Option<Arc<DataPage>>> {
        // First check cache
        if let Some(page) = self.page_cache.get(&self.key).await {
            return Ok(Some(page));
        }

        // In a full implementation, we would:
        // 1. Get root metadata for the table using IndexManager
        // 2. Use SeekIndex to find the page_id containing the key
        // 3. Map logical page_id to physical file_page_id
        // 4. Load the page from disk

        // For now, try to load from mapper if we have any pages
        let snapshot = self.page_mapper.snapshot();

        // Simple linear search through pages (inefficient but works for testing)
        // In production, we'd use the index tree
        for page_id in 0..snapshot.max_page_id() {
            if let Ok(file_page_id) = snapshot.to_file_page(page_id) {
                // Load the page from disk
                match self.file_manager.read_page(
                    file_page_id.file_id() as u64,
                    file_page_id.page_offset()
                ).await {
                    Ok(page_data) => {
                        let data_page = DataPage::from_page(page_id, page_data);
                        // Check if key might be in this page
                        let page_arc = Arc::new(data_page);

                        // Cache the page
                        self.page_cache.insert(self.key.clone(), page_arc.clone()).await;

                        return Ok(Some(page_arc));
                    }
                    Err(_) => continue, // Try next page
                }
            }
        }

        // No page found
        Ok(None)
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

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        // Find the page containing the key
        let page = match self.find_page().await? {
            Some(p) => p,
            None => return Ok(TaskResult::Read(None)),
        };

        // Search for key in page
        let value = page.get(&self.key)?;

        Ok(TaskResult::Read(value.map(Bytes::from)))
    }

    fn can_merge(&self, other: &dyn Task) -> bool {
        // Read tasks for the same key can be merged
        if let Some(other_read) = other.as_any().downcast_ref::<ReadTask>() {
            self.key == other_read.key
        } else {
            false
        }
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        // Nothing to do for read merge - same key means same result
        Ok(())
    }

    fn estimated_cost(&self) -> usize {
        // Cost is one page read
        1
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Batch read task for multiple keys
#[derive(Clone, Debug)]
pub struct BatchReadTask {
    /// Keys to read
    keys: Vec<Key>,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
}

impl BatchReadTask {
    /// Create a new batch read task
    pub fn new(
        keys: Vec<Key>,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
    ) -> Self {
        Self {
            keys,
            page_cache,
            page_mapper,
            file_manager,
        }
    }

    /// Execute reads for all keys
    async fn read_all(&self) -> Result<Vec<Option<Bytes>>> {
        let mut results = Vec::with_capacity(self.keys.len());

        // Group keys by page for efficiency
        let mut page_keys: std::collections::HashMap<u64, Vec<&Key>> = std::collections::HashMap::new();

        for key in &self.keys {
            // Check cache first
            if let Some(page) = self.page_cache.get(key).await {
                let value = page.get(key)?;
                results.push(value.map(Bytes::from));
                continue;
            }

            // TODO: Implement proper page lookup
            // For now, just mark as not found
            results.push(None);
        }

        // TODO: Implement proper batch page reading
        // For now, pages are not read

        Ok(results)
    }
}

#[async_trait]
impl Task for BatchReadTask {
    fn task_type(&self) -> TaskType {
        TaskType::BatchRead
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        let values = self.read_all().await?;
        Ok(TaskResult::BatchRead(values))
    }

    fn can_merge(&self, _other: &dyn Task) -> bool {
        // Batch reads are not merged
        false
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        Err(Error::InvalidState("Cannot merge batch read tasks".into()))
    }

    fn estimated_cost(&self) -> usize {
        // Estimate based on number of keys
        self.keys.len()
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
    use crate::types::TableIdent;

    #[tokio::test]
    async fn test_read_task() {
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

        // Initialize
        file_manager.init().await.unwrap();

        // Create a read task
        let key = Bytes::from("test_key");
        let task = ReadTask::new(
            key.clone(),
            page_cache,
            page_mapper,
            file_manager,
        );

        // Execute (should return None since no data)
        let ctx = TaskContext::default();
        let result = task.execute(&ctx).await.unwrap();

        match result {
            TaskResult::Read(None) => {} // Expected
            _ => panic!("Unexpected result"),
        }
    }
}