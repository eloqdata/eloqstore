//! Scan task implementation following C++ scan_task.cpp
//!
//! Provides range scan functionality over key-value pairs

use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value, TableIdent, PageId, MAX_PAGE_ID};
use crate::page::{DataPage, DataPageIterator, PageCache, PageMapper, MappingSnapshot};
use crate::storage::AsyncFileManager;
use crate::index::IndexPageManager;
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Scan iterator for traversing key-value pairs (following C++ ScanIterator)
pub struct ScanIterator {
    /// Table identifier
    table_id: TableIdent,
    /// Mapping snapshot for consistent view
    mapping: Option<Arc<MappingSnapshot>>,
    /// Current data page
    data_page: Option<DataPage>,
    /// Current position in the page (index-based approach to avoid lifetime issues)
    current_position: Option<usize>,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
    /// Index manager
    index_manager: Arc<IndexPageManager>,
}

impl ScanIterator {
    /// Create a new scan iterator
    pub fn new(
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self {
            table_id,
            mapping: None,
            data_page: None,
            current_position: None,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
        }
    }

    /// Seek to a key position (following C++ Seek)
    pub async fn seek(&mut self, key: &Key, ttl: bool) -> Result<()> {
        // Find root metadata
        let root_id = {
            let meta = self.index_manager.find_root(&self.table_id)?;
            if ttl {
                meta.ttl_root_id
            } else {
                meta.root_id
            }
        }; // meta dropped here


        if root_id == MAX_PAGE_ID {
            return Err(Error::Eof);
        }

        // Get mapping snapshot for consistent view
        self.mapping = Some(Arc::new(self.page_mapper.snapshot()));


        // For simple implementation: root_id might directly be a data page
        // In production, we'd traverse the index tree
        // For now, just load the root as a data page
        let page_id = root_id;

        // Load the data page
        self.load_page(page_id).await?;

        // Seek within the page
        if let Some(ref data_page) = self.data_page {
            // Create a temporary iterator to find the position
            let mut iter = DataPageIterator::new(data_page);
            iter.seek(key);

            // Store the position (iterator tracks its own position)
            self.current_position = Some(0); // Start at beginning after seek
        }

        Ok(())
    }

    /// Move to next entry
    pub async fn next(&mut self) -> Result<()> {
        self.next_internal().await
    }

    /// Internal next implementation
    async fn next_internal(&mut self) -> Result<()> {
        // Check if we can move to next position in current page
        if let Some(ref data_page) = self.data_page {
            if let Some(pos) = self.current_position {
                // Try to advance position
                let mut iter = DataPageIterator::new(data_page);
                // Skip to current position
                for _ in 0..=pos {
                    if iter.next().is_none() {
                        break;
                    }
                }
                // Try one more next
                if iter.next().is_some() {
                    self.current_position = Some(pos + 1);
                    return Ok(());
                }
            }
        }

        // Need to move to next page
        if let Some(ref data_page) = self.data_page {
            let next_page_id = data_page.next_page_id();
            if next_page_id == MAX_PAGE_ID {
                return Err(Error::Eof);
            }

            // Load next page
            self.load_page(next_page_id).await?;
        } else {
            return Err(Error::Eof);
        }

        Ok(())
    }

    /// Load a data page
    async fn load_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(ref mapping) = self.mapping {
            let file_page_id = mapping.to_file_page(page_id)?;

            // Read page from disk
            let page_data = self.file_manager
                .read_page(file_page_id.file_id() as u64, file_page_id.page_offset())
                .await?;

            self.data_page = Some(DataPage::from_page(page_id, page_data));

            // Reset position for new page
            self.current_position = Some(0);
        }

        Ok(())
    }

    /// Get current key
    pub fn key(&self) -> Option<Bytes> {
        if let (Some(ref data_page), Some(pos)) = (&self.data_page, self.current_position) {
            let mut iter = DataPageIterator::new(data_page);
            // Skip to current position
            for _ in 0..pos {
                if iter.next().is_none() {
                    return None;
                }
            }
            iter.key()
        } else {
            None
        }
    }

    /// Get current value
    pub fn value(&self) -> Option<Bytes> {
        if let (Some(ref data_page), Some(pos)) = (&self.data_page, self.current_position) {
            let mut iter = DataPageIterator::new(data_page);
            // Skip to current position
            for _ in 0..pos {
                if iter.next().is_none() {
                    return None;
                }
            }
            iter.value()
        } else {
            None
        }
    }

    /// Check if current value is overflow
    pub fn is_overflow(&self) -> bool {
        if let (Some(ref data_page), Some(pos)) = (&self.data_page, self.current_position) {
            let mut iter = DataPageIterator::new(data_page);
            // Skip to current position
            for _ in 0..pos {
                if iter.next().is_none() {
                    return false;
                }
            }
            iter.is_overflow()
        } else {
            false
        }
    }

    /// Get expiration timestamp
    pub fn expire_ts(&self) -> Option<u64> {
        if let (Some(ref data_page), Some(pos)) = (&self.data_page, self.current_position) {
            let mut iter = DataPageIterator::new(data_page);
            // Skip to current position
            for _ in 0..pos {
                if iter.next().is_none() {
                    return None;
                }
            }
            iter.expire_ts()
        } else {
            None
        }
    }

    /// Get timestamp
    pub fn timestamp(&self) -> u64 {
        if let (Some(ref data_page), Some(pos)) = (&self.data_page, self.current_position) {
            let mut iter = DataPageIterator::new(data_page);
            // Skip to current position
            for _ in 0..pos {
                if iter.next().is_none() {
                    return 0;
                }
            }
            iter.timestamp()
        } else {
            0
        }
    }

    /// Check if has more entries
    pub fn has_next(&self) -> bool {
        // Check if current page has more entries
        if let (Some(ref data_page), Some(pos)) = (&self.data_page, self.current_position) {
            let mut iter = DataPageIterator::new(data_page);
            // Skip to current position + 1
            for _ in 0..=pos {
                if iter.next().is_none() {
                    // Check if there are more pages
                    return data_page.next_page_id() != MAX_PAGE_ID;
                }
            }
            true
        } else {
            false
        }
    }
}

/// Scan task for range queries (following C++ ScanTask)
#[derive(Clone, Debug)]
pub struct ScanTask {
    /// Start key (inclusive)
    start_key: Key,
    /// End key (exclusive), None for no upper bound
    end_key: Option<Key>,
    /// Maximum number of entries to return
    limit: usize,
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

impl ScanTask {
    /// Create a new scan task
    pub fn new(
        start_key: Key,
        end_key: Option<Key>,
        limit: usize,
        table_id: TableIdent,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
    ) -> Self {
        Self {
            start_key,
            end_key,
            limit,
            table_id,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
        }
    }

    /// Execute the scan
    pub async fn scan(&self) -> Result<Vec<(Key, Value)>> {
        let mut results = Vec::new();

        // Create iterator
        let mut iter = ScanIterator::new(
            self.table_id.clone(),
            self.page_cache.clone(),
            self.page_mapper.clone(),
            self.file_manager.clone(),
            self.index_manager.clone(),
        );

        // Seek to start position
        iter.seek(&self.start_key, false).await?;


        // Collect results up to limit
        // First check if we're already positioned at a valid entry after seek
        if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
            // Check if it's within range
            if let Some(ref end_key) = self.end_key {
                if &key < end_key {
                    results.push((key, value));
                }
            } else {
                results.push((key, value));
            }
        }

        // Then continue iterating
        while results.len() < self.limit && iter.next().await.is_ok() {
            if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                // Check if we've reached the end key
                if let Some(ref end_key) = self.end_key {
                    if &key >= end_key {
                        break;
                    }
                }

                results.push((key, value));
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Task for ScanTask {
    fn task_type(&self) -> TaskType {
        TaskType::Scan
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&self, _ctx: &TaskContext) -> Result<TaskResult> {
        let results = self.scan().await?;
        Ok(TaskResult::Scan(results))
    }

    fn can_merge(&self, _other: &dyn Task) -> bool {
        // Scan tasks typically don't merge
        false
    }

    fn merge(&mut self, _other: Box<dyn Task>) -> Result<()> {
        Err(Error::InvalidState("Scan tasks cannot be merged".into()))
    }

    fn estimated_cost(&self) -> usize {
        self.limit
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
    use crate::index::IndexPageManager;

    #[tokio::test]
    async fn test_scan_task() {
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
        let start_key = Bytes::from("start");
        let end_key = Some(Bytes::from("end"));

        // Create scan task
        let task = ScanTask::new(
            start_key,
            end_key,
            10, // limit
            table_id,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
        );

        // Execute (will fail due to no data, but should compile)
        let ctx = TaskContext::default();
        let _ = task.execute(&ctx).await;
    }
}