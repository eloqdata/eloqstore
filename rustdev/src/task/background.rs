//! Background tasks for maintenance operations

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::types::{PageId, FileId};
use crate::page::{Page, DataPage, PageBuilder, PageCache};
use crate::page::PageMapper;
use crate::storage::FileManager;
use crate::io::UringManager;
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Compaction task for merging pages
#[derive(Debug)]
pub struct CompactionTask {
    /// Target file ID
    file_id: FileId,
    /// Pages to compact
    page_ids: Vec<PageId>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<FileManager>,
    /// I/O manager
    io_manager: Arc<UringManager>,
    /// Page cache
    page_cache: Arc<PageCache>,
}

impl CompactionTask {
    /// Create a new compaction task
    pub fn new(
        file_id: FileId,
        page_ids: Vec<PageId>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<FileManager>,
        io_manager: Arc<UringManager>,
        page_cache: Arc<PageCache>,
    ) -> Self {
        Self {
            file_id,
            page_ids,
            page_mapper,
            file_manager,
            io_manager,
            page_cache,
        }
    }

    /// Check if pages should be compacted
    pub async fn should_compact(&self) -> Result<bool> {
        let mut total_entries = 0;
        let mut total_size = 0;

        for page_id in &self.page_ids {
            let mapping = self.page_mapper.get_mapping(*page_id).await?;
            let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;
            let data_page = DataPage::from_page(page)?;

            total_entries += data_page.entry_count();
            total_size += data_page.used_space();
        }

        // Compact if average fill rate is below 50%
        let avg_fill_rate = total_size as f64 / (self.page_ids.len() * 4096) as f64;
        Ok(avg_fill_rate < 0.5)
    }

    /// Perform compaction
    async fn compact_pages(&self) -> Result<CompactionStats> {
        let start_time = Instant::now();
        let mut stats = CompactionStats::default();

        // Collect all entries
        let mut all_entries = Vec::new();

        for page_id in &self.page_ids {
            stats.pages_read += 1;
            let mapping = self.page_mapper.get_mapping(*page_id).await?;
            let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;
            let data_page = DataPage::from_page(page)?;

            for (key, value) in data_page.iter() {
                all_entries.push((key, value));
                stats.entries_moved += 1;
            }

            // Invalidate old page in cache
            self.page_cache.invalidate_page(*page_id).await;
        }

        // Sort entries
        all_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Create new pages
        let mut new_pages = Vec::new();
        let mut builder = PageBuilder::new(4096);

        for (key, value) in all_entries {
            if !builder.can_add(&key, &value) {
                // Flush current page
                let page = builder.build()?;
                new_pages.push(page);
                builder = PageBuilder::new(4096);
            }
            builder.add_entry(key, value)?;
        }

        // Flush last page
        if builder.entry_count() > 0 {
            let page = builder.build()?;
            new_pages.push(page);
        }

        // Write new pages
        for (i, page) in new_pages.iter().enumerate() {
            let new_page_id = self.page_mapper.allocate_page().await?;
            let mapping = self.page_mapper.get_mapping(new_page_id).await?;

            self.io_manager.write_page(mapping.file_id, mapping.offset, &page).await?;
            stats.pages_written += 1;

            // Update page mapper with key range
            if let Ok(data_page) = DataPage::from_page(page.clone()) {
                if let (Some(first), Some(last)) = (data_page.first_key(), data_page.last_key()) {
                    self.page_mapper.update_key_range(new_page_id, first, last).await?;
                }
            }
        }

        // Mark old pages as free
        for page_id in &self.page_ids {
            self.page_mapper.free_page(*page_id).await?;
            stats.pages_freed += 1;
        }

        stats.duration = start_time.elapsed();
        stats.space_saved = (self.page_ids.len() - new_pages.len()) * 4096;

        Ok(stats)
    }
}

#[async_trait]
impl Task for CompactionTask {
    fn task_type(&self) -> TaskType {
        TaskType::Compaction
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Low
    }

    async fn execute(&mut self, context: TaskContext) -> Result<TaskResult> {
        // Check if compaction is needed
        if !self.should_compact().await? {
            return Ok(TaskResult::Background(Box::new(CompactionStats::default())));
        }

        // Perform compaction
        let stats = self.compact_pages().await?;

        Ok(TaskResult::Background(Box::new(stats)))
    }

    fn cancel(&mut self) {
        // Compaction can be cancelled
    }

    fn estimate_cost(&self) -> usize {
        self.page_ids.len() * 2 // Read + write
    }
}

/// Compaction statistics
#[derive(Debug, Default, Clone)]
pub struct CompactionStats {
    /// Pages read
    pub pages_read: usize,
    /// Pages written
    pub pages_written: usize,
    /// Pages freed
    pub pages_freed: usize,
    /// Entries moved
    pub entries_moved: usize,
    /// Space saved in bytes
    pub space_saved: usize,
    /// Duration
    pub duration: Duration,
}

/// Garbage collection task
#[derive(Debug)]
pub struct GarbageCollectionTask {
    /// File manager
    file_manager: Arc<FileManager>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// I/O manager
    io_manager: Arc<UringManager>,
    /// Threshold for GC (percentage)
    threshold: f64,
}

impl GarbageCollectionTask {
    /// Create a new GC task
    pub fn new(
        file_manager: Arc<FileManager>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
        threshold: f64,
    ) -> Self {
        Self {
            file_manager,
            page_mapper,
            io_manager,
            threshold,
        }
    }

    /// Identify pages to collect
    async fn identify_garbage(&self) -> Result<Vec<PageId>> {
        let mut garbage_pages = Vec::new();
        let all_pages = self.page_mapper.get_all_pages().await?;

        for page_id in all_pages {
            let mapping = self.page_mapper.get_mapping(page_id).await?;

            // Check if page is marked as deleted
            if mapping.deleted {
                garbage_pages.push(page_id);
                continue;
            }

            // Load page to check fill rate
            match self.io_manager.read_page(mapping.file_id, mapping.offset).await {
                Ok(page) => {
                    if let Ok(data_page) = DataPage::from_page(page) {
                        let fill_rate = data_page.used_space() as f64 / 4096.0;
                        if fill_rate < self.threshold {
                            garbage_pages.push(page_id);
                        }
                    }
                }
                Err(_) => {
                    // Page read failed, mark as garbage
                    garbage_pages.push(page_id);
                }
            }
        }

        Ok(garbage_pages)
    }

    /// Collect garbage pages
    async fn collect_garbage(&self, pages: Vec<PageId>) -> Result<GCStats> {
        let start_time = Instant::now();
        let mut stats = GCStats::default();

        for page_id in pages {
            // Free the page
            self.page_mapper.free_page(page_id).await?;
            stats.pages_collected += 1;
            stats.space_reclaimed += 4096;
        }

        // Trigger file compaction if needed
        let file_stats = self.file_manager.get_stats().await?;
        if file_stats.fragmentation_ratio > 0.5 {
            stats.files_compacted = self.file_manager.compact_files().await?;
        }

        stats.duration = start_time.elapsed();
        Ok(stats)
    }
}

#[async_trait]
impl Task for GarbageCollectionTask {
    fn task_type(&self) -> TaskType {
        TaskType::GarbageCollection
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Low
    }

    async fn execute(&mut self, context: TaskContext) -> Result<TaskResult> {
        // Identify garbage pages
        let garbage_pages = self.identify_garbage().await?;

        if garbage_pages.is_empty() {
            return Ok(TaskResult::Background(Box::new(GCStats::default())));
        }

        // Collect garbage
        let stats = self.collect_garbage(garbage_pages).await?;

        Ok(TaskResult::Background(Box::new(stats)))
    }

    fn cancel(&mut self) {
        // GC can be cancelled
    }

    fn estimate_cost(&self) -> usize {
        100 // Expensive operation
    }
}

/// Garbage collection statistics
#[derive(Debug, Default, Clone)]
pub struct GCStats {
    /// Pages collected
    pub pages_collected: usize,
    /// Space reclaimed in bytes
    pub space_reclaimed: usize,
    /// Files compacted
    pub files_compacted: usize,
    /// Duration
    pub duration: Duration,
}

/// Flush task for persisting buffered writes
#[derive(Debug)]
pub struct FlushTask {
    /// File ID to flush
    file_id: FileId,
    /// I/O manager
    io_manager: Arc<UringManager>,
}

impl FlushTask {
    /// Create a new flush task
    pub fn new(file_id: FileId, io_manager: Arc<UringManager>) -> Self {
        Self {
            file_id,
            io_manager,
        }
    }
}

#[async_trait]
impl Task for FlushTask {
    fn task_type(&self) -> TaskType {
        TaskType::Flush
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::High
    }

    async fn execute(&mut self, _context: TaskContext) -> Result<TaskResult> {
        // Flush file to disk
        self.io_manager.sync_file(self.file_id).await?;

        Ok(TaskResult::Background(Box::new(())))
    }

    fn cancel(&mut self) {
        // Flush cannot be cancelled
    }

    fn is_cancelable(&self) -> bool {
        false
    }

    fn estimate_cost(&self) -> usize {
        10 // Moderate cost
    }
}

/// Checkpoint task for creating consistent snapshots
#[derive(Debug)]
pub struct CheckpointTask {
    /// Checkpoint ID
    checkpoint_id: u64,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<FileManager>,
    /// I/O manager
    io_manager: Arc<UringManager>,
}

impl CheckpointTask {
    /// Create a new checkpoint task
    pub fn new(
        checkpoint_id: u64,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<FileManager>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        Self {
            checkpoint_id,
            page_mapper,
            file_manager,
            io_manager,
        }
    }

    /// Create checkpoint
    async fn create_checkpoint(&self) -> Result<CheckpointStats> {
        let start_time = Instant::now();
        let mut stats = CheckpointStats::default();

        // Flush all dirty pages
        self.io_manager.sync_all().await?;
        stats.pages_flushed = self.page_mapper.get_dirty_count().await?;

        // Save page mapper state
        self.page_mapper.save_checkpoint(self.checkpoint_id).await?;

        // Save file manager state
        self.file_manager.save_checkpoint(self.checkpoint_id).await?;

        stats.checkpoint_id = self.checkpoint_id;
        stats.duration = start_time.elapsed();

        Ok(stats)
    }
}

#[async_trait]
impl Task for CheckpointTask {
    fn task_type(&self) -> TaskType {
        TaskType::Checkpoint
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Critical
    }

    async fn execute(&mut self, _context: TaskContext) -> Result<TaskResult> {
        let stats = self.create_checkpoint().await?;
        Ok(TaskResult::Background(Box::new(stats)))
    }

    fn cancel(&mut self) {
        // Checkpoint cannot be cancelled
    }

    fn is_cancelable(&self) -> bool {
        false
    }

    fn estimate_cost(&self) -> usize {
        50 // High cost
    }
}

/// Checkpoint statistics
#[derive(Debug, Default, Clone)]
pub struct CheckpointStats {
    /// Checkpoint ID
    pub checkpoint_id: u64,
    /// Pages flushed
    pub pages_flushed: usize,
    /// Duration
    pub duration: Duration,
}