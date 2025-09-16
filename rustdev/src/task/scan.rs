//! Scan task implementation for range queries

use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio::sync::mpsc;

use crate::types::{Key, Value, PageId};
use crate::page::{DataPage, PageCache};
use crate::page::PageMapper;
use crate::io::UringManager;
use crate::Result;
use crate::error::Error;

use super::traits::{Task, TaskResult, TaskPriority, TaskType, TaskContext};

/// Scan task for range queries
#[derive(Debug)]
pub struct ScanTask {
    /// Start key (inclusive)
    start: Option<Key>,
    /// End key (exclusive)
    end: Option<Key>,
    /// Maximum number of results
    limit: usize,
    /// Reverse scan
    reverse: bool,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// I/O manager
    io_manager: Arc<UringManager>,
}

impl ScanTask {
    /// Create a new scan task
    pub fn new(
        start: Option<Key>,
        end: Option<Key>,
        limit: usize,
        reverse: bool,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        Self {
            start,
            end,
            limit,
            reverse,
            page_cache,
            page_mapper,
            io_manager,
        }
    }

    /// Create a scan iterator
    pub fn into_iterator(self) -> ScanIterator {
        ScanIterator::new(
            self.start,
            self.end,
            self.limit,
            self.reverse,
            self.page_cache,
            self.page_mapper,
            self.io_manager,
        )
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

    async fn execute(&mut self, context: TaskContext) -> Result<TaskResult> {
        let mut results = Vec::new();
        let mut count = 0;

        // Find starting page
        let start_page = if let Some(ref start_key) = self.start {
            self.page_mapper.find_page_for_key(start_key).await?
        } else {
            self.page_mapper.get_first_page().await?
        };

        let mut current_page = start_page;

        while let Some(page_id) = current_page {
            // Check cancellation
            if context.cancel_token.is_cancelled() {
                break;
            }

            // Load page
            let mapping = self.page_mapper.get_mapping(page_id).await?;
            let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;
            let data_page = DataPage::from_page(page)?;

            // Scan entries in page
            for (key, value) in data_page.iter() {
                // Check bounds
                if let Some(ref start) = self.start {
                    if key < *start {
                        continue;
                    }
                }

                if let Some(ref end) = self.end {
                    if key >= *end {
                        return Ok(TaskResult::Scan(results));
                    }
                }

                results.push((Bytes::from(key.as_bytes()), Bytes::from(value)));
                count += 1;

                if count >= self.limit {
                    return Ok(TaskResult::Scan(results));
                }
            }

            // Move to next page
            current_page = if self.reverse {
                self.page_mapper.get_prev_page(page_id).await?
            } else {
                self.page_mapper.get_next_page(page_id).await?
            };
        }

        Ok(TaskResult::Scan(results))
    }

    fn cancel(&mut self) {
        // Scan can be cancelled between pages
    }

    fn estimate_cost(&self) -> usize {
        // Estimate based on limit and average entries per page
        (self.limit / 50).max(1)
    }
}

/// Async iterator for scan results
pub struct ScanIterator {
    /// Start key
    start: Option<Key>,
    /// End key
    end: Option<Key>,
    /// Remaining limit
    remaining: usize,
    /// Reverse scan
    reverse: bool,
    /// Current page ID
    current_page: Option<PageId>,
    /// Current page data
    current_data: Option<Arc<DataPage>>,
    /// Position in current page
    current_pos: usize,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// I/O manager
    io_manager: Arc<UringManager>,
}

impl ScanIterator {
    /// Create a new scan iterator
    fn new(
        start: Option<Key>,
        end: Option<Key>,
        limit: usize,
        reverse: bool,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        Self {
            start,
            end,
            remaining: limit,
            reverse,
            current_page: None,
            current_data: None,
            current_pos: 0,
            page_cache,
            page_mapper,
            io_manager,
        }
    }

    /// Load the next page
    async fn load_next_page(&mut self) -> Result<bool> {
        let next_page = if let Some(current) = self.current_page {
            if self.reverse {
                self.page_mapper.get_prev_page(current).await?
            } else {
                self.page_mapper.get_next_page(current).await?
            }
        } else {
            // Initial page
            if let Some(ref start_key) = self.start {
                self.page_mapper.find_page_for_key(start_key).await?
            } else if self.reverse {
                self.page_mapper.get_last_page().await?
            } else {
                self.page_mapper.get_first_page().await?
            }
        };

        if let Some(page_id) = next_page {
            let mapping = self.page_mapper.get_mapping(page_id).await?;
            let page = self.io_manager.read_page(mapping.file_id, mapping.offset).await?;
            let data_page = Arc::new(DataPage::from_page(page)?);

            self.current_page = Some(page_id);
            self.current_data = Some(data_page);
            self.current_pos = 0;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get the next entry
    pub async fn next(&mut self) -> Result<Option<(Key, Value)>> {
        if self.remaining == 0 {
            return Ok(None);
        }

        // Load initial page if needed
        if self.current_data.is_none() {
            if !self.load_next_page().await? {
                return Ok(None);
            }
        }

        loop {
            if let Some(ref data_page) = self.current_data {
                let entries: Vec<(Key, Value)> = data_page.iter().collect();

                if self.current_pos < entries.len() {
                    let (key, value) = &entries[self.current_pos];
                    self.current_pos += 1;

                    // Check bounds
                    if let Some(ref start) = self.start {
                        if key < start {
                            continue;
                        }
                    }

                    if let Some(ref end) = self.end {
                        if key >= end {
                            return Ok(None);
                        }
                    }

                    self.remaining -= 1;
                    return Ok(Some((key.clone(), value.clone())));
                }
            }

            // Need next page
            if !self.load_next_page().await? {
                return Ok(None);
            }
        }
    }
}

/// Stream adapter for scan iterator
pub struct ScanStream {
    /// Inner iterator
    iterator: ScanIterator,
    /// Result channel
    sender: mpsc::Sender<Result<(Key, Value)>>,
    /// Result receiver
    receiver: mpsc::Receiver<Result<(Key, Value)>>,
}

impl ScanStream {
    /// Create a new scan stream
    pub fn new(iterator: ScanIterator) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self {
            iterator,
            sender,
            receiver,
        }
    }

    /// Start the background scan
    pub fn start(mut self) -> impl Stream<Item = Result<(Key, Value)>> {
        let mut iterator = self.iterator;
        let sender = self.sender;

        tokio::spawn(async move {
            while let Ok(Some(entry)) = iterator.next().await {
                if sender.send(Ok(entry)).await.is_err() {
                    break;
                }
            }
        });

        // Return stream wrapper
        ScanStreamWrapper {
            receiver: self.receiver,
        }
    }
}

/// Stream wrapper for scan results
struct ScanStreamWrapper {
    receiver: mpsc::Receiver<Result<(Key, Value)>>,
}

impl Stream for ScanStreamWrapper {
    type Item = Result<(Key, Value)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// Prefix scan task
#[derive(Debug)]
pub struct PrefixScanTask {
    /// Key prefix
    prefix: Key,
    /// Maximum results
    limit: usize,
    /// Inner scan task
    scan: ScanTask,
}

impl PrefixScanTask {
    /// Create a new prefix scan
    pub fn new(
        prefix: Key,
        limit: usize,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        io_manager: Arc<UringManager>,
    ) -> Self {
        // Create end key by incrementing last byte
        let mut end_key = prefix.as_bytes().to_vec();
        let mut carry = true;
        for byte in end_key.iter_mut().rev() {
            if *byte < 0xff {
                *byte += 1;
                carry = false;
                break;
            }
        }

        let end = if carry {
            None // Prefix is all 0xff, scan to end
        } else {
            Some(Key::from(end_key))
        };

        let scan = ScanTask::new(
            Some(prefix.clone()),
            end,
            limit,
            false,
            page_cache,
            page_mapper,
            io_manager,
        );

        Self {
            prefix,
            limit,
            scan,
        }
    }
}

#[async_trait]
impl Task for PrefixScanTask {
    fn task_type(&self) -> TaskType {
        TaskType::Scan
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    async fn execute(&mut self, context: TaskContext) -> Result<TaskResult> {
        // Use inner scan task
        self.scan.execute(context).await
    }

    fn cancel(&mut self) {
        self.scan.cancel();
    }

    fn estimate_cost(&self) -> usize {
        self.scan.estimate_cost()
    }
}