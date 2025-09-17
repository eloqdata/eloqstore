/// Read task implementation following C++ read_task.cpp exactly

use async_trait::async_trait;
use bytes::Bytes;

use crate::types::{Key, Value, TableIdent, PageId, FilePageId, MAX_PAGE_ID};
use crate::page::{DataPage, DataPageIterator, PageCache};
use crate::page::PageMapper;
use crate::storage::AsyncFileManager;
use crate::index::IndexPageManager;
use crate::config::KvOptions;
use crate::Result;
use crate::error::{Error, KvError};

use std::sync::Arc;

use super::traits::{Task, TaskResult, TaskPriority, TaskContext};

/// Read task following C++ ReadTask exactly
#[derive(Debug)]
pub struct ReadTask {
    /// Table identifier
    table_id: TableIdent,
    /// Key to read
    key: Key,
    /// Page cache
    page_cache: Arc<PageCache>,
    /// Page mapper
    page_mapper: Arc<PageMapper>,
    /// File manager
    file_manager: Arc<AsyncFileManager>,
    /// Index manager
    index_manager: Arc<IndexPageManager>,
    /// Options
    options: Arc<KvOptions>,
}

impl ReadTask {
    /// Create a new read task
    pub fn new(
        table_id: TableIdent,
        key: Key,
        page_cache: Arc<PageCache>,
        page_mapper: Arc<PageMapper>,
        file_manager: Arc<AsyncFileManager>,
        index_manager: Arc<IndexPageManager>,
        options: Arc<KvOptions>,
    ) -> Self {
        Self {
            table_id,
            key,
            page_cache,
            page_mapper,
            file_manager,
            index_manager,
            options,
        }
    }

    /// Load data page (following C++ LoadDataPage)
    async fn load_data_page(&self, page_id: PageId, file_page_id: FilePageId) -> Result<DataPage> {
        tracing::debug!("LoadDataPage: page_id={}, file_page={:?}", page_id, file_page_id);

        // Read from disk
        let page_data = self.file_manager.read_page(
            file_page_id.file_id() as u64,
            file_page_id.page_offset()
        ).await?;

        let data_page = DataPage::from_page(page_id, page_data);

        Ok(data_page)
    }

    /// Read implementation (following C++ Read exactly)
    pub async fn read(&self) -> Result<Option<(Value, u64, u64)>> {
        tracing::debug!("ReadTask::read key={:?}, table={:?}",
            String::from_utf8_lossy(&self.key), self.table_id);

        // Following C++ line 17-18: FindRoot
        let root_id = {
            let meta = self.index_manager.find_root(&self.table_id)?;
            let id = meta.root_id;
            drop(meta); // Drop RootMeta before await to avoid Send issues
            id
        };

        // Following C++ line 19-22: Check if empty
        if root_id == MAX_PAGE_ID {
            tracing::debug!("Root is MAX_PAGE_ID, key not found");
            return Ok(None);
        }

        // Following C++ line 23: GetMappingSnapshot
        let mapping = self.page_mapper.snapshot();

        // Following C++ line 25-28: SeekIndex
        let page_id = self.index_manager.seek_index(
            &mapping,
            root_id,
            &self.key
        ).await?;

        tracing::debug!("SeekIndex returned page_id={}", page_id);

        // Following C++ line 29: ToFilePage
        let file_page = mapping.to_file_page(page_id)?;

        // Following C++ line 30-31: LoadDataPage
        let data_page = self.load_data_page(page_id, file_page).await?;

        // Following C++ line 33: Create DataPageIter
        let mut iter = DataPageIterator::new(&data_page);

        // Following C++ line 34: Seek
        let found = iter.seek(&self.key);

        // Following C++ line 35-38: Check if found and exact match
        if !found {
            tracing::debug!("Key not found in data page");
            return Ok(None);
        }

        // Check exact match
        if let Some(iter_key) = iter.key() {
            // Use comparator to check if keys match
            if iter_key != self.key.as_ref() {
                tracing::debug!("Key mismatch: {:?} != {:?}",
                    String::from_utf8_lossy(&iter_key),
                    String::from_utf8_lossy(&self.key));
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        // Following C++ line 40-49: Get value (handle overflow if needed)
        let value = if iter.is_overflow() {
            // TODO: Following C++ line 42-44: GetOverflowValue
            // For now, just use the value as-is
            iter.value().unwrap_or(Bytes::new())
        } else {
            // Following C++ line 48
            iter.value().unwrap_or(Bytes::new())
        };

        // Following C++ line 50-51: Get timestamp and expire_ts
        let timestamp = iter.timestamp();
        let expire_ts = iter.expire_ts().unwrap_or(0);

        Ok(Some((Value::from(value), timestamp, expire_ts)))
    }

    /// Floor implementation (following C++ Floor exactly)
    pub async fn floor(&self) -> Result<Option<(Key, Value, u64, u64)>> {
        tracing::debug!("ReadTask::floor key={:?}, table={:?}",
            String::from_utf8_lossy(&self.key), self.table_id);

        // Following C++ line 62-63: FindRoot
        let root_id = {
            let meta = self.index_manager.find_root(&self.table_id)?;
            let id = meta.root_id;
            drop(meta); // Drop RootMeta before await to avoid Send issues
            id
        };

        // Following C++ line 64-67: Check if empty
        if root_id == MAX_PAGE_ID {
            return Ok(None);
        }

        // Following C++ line 68: GetMappingSnapshot
        let mapping = self.page_mapper.snapshot();

        // Following C++ line 70-73: SeekIndex
        let page_id = self.index_manager.seek_index(
            &mapping,
            root_id,
            &self.key
        ).await?;

        // Following C++ line 74: ToFilePage
        let file_page = mapping.to_file_page(page_id)?;

        // Following C++ line 75-76: LoadDataPage
        let mut data_page = self.load_data_page(page_id, file_page).await?;

        // Following C++ line 78: Create DataPageIter
        let mut iter = DataPageIterator::new(&data_page);

        // Following C++ line 79-93: SeekFloor with prev page handling
        if !iter.seek_floor(&self.key) {
            // Need to check previous page
            let prev_page_id = data_page.prev_page_id();
            if prev_page_id == MAX_PAGE_ID {
                return Ok(None);
            }

            let prev_file_page = mapping.to_file_page(prev_page_id)?;

            data_page = self.load_data_page(prev_page_id, prev_file_page).await?;
            iter = DataPageIterator::new(&data_page);

            if !iter.seek_floor(&self.key) {
                return Ok(None);
            }
        }

        // Following C++ line 94: Get floor_key
        let floor_key = iter.key().ok_or_else(|| Error::InvalidState("No key".into()))?;

        // Following C++ line 95-104: Get value
        let value = if iter.is_overflow() {
            // TODO: GetOverflowValue
            iter.value().unwrap_or(Bytes::new())
        } else {
            iter.value().unwrap_or(Bytes::new())
        };

        let timestamp = iter.timestamp();
        let expire_ts = iter.expire_ts().unwrap_or(0);

        Ok(Some((Key::from(floor_key), Value::from(value), timestamp, expire_ts)))
    }
}

#[async_trait]
impl Task for ReadTask {
    fn task_type(&self) -> crate::task::traits::TaskType {
        crate::task::traits::TaskType::Read
    }

    fn priority(&self) -> TaskPriority {
        TaskPriority::High
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn execute(&self, _context: &TaskContext) -> Result<TaskResult> {
        match self.read().await? {
            Some((value, _timestamp, _expire_ts)) => {
                // TaskResult::Read only contains Option<Bytes>
                Ok(TaskResult::Read(Some(value.into())))
            }
            None => {
                Ok(TaskResult::Read(None))
            }
        }
    }
}