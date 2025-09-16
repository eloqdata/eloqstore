//! Data page builder with restart points for efficient binary search

use bytes::{BufMut, BytesMut};
use std::cmp;

use crate::codec::encoding::{encode_varint_into, KvEncoder};
use crate::types::{PageId, PageType, MAX_PAGE_ID};
use super::page::{Page, HEADER_SIZE};
use super::data_page::DataPage;

/// Default restart interval (every 16 entries)
const DEFAULT_RESTART_INTERVAL: usize = 16;

/// Data page builder
pub struct DataPageBuilder {
    /// Page data buffer
    buffer: BytesMut,
    /// Page size
    page_size: usize,
    /// Restart points (offsets from start of content)
    restart_points: Vec<u16>,
    /// Restart interval
    restart_interval: usize,
    /// Number of entries since last restart
    entries_since_restart: usize,
    /// Current content size
    content_size: usize,
    /// Last key for prefix compression (future enhancement)
    last_key: Vec<u8>,
    /// KV encoder
    encoder: KvEncoder,
}

impl DataPageBuilder {
    /// Create a new data page builder
    pub fn new(page_size: usize) -> Self {
        let mut buffer = BytesMut::zeroed(page_size);

        // Set page type
        buffer[super::page::PAGE_TYPE_OFFSET] = PageType::Data as u8;

        // Set prev/next page IDs to MAX
        let prev_bytes = MAX_PAGE_ID.to_le_bytes();
        buffer[super::page::PREV_PAGE_OFFSET..super::page::PREV_PAGE_OFFSET + 4]
            .copy_from_slice(&prev_bytes);

        let next_bytes = MAX_PAGE_ID.to_le_bytes();
        buffer[super::page::NEXT_PAGE_OFFSET..super::page::NEXT_PAGE_OFFSET + 4]
            .copy_from_slice(&next_bytes);

        Self {
            buffer,
            page_size,
            restart_points: Vec::new(),
            restart_interval: DEFAULT_RESTART_INTERVAL,
            entries_since_restart: 0,
            content_size: 0,
            last_key: Vec::new(),
            encoder: KvEncoder::new(),
        }
    }

    /// Set restart interval
    pub fn with_restart_interval(mut self, interval: usize) -> Self {
        self.restart_interval = interval;
        self
    }

    /// Add a key-value entry
    pub fn add(
        &mut self,
        key: &[u8],
        value: &[u8],
        timestamp: u64,
        expire_ts: Option<u64>,
        is_overflow: bool,
    ) -> bool {
        // Check if we need a restart point
        if self.entries_since_restart >= self.restart_interval {
            self.restart_points.push(self.content_size as u16);
            self.entries_since_restart = 0;
        }

        // Encode the entry
        let encoded = self.encoder.encode_entry(key, value, timestamp, expire_ts, is_overflow);
        let entry_size = encoded.len();

        // Calculate space needed
        let restart_array_size = (self.restart_points.len() + 1) * 2; // +1 for potential new restart
        let total_needed = HEADER_SIZE + self.content_size + entry_size + restart_array_size + 2; // +2 for restart count

        // Check if entry fits
        if total_needed > self.page_size {
            return false;
        }

        // Add entry to buffer
        self.buffer[HEADER_SIZE + self.content_size..HEADER_SIZE + self.content_size + entry_size]
            .copy_from_slice(&encoded);

        self.content_size += entry_size;
        self.entries_since_restart += 1;
        self.last_key = key.to_vec();

        true
    }

    /// Check if the builder is empty
    pub fn is_empty(&self) -> bool {
        self.content_size == 0
    }

    /// Get current size used
    pub fn current_size(&self) -> usize {
        HEADER_SIZE + self.content_size + self.restart_points.len() * 2 + 2
    }

    /// Get available space
    pub fn available_space(&self) -> usize {
        self.page_size.saturating_sub(self.current_size())
    }

    /// Estimate if an entry will fit
    pub fn will_fit(&self, key_size: usize, value_size: usize) -> bool {
        // Rough estimate: key_len(5) + val_len(5) + timestamp(10) + expire(10) + key + value
        let estimated_size = 30 + key_size + value_size;
        let restart_overhead = if self.entries_since_restart >= self.restart_interval {
            2 // New restart point
        } else {
            0
        };

        self.available_space() >= estimated_size + restart_overhead
    }

    /// Finish building and return the data page
    pub fn finish(mut self, page_id: PageId) -> DataPage {
        // Always add the first entry as a restart point
        if self.content_size > 0 && (self.restart_points.is_empty() || self.restart_points[0] != 0) {
            self.restart_points.insert(0, 0);
        }

        // Write content length
        let content_len_bytes = (self.content_size as u16).to_le_bytes();
        self.buffer[super::page::CONTENT_LENGTH_OFFSET..super::page::CONTENT_LENGTH_OFFSET + 2]
            .copy_from_slice(&content_len_bytes);

        // Write restart array at the end
        let restart_array_offset = self.page_size - 2 - (self.restart_points.len() * 2);
        for (i, &restart) in self.restart_points.iter().enumerate() {
            let offset = restart_array_offset + i * 2;
            self.buffer[offset..offset + 2].copy_from_slice(&restart.to_le_bytes());
        }

        // Write restart count
        let restart_count_offset = self.page_size - 2;
        let restart_count = self.restart_points.len() as u16;
        self.buffer[restart_count_offset..restart_count_offset + 2]
            .copy_from_slice(&restart_count.to_le_bytes());

        // Calculate and set checksum
        let mut page = Page::from_bytes(self.buffer.freeze());
        page.update_checksum();

        DataPage::from_page(page_id, page)
    }

    /// Reset the builder for reuse
    pub fn reset(&mut self) {
        self.buffer.fill(0);

        // Reset page type
        self.buffer[super::page::PAGE_TYPE_OFFSET] = PageType::Data as u8;

        // Reset prev/next page IDs
        let max_bytes = MAX_PAGE_ID.to_le_bytes();
        self.buffer[super::page::PREV_PAGE_OFFSET..super::page::PREV_PAGE_OFFSET + 4]
            .copy_from_slice(&max_bytes);
        self.buffer[super::page::NEXT_PAGE_OFFSET..super::page::NEXT_PAGE_OFFSET + 4]
            .copy_from_slice(&max_bytes);

        self.restart_points.clear();
        self.entries_since_restart = 0;
        self.content_size = 0;
        self.last_key.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic() {
        let mut builder = DataPageBuilder::new(4096);

        // Add some entries
        assert!(builder.add(b"key1", b"value1", 100, None, false));
        assert!(builder.add(b"key2", b"value2", 101, Some(1000), false));
        assert!(builder.add(b"key3", b"value3", 102, None, false));

        assert!(!builder.is_empty());

        let page = builder.finish(123);
        assert_eq!(page.page_id(), 123);
        assert!(page.content_length() > 0);
        assert!(page.restart_count() > 0);
    }

    #[test]
    fn test_builder_restart_points() {
        let mut builder = DataPageBuilder::new(4096)
            .with_restart_interval(2); // Restart every 2 entries

        // Add entries
        for i in 0..10 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            assert!(builder.add(key.as_bytes(), value.as_bytes(), i as u64, None, false));
        }

        let page = builder.finish(456);

        // Should have restart points: 0, 2, 4, 6, 8
        assert_eq!(page.restart_count(), 5);
        assert_eq!(page.restart_point(0), Some(0));
    }

    #[test]
    fn test_builder_overflow_check() {
        let mut builder = DataPageBuilder::new(256); // Small page

        // Try to add entries until full
        let mut added = 0;
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            if builder.add(key.as_bytes(), value.as_bytes(), i as u64, None, false) {
                added += 1;
            } else {
                break;
            }
        }

        assert!(added > 0);
        assert!(added < 100); // Should not fit all entries

        let page = builder.finish(789);
        assert!(page.page().verify_checksum());
    }

    #[test]
    fn test_builder_reset() {
        let mut builder = DataPageBuilder::new(4096);

        // Add an entry
        assert!(builder.add(b"key1", b"value1", 100, None, false));
        assert!(!builder.is_empty());

        // Reset and reuse
        builder.reset();
        assert!(builder.is_empty());

        // Add new entries
        assert!(builder.add(b"key2", b"value2", 200, None, false));

        let page = builder.finish(999);
        assert_eq!(page.page_id(), 999);
    }
}