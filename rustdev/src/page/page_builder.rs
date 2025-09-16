//! Page builder for constructing data pages

use bytes::{Bytes, BytesMut};

use crate::types::{Key, Value, PageId};
use crate::codec::encoding::KvEncoder;
use crate::Result;
use crate::error::Error;

use super::{Page, PageHeader, data_page::RestartPoint};

/// Page builder for creating data pages
pub struct PageBuilder {
    /// Page size
    page_size: usize,
    /// Page buffer
    buffer: BytesMut,
    /// Key-value entries
    entries: Vec<(Key, Value)>,
    /// Restart points
    restart_points: Vec<RestartPoint>,
    /// Restart interval
    restart_interval: usize,
    /// Current size estimate
    current_size: usize,
}

impl PageBuilder {
    /// Create a new page builder
    pub fn new(page_size: usize) -> Self {
        Self {
            page_size,
            buffer: BytesMut::with_capacity(page_size),
            entries: Vec::new(),
            restart_points: Vec::new(),
            restart_interval: 16, // Default restart interval
            current_size: PageHeader::SIZE,
        }
    }

    /// Set restart interval
    pub fn with_restart_interval(mut self, interval: usize) -> Self {
        self.restart_interval = interval;
        self
    }

    /// Check if an entry can fit
    pub fn can_add(&self, key: &Key, value: &Value) -> bool {
        let entry_size = Self::estimate_entry_size(key, value);
        let restart_space = if (self.entries.len() + 1) % self.restart_interval == 0 {
            8 // Size of RestartPoint
        } else {
            0
        };

        self.current_size + entry_size + restart_space <= self.page_size - 100 // Leave some margin
    }

    /// Add an entry to the page
    pub fn add_entry(&mut self, key: Key, value: Value) -> Result<()> {
        if !self.can_add(&key, &value) {
            return Err(Error::PageFull);
        }

        // Add restart point if needed
        if self.entries.len() % self.restart_interval == 0 {
            self.restart_points.push(RestartPoint {
                offset: self.current_size as u32 - PageHeader::SIZE as u32,
                key_len: key.len() as u32,
            });
        }

        self.current_size += Self::estimate_entry_size(&key, &value);
        self.entries.push((key, value));

        Ok(())
    }

    /// Get number of entries
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Get first key
    pub fn first_key(&self) -> Option<Key> {
        self.entries.first().map(|(k, _)| k.clone())
    }

    /// Get last key
    pub fn last_key(&self) -> Option<Key> {
        self.entries.last().map(|(k, _)| k.clone())
    }

    /// Build the page
    pub fn build(mut self) -> Result<Page> {
        if self.entries.is_empty() {
            return Err(Error::InvalidInput("Cannot build empty page".into()));
        }

        // Create page
        let mut page = Page::new(self.page_size);
        page.clear();

        // Write header placeholder
        let header_pos = page.len();
        page.extend_from_slice(&vec![0u8; PageHeader::SIZE]);

        // Write entries
        let mut encoder = KvEncoder::new();
        let mut data_start = page.len();

        for (key, value) in &self.entries {
            encoder.encode_key(&mut page, key)?;
            encoder.encode_value(&mut page, value)?;
        }

        let data_end = page.len();

        // Write restart points
        let restart_offset = page.len();
        for restart in &self.restart_points {
            page.extend_from_slice(&restart.offset.to_le_bytes());
            page.extend_from_slice(&restart.key_len.to_le_bytes());
        }

        // Write number of restart points
        page.extend_from_slice(&(self.restart_points.len() as u32).to_le_bytes());

        // Create and write header
        let header = PageHeader::new_data(
            0, // Page ID will be set later
            self.entries.len() as u16,
            (data_end - data_start) as u16,
        );

        // Update header in page
        let header_bytes = header.to_bytes();
        page[header_pos..header_pos + PageHeader::SIZE].copy_from_slice(&header_bytes);

        // Update checksum
        page.update_checksum();

        Ok(page)
    }

    /// Clear the builder
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.entries.clear();
        self.restart_points.clear();
        self.current_size = PageHeader::SIZE;
    }

    /// Estimate entry size
    fn estimate_entry_size(key: &Key, value: &Value) -> usize {
        4 + key.len() + 4 + value.len() // Length prefix + data for both key and value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::DataPage;

    #[test]
    fn test_page_builder() {
        let mut builder = PageBuilder::new(4096);

        // Add some entries
        for i in 0..10 {
            let key = Bytes::from(format!("key_{:04}", i));
            let value = Bytes::from(format!("value_{}", i));
            builder.add_entry(key, value).unwrap();
        }

        assert_eq!(builder.entry_count(), 10);
        assert_eq!(builder.first_key(), Some(Bytes::from("key_0000")));
        assert_eq!(builder.last_key(), Some(Bytes::from("key_0009")));

        // Build page
        let page = builder.build().unwrap();
        assert!(page.verify_checksum());

        // Verify we can read it back
        let data_page = DataPage::from_page(page).unwrap();
        assert_eq!(data_page.entry_count(), 10);
    }

    #[test]
    fn test_page_builder_full() {
        let mut builder = PageBuilder::new(256); // Small page

        // Add entries until full
        let mut count = 0;
        loop {
            let key = Bytes::from(format!("key_{:04}", count));
            let value = Bytes::from(format!("value_{}", count));

            if !builder.can_add(&key, &value) {
                break;
            }

            builder.add_entry(key, value).unwrap();
            count += 1;
        }

        assert!(count > 0);

        // Try to add one more - should fail
        let key = Bytes::from("extra_key");
        let value = Bytes::from("extra_value");
        assert!(!builder.can_add(&key, &value));
        assert!(builder.add_entry(key, value).is_err());
    }
}