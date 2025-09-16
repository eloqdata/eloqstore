//! Overflow page for storing large values that don't fit in a single data page

use bytes::{Bytes, BytesMut};
use crate::types::{PageId, PageType, MAX_PAGE_ID};
use super::page::{Page, HEADER_SIZE};

/// Overflow page structure
///
/// Format:
/// ```text
/// +------------+--------+------------------+-------------+-------------+
/// |checksum(8B)|type(1B)|content length(2B)|prev page(4B)|next page(4B)|
/// +------------+--------+------------------+-------------+-------------+
/// +--------------------+-------------+
/// |value data          |padding bytes|
/// +--------------------+-------------+
/// ```
pub struct OverflowPage {
    /// Page ID
    page_id: PageId,
    /// Underlying page
    page: Page,
}

impl OverflowPage {
    /// Create a new empty overflow page
    pub fn new(page_id: PageId, page_size: usize) -> Self {
        let mut page = Page::new(page_size);
        page.set_page_type(PageType::Overflow);
        page.set_prev_page_id(MAX_PAGE_ID);
        page.set_next_page_id(MAX_PAGE_ID);
        page.set_content_length(0);

        Self { page_id, page }
    }

    /// Create from an existing page
    pub fn from_page(page_id: PageId, page: Page) -> Self {
        Self { page_id, page }
    }

    /// Get page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Set page ID
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    /// Get previous overflow page ID
    pub fn prev_page_id(&self) -> PageId {
        self.page.prev_page_id()
    }

    /// Set previous overflow page ID
    pub fn set_prev_page_id(&mut self, page_id: PageId) {
        self.page.set_prev_page_id(page_id);
    }

    /// Get next overflow page ID
    pub fn next_page_id(&self) -> PageId {
        self.page.next_page_id()
    }

    /// Set next overflow page ID
    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.page.set_next_page_id(page_id);
    }

    /// Get the value data stored in this page
    pub fn value_data(&self) -> Bytes {
        let content_len = self.page.content_length() as usize;
        if content_len == 0 {
            return Bytes::new();
        }

        let data = self.page.as_bytes();
        Bytes::copy_from_slice(&data[HEADER_SIZE..HEADER_SIZE + content_len])
    }

    /// Set the value data for this page
    pub fn set_value_data(&mut self, data: &[u8]) -> bool {
        let max_content = self.page.size() - HEADER_SIZE;

        if data.len() > max_content {
            return false; // Data too large
        }

        // Copy data
        let page_bytes = self.page.as_bytes_mut();
        page_bytes[HEADER_SIZE..HEADER_SIZE + data.len()].copy_from_slice(data);

        // Update content length
        self.page.set_content_length(data.len() as u16);

        // Update checksum
        self.page.update_checksum();

        true
    }

    /// Get available space for value data
    pub fn available_space(&self) -> usize {
        self.page.size() - HEADER_SIZE
    }

    /// Check if page is empty
    pub fn is_empty(&self) -> bool {
        self.page.content_length() == 0
    }

    /// Clear the page
    pub fn clear(&mut self) {
        self.page.clear();
        self.page.set_page_type(PageType::Overflow);
    }

    /// Get underlying page
    pub fn page(&self) -> &Page {
        &self.page
    }

    /// Get mutable underlying page
    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.page
    }

    /// Take ownership of underlying page
    pub fn into_page(self) -> Page {
        self.page
    }
}

/// Builder for creating overflow page chains
pub struct OverflowChainBuilder {
    /// Page size
    page_size: usize,
    /// Current page being built
    current_page: Option<OverflowPage>,
    /// Completed pages
    pages: Vec<OverflowPage>,
    /// Next page ID to allocate
    next_page_id: PageId,
}

impl OverflowChainBuilder {
    /// Create a new overflow chain builder
    pub fn new(page_size: usize, start_page_id: PageId) -> Self {
        Self {
            page_size,
            current_page: None,
            pages: Vec::new(),
            next_page_id: start_page_id,
        }
    }

    /// Add data to the overflow chain
    pub fn add_data(&mut self, data: &[u8]) -> Vec<PageId> {
        let mut remaining = data;
        let mut page_ids = Vec::new();

        while !remaining.is_empty() {
            // Create new page if needed
            if self.current_page.is_none() {
                let page_id = self.next_page_id;
                self.next_page_id += 1;
                self.current_page = Some(OverflowPage::new(page_id, self.page_size));
            }

            let page = self.current_page.as_mut().unwrap();
            let available = page.available_space();

            let to_write = remaining.len().min(available);
            page.set_value_data(&remaining[..to_write]);

            page_ids.push(page.page_id());

            // Move to next chunk
            remaining = &remaining[to_write..];

            // Finalize current page if full or done
            if to_write == available || remaining.is_empty() {
                let mut finished = self.current_page.take().unwrap();

                // Link pages
                if !self.pages.is_empty() {
                    let prev_idx = self.pages.len() - 1;
                    let prev_id = self.pages[prev_idx].page_id();
                    finished.set_prev_page_id(prev_id);
                    self.pages[prev_idx].set_next_page_id(finished.page_id());
                }

                self.pages.push(finished);
            }
        }

        page_ids
    }

    /// Finish building and return all overflow pages
    pub fn finish(mut self) -> Vec<OverflowPage> {
        // Finalize any remaining page
        if let Some(page) = self.current_page.take() {
            if !page.is_empty() {
                self.pages.push(page);
            }
        }

        // Update checksums
        for page in &mut self.pages {
            page.page_mut().update_checksum();
        }

        self.pages
    }
}

/// Reader for overflow page chains
pub struct OverflowChainReader {
    /// Buffer to accumulate value data
    buffer: BytesMut,
}

impl OverflowChainReader {
    /// Create a new overflow chain reader
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }

    /// Add a page to the chain
    pub fn add_page(&mut self, page: &OverflowPage) {
        let data = page.value_data();
        self.buffer.extend_from_slice(&data);
    }

    /// Get the complete value
    pub fn finish(self) -> Bytes {
        self.buffer.freeze()
    }

    /// Get current buffer size
    pub fn current_size(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overflow_page_basic() {
        let mut page = OverflowPage::new(100, 4096);

        assert_eq!(page.page_id(), 100);
        assert!(page.is_empty());

        let data = b"Hello, overflow page!";
        assert!(page.set_value_data(data));

        let retrieved = page.value_data();
        assert_eq!(retrieved.as_ref(), data);
    }

    #[test]
    fn test_overflow_page_links() {
        let mut page = OverflowPage::new(200, 4096);

        page.set_prev_page_id(199);
        page.set_next_page_id(201);

        assert_eq!(page.prev_page_id(), 199);
        assert_eq!(page.next_page_id(), 201);
    }

    #[test]
    fn test_overflow_chain_builder() {
        let page_size = 256; // Small pages for testing
        let mut builder = OverflowChainBuilder::new(page_size, 1000);

        // Create data larger than one page
        let data = vec![b'x'; 500];

        let page_ids = builder.add_data(&data);
        assert!(page_ids.len() >= 2); // Should need at least 2 pages

        let pages = builder.finish();
        assert_eq!(pages.len(), page_ids.len());

        // Verify linking
        for i in 1..pages.len() {
            assert_eq!(pages[i].prev_page_id(), pages[i - 1].page_id());
            assert_eq!(pages[i - 1].next_page_id(), pages[i].page_id());
        }
    }

    #[test]
    fn test_overflow_chain_reader() {
        let page_size = 256;
        let mut builder = OverflowChainBuilder::new(page_size, 2000);

        let original_data = b"This is a test of overflow chain reading with multiple pages";
        builder.add_data(original_data);

        let pages = builder.finish();

        // Read the chain
        let mut reader = OverflowChainReader::new();
        for page in &pages {
            reader.add_page(page);
        }

        let recovered = reader.finish();
        assert_eq!(recovered.as_ref(), original_data);
    }

    #[test]
    fn test_large_overflow_value() {
        let page_size = 4096;
        let mut builder = OverflowChainBuilder::new(page_size, 3000);

        // Create a large value
        let large_data = vec![b'a'; 10000];

        let page_ids = builder.add_data(&large_data);
        let pages = builder.finish();

        assert!(pages.len() >= 3); // Should need multiple pages

        // Verify we can read it back
        let mut reader = OverflowChainReader::new();
        for page in &pages {
            reader.add_page(page);
        }

        let recovered = reader.finish();
        assert_eq!(recovered.len(), large_data.len());
        assert_eq!(recovered.as_ref(), large_data.as_slice());
    }
}