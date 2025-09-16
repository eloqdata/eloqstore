//! Base page structure and operations

use bytes::{Bytes, BytesMut, BufMut};
use crc32c::crc32c;
use std::mem;
use std::ptr;
use std::ops::{Index, IndexMut, Range};

use crate::types::{PageId, PageType, DEFAULT_PAGE_SIZE};
use crate::Result;

/// Page header offsets
pub const CHECKSUM_OFFSET: usize = 0;
pub const PAGE_TYPE_OFFSET: usize = 8;
pub const CONTENT_LENGTH_OFFSET: usize = 9;
pub const PREV_PAGE_OFFSET: usize = 11;
pub const NEXT_PAGE_OFFSET: usize = 15;
pub const HEADER_SIZE: usize = 19;

/// Page header structure
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    /// Checksum
    pub checksum: u64,
    /// Page type
    pub page_type: PageType,
    /// Content length
    pub content_length: u16,
    /// Previous page ID
    pub prev_page: u32,
    /// Next page ID
    pub next_page: u32,
}

impl PageHeader {
    /// Header size in bytes
    pub const SIZE: usize = HEADER_SIZE;

    /// Create a new data page header
    pub fn new_data(page_id: u32, entry_count: u16, content_len: u16) -> Self {
        Self {
            checksum: 0,
            page_type: PageType::Data,
            content_length: content_len,
            prev_page: u32::MAX,
            next_page: u32::MAX,
        }
    }

    /// Convert to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[8] = self.page_type as u8;
        bytes[9..11].copy_from_slice(&self.content_length.to_le_bytes());
        bytes[11..15].copy_from_slice(&self.prev_page.to_le_bytes());
        bytes[15..19].copy_from_slice(&self.next_page.to_le_bytes());
        bytes
    }
}

/// A page in the storage system
#[derive(Clone)]
pub struct Page {
    /// Page data
    data: Bytes,
    /// Page size
    size: usize,
}

impl Page {
    /// Create a new empty page
    pub fn new(size: usize) -> Self {
        let mut data = BytesMut::zeroed(size);
        Self {
            data: data.freeze(),
            size,
        }
    }

    /// Create a page from existing bytes
    pub fn from_bytes(data: Bytes) -> Self {
        let size = data.len();
        Self { data, size }
    }

    /// Get page size
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get page data as bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable page data
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        // Make the data unique if it's shared
        if !self.data.is_empty() {
            let mut data = BytesMut::from(self.data.as_ref());
            self.data = data.freeze();
        }

        // SAFETY: We just ensured the data is unique
        unsafe {
            let ptr = self.data.as_ptr() as *mut u8;
            std::slice::from_raw_parts_mut(ptr, self.data.len())
        }
    }

    /// Read checksum from page
    pub fn checksum(&self) -> u64 {
        if self.data.len() < CHECKSUM_OFFSET + 8 {
            return 0;
        }
        u64::from_le_bytes(self.data[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 8].try_into().unwrap())
    }

    /// Write checksum to page
    pub fn set_checksum(&mut self, checksum: u64) {
        if self.data.len() >= CHECKSUM_OFFSET + 8 {
            let bytes = checksum.to_le_bytes();
            self.as_bytes_mut()[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 8].copy_from_slice(&bytes);
        }
    }

    /// Calculate and set checksum for the page
    pub fn update_checksum(&mut self) {
        // Zero out checksum field first
        self.set_checksum(0);

        // Calculate CRC32C of entire page except checksum field
        let checksum = crc32c(&self.data[CHECKSUM_OFFSET + 8..]) as u64;
        self.set_checksum(checksum);
    }

    /// Verify page checksum
    pub fn verify_checksum(&self) -> bool {
        let stored = self.checksum();
        let calculated = crc32c(&self.data[CHECKSUM_OFFSET + 8..]) as u64;
        stored == calculated
    }

    /// Get page length
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Extend page from slice
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        let mut data = BytesMut::from(self.data.as_ref());
        data.put_slice(slice);
        self.data = data.freeze();
    }

    /// Get page type
    pub fn page_type(&self) -> Option<PageType> {
        if self.data.len() > PAGE_TYPE_OFFSET {
            PageType::from_u8(self.data[PAGE_TYPE_OFFSET])
        } else {
            None
        }
    }

    /// Set page type
    pub fn set_page_type(&mut self, page_type: PageType) {
        if self.data.len() > PAGE_TYPE_OFFSET {
            self.as_bytes_mut()[PAGE_TYPE_OFFSET] = page_type as u8;
        }
    }

    /// Get content length
    pub fn content_length(&self) -> u16 {
        if self.data.len() >= CONTENT_LENGTH_OFFSET + 2 {
            u16::from_le_bytes(
                self.data[CONTENT_LENGTH_OFFSET..CONTENT_LENGTH_OFFSET + 2]
                    .try_into()
                    .unwrap()
            )
        } else {
            0
        }
    }

    /// Set content length
    pub fn set_content_length(&mut self, length: u16) {
        if self.data.len() >= CONTENT_LENGTH_OFFSET + 2 {
            let bytes = length.to_le_bytes();
            self.as_bytes_mut()[CONTENT_LENGTH_OFFSET..CONTENT_LENGTH_OFFSET + 2]
                .copy_from_slice(&bytes);
        }
    }

    /// Get previous page ID
    pub fn prev_page_id(&self) -> PageId {
        if self.data.len() >= PREV_PAGE_OFFSET + 4 {
            u32::from_le_bytes(
                self.data[PREV_PAGE_OFFSET..PREV_PAGE_OFFSET + 4]
                    .try_into()
                    .unwrap()
            )
        } else {
            crate::types::MAX_PAGE_ID
        }
    }

    /// Set previous page ID
    pub fn set_prev_page_id(&mut self, page_id: PageId) {
        if self.data.len() >= PREV_PAGE_OFFSET + 4 {
            let bytes = page_id.to_le_bytes();
            self.as_bytes_mut()[PREV_PAGE_OFFSET..PREV_PAGE_OFFSET + 4]
                .copy_from_slice(&bytes);
        }
    }

    /// Get next page ID
    pub fn next_page_id(&self) -> PageId {
        if self.data.len() >= NEXT_PAGE_OFFSET + 4 {
            u32::from_le_bytes(
                self.data[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 4]
                    .try_into()
                    .unwrap()
            )
        } else {
            crate::types::MAX_PAGE_ID
        }
    }

    /// Set next page ID
    pub fn set_next_page_id(&mut self, page_id: PageId) {
        if self.data.len() >= NEXT_PAGE_OFFSET + 4 {
            let bytes = page_id.to_le_bytes();
            self.as_bytes_mut()[NEXT_PAGE_OFFSET..NEXT_PAGE_OFFSET + 4]
                .copy_from_slice(&bytes);
        }
    }

    /// Clear the page
    pub fn clear(&mut self) {
        self.as_bytes_mut().fill(0);
        self.set_page_type(PageType::Data);
        self.set_prev_page_id(crate::types::MAX_PAGE_ID);
        self.set_next_page_id(crate::types::MAX_PAGE_ID);
        self.set_content_length(0);
    }

    /// Check if page is empty
    pub fn is_empty(&self) -> bool {
        self.content_length() == 0
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new(DEFAULT_PAGE_SIZE)
    }
}


impl Index<Range<usize>> for Page {
    type Output = [u8];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        &self.data[range]
    }
}

impl IndexMut<Range<usize>> for Page {
    fn index_mut(&mut self, range: Range<usize>) -> &mut Self::Output {
        &mut self.as_bytes_mut()[range]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(4096);
        assert_eq!(page.size(), 4096);
        assert!(page.is_empty());
    }

    #[test]
    fn test_page_type() {
        let mut page = Page::new(4096);
        page.set_page_type(PageType::Data);
        assert_eq!(page.page_type(), Some(PageType::Data));

        page.set_page_type(PageType::Index);
        assert_eq!(page.page_type(), Some(PageType::Index));
    }

    #[test]
    fn test_page_links() {
        let mut page = Page::new(4096);
        page.set_prev_page_id(123);
        page.set_next_page_id(456);

        assert_eq!(page.prev_page_id(), 123);
        assert_eq!(page.next_page_id(), 456);
    }

    #[test]
    fn test_checksum() {
        let mut page = Page::new(4096);
        page.set_page_type(PageType::Data);
        page.set_content_length(100);

        page.update_checksum();
        assert!(page.verify_checksum());

        // Corrupt the page
        page.as_bytes_mut()[100] = 255;
        assert!(!page.verify_checksum());
    }
}
