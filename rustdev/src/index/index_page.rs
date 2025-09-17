//! In-memory index page implementation
//! Following the C++ mem_index_page.h/cpp

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};


use crate::config::KvOptions;
use crate::types::{PageId, FilePageId, TableIdent, MAX_PAGE_ID, MAX_FILE_PAGE_ID};
use crate::page::{Page, HEADER_SIZE};
use crate::codec::Comparator;

/// Maximum index page size (64KB)
pub const MAX_INDEX_PAGE_SIZE: usize = 1 << 16;

/// Offsets in index page header
pub const PAGE_SIZE_OFFSET: usize = HEADER_SIZE;
pub const LEFTMOST_PTR_OFFSET: usize = PAGE_SIZE_OFFSET + 2;

/// In-memory index page
#[derive(Debug)]
pub struct MemIndexPage {
    /// Logical page ID
    page_id: PageId,
    /// Physical file page ID
    file_page_id: FilePageId,
    /// Page data
    page: Page,
    /// Reference count for pinning
    ref_cnt: AtomicU32,
    /// Waiting zone for I/O completion
    waiting: Arc<Mutex<Vec<tokio::sync::oneshot::Sender<()>>>>,
    /// Next page in LRU list
    next: Option<*mut MemIndexPage>,
    /// Previous page in LRU list
    prev: Option<*mut MemIndexPage>,
    /// Table identifier this page belongs to
    table_ident: Option<Arc<TableIdent>>,
}

// Safe because we carefully manage the raw pointers
unsafe impl Send for MemIndexPage {}
unsafe impl Sync for MemIndexPage {}

impl MemIndexPage {
    /// Create a new index page
    pub fn new(alloc: bool) -> Self {
        let page = if alloc {
            Page::new(4096) // Default page size
        } else {
            Page::empty()
        };

        Self {
            page_id: MAX_PAGE_ID,
            file_page_id: MAX_FILE_PAGE_ID,
            page,
            ref_cnt: AtomicU32::new(0),
            waiting: Arc::new(Mutex::new(Vec::new())),
            next: None,
            prev: None,
            table_ident: None,
        }
    }

    /// Get content length from page header
    pub fn content_length(&self) -> u16 {
        self.page.content_length()
    }

    /// Get number of restart points
    pub fn restart_num(&self) -> u16 {
        // Restart points are stored at the end of the page
        let data = self.page.as_bytes();
        if data.len() < 2 {
            return 0;
        }
        let offset = data.len() - 2;
        u16::from_le_bytes([data[offset], data[offset + 1]])
    }

    /// Get page data pointer (for C++ compatibility)
    pub fn page_ptr(&self) -> &[u8] {
        self.page.as_bytes()
    }

    /// Check if this index page points to leaf pages (data pages)
    pub fn is_pointing_to_leaf(&self) -> bool {
        // Check a flag in the page header or metadata
        // In the C++ code this checks the page type or a specific flag
        let data = self.page.as_bytes();
        if data.len() > HEADER_SIZE {
            // Check page type byte
            data[8] & 0x01 != 0 // Bit flag for leaf pointer
        } else {
            false
        }
    }

    /// Pin the page (increment reference count)
    pub fn pin(&self) {
        self.ref_cnt.fetch_add(1, Ordering::SeqCst);
    }

    /// Unpin the page (decrement reference count)
    pub fn unpin(&self) {
        let prev = self.ref_cnt.fetch_sub(1, Ordering::SeqCst);
        assert!(prev > 0, "Unpinning page with zero ref count");
    }

    /// Check if page is pinned
    pub fn is_pinned(&self) -> bool {
        self.ref_cnt.load(Ordering::SeqCst) > 0
    }

    /// Check if page is detached from LRU list
    pub fn is_detached(&self) -> bool {
        self.prev.is_none() && self.next.is_none()
    }

    /// Get page ID
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Get file page ID
    pub fn get_file_page_id(&self) -> FilePageId {
        self.file_page_id
    }

    /// Set page ID
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    /// Set file page ID
    pub fn set_file_page_id(&mut self, file_page_id: FilePageId) {
        self.file_page_id = file_page_id;
    }

    /// Check if page ID is valid
    pub fn is_page_id_valid(&self) -> bool {
        self.page_id < MAX_PAGE_ID
    }

    /// Dequeue from LRU list
    pub fn deque(&mut self) {
        // Remove from doubly linked list
        if let Some(prev_ptr) = self.prev {
            unsafe {
                (*prev_ptr).next = self.next;
            }
        }
        if let Some(next_ptr) = self.next {
            unsafe {
                (*next_ptr).prev = self.prev;
            }
        }
        self.prev = None;
        self.next = None;
    }

    /// Get next page in LRU list
    pub fn deque_next(&self) -> Option<&mut MemIndexPage> {
        self.next.map(|ptr| unsafe { &mut *ptr })
    }

    /// Get previous page in LRU list
    pub fn deque_prev(&self) -> Option<&mut MemIndexPage> {
        self.prev.map(|ptr| unsafe { &mut *ptr })
    }

    /// Enqueue after this page
    pub fn enque_next(&mut self, new_page: &mut MemIndexPage) {
        new_page.next = self.next;
        new_page.prev = Some(self as *mut _);

        if let Some(next_ptr) = self.next {
            unsafe {
                (*next_ptr).prev = Some(new_page as *mut _);
            }
        }

        self.next = Some(new_page as *mut _);
    }

    /// Set table identifier
    pub fn set_table_ident(&mut self, table_ident: Arc<TableIdent>) {
        self.table_ident = Some(table_ident);
    }

    /// Get table identifier
    pub fn table_ident(&self) -> Option<&Arc<TableIdent>> {
        self.table_ident.as_ref()
    }

    /// Convert to string for debugging (following C++ String method)
    pub fn to_string(&self, opts: &KvOptions) -> String {
        format!(
            "MemIndexPage {{ page_id: {}, file_page_id: {}, ref_cnt: {}, pinned: {} }}",
            self.page_id,
            self.file_page_id,
            self.ref_cnt.load(Ordering::SeqCst),
            self.is_pinned()
        )
    }
}

/// Iterator for index page entries
pub struct IndexPageIter<'a> {
    /// Page data
    page_data: &'a [u8],
    /// Comparator for key comparison
    comparator: Arc<dyn Comparator>,
    /// Current offset in page
    curr_offset: usize,
    /// Restart array offset
    restart_offset: usize,
    /// Number of restarts
    restart_count: u16,
    /// Current key buffer
    current_key: Vec<u8>,
}

impl<'a> IndexPageIter<'a> {
    /// Create iterator from index page
    pub fn new(index_page: &'a MemIndexPage, opts: &KvOptions) -> Self {
        let page_data = index_page.page_ptr();
        Self::from_page_data(page_data, opts.comparator())
    }

    /// Create iterator from page data
    pub fn from_page_data(page_data: &'a [u8], comparator: Arc<dyn Comparator>) -> Self {
        // Parse page structure to find restart points
        let restart_count = if page_data.len() >= 2 {
            u16::from_le_bytes([
                page_data[page_data.len() - 2],
                page_data[page_data.len() - 1],
            ])
        } else {
            0
        };

        let restart_offset = if restart_count > 0 {
            page_data.len() - 2 - (restart_count as usize * 2)
        } else {
            page_data.len()
        };

        // Start after header
        let curr_offset = if page_data.len() > LEFTMOST_PTR_OFFSET + 4 {
            LEFTMOST_PTR_OFFSET + 4
        } else {
            page_data.len()
        };

        Self {
            page_data,
            comparator,
            curr_offset,
            restart_offset,
            restart_count,
            current_key: Vec::new(),
        }
    }

    /// Check if there are more entries
    pub fn has_next(&self) -> bool {
        self.curr_offset < self.restart_offset
    }

    /// Move to next entry
    pub fn next(&mut self) -> bool {
        if !self.has_next() {
            return false;
        }
        self.parse_next_key()
    }

    /// Seek to the entry containing or after the given key
    pub fn seek(&mut self, key: &[u8]) {
        // Binary search in restart points
        let mut left = 0;
        let mut right = self.restart_count;

        while left < right {
            let mid = left + (right - left) / 2;
            let restart_offset = self.get_restart_offset(mid);

            // Parse key at restart point
            self.curr_offset = restart_offset;
            if self.parse_next_key() {
                let cmp = self.comparator.compare(&self.current_key, key);
                if cmp < 0 {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                break;
            }
        }

        // Linear search from the restart point
        if left > 0 {
            self.curr_offset = self.get_restart_offset(left - 1);
            self.current_key.clear();
        }

        while self.parse_next_key() {
            if self.comparator.compare(&self.current_key, key) >= 0 {
                break;
            }
        }
    }

    /// Get current key
    pub fn key(&self) -> &[u8] {
        &self.current_key
    }

    /// Get page ID pointed to by current entry
    pub fn get_page_id(&self) -> PageId {
        // Page ID is stored after the key
        if self.curr_offset >= 4 && self.curr_offset <= self.page_data.len() {
            let offset = self.curr_offset - 4;
            // Check bounds before accessing
            if offset + 3 < self.page_data.len() {
                u32::from_le_bytes([
                    self.page_data[offset],
                    self.page_data[offset + 1],
                    self.page_data[offset + 2],
                    self.page_data[offset + 3],
                ])
            } else {
                MAX_PAGE_ID
            }
        } else {
            MAX_PAGE_ID
        }
    }

    /// Parse next key from current position
    fn parse_next_key(&mut self) -> bool {
        if self.curr_offset >= self.restart_offset {
            return false;
        }

        // Read shared key length (varint)
        let (shared_len, offset) = self.decode_varint(self.curr_offset);
        if offset >= self.restart_offset {
            return false;
        }

        // Read unshared key length (varint)
        let (unshared_len, offset) = self.decode_varint(offset);
        if offset >= self.restart_offset {
            return false;
        }

        // Read unshared key data
        let key_end = offset + unshared_len as usize;
        if key_end > self.restart_offset {
            return false;
        }

        // Build current key
        self.current_key.truncate(shared_len as usize);
        self.current_key.extend_from_slice(&self.page_data[offset..key_end]);

        // Skip to next entry (key data + page_id)
        self.curr_offset = key_end + 4; // 4 bytes for page ID

        true
    }

    /// Get offset of a restart point
    fn get_restart_offset(&self, index: u16) -> usize {
        if index >= self.restart_count {
            return self.restart_offset;
        }

        let offset_pos = self.restart_offset + (index as usize * 2);
        if offset_pos + 2 <= self.page_data.len() - 2 {
            u16::from_le_bytes([
                self.page_data[offset_pos],
                self.page_data[offset_pos + 1],
            ]) as usize
        } else {
            self.restart_offset
        }
    }

    /// Decode varint from data
    fn decode_varint(&self, offset: usize) -> (u32, usize) {
        let mut value = 0u32;
        let mut shift = 0;
        let mut pos = offset;

        while pos < self.page_data.len() {
            let byte = self.page_data[pos];
            pos += 1;

            value |= ((byte & 0x7F) as u32) << shift;

            if byte & 0x80 == 0 {
                return (value, pos);
            }

            shift += 7;
            if shift >= 32 {
                break;
            }
        }

        (0, self.page_data.len())
    }

    /// Peek at the next key without advancing
    pub fn peek_next_key(&self) -> Option<Vec<u8>> {
        let mut temp_iter = Self {
            page_data: self.page_data,
            comparator: self.comparator.clone(),
            curr_offset: self.curr_offset,
            restart_offset: self.restart_offset,
            restart_count: self.restart_count,
            current_key: self.current_key.clone(),
        };

        if temp_iter.next() {
            Some(temp_iter.current_key)
        } else {
            None
        }
    }
}