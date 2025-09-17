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

/// Offsets in index page header (following C++ mem_index_page.h)
pub const CHECKSUM_BYTES: usize = 8;
pub const PAGE_TYPE_OFFSET: usize = CHECKSUM_BYTES; // 8
pub const PAGE_SIZE_OFFSET: usize = PAGE_TYPE_OFFSET + 1; // 9
pub const LEFTMOST_PTR_OFFSET: usize = PAGE_SIZE_OFFSET + 2; // 11

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
        // Check the page type in the header
        use crate::types::PageType;
        let data = self.page.as_bytes();
        if data.len() > PAGE_TYPE_OFFSET {
            // Check page type byte at offset 8
            data[PAGE_TYPE_OFFSET] == PageType::LeafIndex as u8
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

    /// Set page data from bytes
    pub fn set_page_data(&mut self, data: bytes::Bytes) {
        self.page = Page::from_bytes(data);
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

    /// Enqueue next page into LRU list
    pub fn enque_next(&mut self, new_page: &mut MemIndexPage) {
        let old_next = self.next;
        self.next = Some(new_page as *mut _);
        new_page.prev = Some(self as *mut _);

        new_page.next = old_next;
        if let Some(old_next_ptr) = old_next {
            unsafe {
                (*old_next_ptr).prev = Some(new_page as *mut _);
            }
        }
    }

    /// Set table identifier
    pub fn set_table_ident(&mut self, table_ident: Arc<TableIdent>) {
        self.table_ident = Some(table_ident);
    }

    /// Get table identifier
    pub fn table_ident(&self) -> Option<&Arc<TableIdent>> {
        self.table_ident.as_ref()
    }

    /// Wait for I/O completion
    pub async fn wait_io(&self) {
        let rx = {
            let mut waiting = self.waiting.lock().unwrap();
            if !self.is_pinned() {
                return; // Already completed
            }

            let (tx, rx) = tokio::sync::oneshot::channel();
            waiting.push(tx);
            rx
        };

        let _ = rx.await;
    }

    /// Notify waiters that I/O is complete
    pub fn notify_io_complete(&self) {
        let mut waiting = self.waiting.lock().unwrap();
        for tx in waiting.drain(..) {
            let _ = tx.send(());
        }
    }

    /// String representation for debugging
    pub fn debug_string(&self) -> String {
        format!(
            "IndexPage{{id:{}, file_id:{:?}, ref_cnt:{}, pinned:{}}}",
            self.page_id,
            self.file_page_id,
            self.ref_cnt.load(Ordering::SeqCst),
            self.is_pinned()
        )
    }
}

// Re-export IndexPageIter from the separate module
pub use super::index_page_iter::IndexPageIter;