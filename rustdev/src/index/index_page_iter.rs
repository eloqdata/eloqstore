//! Index page iterator following C++ mem_index_page.cpp exactly

use std::sync::Arc;

use crate::codec::{Comparator, decode_varint32};
use crate::types::{PageId, MAX_PAGE_ID};
use crate::index::index_page_builder::{IndexPageBuilder, LEFTMOST_PTR_OFFSET};

/// Index page iterator following C++ IndexPageIter exactly
pub struct IndexPageIter<'a> {
    /// Comparator for keys
    comparator: Arc<dyn Comparator>,
    /// Page data
    page_data: &'a [u8],
    /// Number of restart points
    restart_num: u16,
    /// Offset where restart array begins
    restart_offset: usize,
    /// Current offset in page
    curr_offset: usize,
    /// Current restart index
    curr_restart_idx: usize,
    /// Current key
    key: Vec<u8>,
    /// Current page ID
    page_id: PageId,
}

impl<'a> IndexPageIter<'a> {
    /// Create from page data (following C++ constructor)
    pub fn new(page_data: &'a [u8], comparator: Arc<dyn Comparator>) -> Self {
        // Handle empty page data
        if page_data.is_empty() {
            return Self {
                comparator,
                page_data,
                restart_num: 0,
                restart_offset: 0,
                curr_offset: LEFTMOST_PTR_OFFSET,
                curr_restart_idx: 0,
                key: Vec::new(),
                page_id: MAX_PAGE_ID,
            };
        }

        // Get restart count from end of page
        let restart_num = if page_data.len() >= 2 {
            u16::from_le_bytes([
                page_data[page_data.len() - 2],
                page_data[page_data.len() - 1],
            ])
        } else {
            0
        };

        // Calculate restart array offset
        let restart_offset = if restart_num > 0 && page_data.len() >= (1 + restart_num as usize) * 2 {
            page_data.len() - (1 + restart_num as usize) * 2
        } else if page_data.len() >= 2 {
            page_data.len() - 2
        } else {
            0
        };

        // Read the leftmost pointer
        let leftmost_page_id = if page_data.len() >= LEFTMOST_PTR_OFFSET + 4 {
            let bytes = &page_data[LEFTMOST_PTR_OFFSET..LEFTMOST_PTR_OFFSET + 4];
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        } else {
            MAX_PAGE_ID
        };

        let mut iter = Self {
            comparator,
            page_data,
            restart_num,
            restart_offset,
            curr_offset: LEFTMOST_PTR_OFFSET,
            curr_restart_idx: 0,
            key: Vec::new(),
            page_id: leftmost_page_id, // Start with leftmost pointer value
        };

        // Position after the leftmost pointer
        iter.curr_offset = LEFTMOST_PTR_OFFSET + 4;

        iter
    }

    /// Check if there are more entries
    pub fn has_next(&self) -> bool {
        self.curr_offset < self.restart_offset
    }

    /// Move to next entry
    pub fn next(&mut self) -> bool {
        self.parse_next_key()
    }

    /// Get current key
    pub fn key(&self) -> Option<&[u8]> {
        if self.key.is_empty() && self.page_id == MAX_PAGE_ID {
            None
        } else {
            Some(&self.key)
        }
    }

    /// Get current page ID
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Seek to key (following C++ Seek)
    pub fn seek(&mut self, search_key: &[u8]) {
        // Special case: if no restart points, just use leftmost pointer
        if self.restart_num == 0 {
            // The index page only has a leftmost pointer, no key entries
            // All keys map to the leftmost pointer's page
            return;
        }

        let mut left = 0;
        let mut right = self.restart_num - 1;

        // Use current position as hint if available
        if !self.key.is_empty() {
            let cmp = self.comparator.compare(&self.key, search_key);
            if cmp < 0 {
                left = self.curr_restart_idx as u16;
            } else if cmp > 0 {
                right = self.curr_restart_idx as u16;
            } else {
                return; // Already at the right position
            }
        }

        // Binary search in restart points
        while left < right {
            let mid = left + (right - left + 1) / 2;

            // Seek to restart point
            if !self.seek_to_restart(mid as usize) {
                break;
            }

            let cmp = self.comparator.compare(&self.key, search_key);
            if cmp < 0 {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        // Seek to the final restart point
        self.seek_to_restart(left as usize);

        // Linear scan to find exact position
        loop {
            let cmp = self.comparator.compare(&self.key, search_key);
            if cmp >= 0 {
                break;
            }

            if !self.parse_next_key() {
                break;
            }
        }
    }

    /// Parse next key from current position (following C++ ParseNextKey)
    fn parse_next_key(&mut self) -> bool {
        if self.curr_offset >= self.restart_offset {
            self.invalidate();
            return false;
        }

        // Decode entry
        let mut offset = self.curr_offset;

        // Decode shared and non_shared lengths
        let (shared, non_shared, new_offset) = match self.decode_entry(offset) {
            Some((s, n, o)) => (s, n, o),
            None => {
                self.invalidate();
                return false;
            }
        };
        offset = new_offset;

        // Check key buffer size
        if shared as usize > self.key.len() {
            self.invalidate();
            return false;
        }

        // Read non-shared key data
        if offset + non_shared as usize > self.restart_offset {
            self.invalidate();
            return false;
        }

        // Update key
        self.key.truncate(shared as usize);
        self.key.extend_from_slice(&self.page_data[offset..offset + non_shared as usize]);
        offset += non_shared as usize;

        // Decode page ID delta
        let (p_delta, new_offset) = match decode_varint32(self.page_data, offset) {
            Some((val, off)) => (val, off),
            None => {
                self.invalidate();
                return false;
            }
        };
        offset = new_offset;

        // Decode delta and update page ID
        let delta = IndexPageBuilder::decode_int32_delta(p_delta);
        self.page_id = (self.page_id as i32 + delta) as PageId;

        // Update current offset
        self.curr_offset = offset;

        // Update restart index if needed
        if self.restart_num > 0 &&
           self.curr_restart_idx + 1 < self.restart_num as usize &&
           self.curr_offset >= self.get_restart_offset(self.curr_restart_idx + 1) {
            self.curr_restart_idx += 1;
        }

        true
    }

    /// Decode entry (following C++ DecodeEntry)
    fn decode_entry(&self, offset: usize) -> Option<(u32, u32, usize)> {
        if offset + 2 > self.restart_offset {
            return None;
        }

        // Try fast path: both values encoded in single bytes
        let byte1 = self.page_data[offset] as u32;
        let byte2 = self.page_data[offset + 1] as u32;

        if (byte1 | byte2) < 128 {
            // Fast path
            return Some((byte1, byte2, offset + 2));
        }

        // Slow path: varint decoding
        let (shared, offset) = decode_varint32(self.page_data, offset)?;
        let (non_shared, offset) = decode_varint32(self.page_data, offset)?;

        Some((shared, non_shared, offset))
    }

    /// Seek to restart point
    fn seek_to_restart(&mut self, restart_idx: usize) -> bool {
        if restart_idx >= self.restart_num as usize {
            return false;
        }

        self.curr_restart_idx = restart_idx;
        self.curr_offset = self.get_restart_offset(restart_idx);
        self.key.clear();
        self.page_id = self.get_leftmost_pointer();

        self.parse_next_key()
    }

    /// Get restart offset
    fn get_restart_offset(&self, idx: usize) -> usize {
        if idx >= self.restart_num as usize {
            return self.restart_offset;
        }

        let offset = self.restart_offset + idx * 2;
        if offset + 1 < self.page_data.len() {
            u16::from_le_bytes([
                self.page_data[offset],
                self.page_data[offset + 1],
            ]) as usize
        } else {
            self.restart_offset
        }
    }

    /// Get leftmost pointer
    fn get_leftmost_pointer(&self) -> PageId {
        if self.page_data.len() >= LEFTMOST_PTR_OFFSET + 4 {
            u32::from_le_bytes([
                self.page_data[LEFTMOST_PTR_OFFSET],
                self.page_data[LEFTMOST_PTR_OFFSET + 1],
                self.page_data[LEFTMOST_PTR_OFFSET + 2],
                self.page_data[LEFTMOST_PTR_OFFSET + 3],
            ])
        } else {
            MAX_PAGE_ID
        }
    }

    /// Invalidate iterator
    fn invalidate(&mut self) {
        self.curr_offset = self.restart_offset;
        self.curr_restart_idx = self.restart_num as usize;
        self.key.clear();
        self.page_id = MAX_PAGE_ID;
    }
}