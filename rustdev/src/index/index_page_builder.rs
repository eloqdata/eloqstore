//! Index page builder following C++ index_page_builder.cpp exactly

use bytes::{Bytes, BytesMut, BufMut};
use std::sync::Arc;

use crate::config::KvOptions;
use crate::types::{PageId, MAX_PAGE_ID};
use crate::codec::{Comparator, encode_varint32, decode_varint32, varint32_size};
use crate::page::HEADER_SIZE;
use crate::types::PageType;

/// Offsets in index page header (following C++ mem_index_page.h)
pub const CHECKSUM_BYTES: usize = 8;
pub const PAGE_TYPE_OFFSET: usize = CHECKSUM_BYTES; // 8
pub const PAGE_SIZE_OFFSET: usize = PAGE_TYPE_OFFSET + 1; // 9
pub const LEFTMOST_PTR_OFFSET: usize = PAGE_SIZE_OFFSET + 2; // 11

/// Index page builder following C++ IndexPageBuilder exactly
pub struct IndexPageBuilder {
    /// KV options
    options: Arc<KvOptions>,
    /// Page buffer
    buffer: BytesMut,
    /// Restart offsets
    restarts: Vec<u16>,
    /// Counter for restart interval
    counter: u16,
    /// Total entry count
    cnt: u16,
    /// Finished flag
    finished: bool,
    /// Last key for prefix compression
    last_key: Vec<u8>,
    /// Last page ID for delta encoding
    last_page_id: i32,
}

impl IndexPageBuilder {
    /// Create a new index page builder (following C++ constructor)
    pub fn new(options: Arc<KvOptions>) -> Self {
        let mut buffer = BytesMut::with_capacity(options.data_page_size);
        buffer.resize(Self::header_size(), 0);

        let mut restarts = Vec::new();
        restarts.push(buffer.len() as u16);

        Self {
            options,
            buffer,
            restarts,
            counter: 0,
            cnt: 0,
            finished: false,
            last_key: Vec::new(),
            last_page_id: 0,
        }
    }

    /// Reset the builder (following C++ Reset)
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.buffer.resize(Self::header_size(), 0);
        self.restarts.clear();
        self.restarts.push(self.buffer.len() as u16);
        self.counter = 0;
        self.cnt = 0;
        self.finished = false;
        self.last_key.clear();
        self.last_page_id = 0;
    }

    /// Get header size (following C++ HeaderSize)
    fn header_size() -> usize {
        // 8 bytes checksum + 1 byte page type + 2 bytes page size + 4 bytes leftmost pointer
        LEFTMOST_PTR_OFFSET + 4  // 11 + 4 = 15
    }

    /// Get tail metadata size
    fn tail_meta_size(&self) -> usize {
        // restart array + restart array size
        self.restarts.len() * 2 + 2
    }

    /// Get current size estimate (following C++ CurrentSizeEstimate)
    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + self.tail_meta_size()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.cnt == 0
    }

    /// Add an index entry (following C++ Add exactly)
    pub fn add(&mut self, key: &[u8], page_id: PageId, is_leaf_index: bool) -> bool {
        tracing::debug!("IndexPageBuilder::add: key_len={}, page_id={}, is_leaf={}, is_empty={}",
                       key.len(), page_id, is_leaf_index, self.is_empty());

        if self.is_empty() {
            // Set page type
            let page_type = if is_leaf_index {
                PageType::LeafIndex as u8
            } else {
                PageType::NonLeafIndex as u8
            };
            self.buffer[8] = page_type;

            // Set leftmost pointer
            let leftmost_offset = LEFTMOST_PTR_OFFSET;
            self.buffer[leftmost_offset..leftmost_offset + 4]
                .copy_from_slice(&page_id.to_le_bytes());
            self.cnt += 1;
            return true;
        }

        assert!(!key.is_empty());
        assert!(!self.finished);

        // Check page type matches
        let expected_type = if is_leaf_index {
            PageType::LeafIndex as u8
        } else {
            PageType::NonLeafIndex as u8
        };
        if self.buffer[8] != expected_type {
            return false;
        }

        // Calculate shared prefix length
        let mut shared = 0;
        if self.counter < self.options.index_page_restart_interval {
            let min_len = self.last_key.len().min(key.len());
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        }

        let non_shared = key.len() - shared;

        // Calculate addition delta
        let mut addition_delta = 0;

        // Restart point overhead
        if self.counter >= self.options.index_page_restart_interval {
            addition_delta += 2; // sizeof(uint16_t)
            self.last_page_id = 0;
        }

        // Shared/non-shared varint sizes
        addition_delta += varint32_size(shared as u32);
        addition_delta += varint32_size(non_shared as u32);
        addition_delta += non_shared;

        // Page ID delta encoding
        let delta = page_id as i32 - self.last_page_id;
        let p_delta = Self::encode_int32_delta(delta);
        addition_delta += varint32_size(p_delta);

        // Check if would overflow
        if self.current_size_estimate() + addition_delta > self.options.data_page_size {
            return false;
        }

        // Add restart point if needed
        if self.counter >= self.options.index_page_restart_interval {
            self.restarts.push(self.buffer.len() as u16);
            self.counter = 0;
        }

        // Write the entry
        encode_varint32(&mut self.buffer, shared as u32);
        encode_varint32(&mut self.buffer, non_shared as u32);
        self.buffer.extend_from_slice(&key[shared..]);
        encode_varint32(&mut self.buffer, p_delta);

        // Update state
        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(&key[shared..]);
        self.last_page_id = page_id as i32;
        self.counter += 1;
        self.cnt += 1;

        true
    }

    /// Finish the index page (following C++ Finish)
    pub fn finish(&mut self) -> Bytes {
        if self.cnt == 1 {
            // Only contains leftmost pointer, no restart points
            self.restarts.clear();
        }

        // Append restart array
        for &offset in &self.restarts {
            self.buffer.put_u16_le(offset);
        }

        // Append restart count
        self.buffer.put_u16_le(self.restarts.len() as u16);

        // Store content size at header
        let content_size = self.buffer.len() as u16;
        self.buffer[PAGE_SIZE_OFFSET..PAGE_SIZE_OFFSET + 2]
            .copy_from_slice(&content_size.to_le_bytes());

        // Resize to page size
        assert!(self.buffer.len() <= self.options.data_page_size);
        self.buffer.resize(self.options.data_page_size, 0);

        self.finished = true;

        self.buffer.clone().freeze()
    }

    /// Encode int32 delta (following C++ EncodeInt32Delta)
    fn encode_int32_delta(delta: i32) -> u32 {
        // Following C++ coding.h EncodeInt32Delta
        if delta < 0 {
            ((-delta as u32) << 1) | 1
        } else {
            (delta as u32) << 1
        }
    }

    /// Decode int32 delta (following C++ DecodeInt32Delta)
    pub fn decode_int32_delta(encoded: u32) -> i32 {
        if encoded & 1 != 0 {
            -((encoded >> 1) as i32)
        } else {
            (encoded >> 1) as i32
        }
    }
}