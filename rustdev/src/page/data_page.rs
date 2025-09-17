//! Data page implementation with restart points for efficient binary search

use bytes::Bytes;
use std::cmp::Ordering;

use crate::types::{PageId, PageType, MAX_PAGE_ID};
use crate::Result;
use super::page::{Page, HEADER_SIZE};

/// Restart point for binary search
#[derive(Debug, Clone, Copy)]
pub struct RestartPoint {
    /// Offset in page data
    pub offset: u32,
    /// Key length
    pub key_len: u32,
}

/// Value length special bits
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum ValueLenBit {
    /// Value is overflow (stored in overflow pages)
    Overflow = 0,
    /// Entry has expiration timestamp
    Expire = 1,
    /// Reserved for future use
    Reserved2 = 2,
    /// Reserved for future use
    Reserved3 = 3,
}

/// Data page structure
///
/// Format:
/// ```text
/// +------------+--------+------------------+-------------+-------------+
/// |checksum(8B)|type(1B)|content length(2B)|prev page(4B)|next page(4B)|
/// +------------+--------+------------------+-------------+-------------+
/// +---------+-------------------+---------------+-------------+
/// |data blob|restart array(N*2B)|restart num(2B)|padding bytes|
/// +---------+-------------------+---------------+-------------+
/// ```
#[derive(Debug)]
pub struct DataPage {
    /// Page ID
    page_id: PageId,
    /// Underlying page
    page: Page,
}

impl DataPage {
    /// Create a new empty data page
    pub fn new(page_id: PageId, page_size: usize) -> Self {
        let mut page = Page::new(page_size);
        page.set_page_type(PageType::Data);
        page.set_prev_page_id(MAX_PAGE_ID);
        page.set_next_page_id(MAX_PAGE_ID);
        page.set_content_length(0);

        // Initialize restart array
        let restart_num_offset = page_size - 2;
        page.as_bytes_mut()[restart_num_offset..restart_num_offset + 2]
            .copy_from_slice(&0u16.to_le_bytes());

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

    /// Get previous page ID
    pub fn prev_page_id(&self) -> PageId {
        self.page.prev_page_id()
    }

    /// Set previous page ID
    pub fn set_prev_page_id(&mut self, page_id: PageId) {
        self.page.set_prev_page_id(page_id);
    }

    /// Get next page ID
    pub fn next_page_id(&self) -> PageId {
        self.page.next_page_id()
    }

    /// Set next page ID
    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.page.set_next_page_id(page_id);
    }

    /// Get content length
    pub fn content_length(&self) -> u16 {
        self.page.content_length()
    }

    /// Get number of restart points (following C++ RestartNum)
    pub fn restart_num(&self) -> u16 {
        self.restart_count()
    }

    /// Get number of restart points
    pub fn restart_count(&self) -> u16 {
        let offset = self.page.size() - 2;
        u16::from_le_bytes(
            self.page.as_bytes()[offset..offset + 2]
                .try_into()
                .unwrap()
        )
    }

    /// Get restart point at index
    pub fn restart_point(&self, index: u16) -> Option<u16> {
        if index >= self.restart_count() {
            return None;
        }

        let restart_array_offset = self.page.size() - 2 - (self.restart_count() as usize * 2);
        let offset = restart_array_offset + (index as usize * 2);

        Some(u16::from_le_bytes(
            self.page.as_bytes()[offset..offset + 2]
                .try_into()
                .unwrap()
        ))
    }

    /// Check if page is empty
    pub fn is_empty(&self) -> bool {
        self.content_length() == 0
    }

    /// Clear the page
    pub fn clear(&mut self) {
        self.page.clear();
        // Reset restart count
        let restart_num_offset = self.page.size() - 2;
        self.page.as_bytes_mut()[restart_num_offset..restart_num_offset + 2]
            .copy_from_slice(&0u16.to_le_bytes());
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

} // End of first impl DataPage block

/// Iterator over entries in a data page
pub struct DataPageIterator<'a> {
    /// Reference to the data page
    page: &'a DataPage,
    /// Current position in the page
    current_pos: usize,
    /// End position (exclusive)
    end_pos: usize,
}

impl<'a> DataPageIterator<'a> {
    /// Create a new iterator
    pub fn new(page: &'a DataPage) -> Self {
        let end_pos = HEADER_SIZE + page.content_length() as usize;
        Self {
            page,
            current_pos: HEADER_SIZE,
            end_pos,
        }
    }

    /// Reset iterator to beginning
    pub fn reset(&mut self) {
        self.current_pos = HEADER_SIZE;
    }

    /// Seek to the first key >= target
    pub fn seek(&mut self, target_key: &[u8]) -> bool {
        // Binary search using restart points
        let restart_count = self.page.restart_count();
        if restart_count == 0 {
            return false;
        }

        // Find the right restart point
        let mut left = 0;
        let mut right = restart_count - 1;

        while left < right {
            let mid = left + (right - left + 1) / 2;
            if let Some(offset) = self.page.restart_point(mid) {
                self.current_pos = HEADER_SIZE + offset as usize;
                if let Some((key, _, _, _)) = self.parse_entry() {
                    match key.as_ref().cmp(target_key) {
                        Ordering::Less => left = mid,
                        Ordering::Greater => {
                            if mid == 0 {
                                break;
                            }
                            right = mid - 1;
                        }
                        Ordering::Equal => return true,
                    }
                }
            }
        }

        // Start from the selected restart point
        if let Some(offset) = self.page.restart_point(left) {
            self.current_pos = HEADER_SIZE + offset as usize;
        }

        // Linear search from restart point
        while let Some((key, _, _, _)) = self.peek() {
            match key.as_ref().cmp(target_key) {
                Ordering::Less => {
                    self.next();
                }
                Ordering::Greater | Ordering::Equal => {
                    return true;
                }
            }
        }

        false
    }

    /// Seek to the last key <= target
    pub fn seek_floor(&mut self, target_key: &[u8]) -> bool {
        let mut found = false;
        let mut last_valid_pos = self.current_pos;

        // Use binary search with restart points
        let restart_count = self.page.restart_count();
        if restart_count == 0 {
            return false;
        }

        // Find the right restart point
        let mut left = 0;
        let mut right = restart_count - 1;

        while left <= right && right < restart_count {
            let mid = left + (right - left) / 2;
            if let Some(offset) = self.page.restart_point(mid) {
                self.current_pos = HEADER_SIZE + offset as usize;
                if let Some((key, _, _, _)) = self.parse_entry() {
                    match key.as_ref().cmp(target_key) {
                        Ordering::Less | Ordering::Equal => {
                            found = true;
                            last_valid_pos = HEADER_SIZE + offset as usize;
                            left = mid + 1;
                        }
                        Ordering::Greater => {
                            if mid == 0 {
                                break;
                            }
                            right = mid - 1;
                        }
                    }
                }
            }
        }

        if found {
            // Linear search from the restart point
            self.current_pos = last_valid_pos;
            let mut floor_pos = last_valid_pos;

            while let Some((key, _, _, _)) = self.peek() {
                if key.as_ref() <= target_key {
                    floor_pos = self.current_pos;
                    self.next();
                } else {
                    break;
                }
            }

            self.current_pos = floor_pos;
            true
        } else {
            false
        }
    }

    /// Parse entry at current position
    fn parse_entry(&self) -> Option<(Bytes, Bytes, u64, Option<u64>)> {
        if self.current_pos >= self.end_pos {
            return None;
        }

        let data = self.page.page().as_bytes();
        let mut pos = self.current_pos;

        // Read key length (varint)
        let (key_len, bytes_read) = decode_varint(&data[pos..])?;
        pos += bytes_read;

        // Read value length and flags (varint)
        let (val_len_with_flags, bytes_read) = decode_varint(&data[pos..])?;
        pos += bytes_read;

        // Extract flags
        let is_overflow = (val_len_with_flags & (1 << ValueLenBit::Overflow as u32)) != 0;
        let has_expire = (val_len_with_flags & (1 << ValueLenBit::Expire as u32)) != 0;
        let val_len = val_len_with_flags >> 4;

        // Read timestamp (varint)
        let (timestamp, bytes_read) = decode_varint(&data[pos..]).map(|(v, b)| (v as u64, b))?;
        pos += bytes_read;

        // Read expiration if present
        let expire_ts = if has_expire {
            let (expire, bytes_read) = decode_varint(&data[pos..]).map(|(v, b)| (v as u64, b))?;
            pos += bytes_read;
            Some(expire)
        } else {
            None
        };

        // Read key
        if pos + key_len as usize > data.len() {
            return None;
        }
        let key = Bytes::copy_from_slice(&data[pos..pos + key_len as usize]);
        pos += key_len as usize;

        // Read value
        if pos + val_len as usize > data.len() {
            return None;
        }
        let value = if is_overflow {
            // For overflow values, the data contains overflow page pointers
            Bytes::copy_from_slice(&data[pos..pos + val_len as usize])
        } else {
            Bytes::copy_from_slice(&data[pos..pos + val_len as usize])
        };

        Some((key, value, timestamp, expire_ts))
    }

    /// Peek at current entry without advancing
    pub fn peek(&self) -> Option<(Bytes, Bytes, u64, Option<u64>)> {
        self.parse_entry()
    }

    /// Get current key
    pub fn key(&self) -> Option<Bytes> {
        self.peek().map(|(k, _, _, _)| k)
    }

    /// Get current value
    pub fn value(&self) -> Option<Bytes> {
        self.peek().map(|(_, v, _, _)| v)
    }

    /// Get expiration timestamp
    pub fn expire_ts(&self) -> Option<u64> {
        self.peek().and_then(|(_, _, _, expire)| expire)
    }

    /// Get timestamp
    pub fn timestamp(&self) -> u64 {
        self.peek().map(|(_, _, ts, _)| ts).unwrap_or(0)
    }

    /// Check if has more entries
    pub fn has_next(&self) -> bool {
        self.current_pos < self.end_pos
    }

    /// Check if current value is overflow
    pub fn is_overflow(&self) -> bool {
        if self.current_pos >= self.end_pos {
            return false;
        }

        let data = self.page.page().as_bytes();
        let mut pos = self.current_pos;

        // Skip key length
        if let Some((_, bytes_read)) = decode_varint(&data[pos..]) {
            pos += bytes_read;

            // Read value length and check overflow bit
            if let Some((val_len_with_flags, _)) = decode_varint(&data[pos..]) {
                return (val_len_with_flags & (1 << ValueLenBit::Overflow as u32)) != 0;
            }
        }

        false
    }
}

impl<'a> Iterator for DataPageIterator<'a> {
    type Item = (Bytes, Bytes, u64, Option<u64>);

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.parse_entry()?;

        // Advance position
        let data = self.page.page().as_bytes();
        let mut pos = self.current_pos;

        // Skip key length
        let (key_len, bytes_read) = decode_varint(&data[pos..])?;
        pos += bytes_read;

        // Skip value length
        let (val_len_with_flags, bytes_read) = decode_varint(&data[pos..])?;
        pos += bytes_read;
        let has_expire = (val_len_with_flags & (1 << ValueLenBit::Expire as u32)) != 0;
        let val_len = val_len_with_flags >> 4;

        // Skip timestamp
        let (_, bytes_read) = decode_varint(&data[pos..])?;
        pos += bytes_read;

        // Skip expiration if present
        if has_expire {
            let (_, bytes_read) = decode_varint(&data[pos..])?;
            pos += bytes_read;
        }

        // Skip key and value
        pos += key_len as usize + val_len as usize;
        self.current_pos = pos;

        Some(entry)
    }
}

/// Decode varint from bytes
fn decode_varint(data: &[u8]) -> Option<(u32, usize)> {
    let mut result = 0u32;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if i >= 5 {
            return None; // Varint too long
        }

        result |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }

        shift += 7;
    }

    None
}

/// Encode varint to bytes
fn encode_varint(mut value: u32, buf: &mut [u8]) -> usize {
    let mut i = 0;

    while value >= 0x80 {
        buf[i] = (value as u8) | 0x80;
        value >>= 7;
        i += 1;
    }

    buf[i] = value as u8;
    i + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_page_creation() {
        let page = DataPage::new(123, 4096);
        assert_eq!(page.page_id(), 123);
        assert!(page.is_empty());
        assert_eq!(page.restart_count(), 0);
    }

    #[test]
    fn test_varint_encoding() {
        let mut buf = [0u8; 10];

        // Test small value
        let len = encode_varint(127, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 127);

        let (val, bytes) = decode_varint(&buf).unwrap();
        assert_eq!(val, 127);
        assert_eq!(bytes, 1);

        // Test larger value
        let len = encode_varint(300, &mut buf);
        assert_eq!(len, 2);

        let (val, bytes) = decode_varint(&buf).unwrap();
        assert_eq!(val, 300);
        assert_eq!(bytes, 2);

        // Test maximum value
        let len = encode_varint(u32::MAX, &mut buf);
        assert_eq!(len, 5);

        let (val, bytes) = decode_varint(&buf).unwrap();
        assert_eq!(val, u32::MAX);
        assert_eq!(bytes, 5);
    }
}

impl DataPage {
    /// Get entry by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // TODO: Implement binary search
        Ok(None)
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        // TODO: Implement insertion
        Ok(())
    }

    /// Check if key-value can fit
    pub fn can_fit(&self, key: &[u8], value: &[u8]) -> bool {
        // TODO: Implement size check
        let entry_size = 4 + key.len() + 4 + value.len();
        let current_size = HEADER_SIZE + self.content_length() as usize;
        current_size + entry_size < self.page.size() - 100
    }

    /// Iterator over entries
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        // TODO: Implement proper iteration
        std::iter::empty()
    }

    /// Find floor entry (largest key <= target)
    pub fn floor(&self, key: &[u8]) -> Result<Option<(Bytes, Bytes)>> {
        // TODO: Implement floor search
        Ok(None)
    }

    /// Get last entry
    pub fn last_entry(&self) -> Result<Option<(Bytes, Bytes)>> {
        // TODO: Implement
        Ok(None)
    }

    /// Get first key
    pub fn first_key(&self) -> Option<Bytes> {
        // TODO: Implement
        None
    }

    /// Get last key
    pub fn last_key(&self) -> Option<Bytes> {
        // TODO: Implement
        None
    }

    /// Get entry count
    pub fn entry_count(&self) -> usize {
        // Count entries by iterating through the page
        let mut count = 0;
        let mut iter = DataPageIterator::new(self);
        while iter.next().is_some() {
            count += 1;
        }
        count
    }

    /// Get used space
    pub fn used_space(&self) -> usize {
        HEADER_SIZE + self.content_length() as usize
    }

    /// Get as page reference
    pub fn as_page(&self) -> &Page {
        &self.page
    }

    /// Get size
    pub fn size(&self) -> usize {
        self.page.size()
    }
}
