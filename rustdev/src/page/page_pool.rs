//! Memory pool for efficient page allocation and reuse

use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use bytes::{Bytes, BytesMut};
use crossbeam::queue::ArrayQueue;

use crate::page::Page;
use crate::types::DEFAULT_PAGE_SIZE;

/// Statistics for page pool
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total allocations
    pub allocations: u64,
    /// Total deallocations
    pub deallocations: u64,
    /// Cache hits (reused pages)
    pub hits: u64,
    /// Cache misses (new allocations)
    pub misses: u64,
    /// Current pool size
    pub current_size: usize,
    /// Peak pool size
    pub peak_size: usize,
}

/// Page pool for efficient memory management
pub struct PagePool {
    /// Page size
    page_size: usize,
    /// Maximum pool size
    max_size: usize,
    /// Pool of available pages
    pool: ArrayQueue<BytesMut>,
    /// Statistics
    stats: Arc<Mutex<PoolStats>>,
}

impl PagePool {
    /// Create a new page pool
    pub fn new(page_size: usize, max_size: usize) -> Self {
        Self {
            page_size,
            max_size,
            pool: ArrayQueue::new(max_size),
            stats: Arc::new(Mutex::new(PoolStats::default())),
        }
    }

    /// Allocate a page from the pool
    pub fn allocate(&self) -> Page {
        let mut stats = self.stats.lock().unwrap();
        stats.allocations += 1;

        // Try to get from pool
        if let Some(mut buffer) = self.pool.pop() {
            stats.hits += 1;
            stats.current_size = self.pool.len();

            // Clear the buffer for reuse
            buffer.clear();
            buffer.resize(self.page_size, 0);

            Page::from_bytes(buffer.freeze())
        } else {
            stats.misses += 1;

            // Allocate new page
            let buffer = BytesMut::zeroed(self.page_size);
            Page::from_bytes(buffer.freeze())
        }
    }

    /// Return a page to the pool
    pub fn deallocate(&self, page: Page) {
        let mut stats = self.stats.lock().unwrap();
        stats.deallocations += 1;

        // Only keep pages of the correct size
        if page.size() != self.page_size {
            return;
        }

        // Convert back to BytesMut for reuse
        let buffer = BytesMut::from(page.as_bytes());

        // Try to return to pool
        if self.pool.push(buffer).is_ok() {
            let new_size = self.pool.len();
            stats.current_size = new_size;
            if new_size > stats.peak_size {
                stats.peak_size = new_size;
            }
        }
        // If pool is full, just drop the page
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        self.stats.lock().unwrap().clone()
    }

    /// Clear the pool
    pub fn clear(&self) {
        while self.pool.pop().is_some() {
            // Drain the pool
        }

        let mut stats = self.stats.lock().unwrap();
        stats.current_size = 0;
    }

    /// Get the page size
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Get the maximum pool size
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Get current pool size
    pub fn current_size(&self) -> usize {
        self.pool.len()
    }

    /// Check if pool is empty
    pub fn is_empty(&self) -> bool {
        self.pool.is_empty()
    }

    /// Check if pool is full
    pub fn is_full(&self) -> bool {
        self.pool.len() >= self.max_size
    }
}

/// Global page pool for default page size
pub struct GlobalPagePool {
    /// Pool for different page sizes
    pools: Arc<Mutex<HashMap<usize, Arc<PagePool>>>>,
    /// Default pool size
    default_pool_size: usize,
}

use std::collections::HashMap;

impl GlobalPagePool {
    /// Create a new global page pool
    pub fn new(default_pool_size: usize) -> Self {
        Self {
            pools: Arc::new(Mutex::new(HashMap::new())),
            default_pool_size,
        }
    }

    /// Get or create a pool for the given page size
    pub fn get_pool(&self, page_size: usize) -> Arc<PagePool> {
        let mut pools = self.pools.lock().unwrap();

        pools.entry(page_size)
            .or_insert_with(|| Arc::new(PagePool::new(page_size, self.default_pool_size)))
            .clone()
    }

    /// Allocate a page of the given size
    pub fn allocate(&self, page_size: usize) -> Page {
        self.get_pool(page_size).allocate()
    }

    /// Deallocate a page
    pub fn deallocate(&self, page: Page) {
        let page_size = page.size();
        self.get_pool(page_size).deallocate(page);
    }

    /// Get statistics for all pools
    pub fn all_stats(&self) -> HashMap<usize, PoolStats> {
        let pools = self.pools.lock().unwrap();
        pools.iter()
            .map(|(&size, pool)| (size, pool.stats()))
            .collect()
    }

    /// Clear all pools
    pub fn clear_all(&self) {
        let pools = self.pools.lock().unwrap();
        for pool in pools.values() {
            pool.clear();
        }
    }
}

/// Thread-local page allocator with batching
pub struct LocalPageAllocator {
    /// Page size
    page_size: usize,
    /// Local cache of pages
    local_cache: VecDeque<Page>,
    /// Maximum local cache size
    max_cache_size: usize,
    /// Reference to global pool
    global_pool: Arc<PagePool>,
}

impl LocalPageAllocator {
    /// Create a new local allocator
    pub fn new(page_size: usize, max_cache_size: usize, global_pool: Arc<PagePool>) -> Self {
        Self {
            page_size,
            local_cache: VecDeque::with_capacity(max_cache_size),
            max_cache_size,
            global_pool,
        }
    }

    /// Allocate a page
    pub fn allocate(&mut self) -> Page {
        // Try local cache first
        if let Some(page) = self.local_cache.pop_front() {
            return page;
        }

        // Refill from global pool
        self.refill();

        // Try again
        self.local_cache.pop_front()
            .unwrap_or_else(|| self.global_pool.allocate())
    }

    /// Deallocate a page
    pub fn deallocate(&mut self, page: Page) {
        if page.size() != self.page_size {
            return; // Wrong size
        }

        // Add to local cache
        if self.local_cache.len() < self.max_cache_size {
            self.local_cache.push_back(page);
        } else {
            // Local cache full, return to global pool
            self.global_pool.deallocate(page);
        }
    }

    /// Refill local cache from global pool
    fn refill(&mut self) {
        let refill_count = self.max_cache_size / 2;
        for _ in 0..refill_count {
            let page = self.global_pool.allocate();
            self.local_cache.push_back(page);
        }
    }

    /// Flush local cache to global pool
    pub fn flush(&mut self) {
        while let Some(page) = self.local_cache.pop_front() {
            self.global_pool.deallocate(page);
        }
    }
}

impl Drop for LocalPageAllocator {
    fn drop(&mut self) {
        self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_pool_basic() {
        let pool = PagePool::new(4096, 10);

        // Allocate some pages
        let page1 = pool.allocate();
        let page2 = pool.allocate();

        assert_eq!(page1.size(), 4096);
        assert_eq!(page2.size(), 4096);

        let stats = pool.stats();
        assert_eq!(stats.allocations, 2);
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.hits, 0);

        // Return pages to pool
        pool.deallocate(page1);
        pool.deallocate(page2);

        assert_eq!(pool.current_size(), 2);

        // Allocate again - should reuse
        let page3 = pool.allocate();
        assert_eq!(page3.size(), 4096);

        let stats = pool.stats();
        assert_eq!(stats.allocations, 3);
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_page_pool_max_size() {
        let pool = PagePool::new(1024, 2);

        // Fill the pool
        let page1 = pool.allocate();
        let page2 = pool.allocate();
        let page3 = pool.allocate();

        pool.deallocate(page1);
        pool.deallocate(page2);
        pool.deallocate(page3); // This one should be dropped

        assert_eq!(pool.current_size(), 2); // Max size is 2
    }

    #[test]
    fn test_global_page_pool() {
        let global_pool = GlobalPagePool::new(10);

        // Allocate pages of different sizes
        let page1 = global_pool.allocate(4096);
        let page2 = global_pool.allocate(4096);
        let page3 = global_pool.allocate(8192);

        assert_eq!(page1.size(), 4096);
        assert_eq!(page2.size(), 4096);
        assert_eq!(page3.size(), 8192);

        // Return pages
        global_pool.deallocate(page1);
        global_pool.deallocate(page2);
        global_pool.deallocate(page3);

        // Check stats
        let stats = global_pool.all_stats();
        assert_eq!(stats.len(), 2); // Two different sizes
    }

    #[test]
    fn test_local_allocator() {
        let global_pool = Arc::new(PagePool::new(4096, 20));
        let mut local_alloc = LocalPageAllocator::new(4096, 4, global_pool.clone());

        // Allocate some pages
        let mut pages = Vec::new();
        for _ in 0..5 {
            pages.push(local_alloc.allocate());
        }

        // Return them
        for page in pages {
            local_alloc.deallocate(page);
        }

        // Local cache should have some pages
        assert!(local_alloc.local_cache.len() > 0);

        // Flush to global pool
        local_alloc.flush();
        assert_eq!(local_alloc.local_cache.len(), 0);
    }
}