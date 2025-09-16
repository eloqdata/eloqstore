//! Page cache for in-memory page storage

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::types::{Key, PageId};
use crate::Result;
use crate::error::Error;

use super::DataPage;

/// Page cache configuration
#[derive(Debug, Clone)]
pub struct PageCacheConfig {
    /// Maximum cache size in bytes
    pub max_size: usize,
    /// TTL for cached pages
    pub ttl: Duration,
    /// Enable LRU eviction
    pub enable_lru: bool,
}

impl Default for PageCacheConfig {
    fn default() -> Self {
        Self {
            max_size: 256 * 1024 * 1024, // 256MB
            ttl: Duration::from_secs(300), // 5 minutes
            enable_lru: true,
        }
    }
}

/// Cached page entry
struct CachedPage {
    /// Page data
    page: Arc<DataPage>,
    /// Last access time
    last_access: Instant,
    /// Access count
    access_count: u64,
    /// Page size
    size: usize,
}

/// Page cache implementation
pub struct PageCache {
    /// Configuration
    config: PageCacheConfig,
    /// Cached pages by page ID
    pages_by_id: Arc<DashMap<PageId, CachedPage>>,
    /// Index for key to page ID mapping
    key_index: Arc<DashMap<Key, PageId>>,
    /// Current cache size
    current_size: Arc<RwLock<usize>>,
    /// Cache statistics
    stats: Arc<RwLock<CacheStats>>,
}

/// Cache statistics
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Total hits
    pub hits: u64,
    /// Total misses
    pub misses: u64,
    /// Total evictions
    pub evictions: u64,
    /// Total insertions
    pub insertions: u64,
}

impl PageCache {
    /// Create a new page cache
    pub fn new(config: PageCacheConfig) -> Self {
        Self {
            config,
            pages_by_id: Arc::new(DashMap::new()),
            key_index: Arc::new(DashMap::new()),
            current_size: Arc::new(RwLock::new(0)),
            stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }

    /// Get a page by key
    pub async fn get(&self, key: &Key) -> Option<Arc<DataPage>> {
        // Look up page ID from key index
        let page_id = self.key_index.get(key)?;
        let page_id = *page_id;
        drop(page_id); // Drop the guard

        // Get page by ID
        self.get_by_id(page_id).await
    }

    /// Get a page by ID
    pub async fn get_by_id(&self, page_id: PageId) -> Option<Arc<DataPage>> {
        let mut entry = self.pages_by_id.get_mut(&page_id)?;

        // Check TTL
        if entry.last_access.elapsed() > self.config.ttl {
            drop(entry);
            self.pages_by_id.remove(&page_id);

            let mut stats = self.stats.write().await;
            stats.evictions += 1;
            stats.misses += 1;
            return None;
        }

        // Update access info
        entry.last_access = Instant::now();
        entry.access_count += 1;

        let page = entry.page.clone();
        drop(entry);

        let mut stats = self.stats.write().await;
        stats.hits += 1;

        Some(page)
    }

    /// Insert a page into the cache
    pub async fn insert(&self, key: Key, page: Arc<DataPage>) {
        let page_id = page.page_id();
        let page_size = page.size();

        // Check if we need to evict
        if self.config.enable_lru {
            let current = *self.current_size.read().await;
            if current + page_size > self.config.max_size {
                self.evict_lru(page_size).await;
            }
        }

        // Create cached entry
        let cached = CachedPage {
            page: page.clone(),
            last_access: Instant::now(),
            access_count: 1,
            size: page_size,
        };

        // Insert into cache
        self.pages_by_id.insert(page_id, cached);

        // Update key index with all keys in the page
        for (k, _) in page.iter() {
            self.key_index.insert(k, page_id);
        }

        // Update size and stats
        let mut size = self.current_size.write().await;
        *size += page_size;

        let mut stats = self.stats.write().await;
        stats.insertions += 1;
    }

    /// Invalidate a key from the cache
    pub async fn invalidate(&self, key: &Key) {
        if let Some((_, page_id)) = self.key_index.remove(key) {
            // Check if we should remove the whole page
            let should_remove = !self.key_index.iter().any(|entry| *entry.value() == page_id);

            if should_remove {
                if let Some((_, cached)) = self.pages_by_id.remove(&page_id) {
                    let mut size = self.current_size.write().await;
                    *size = size.saturating_sub(cached.size);

                    let mut stats = self.stats.write().await;
                    stats.evictions += 1;
                }
            }
        }
    }

    /// Invalidate a page from the cache
    pub async fn invalidate_page(&self, page_id: PageId) {
        if let Some((_, cached)) = self.pages_by_id.remove(&page_id) {
            // Remove all keys from index
            self.key_index.retain(|_, pid| *pid != page_id);

            let mut size = self.current_size.write().await;
            *size = size.saturating_sub(cached.size);

            let mut stats = self.stats.write().await;
            stats.evictions += 1;
        }
    }

    /// Clear the entire cache
    pub async fn clear(&self) {
        self.pages_by_id.clear();
        self.key_index.clear();

        let mut size = self.current_size.write().await;
        *size = 0;

        let mut stats = self.stats.write().await;
        stats.evictions += stats.insertions - stats.evictions;
    }

    /// Evict pages using LRU policy
    async fn evict_lru(&self, needed_space: usize) {
        let mut freed_space = 0;
        let mut to_evict = Vec::new();

        // Find pages to evict (sorted by last access time)
        let mut pages: Vec<_> = self.pages_by_id.iter()
            .map(|entry| (entry.key().clone(), entry.last_access, entry.size))
            .collect();

        pages.sort_by_key(|&(_, access, _)| access);

        for (page_id, _, size) in pages {
            to_evict.push(page_id);
            freed_space += size;

            if freed_space >= needed_space {
                break;
            }
        }

        // Evict pages
        for page_id in to_evict {
            self.invalidate_page(page_id).await;
        }
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }

    /// Get current cache size
    pub async fn size(&self) -> usize {
        *self.current_size.read().await
    }
}

impl Clone for CacheStats {
    fn clone(&self) -> Self {
        Self {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            insertions: self.insertions,
        }
    }
}