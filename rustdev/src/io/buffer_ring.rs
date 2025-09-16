//! Buffer ring management for zero-copy I/O

use std::collections::VecDeque;
use std::sync::Arc;
use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;

use crate::Result;
use crate::error::Error;

/// Buffer ring configuration
#[derive(Debug, Clone)]
pub struct BufferRingConfig {
    /// Number of buffers in the ring
    pub buffer_count: usize,
    /// Size of each buffer
    pub buffer_size: usize,
    /// Maximum pending operations
    pub max_pending: usize,
}

impl Default for BufferRingConfig {
    fn default() -> Self {
        Self {
            buffer_count: 256,
            buffer_size: 4096,
            max_pending: 128,
        }
    }
}

/// A single buffer in the ring
#[derive(Clone)]
pub struct RingBuffer {
    /// Buffer ID
    id: usize,
    /// Buffer data
    data: BytesMut,
    /// Is buffer currently in use
    in_use: Arc<Mutex<bool>>,
}

impl RingBuffer {
    /// Create a new ring buffer
    fn new(id: usize, size: usize) -> Self {
        Self {
            id,
            data: BytesMut::zeroed(size),
            in_use: Arc::new(Mutex::new(false)),
        }
    }

    /// Get buffer ID
    pub fn id(&self) -> usize {
        self.id
    }

    /// Get buffer as slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable buffer as slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Mark buffer as in use
    pub fn acquire(&self) -> bool {
        let mut in_use = self.in_use.lock();
        if !*in_use {
            *in_use = true;
            true
        } else {
            false
        }
    }

    /// Mark buffer as available
    pub fn release(&self) {
        let mut in_use = self.in_use.lock();
        *in_use = false;
    }

    /// Check if buffer is in use
    pub fn is_in_use(&self) -> bool {
        *self.in_use.lock()
    }

    /// Clear buffer content
    pub fn clear(&mut self) {
        self.data.clear();
        self.data.resize(self.data.capacity(), 0);
    }

    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
}

/// Buffer ring for managing I/O buffers
pub struct BufferRing {
    /// Configuration
    config: BufferRingConfig,
    /// All buffers
    buffers: Vec<RingBuffer>,
    /// Available buffer indices
    available: Arc<Mutex<VecDeque<usize>>>,
    /// Statistics
    stats: Arc<Mutex<BufferRingStats>>,
}

/// Buffer ring statistics
#[derive(Debug, Default, Clone)]
pub struct BufferRingStats {
    /// Total allocations
    pub allocations: u64,
    /// Total releases
    pub releases: u64,
    /// Current buffers in use
    pub in_use: usize,
    /// Peak buffers in use
    pub peak_in_use: usize,
    /// Allocation failures
    pub allocation_failures: u64,
}

impl BufferRing {
    /// Create a new buffer ring
    pub fn new(config: BufferRingConfig) -> Self {
        let mut buffers = Vec::with_capacity(config.buffer_count);
        let mut available = VecDeque::with_capacity(config.buffer_count);

        for i in 0..config.buffer_count {
            buffers.push(RingBuffer::new(i, config.buffer_size));
            available.push_back(i);
        }

        Self {
            config,
            buffers,
            available: Arc::new(Mutex::new(available)),
            stats: Arc::new(Mutex::new(BufferRingStats::default())),
        }
    }

    /// Allocate a buffer from the ring
    pub fn allocate(&self) -> Option<RingBuffer> {
        let mut available = self.available.lock();
        let mut stats = self.stats.lock();

        if let Some(idx) = available.pop_front() {
            let buffer = self.buffers[idx].clone();
            if buffer.acquire() {
                stats.allocations += 1;
                stats.in_use += 1;
                if stats.in_use > stats.peak_in_use {
                    stats.peak_in_use = stats.in_use;
                }
                Some(buffer)
            } else {
                // Buffer was already in use, shouldn't happen
                available.push_back(idx);
                stats.allocation_failures += 1;
                None
            }
        } else {
            stats.allocation_failures += 1;
            None
        }
    }

    /// Release a buffer back to the ring
    pub fn release(&self, buffer: RingBuffer) {
        buffer.release();

        let mut available = self.available.lock();
        let mut stats = self.stats.lock();

        available.push_back(buffer.id());
        stats.releases += 1;
        stats.in_use = stats.in_use.saturating_sub(1);
    }

    /// Get ring statistics
    pub fn stats(&self) -> BufferRingStats {
        self.stats.lock().clone()
    }

    /// Get number of available buffers
    pub fn available_count(&self) -> usize {
        self.available.lock().len()
    }

    /// Get total buffer count
    pub fn total_count(&self) -> usize {
        self.config.buffer_count
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.config.buffer_size
    }
}

/// Buffer pool for different buffer sizes
pub struct BufferPool {
    /// Rings for different buffer sizes
    rings: Arc<Mutex<HashMap<usize, Arc<BufferRing>>>>,
    /// Default ring configuration
    default_config: BufferRingConfig,
}

use std::collections::HashMap;

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(default_config: BufferRingConfig) -> Self {
        Self {
            rings: Arc::new(Mutex::new(HashMap::new())),
            default_config,
        }
    }

    /// Get or create a buffer ring for the given size
    pub fn get_ring(&self, buffer_size: usize) -> Arc<BufferRing> {
        let mut rings = self.rings.lock();

        rings.entry(buffer_size)
            .or_insert_with(|| {
                let mut config = self.default_config.clone();
                config.buffer_size = buffer_size;
                Arc::new(BufferRing::new(config))
            })
            .clone()
    }

    /// Allocate a buffer of the given size
    pub fn allocate(&self, size: usize) -> Option<RingBuffer> {
        // Round up to nearest power of 2 for better pooling
        let buffer_size = size.next_power_of_two();
        let ring = self.get_ring(buffer_size);
        ring.allocate()
    }

    /// Release a buffer
    pub fn release(&self, buffer: RingBuffer) {
        let ring = self.get_ring(buffer.capacity());
        ring.release(buffer);
    }

    /// Get statistics for all rings
    pub fn all_stats(&self) -> HashMap<usize, BufferRingStats> {
        let rings = self.rings.lock();
        rings.iter()
            .map(|(&size, ring)| (size, ring.stats()))
            .collect()
    }
}

/// Scoped buffer guard that automatically releases on drop
pub struct ScopedBuffer {
    buffer: Option<RingBuffer>,
    ring: Arc<BufferRing>,
}

impl ScopedBuffer {
    /// Create a new scoped buffer
    pub fn new(ring: Arc<BufferRing>) -> Option<Self> {
        ring.allocate().map(|buffer| Self {
            buffer: Some(buffer),
            ring,
        })
    }

    /// Get the underlying buffer
    pub fn buffer(&self) -> &RingBuffer {
        self.buffer.as_ref().unwrap()
    }

    /// Get mutable reference to the buffer
    pub fn buffer_mut(&mut self) -> &mut RingBuffer {
        self.buffer.as_mut().unwrap()
    }

    /// Take ownership of the buffer (prevents automatic release)
    pub fn take(mut self) -> RingBuffer {
        self.buffer.take().unwrap()
    }
}

impl Drop for ScopedBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.ring.release(buffer);
        }
    }
}

/// Buffer chain for handling large data
pub struct BufferChain {
    /// Buffers in the chain with their data sizes
    buffers: Vec<(RingBuffer, usize)>,
    /// Total data size
    total_size: usize,
}

impl BufferChain {
    /// Create a new buffer chain
    pub fn new() -> Self {
        Self {
            buffers: Vec::new(),
            total_size: 0,
        }
    }

    /// Add a buffer to the chain
    pub fn add_buffer(&mut self, buffer: RingBuffer, data_size: usize) {
        self.total_size += data_size;
        self.buffers.push((buffer, data_size));
    }

    /// Get total size of data in chain
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// Get number of buffers in chain
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// Consolidate chain into a single Bytes
    pub fn consolidate(&self) -> Bytes {
        if self.buffers.is_empty() {
            return Bytes::new();
        }

        let mut result = BytesMut::with_capacity(self.total_size);

        for (buffer, data_size) in &self.buffers {
            result.extend_from_slice(&buffer.as_slice()[..*data_size]);
        }

        result.freeze()
    }

    /// Release all buffers in the chain
    pub fn release(self, pool: &BufferPool) {
        for (buffer, _) in self.buffers {
            pool.release(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer() {
        let mut buffer = RingBuffer::new(0, 1024);

        assert_eq!(buffer.id(), 0);
        assert_eq!(buffer.capacity(), 1024);
        assert!(!buffer.is_in_use());

        assert!(buffer.acquire());
        assert!(buffer.is_in_use());
        assert!(!buffer.acquire()); // Can't acquire twice

        buffer.release();
        assert!(!buffer.is_in_use());
    }

    #[test]
    fn test_buffer_ring() {
        let config = BufferRingConfig {
            buffer_count: 4,
            buffer_size: 1024,
            max_pending: 2,
        };

        let ring = BufferRing::new(config);

        // Allocate all buffers
        let mut buffers = Vec::new();
        for _ in 0..4 {
            let buffer = ring.allocate().unwrap();
            buffers.push(buffer);
        }

        // Ring should be empty
        assert!(ring.allocate().is_none());
        assert_eq!(ring.available_count(), 0);

        // Release one buffer
        let buffer = buffers.pop().unwrap();
        ring.release(buffer);

        assert_eq!(ring.available_count(), 1);

        // Can allocate again
        let _buffer = ring.allocate().unwrap();
        assert_eq!(ring.available_count(), 0);

        // Check stats
        let stats = ring.stats();
        assert_eq!(stats.allocations, 5);
        assert_eq!(stats.releases, 1);
        assert_eq!(stats.peak_in_use, 4);
    }

    #[test]
    fn test_buffer_pool() {
        let config = BufferRingConfig::default();
        let pool = BufferPool::new(config);

        // Allocate buffers of different sizes
        let buf1 = pool.allocate(1000).unwrap();
        let buf2 = pool.allocate(2000).unwrap();
        let buf3 = pool.allocate(1000).unwrap();

        assert_eq!(buf1.capacity(), 1024); // Rounded to power of 2
        assert_eq!(buf2.capacity(), 2048);
        assert_eq!(buf3.capacity(), 1024);

        pool.release(buf1);
        pool.release(buf2);
        pool.release(buf3);

        // Check we have two different rings
        let stats = pool.all_stats();
        assert_eq!(stats.len(), 2);
    }

    #[test]
    fn test_scoped_buffer() {
        let config = BufferRingConfig::default();
        let ring = Arc::new(BufferRing::new(config));

        {
            let _scoped = ScopedBuffer::new(ring.clone()).unwrap();
            assert_eq!(ring.available_count(), ring.total_count() - 1);
        }
        // Buffer automatically released
        assert_eq!(ring.available_count(), ring.total_count());
    }

    #[test]
    fn test_buffer_chain() {
        let config = BufferRingConfig {
            buffer_count: 4,
            buffer_size: 100,
            max_pending: 2,
        };
        let pool = BufferPool::new(config);

        let mut chain = BufferChain::new();

        // Add buffers to chain
        for i in 0..3 {
            let mut buffer = pool.allocate(128).unwrap(); // Will round to power of 2
            // Fill buffer with pattern
            for j in 0..100 {
                buffer.as_mut_slice()[j] = (i * 100 + j) as u8;
            }
            chain.add_buffer(buffer, 100); // Only use 100 bytes of data
        }

        assert_eq!(chain.buffer_count(), 3);
        assert_eq!(chain.total_size(), 300);

        // Consolidate to single buffer
        let consolidated = chain.consolidate();
        assert_eq!(consolidated.len(), 300);
        assert_eq!(consolidated[0], 0);
        assert_eq!(consolidated[99], 99);
        assert_eq!(consolidated[100], 100);
        assert_eq!(consolidated[199], 199);
        assert_eq!(consolidated[200], 200);
    }
}