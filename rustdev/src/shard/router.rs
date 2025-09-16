//! Shard routing strategies

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::types::Key;
use super::shard::ShardId;

/// Routing strategy for sharding
#[derive(Debug, Clone)]
pub enum RoutingStrategy {
    /// Hash-based routing
    Hash,
    /// Range-based routing
    Range,
    /// Consistent hashing
    ConsistentHash,
    /// Round-robin routing
    RoundRobin,
    /// Custom routing function
    Custom(Box<dyn CustomRouter>),
}

/// Custom router trait
pub trait CustomRouter: Send + Sync {
    /// Route a key to a shard
    fn route(&self, key: &Key, num_shards: u16) -> ShardId;

    /// Clone the router
    fn clone_box(&self) -> Box<dyn CustomRouter>;
}

impl Clone for Box<dyn CustomRouter> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl std::fmt::Debug for Box<dyn CustomRouter> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CustomRouter")
    }
}

/// Shard router
pub struct ShardRouter {
    /// Routing strategy
    strategy: RoutingStrategy,
    /// Round-robin counter
    round_robin_counter: std::sync::atomic::AtomicU64,
    /// Consistent hash ring (if using consistent hashing)
    hash_ring: Option<ConsistentHashRing>,
}

impl ShardRouter {
    /// Create a new router
    pub fn new(strategy: RoutingStrategy) -> Self {
        let hash_ring = match &strategy {
            RoutingStrategy::ConsistentHash => Some(ConsistentHashRing::new()),
            _ => None,
        };

        Self {
            strategy,
            round_robin_counter: std::sync::atomic::AtomicU64::new(0),
            hash_ring,
        }
    }

    /// Route a key to a shard
    pub fn route_key(&self, key: &Key, num_shards: u16) -> ShardId {
        match &self.strategy {
            RoutingStrategy::Hash => self.hash_route(key, num_shards),
            RoutingStrategy::Range => self.range_route(key, num_shards),
            RoutingStrategy::ConsistentHash => self.consistent_hash_route(key, num_shards),
            RoutingStrategy::RoundRobin => self.round_robin_route(num_shards),
            RoutingStrategy::Custom(router) => router.route(key, num_shards),
        }
    }

    /// Hash-based routing
    fn hash_route(&self, key: &Key, num_shards: u16) -> ShardId {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % num_shards as u64) as ShardId
    }

    /// Range-based routing
    fn range_route(&self, key: &Key, num_shards: u16) -> ShardId {
        if key.is_empty() {
            return 0;
        }

        // Simple range partitioning based on first byte
        let first_byte = key[0];
        let range_size = 256 / num_shards as usize;
        let shard = (first_byte as usize) / range_size.max(1);

        (shard as ShardId).min((num_shards - 1) as ShardId)
    }

    /// Consistent hash routing
    fn consistent_hash_route(&self, key: &Key, num_shards: u16) -> ShardId {
        if let Some(ring) = &self.hash_ring {
            ring.route(key, num_shards)
        } else {
            self.hash_route(key, num_shards)
        }
    }

    /// Round-robin routing
    fn round_robin_route(&self, num_shards: u16) -> ShardId {
        let counter = self.round_robin_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        (counter % num_shards as u64) as ShardId
    }

    /// Update routing strategy
    pub fn update_strategy(&mut self, strategy: RoutingStrategy) {
        self.strategy = strategy;

        // Update hash ring if needed
        self.hash_ring = match &self.strategy {
            RoutingStrategy::ConsistentHash => Some(ConsistentHashRing::new()),
            _ => None,
        };
    }
}

/// Consistent hash ring implementation
struct ConsistentHashRing {
    /// Virtual nodes per shard
    virtual_nodes: usize,
    /// Ring nodes
    ring: Vec<(u64, ShardId)>,
}

impl ConsistentHashRing {
    /// Create a new consistent hash ring
    fn new() -> Self {
        Self {
            virtual_nodes: 150,
            ring: Vec::new(),
        }
    }

    /// Initialize ring with shards
    pub fn init(&mut self, num_shards: u16) {
        self.ring.clear();

        for shard_id in 0..num_shards {
            for vnode in 0..self.virtual_nodes {
                let node_key = format!("shard-{}-vnode-{}", shard_id, vnode);
                let hash = Self::hash_string(&node_key);
                self.ring.push((hash, shard_id as ShardId));
            }
        }

        self.ring.sort_by_key(|&(hash, _)| hash);
    }

    /// Route a key to a shard
    fn route(&self, key: &Key, num_shards: u16) -> ShardId {
        if self.ring.is_empty() {
            // Initialize ring if not already done
            let mut ring = ConsistentHashRing::new();
            ring.init(num_shards);
            return ring.route(key, num_shards);
        }

        let key_hash = Self::hash_key(key);

        // Binary search for the first node >= key_hash
        match self.ring.binary_search_by_key(&key_hash, |&(hash, _)| hash) {
            Ok(idx) => self.ring[idx].1,
            Err(idx) => {
                if idx >= self.ring.len() {
                    self.ring[0].1 // Wrap around
                } else {
                    self.ring[idx].1
                }
            }
        }
    }

    /// Hash a string
    fn hash_string(s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// Hash a key
    fn hash_key(key: &Key) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Add a shard to the ring
    pub fn add_shard(&mut self, shard_id: ShardId) {
        for vnode in 0..self.virtual_nodes {
            let node_key = format!("shard-{}-vnode-{}", shard_id, vnode);
            let hash = Self::hash_string(&node_key);
            self.ring.push((hash, shard_id));
        }
        self.ring.sort_by_key(|&(hash, _)| hash);
    }

    /// Remove a shard from the ring
    pub fn remove_shard(&mut self, shard_id: ShardId) {
        self.ring.retain(|&(_, id)| id != shard_id);
    }
}

/// Range router for specific key ranges
pub struct RangeRouter {
    /// Range boundaries
    ranges: Vec<(Key, ShardId)>,
}

impl RangeRouter {
    /// Create a new range router
    pub fn new() -> Self {
        Self {
            ranges: Vec::new(),
        }
    }

    /// Add a range mapping
    pub fn add_range(&mut self, end_key: Key, shard_id: ShardId) {
        self.ranges.push((end_key, shard_id));
        self.ranges.sort_by(|a, b| a.0.cmp(&b.0));
    }

    /// Route a key to a shard
    pub fn route(&self, key: &Key) -> Option<ShardId> {
        for (end_key, shard_id) in &self.ranges {
            if key <= end_key {
                return Some(*shard_id);
            }
        }

        // If key is beyond all ranges, use the last shard
        self.ranges.last().map(|(_, shard_id)| *shard_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_hash_routing() {
        let router = ShardRouter::new(RoutingStrategy::Hash);
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        let shard1 = router.route_key(&key1, 16);
        let shard2 = router.route_key(&key2, 16);

        assert!(shard1 < 16);
        assert!(shard2 < 16);

        // Same key should always route to same shard
        assert_eq!(router.route_key(&key1, 16), shard1);
    }

    #[test]
    fn test_range_routing() {
        let router = ShardRouter::new(RoutingStrategy::Range);
        let key1 = Bytes::from(vec![0u8]);
        let key2 = Bytes::from(vec![128u8]);
        let key3 = Bytes::from(vec![255u8]);

        let shard1 = router.route_key(&key1, 4);
        let shard2 = router.route_key(&key2, 4);
        let shard3 = router.route_key(&key3, 4);

        assert_eq!(shard1, 0);
        assert_eq!(shard2, 2);
        assert_eq!(shard3, 3);
    }

    #[test]
    fn test_round_robin_routing() {
        let router = ShardRouter::new(RoutingStrategy::RoundRobin);
        let key = Bytes::from("key");

        let shard1 = router.route_key(&key, 4);
        let shard2 = router.route_key(&key, 4);
        let shard3 = router.route_key(&key, 4);
        let shard4 = router.route_key(&key, 4);
        let shard5 = router.route_key(&key, 4);

        assert_eq!(shard1, 0);
        assert_eq!(shard2, 1);
        assert_eq!(shard3, 2);
        assert_eq!(shard4, 3);
        assert_eq!(shard5, 0); // Wraps around
    }

    #[test]
    fn test_consistent_hash_routing() {
        let mut ring = ConsistentHashRing::new();
        ring.init(4);

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        let shard1 = ring.route(&key1, 4);
        let shard2 = ring.route(&key2, 4);

        assert!(shard1 < 4);
        assert!(shard2 < 4);

        // Same key should always route to same shard
        assert_eq!(ring.route(&key1, 4), shard1);
    }
}