//! Shard manager for coordinating multiple shards

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use dashmap::DashMap;

use crate::config::KvOptions;
use crate::types::{Key, TableIdent};
use crate::task::Task;
use crate::Result;
use crate::error::Error;

use super::shard::{Shard, ShardId, ShardState};
use super::router::{ShardRouter, RoutingStrategy};
use super::coordinator::ShardCoordinator;
use super::stats::ShardMetrics;

/// Shard manager configuration
#[derive(Debug, Clone)]
pub struct ShardManagerConfig {
    /// Number of shards
    pub num_shards: u16,
    /// Routing strategy
    pub routing_strategy: RoutingStrategy,
    /// Enable auto-balancing
    pub auto_balance: bool,
    /// Balance check interval in seconds
    pub balance_interval: u64,
    /// Maximum load imbalance ratio
    pub max_imbalance_ratio: f64,
}

impl Default for ShardManagerConfig {
    fn default() -> Self {
        Self {
            num_shards: 16,
            routing_strategy: RoutingStrategy::Hash,
            auto_balance: true,
            balance_interval: 60,
            max_imbalance_ratio: 1.5,
        }
    }
}

/// Shard manager
pub struct ShardManager {
    /// Configuration
    config: ShardManagerConfig,
    /// KV options
    options: Arc<KvOptions>,
    /// All shards
    shards: Arc<RwLock<HashMap<ShardId, Arc<Shard>>>>,
    /// Router for distributing requests
    router: Arc<ShardRouter>,
    /// Coordinator for inter-shard operations
    coordinator: Arc<ShardCoordinator>,
    /// Metrics collection
    metrics: Arc<ShardMetrics>,
    /// Table to shard mapping
    table_shards: Arc<DashMap<TableIdent, Vec<ShardId>>>,
}

impl ShardManager {
    /// Create a new shard manager
    pub fn new(config: ShardManagerConfig, options: Arc<KvOptions>) -> Result<Self> {
        let router = Arc::new(ShardRouter::new(config.routing_strategy.clone()));
        let coordinator = Arc::new(ShardCoordinator::new(config.num_shards));
        let metrics = Arc::new(ShardMetrics::new());

        Ok(Self {
            config,
            options,
            shards: Arc::new(RwLock::new(HashMap::new())),
            router,
            coordinator,
            metrics,
            table_shards: Arc::new(DashMap::new()),
        })
    }

    /// Initialize all shards
    pub async fn initialize(&self) -> Result<()> {
        let mut shards = self.shards.write().await;

        // Calculate per-shard file descriptor limit
        let fd_limit = self.options.fd_limit / (self.config.num_shards as u64);

        for shard_id in 0..self.config.num_shards {
            let mut shard = Shard::new(
                shard_id as usize,
                self.options.clone(),
                fd_limit as u32,
            );

            // Initialize the shard
            shard.init().await?;

            shards.insert(shard_id as usize, Arc::new(shard));
        }

        Ok(())
    }

    /// Start all shards
    pub async fn start_all(&self) -> Result<()> {
        let shards = self.shards.read().await;

        // Shards are started by spawning their run() method
        // This is handled in EloqStore, not here

        // Start coordinator
        self.coordinator.start().await?;

        // Start auto-balancing if enabled
        if self.config.auto_balance {
            self.start_auto_balancing().await;
        }

        Ok(())
    }

    /// Stop all shards
    pub async fn stop_all(&self) -> Result<()> {
        let shards = self.shards.read().await;

        for shard in shards.values() {
            shard.stop().await;
        }

        // Stop coordinator
        self.coordinator.stop().await?;

        Ok(())
    }

    /// Route a request to appropriate shard
    pub async fn route_request(&self, table: &TableIdent, key: &Key) -> Result<ShardId> {
        // For now, use simple hash-based routing
        // The router expects number of shards, not a list
        let shard_id = self.router.route_key(key, self.config.num_shards);

        Ok(shard_id)
    }

    /// Submit a task to appropriate shard
    pub async fn submit_task(&self, shard_id: ShardId, task: Box<dyn Task>) -> Result<()> {
        let shards = self.shards.read().await;

        let shard = shards
            .get(&shard_id)
            .ok_or_else(|| Error::InvalidState(format!("Shard {} not found", shard_id)))?;

        // TODO: Submit task to shard's queue
        // shard.submit_task(task).await?;

        Ok(())
    }

    /// Get shards assigned to a table
    async fn get_table_shards(&self, table: &TableIdent) -> Result<Vec<ShardId>> {
        // Check cache
        if let Some(shards) = self.table_shards.get(table) {
            return Ok(shards.clone());
        }

        // Assign shards to table (simple round-robin for now)
        let num_shards = self.config.num_shards;
        let shards: Vec<ShardId> = (0..num_shards).map(|i| i as usize).collect();

        self.table_shards.insert(table.clone(), shards.clone());
        Ok(shards)
    }

    /// Rebalance shards
    pub async fn rebalance(&self) -> Result<()> {
        // Collect metrics
        let shards = self.shards.read().await;
        let mut shard_loads = Vec::new();

        for (id, shard) in shards.iter() {
            let stats = shard.stats();
            let load = stats.reads.load(std::sync::atomic::Ordering::Relaxed) +
                      stats.writes.load(std::sync::atomic::Ordering::Relaxed);
            shard_loads.push((*id, load));
        }

        // Check imbalance
        if let (Some(min), Some(max)) = (
            shard_loads.iter().min_by_key(|x| x.1),
            shard_loads.iter().max_by_key(|x| x.1)
        ) {
            let ratio = max.1 as f64 / (min.1 as f64 + 1.0);
            if ratio > self.config.max_imbalance_ratio {
                // TODO: Implement actual rebalancing
                tracing::info!("Rebalancing needed: max_load={}, min_load={}", max.1, min.1);
            }
        }

        Ok(())
    }

    /// Start auto-balancing task
    async fn start_auto_balancing(&self) {
        let interval = self.config.balance_interval;
        let manager = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                tokio::time::Duration::from_secs(interval)
            );

            loop {
                interval.tick().await;
                if let Err(e) = manager.rebalance().await {
                    tracing::warn!("Rebalance failed: {:?}", e);
                }
            }
        });
    }

    /// Get metrics reference for all shards
    pub fn get_metrics(&self) -> &ShardMetrics {
        &self.metrics
    }

    /// Get specific shard
    pub async fn get_shard(&self, shard_id: ShardId) -> Option<Arc<Shard>> {
        let shards = self.shards.read().await;
        shards.get(&shard_id).cloned()
    }

    /// Get all shard states
    pub async fn get_shard_states(&self) -> HashMap<ShardId, ShardState> {
        let shards = self.shards.read().await;
        let mut states = HashMap::new();

        for (id, _shard) in shards.iter() {
            // TODO: Get actual state from shard
            states.insert(*id, ShardState::Running);
        }

        states
    }
}

// Make ShardManager cloneable
impl Clone for ShardManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            options: self.options.clone(),
            shards: self.shards.clone(),
            router: self.router.clone(),
            coordinator: self.coordinator.clone(),
            metrics: self.metrics.clone(),
            table_shards: self.table_shards.clone(),
        }
    }
}