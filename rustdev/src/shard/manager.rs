//! Shard manager for coordinating multiple shards

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use dashmap::DashMap;

use crate::types::{Key, TableIdent};
use crate::task::Task;
use crate::Result;
use crate::error::Error;

use super::shard::{Shard, ShardId, ShardConfig, ShardState};
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
    /// All shards
    shards: Arc<RwLock<HashMap<ShardId, Arc<Shard>>>>,
    /// Router for key routing
    router: Arc<ShardRouter>,
    /// Coordinator for shard coordination
    coordinator: Arc<ShardCoordinator>,
    /// Metrics collector
    metrics: Arc<ShardMetrics>,
    /// Table to shard mapping
    table_shards: Arc<DashMap<TableIdent, Vec<ShardId>>>,
}

impl ShardManager {
    /// Create a new shard manager
    pub async fn new(config: ShardManagerConfig) -> Result<Self> {
        let router = Arc::new(ShardRouter::new(config.routing_strategy.clone()));
        let coordinator = Arc::new(ShardCoordinator::new(config.num_shards));
        let metrics = Arc::new(ShardMetrics::new());

        Ok(Self {
            config,
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

        for shard_id in 0..self.config.num_shards {
            let shard_config = ShardConfig {
                id: shard_id,
                ..Default::default()
            };

            let shard = Arc::new(Shard::new(shard_config).await?);
            shards.insert(shard_id, shard);
        }

        Ok(())
    }

    /// Start all shards
    pub async fn start_all(&self) -> Result<()> {
        let shards = self.shards.read().await;

        for shard in shards.values() {
            shard.start().await?;
        }

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
            shard.stop().await?;
        }

        self.coordinator.stop().await?;

        Ok(())
    }

    /// Get shard for a key
    pub async fn get_shard(&self, key: &Key) -> Result<Arc<Shard>> {
        let shard_id = self.router.route_key(key, self.config.num_shards);
        self.get_shard_by_id(shard_id).await
    }

    /// Get shard by ID
    pub async fn get_shard_by_id(&self, shard_id: ShardId) -> Result<Arc<Shard>> {
        let shards = self.shards.read().await;
        shards.get(&shard_id)
            .cloned()
            .ok_or_else(|| Error::NotFound)
    }

    /// Route a task to appropriate shard
    pub async fn route_task(&self, key: &Key, task: Box<dyn Task>) -> Result<()> {
        let shard = self.get_shard(key).await?;
        shard.submit_task(task).await?;
        Ok(())
    }

    /// Get all shards for a table
    pub async fn get_table_shards(&self, table: &TableIdent) -> Vec<ShardId> {
        self.table_shards.get(table)
            .map(|entry| entry.value().clone())
            .unwrap_or_else(|| (0..self.config.num_shards).collect())
    }

    /// Register table with specific shards
    pub async fn register_table(&self, table: TableIdent, shard_ids: Vec<ShardId>) -> Result<()> {
        // Validate shard IDs
        for &shard_id in &shard_ids {
            if shard_id >= self.config.num_shards {
                return Err(Error::InvalidInput(format!("Invalid shard ID: {}", shard_id)));
            }
        }

        self.table_shards.insert(table, shard_ids);
        Ok(())
    }

    /// Get shard states
    pub async fn get_shard_states(&self) -> HashMap<ShardId, ShardState> {
        let shards = self.shards.read().await;
        let mut states = HashMap::new();

        for (&id, shard) in shards.iter() {
            if let Ok(state) = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                shard.state()
            ).await {
                states.insert(id, state);
            }
        }

        states
    }

    /// Rebalance shards
    pub async fn rebalance(&self) -> Result<()> {
        // Get current load distribution
        let loads = self.get_shard_loads().await;

        // Calculate average load
        let total_load: f64 = loads.values().sum();
        let avg_load = total_load / self.config.num_shards as f64;

        // Find overloaded and underloaded shards
        let mut overloaded = Vec::new();
        let mut underloaded = Vec::new();

        for (&shard_id, &load) in &loads {
            let ratio = load / avg_load;
            if ratio > self.config.max_imbalance_ratio {
                overloaded.push((shard_id, load));
            } else if ratio < (1.0 / self.config.max_imbalance_ratio) {
                underloaded.push((shard_id, load));
            }
        }

        // Perform rebalancing if needed
        if !overloaded.is_empty() && !underloaded.is_empty() {
            self.perform_rebalancing(overloaded, underloaded).await?;
        }

        Ok(())
    }

    /// Get shard loads
    async fn get_shard_loads(&self) -> HashMap<ShardId, f64> {
        let shards = self.shards.read().await;
        let mut loads = HashMap::new();

        for (&id, shard) in shards.iter() {
            let stats = shard.stats();
            let load = stats.requests.load(std::sync::atomic::Ordering::Relaxed) as f64;
            loads.insert(id, load);
        }

        loads
    }

    /// Perform rebalancing
    async fn perform_rebalancing(
        &self,
        overloaded: Vec<(ShardId, f64)>,
        underloaded: Vec<(ShardId, f64)>,
    ) -> Result<()> {
        // This is a placeholder for actual rebalancing logic
        // In production, this would involve:
        // 1. Identifying key ranges to move
        // 2. Pausing writes to affected ranges
        // 3. Moving data between shards
        // 4. Updating routing tables
        // 5. Resuming normal operation

        self.coordinator.broadcast_rebalance(overloaded, underloaded).await;

        Ok(())
    }

    /// Start auto-balancing task
    async fn start_auto_balancing(&self) {
        let interval = std::time::Duration::from_secs(self.config.balance_interval);
        let manager = Arc::new(self.clone());

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                if let Err(e) = manager.rebalance().await {
                    eprintln!("Rebalancing failed: {}", e);
                }
            }
        });
    }

    /// Get metrics
    pub fn metrics(&self) -> &ShardMetrics {
        &self.metrics
    }
}

impl Clone for ShardManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            shards: self.shards.clone(),
            router: self.router.clone(),
            coordinator: self.coordinator.clone(),
            metrics: self.metrics.clone(),
            table_shards: self.table_shards.clone(),
        }
    }
}