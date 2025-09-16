//! Shard coordination for distributed operations

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, broadcast, RwLock};
use tokio::task::JoinHandle;

use crate::Result;
use crate::error::Error;

use super::shard::ShardId;

/// Coordination message types
#[derive(Debug, Clone)]
pub enum CoordinationMessage {
    /// Heartbeat from a shard
    Heartbeat { shard_id: ShardId, timestamp: Instant },
    /// Rebalance request
    Rebalance {
        overloaded: Vec<(ShardId, f64)>,
        underloaded: Vec<(ShardId, f64)>,
    },
    /// Shard state change
    StateChange { shard_id: ShardId, state: ShardStateInfo },
    /// Migration request
    Migration {
        from_shard: ShardId,
        to_shard: ShardId,
        key_range: KeyRange,
    },
    /// Shutdown signal
    Shutdown,
    /// Custom message
    Custom(Box<dyn CustomMessage>),
}

/// Custom message trait
pub trait CustomMessage: Send + Sync {
    /// Clone the message
    fn clone_box(&self) -> Box<dyn CustomMessage>;
}

impl Clone for Box<dyn CustomMessage> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl std::fmt::Debug for Box<dyn CustomMessage> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CustomMessage")
    }
}

/// Shard state information
#[derive(Debug, Clone)]
pub struct ShardStateInfo {
    /// Is shard active
    pub active: bool,
    /// Current load
    pub load: f64,
    /// Queue depth
    pub queue_depth: usize,
    /// Last update time
    pub last_update: Instant,
}

/// Key range for migration
#[derive(Debug, Clone)]
pub struct KeyRange {
    /// Start key (inclusive)
    pub start: Option<Vec<u8>>,
    /// End key (exclusive)
    pub end: Option<Vec<u8>>,
}

/// Shard coordinator
pub struct ShardCoordinator {
    /// Number of shards
    num_shards: u16,
    /// Message sender
    tx: broadcast::Sender<CoordinationMessage>,
    /// Message receiver
    rx: Arc<RwLock<broadcast::Receiver<CoordinationMessage>>>,
    /// Shard states
    shard_states: Arc<RwLock<HashMap<ShardId, ShardStateInfo>>>,
    /// Coordinator handle
    handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Shutdown signal
    shutdown: tokio_util::sync::CancellationToken,
}

impl ShardCoordinator {
    /// Create a new coordinator
    pub fn new(num_shards: u16) -> Self {
        let (tx, rx) = broadcast::channel(1000);

        Self {
            num_shards,
            tx,
            rx: Arc::new(RwLock::new(rx)),
            shard_states: Arc::new(RwLock::new(HashMap::new())),
            handle: Arc::new(RwLock::new(None)),
            shutdown: tokio_util::sync::CancellationToken::new(),
        }
    }

    /// Start the coordinator
    pub async fn start(&self) -> Result<()> {
        let mut handle_guard = self.handle.write().await;
        if handle_guard.is_some() {
            return Err(Error::InvalidState("Coordinator already started".into()));
        }

        let mut rx = self.tx.subscribe();
        let shard_states = self.shard_states.clone();
        let shutdown = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            Self::coordinator_loop(rx, shard_states, shutdown).await;
        });

        *handle_guard = Some(handle);
        Ok(())
    }

    /// Stop the coordinator
    pub async fn stop(&self) -> Result<()> {
        self.shutdown.cancel();
        self.tx.send(CoordinationMessage::Shutdown).ok();

        let mut handle_guard = self.handle.write().await;
        if let Some(handle) = handle_guard.take() {
            handle.await.map_err(|e| Error::Internal(e.to_string()))?;
        }

        Ok(())
    }

    /// Send a coordination message
    pub fn send(&self, message: CoordinationMessage) -> Result<()> {
        self.tx.send(message)
            .map_err(|_| Error::Internal("Failed to send coordination message".into()))?;
        Ok(())
    }

    /// Subscribe to coordination messages
    pub fn subscribe(&self) -> broadcast::Receiver<CoordinationMessage> {
        self.tx.subscribe()
    }

    /// Register shard heartbeat
    pub async fn heartbeat(&self, shard_id: ShardId) {
        let message = CoordinationMessage::Heartbeat {
            shard_id,
            timestamp: Instant::now(),
        };
        self.send(message).ok();
    }

    /// Broadcast rebalance request
    pub async fn broadcast_rebalance(
        &self,
        overloaded: Vec<(ShardId, f64)>,
        underloaded: Vec<(ShardId, f64)>,
    ) {
        let message = CoordinationMessage::Rebalance {
            overloaded,
            underloaded,
        };
        self.send(message).ok();
    }

    /// Get shard state
    pub async fn get_shard_state(&self, shard_id: ShardId) -> Option<ShardStateInfo> {
        let states = self.shard_states.read().await;
        states.get(&shard_id).cloned()
    }

    /// Get all shard states
    pub async fn get_all_states(&self) -> HashMap<ShardId, ShardStateInfo> {
        self.shard_states.read().await.clone()
    }

    /// Check shard health
    pub async fn check_health(&self, timeout: Duration) -> HashMap<ShardId, bool> {
        let states = self.shard_states.read().await;
        let now = Instant::now();
        let mut health = HashMap::new();

        for i in 0..self.num_shards {
            let shard_id = i as ShardId;
            if let Some(state) = states.get(&shard_id) {
                let is_healthy = now.duration_since(state.last_update) < timeout;
                health.insert(shard_id, is_healthy);
            } else {
                health.insert(shard_id, false);
            }
        }

        health
    }

    /// Coordinator main loop
    async fn coordinator_loop(
        mut rx: broadcast::Receiver<CoordinationMessage>,
        shard_states: Arc<RwLock<HashMap<ShardId, ShardStateInfo>>>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                }
                Ok(message) = rx.recv() => {
                    Self::handle_message(message, &shard_states).await;
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    // Periodic health check
                    Self::check_stale_shards(&shard_states).await;
                }
            }
        }
    }

    /// Handle coordination message
    async fn handle_message(
        message: CoordinationMessage,
        shard_states: &Arc<RwLock<HashMap<ShardId, ShardStateInfo>>>,
    ) {
        match message {
            CoordinationMessage::Heartbeat { shard_id, timestamp } => {
                let mut states = shard_states.write().await;
                states.entry(shard_id)
                    .and_modify(|state| state.last_update = timestamp)
                    .or_insert_with(|| ShardStateInfo {
                        active: true,
                        load: 0.0,
                        queue_depth: 0,
                        last_update: timestamp,
                    });
            }
            CoordinationMessage::StateChange { shard_id, state } => {
                let mut states = shard_states.write().await;
                states.insert(shard_id, state);
            }
            CoordinationMessage::Rebalance { overloaded, underloaded } => {
                // Log rebalance request
                println!("Rebalance requested: {} overloaded, {} underloaded",
                    overloaded.len(), underloaded.len());
            }
            CoordinationMessage::Migration { from_shard, to_shard, key_range } => {
                // Log migration request
                println!("Migration requested: shard {} -> shard {}", from_shard, to_shard);
            }
            CoordinationMessage::Shutdown => {
                // Handled by select
            }
            CoordinationMessage::Custom(_) => {
                // Handle custom messages
            }
        }
    }

    /// Check for stale shards
    async fn check_stale_shards(
        shard_states: &Arc<RwLock<HashMap<ShardId, ShardStateInfo>>>,
    ) {
        let mut states = shard_states.write().await;
        let now = Instant::now();
        let stale_timeout = Duration::from_secs(30);

        let stale_shards: Vec<ShardId> = states
            .iter()
            .filter(|(_, state)| now.duration_since(state.last_update) > stale_timeout)
            .map(|(&id, _)| id)
            .collect();

        for shard_id in stale_shards {
            if let Some(state) = states.get_mut(&shard_id) {
                state.active = false;
            }
        }
    }
}