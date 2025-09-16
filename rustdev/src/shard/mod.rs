//! Sharding system for distributed work management

pub mod shard;
pub mod manager;
pub mod router;
pub mod worker;
pub mod queue;
pub mod coordinator;
pub mod stats;

pub use shard::{Shard, ShardId, ShardConfig, ShardState};
pub use manager::{ShardManager, ShardManagerConfig};
pub use router::{ShardRouter, RoutingStrategy};
pub use worker::{ShardWorker, WorkerPool};
pub use queue::{WorkQueue, WorkItem, WorkPriority};
pub use coordinator::{ShardCoordinator, CoordinationMessage};
pub use stats::{ShardStats, ShardMetrics};