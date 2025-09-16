//! Shard statistics and metrics

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use prometheus::{Counter, Gauge, Histogram, HistogramOpts, Registry};

use super::shard::ShardId;

/// Shard metrics for monitoring
pub struct ShardMetrics {
    /// Prometheus registry
    registry: Registry,
    /// Request counter
    requests: Counter,
    /// Bytes read counter
    bytes_read: Counter,
    /// Bytes written counter
    bytes_written: Counter,
    /// Queue depth gauge
    queue_depth: Gauge,
    /// Response time histogram
    response_time: Histogram,
    /// Per-shard metrics
    shard_metrics: Arc<RwLock<HashMap<ShardId, ShardMetricSet>>>,
}

impl ShardMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        let registry = Registry::new();

        let requests = Counter::new("shard_requests_total", "Total shard requests")
            .expect("metric creation failed");
        registry.register(Box::new(requests.clone())).ok();

        let bytes_read = Counter::new("shard_bytes_read_total", "Total bytes read")
            .expect("metric creation failed");
        registry.register(Box::new(bytes_read.clone())).ok();

        let bytes_written = Counter::new("shard_bytes_written_total", "Total bytes written")
            .expect("metric creation failed");
        registry.register(Box::new(bytes_written.clone())).ok();

        let queue_depth = Gauge::new("shard_queue_depth", "Current queue depth")
            .expect("metric creation failed");
        registry.register(Box::new(queue_depth.clone())).ok();

        let response_time = Histogram::with_opts(
            HistogramOpts::new("shard_response_time_seconds", "Response time distribution")
        ).expect("metric creation failed");
        registry.register(Box::new(response_time.clone())).ok();

        Self {
            registry,
            requests,
            bytes_read,
            bytes_written,
            queue_depth,
            response_time,
            shard_metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a request
    pub fn record_request(&self, shard_id: ShardId) {
        self.requests.inc();
        tokio::spawn({
            let shard_metrics = self.shard_metrics.clone();
            async move {
                let mut metrics = shard_metrics.write().await;
                metrics.entry(shard_id)
                    .or_insert_with(ShardMetricSet::new)
                    .requests
                    .fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    /// Record bytes read
    pub fn record_bytes_read(&self, shard_id: ShardId, bytes: u64) {
        self.bytes_read.inc_by(bytes as f64);
        tokio::spawn({
            let shard_metrics = self.shard_metrics.clone();
            async move {
                let mut metrics = shard_metrics.write().await;
                metrics.entry(shard_id)
                    .or_insert_with(ShardMetricSet::new)
                    .bytes_read
                    .fetch_add(bytes, Ordering::Relaxed);
            }
        });
    }

    /// Record bytes written
    pub fn record_bytes_written(&self, shard_id: ShardId, bytes: u64) {
        self.bytes_written.inc_by(bytes as f64);
        tokio::spawn({
            let shard_metrics = self.shard_metrics.clone();
            async move {
                let mut metrics = shard_metrics.write().await;
                metrics.entry(shard_id)
                    .or_insert_with(ShardMetricSet::new)
                    .bytes_written
                    .fetch_add(bytes, Ordering::Relaxed);
            }
        });
    }

    /// Update queue depth
    pub fn update_queue_depth(&self, depth: usize) {
        self.queue_depth.set(depth as f64);
    }

    /// Record response time
    pub fn record_response_time(&self, duration: Duration) {
        self.response_time.observe(duration.as_secs_f64());
    }

    /// Get shard statistics
    pub async fn get_shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
        let metrics = self.shard_metrics.read().await;
        metrics.get(&shard_id).map(|m| m.to_stats())
    }

    /// Get all shard statistics
    pub async fn get_all_stats(&self) -> HashMap<ShardId, ShardStats> {
        let metrics = self.shard_metrics.read().await;
        metrics.iter()
            .map(|(&id, m)| (id, m.to_stats()))
            .collect()
    }

    /// Get prometheus registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Reset metrics for a shard
    pub async fn reset_shard(&self, shard_id: ShardId) {
        let mut metrics = self.shard_metrics.write().await;
        if let Some(m) = metrics.get_mut(&shard_id) {
            m.reset();
        }
    }

    /// Generate metrics report
    pub async fn generate_report(&self) -> MetricsReport {
        let all_stats = self.get_all_stats().await;

        let total_requests: u64 = all_stats.values().map(|s| s.requests).sum();
        let total_bytes_read: u64 = all_stats.values().map(|s| s.bytes_read).sum();
        let total_bytes_written: u64 = all_stats.values().map(|s| s.bytes_written).sum();

        let avg_latency = if !all_stats.is_empty() {
            let total_latency: u64 = all_stats.values().map(|s| s.avg_latency_us).sum();
            total_latency / all_stats.len() as u64
        } else {
            0
        };

        MetricsReport {
            timestamp: Instant::now(),
            shard_count: all_stats.len(),
            total_requests,
            total_bytes_read,
            total_bytes_written,
            avg_latency_us: avg_latency,
            shard_stats: all_stats,
        }
    }
}

/// Per-shard metric set
struct ShardMetricSet {
    /// Request count
    requests: AtomicU64,
    /// Bytes read
    bytes_read: AtomicU64,
    /// Bytes written
    bytes_written: AtomicU64,
    /// Error count
    errors: AtomicU64,
    /// Total latency in microseconds
    total_latency_us: AtomicU64,
    /// Peak queue depth
    peak_queue_depth: AtomicUsize,
}

impl ShardMetricSet {
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            peak_queue_depth: AtomicUsize::new(0),
        }
    }

    fn reset(&mut self) {
        self.requests.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.total_latency_us.store(0, Ordering::Relaxed);
        self.peak_queue_depth.store(0, Ordering::Relaxed);
    }

    fn to_stats(&self) -> ShardStats {
        let requests = self.requests.load(Ordering::Relaxed);
        let total_latency = self.total_latency_us.load(Ordering::Relaxed);

        ShardStats {
            requests,
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            avg_latency_us: if requests > 0 { total_latency / requests } else { 0 },
            peak_queue_depth: self.peak_queue_depth.load(Ordering::Relaxed),
        }
    }
}

/// Shard statistics snapshot
#[derive(Debug, Clone)]
pub struct ShardStats {
    /// Total requests
    pub requests: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Total errors
    pub errors: u64,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// Peak queue depth
    pub peak_queue_depth: usize,
}

/// Metrics report
#[derive(Debug)]
pub struct MetricsReport {
    /// Report timestamp
    pub timestamp: Instant,
    /// Number of shards
    pub shard_count: usize,
    /// Total requests across all shards
    pub total_requests: u64,
    /// Total bytes read
    pub total_bytes_read: u64,
    /// Total bytes written
    pub total_bytes_written: u64,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// Per-shard statistics
    pub shard_stats: HashMap<ShardId, ShardStats>,
}

impl MetricsReport {
    /// Generate summary string
    pub fn summary(&self) -> String {
        format!(
            "Shards: {}, Requests: {}, Read: {} MB, Written: {} MB, Avg Latency: {} µs",
            self.shard_count,
            self.total_requests,
            self.total_bytes_read / (1024 * 1024),
            self.total_bytes_written / (1024 * 1024),
            self.avg_latency_us
        )
    }

    /// Get top N shards by request count
    pub fn top_shards_by_requests(&self, n: usize) -> Vec<(ShardId, u64)> {
        let mut shards: Vec<_> = self.shard_stats
            .iter()
            .map(|(&id, stats)| (id, stats.requests))
            .collect();

        shards.sort_by(|a, b| b.1.cmp(&a.1));
        shards.truncate(n);
        shards
    }

    /// Get load distribution
    pub fn load_distribution(&self) -> LoadDistribution {
        if self.shard_stats.is_empty() {
            return LoadDistribution::default();
        }

        let loads: Vec<f64> = self.shard_stats
            .values()
            .map(|s| s.requests as f64)
            .collect();

        let total: f64 = loads.iter().sum();
        let avg = total / loads.len() as f64;

        let variance = loads.iter()
            .map(|&load| (load - avg).powi(2))
            .sum::<f64>() / loads.len() as f64;

        let std_dev = variance.sqrt();
        let max = loads.iter().cloned().fold(0.0, f64::max);
        let min = loads.iter().cloned().fold(f64::MAX, f64::min);

        LoadDistribution {
            avg_load: avg,
            std_deviation: std_dev,
            max_load: max,
            min_load: min,
            imbalance_ratio: if min > 0.0 { max / min } else { f64::INFINITY },
        }
    }
}

/// Load distribution statistics
#[derive(Debug, Default)]
pub struct LoadDistribution {
    /// Average load
    pub avg_load: f64,
    /// Standard deviation
    pub std_deviation: f64,
    /// Maximum load
    pub max_load: f64,
    /// Minimum load
    pub min_load: f64,
    /// Imbalance ratio (max/min)
    pub imbalance_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shard_metrics() {
        let metrics = ShardMetrics::new();

        // Record some metrics
        metrics.record_request(0);
        metrics.record_request(0);
        metrics.record_request(1);

        metrics.record_bytes_read(0, 1024);
        metrics.record_bytes_written(1, 2048);

        // Wait for async updates
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Check shard stats
        let stats_0 = metrics.get_shard_stats(0).await.unwrap();
        assert_eq!(stats_0.requests, 2);
        assert_eq!(stats_0.bytes_read, 1024);

        let stats_1 = metrics.get_shard_stats(1).await.unwrap();
        assert_eq!(stats_1.requests, 1);
        assert_eq!(stats_1.bytes_written, 2048);
    }

    #[tokio::test]
    async fn test_metrics_report() {
        let metrics = ShardMetrics::new();

        // Add some data
        for shard_id in 0..4 {
            for _ in 0..10 {
                metrics.record_request(shard_id);
            }
            metrics.record_bytes_read(shard_id, 1000 * (shard_id + 1) as u64);
        }

        // Wait for async updates
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Generate report
        let report = metrics.generate_report().await;
        assert_eq!(report.shard_count, 4);
        assert_eq!(report.total_requests, 40);

        // Check load distribution
        let dist = report.load_distribution();
        assert_eq!(dist.avg_load, 10.0);
    }
}