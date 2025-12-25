// src/redis/health/poller.rs

use crate::error::AppResult;
use crate::redis::config::CapacityConfig;
use crate::redis::health::types::RedisSnapshot;
use async_trait::async_trait;
use std::time::{Duration, Instant, SystemTime};

/// Minimal interface the poller needs.
/// Your RedisClient will implement this later.
#[async_trait]
pub trait RedisProbe: Send + Sync {
    async fn ping(&self) -> AppResult<()>;

    /// Returns (used_bytes, max_bytes, used_pct).
    /// used_pct should be None if maxmemory is not configured/known.
    async fn memory_info(&self) -> AppResult<(u64, Option<u64>, Option<f64>)>;

    /// App-defined backlog metric.
    async fn pending_total(&self) -> AppResult<u64>;
}

/// Polls Redis periodically (caller controls scheduling).
/// This does NOT decide healthy/unhealthy; it only measures.
#[derive(Debug, Clone)]
pub struct HealthPoller {
    poll_interval: Duration,
}

impl HealthPoller {
    pub fn from_config(cap: &CapacityConfig) -> Self {
        Self {
            poll_interval: Duration::from_secs(cap.poll_interval_sec),
        }
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Poll once and produce a snapshot.
    ///
    /// `p99_cmd_ms` is injected from your in-app latency tracker.
    pub async fn poll_once<P: RedisProbe>(
        &self,
        probe: &P,
        p99_cmd_ms: Option<f64>,
    ) -> RedisSnapshot {
        let ts = SystemTime::now();

        // 1) Ping + RTT
        let t0 = Instant::now();
        let ping_res = probe.ping().await;
        let ping_ok = ping_res.is_ok();

        let ping_rtt_ms = if ping_ok {
            Some(t0.elapsed().as_secs_f64() * 1000.0)
        } else {
            None
        };

        if !ping_ok {
            // Redis is down: everything else is unknown
            return RedisSnapshot {
                ts,
                is_up: false,
                ping_rtt_ms,
                used_memory_bytes: None,
                maxmemory_bytes: None,
                used_memory_pct: None,
                pending_total: None,
                p99_cmd_ms,
            };
        }

        // 2) Memory info (best-effort)
        let (used_memory_bytes, maxmemory_bytes, used_memory_pct) =
            match probe.memory_info().await {
                Ok((used, max, pct)) => (Some(used), max, pct),
                Err(_) => (None, None, None),
            };

        // 3) Pending / backlog (best-effort)
        let pending_total = match probe.pending_total().await {
            Ok(v) => Some(v),
            Err(_) => None,
        };

        RedisSnapshot {
            ts,
            is_up: true,
            ping_rtt_ms,
            used_memory_bytes,
            maxmemory_bytes,
            used_memory_pct,
            pending_total,
            p99_cmd_ms,
        }
    }
}

