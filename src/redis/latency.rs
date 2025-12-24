// src/redis/latency.rs

use crate::redis::config::RedisConfig;
use std::sync::Mutex;

/// Rolling latency tracker for **Redis STREAM publish commands** (XADD).
///
/// - Stores the last N latency samples (milliseconds) in a ring buffer
/// - Computes p99 on demand by sorting a snapshot
///
/// Intended use:
/// - observe_ms() called on every successful (or attempted) XADD
/// - p99_ms() read by the health evaluator
#[derive(Debug)]
pub struct RedisPublishLatency {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    buf: Vec<f64>,
    cap: usize,
    len: usize,
    idx: usize,
}

impl RedisPublishLatency {
    /// Create from config: uses `capacity.redis_publish_latency_window`.
    pub fn from_config(cfg: &RedisConfig) -> Self {
        Self::new(cfg.capacity.redis_publish_latency_window as usize)
    }

    /// Create with an explicit sample window size.
    ///
    /// `window_samples` is a count of recent publish latencies to retain,
    /// not a time duration.
    pub fn new(window_samples: usize) -> Self {
        assert!(window_samples > 0, "latency window must be > 0");

        Self {
            inner: Mutex::new(Inner {
                buf: vec![0.0; window_samples],
                cap: window_samples,
                len: 0,
                idx: 0,
            }),
        }
    }

    /// Add one publish latency sample in milliseconds.
    ///
    /// Ignores negative and non-finite values.
    #[inline]
    pub fn observe_ms(&self, ms: f64) {
        if !ms.is_finite() || ms < 0.0 {
            return;
        }

        let mut g = self
            .inner
            .lock()
            .expect("redis publish latency mutex poisoned");

        // avoid simultaneous mutable+immutable borrows of `g`
        let idx = g.idx;
        g.buf[idx] = ms;

        g.idx = (idx + 1) % g.cap;

        if g.len < g.cap {
            g.len += 1;
        }
    }

    /// Rolling p99 latency in milliseconds over the current window.
    /// Returns None if there are no samples yet.
    //is  = Some (12.5) “Over the last redis_publish_latency_window Redis publishes, 99% took ≤ 12.5 ms, and the slowest ~1% took longer.”
    pub fn p99_ms(&self) -> Option<f64> {
        let g = self
            .inner
            .lock()
            .expect("redis publish latency mutex poisoned");
        if g.len == 0 {
            return None;
        }

        // Snapshot the populated slice.
        let mut snap = Vec::with_capacity(g.len);
        snap.extend_from_slice(&g.buf[..g.len]);
        drop(g);

        // Sort ascending.
        snap.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // p99 index: ceil(0.99 * n) - 1 (clamped to [0, n-1]).
        let n = snap.len();
        let mut idx = ((0.99 * (n as f64)).ceil() as isize) - 1;
        if idx < 0 {
            idx = 0;
        }
        let idx = (idx as usize).min(n - 1);

        Some(snap[idx])
    }

    #[inline]
    pub fn len(&self) -> usize {
        let g = self
            .inner
            .lock()
            .expect("redis publish latency mutex poisoned");
        g.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all samples.
    pub fn clear(&self) {
        let mut g = self
            .inner
            .lock()
            .expect("redis publish latency mutex poisoned");
        g.len = 0;
        g.idx = 0;
        for x in &mut g.buf {
            *x = 0.0;
        }
    }
}
