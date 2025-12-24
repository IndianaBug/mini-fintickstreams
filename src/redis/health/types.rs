// src/redis/health/types.rs

use std::time::SystemTime;

/// Why Redis was (or should be) disabled.
/// Keep these aligned with your Prometheus label values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DisableReason {
    Down,
    MaxMemory,
    MaxPending,
    Latency,
    Manual,
    Saturated,
}

impl DisableReason {
    /// Prometheus label / log-friendly string.
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            DisableReason::Down => "down",
            DisableReason::MaxMemory => "max_memory",
            DisableReason::MaxPending => "max_pending",
            DisableReason::Latency => "latency",
            DisableReason::Manual => "manual",
            DisableReason::Saturated => "saturated",
        }
    }
}

/// Raw measurements collected about Redis (and app-side latency) at a point in time.
/// Poller fills most of these; latency tracker fills p99; evaluator consumes them.
#[derive(Debug, Clone)]
pub struct RedisSnapshot {
    pub ts: SystemTime,

    // --------------------------
    // Connectivity
    // --------------------------
    /// Whether ping/command succeeded during polling.
    pub is_up: bool,

    /// Optional: measured ping RTT (ms) during polling.
    pub ping_rtt_ms: Option<f64>,

    // --------------------------
    // Memory
    // --------------------------
    /// INFO memory used_memory (bytes).
    pub used_memory_bytes: Option<u64>,

    /// INFO memory maxmemory (bytes). None means "no max configured / unknown".
    pub maxmemory_bytes: Option<u64>,

    /// Convenience: used/max as percent (0..=100). None if maxmemory is unknown.
    pub used_memory_pct: Option<f64>,

    // --------------------------
    // Streams backlog (definition is app-specific)
    // --------------------------
    /// App-defined "pending" / backlog estimate.
    /// Example definitions:
    /// - sum of XINFO GROUPS pending across a sample of keys
    /// - sum across aggregate streams only
    pub pending_total: Option<u64>,

    // --------------------------
    // App-side Redis command latency
    // --------------------------
    /// Rolling p99 of publish command latency, computed in-app (ms).
    pub p99_cmd_ms: Option<f64>,
}

impl RedisSnapshot {
    pub fn down_now() -> Self {
        Self {
            ts: SystemTime::now(),
            is_up: false,
            ping_rtt_ms: None,
            used_memory_bytes: None,
            maxmemory_bytes: None,
            used_memory_pct: None,
            pending_total: None,
            p99_cmd_ms: None,
        }
    }
}

/// Evaluated health status: "should we use Redis right now?"
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub ok: bool,
    pub reason: Option<DisableReason>,
    pub snapshot: RedisSnapshot,
}

impl HealthStatus {
    pub fn healthy(snapshot: RedisSnapshot) -> Self {
        Self {
            ok: true,
            reason: None,
            snapshot,
        }
    }

    pub fn unhealthy(reason: DisableReason, snapshot: RedisSnapshot) -> Self {
        Self {
            ok: false,
            reason: Some(reason),
            snapshot,
        }
    }
}
