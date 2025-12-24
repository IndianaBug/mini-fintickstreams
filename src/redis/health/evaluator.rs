// src/redis/health/evaluator.rs

use crate::redis::config::CapacityConfig;
use crate::redis::health::types::{DisableReason, HealthStatus, RedisSnapshot};

#[derive(Debug, Clone)]
pub struct HealthEvaluator {
    cap: CapacityConfig,
}

impl HealthEvaluator {
    pub fn new(cap: CapacityConfig) -> Self {
        Self { cap }
    }

    /// Evaluate whether Redis is safe to use right now.
    ///
    /// Rules (in priority order):
    /// 1) If Redis is down -> Down
    /// 2) If memory pct known and above threshold -> MaxMemory
    /// 3) If pending known and above threshold -> MaxPending
    /// 4) If p99 known and above threshold -> Latency
    ///
    /// Any "unknown" measurement (None) simply does not trigger that rule.
    pub fn evaluate(&self, snapshot: RedisSnapshot) -> HealthStatus {
        // 1) Connectivity
        if !snapshot.is_up {
            return HealthStatus::unhealthy(DisableReason::Down, snapshot);
        }

        // 2) Memory guardrail (only if pct is known)
        if let Some(pct) = snapshot.used_memory_pct {
            if pct > self.cap.max_memory_pct as f64 {
                return HealthStatus::unhealthy(DisableReason::MaxMemory, snapshot);
            }
        }

        // 3) Pending / backlog guardrail (only if known)
        if let Some(pending) = snapshot.pending_total {
            if pending > self.cap.max_pending {
                return HealthStatus::unhealthy(DisableReason::MaxPending, snapshot);
            }
        }

        // 4) Rolling publish p99 latency (only if known)
        if let Some(p99) = snapshot.p99_cmd_ms {
            if p99 > self.cap.max_p99_cmd_ms as f64 {
                return HealthStatus::unhealthy(DisableReason::Latency, snapshot);
            }
        }

        HealthStatus::healthy(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn cap() -> CapacityConfig {
        CapacityConfig {
            poll_interval_sec: 2,
            max_memory_pct: 85,
            max_pending: 200_000,
            max_p99_cmd_ms: 10,
            redis_publish_latency_window: 2048,
        }
    }

    fn base_up_snapshot() -> RedisSnapshot {
        RedisSnapshot {
            ts: SystemTime::now(),
            is_up: true,
            ping_rtt_ms: Some(0.5),
            used_memory_bytes: Some(100),
            maxmemory_bytes: Some(1000),
            used_memory_pct: Some(10.0),
            pending_total: Some(0),
            p99_cmd_ms: Some(1.0),
        }
    }

    #[test]
    fn down_wins() {
        let ev = HealthEvaluator::new(cap());
        let snap = RedisSnapshot::down_now();
        let h = ev.evaluate(snap);
        assert!(!h.ok);
        assert_eq!(h.reason, Some(DisableReason::Down));
    }

    #[test]
    fn memory_threshold() {
        let ev = HealthEvaluator::new(cap());
        let mut snap = base_up_snapshot();
        snap.used_memory_pct = Some(90.0);

        let h = ev.evaluate(snap);
        assert!(!h.ok);
        assert_eq!(h.reason, Some(DisableReason::MaxMemory));
    }

    #[test]
    fn pending_threshold() {
        let ev = HealthEvaluator::new(cap());
        let mut snap = base_up_snapshot();
        snap.pending_total = Some(250_000);

        let h = ev.evaluate(snap);
        assert!(!h.ok);
        assert_eq!(h.reason, Some(DisableReason::MaxPending));
    }

    #[test]
    fn latency_threshold() {
        let ev = HealthEvaluator::new(cap());
        let mut snap = base_up_snapshot();
        snap.p99_cmd_ms = Some(12.5);

        let h = ev.evaluate(snap);
        assert!(!h.ok);
        assert_eq!(h.reason, Some(DisableReason::Latency));
    }

    #[test]
    fn unknown_fields_do_not_trigger() {
        let ev = HealthEvaluator::new(cap());
        let mut snap = base_up_snapshot();
        snap.used_memory_pct = None;
        snap.pending_total = None;
        snap.p99_cmd_ms = None;

        let h = ev.evaluate(snap);
        assert!(h.ok);
        assert!(h.reason.is_none());
    }
}
