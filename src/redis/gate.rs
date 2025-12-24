// src/redis/gate.rs

use crate::redis::config::{DownPolicy, FailoverConfig, SaturationPolicy};
use crate::redis::health::types::{DisableReason, HealthStatus};
use crate::redis::metrics::RedisMetrics;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Redis usage gate for the producer:
/// - Redis is optional acceleration
/// - DB path is source of truth
///
/// This gate decides:
/// - can_publish(): should producer attempt XADD?
/// - can_assign_new_symbol(): should producer onboard NEW symbols into Redis streams?
///
/// It also records state transitions to metrics (if enabled).
#[derive(Debug)]
pub struct RedisGate {
    // "Hard" enabled state (manual disable or health disable sets false).
    enabled: AtomicBool,

    // Whether we should stop assigning *new* symbols due to saturation.
    stop_assigning_new: AtomicBool,

    // Last disable reason (for debugging/visibility).
    last_disable: Mutex<Option<DisableReason>>,

    failover: FailoverConfig,
    metrics: RedisMetrics,
}

impl RedisGate {
    pub fn new(failover: FailoverConfig, metrics: RedisMetrics) -> Self {
        metrics.set_enabled_state(true);

        Self {
            enabled: AtomicBool::new(true),
            stop_assigning_new: AtomicBool::new(false),
            last_disable: Mutex::new(None),
            failover,
            metrics,
        }
    }

    /// Manual override: disable Redis usage.
    pub fn disable_manual(&self) {
        self.set_disabled(Some(DisableReason::Manual));
    }

    /// Manual override: re-enable Redis usage (health loop will still disable again if unhealthy).
    pub fn enable_manual(&self) {
        self.enabled.store(true, Ordering::Relaxed);
        self.stop_assigning_new.store(false, Ordering::Relaxed);
        *self.last_disable.lock().expect("gate mutex poisoned") = None;
        self.metrics.set_enabled_state(true);
    }

    /// Fast-path: should the producer attempt Redis XADD right now?
    #[inline]
    pub fn can_publish(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Should the producer assign a *new symbol* into Redis stream publishing?
    ///
    /// This is used by your "symbol onboarding" logic:
    /// - If saturated and policy is StopAssigningNew => return false
    /// - Existing symbols may continue publishing (if enabled), new ones do not start.
    #[inline]
    pub fn can_assign_new_symbol(&self) -> bool {
        self.enabled.load(Ordering::Relaxed) && !self.stop_assigning_new.load(Ordering::Relaxed)
    }

    /// Returns last disable reason (if any).
    pub fn last_disable_reason(&self) -> Option<DisableReason> {
        *self.last_disable.lock().expect("gate mutex poisoned")
    }

    /// Apply evaluated health status.
    ///
    /// Expected caller: health loop (every poll_interval_sec).
    pub fn apply_health(&self, status: &HealthStatus) {
        if status.ok {
            // If healthy, we can allow publishing.
            // NOTE: we do NOT automatically clear stop_assigning_new here unless you want it.
            // For now, if healthy again, we clear it.
            self.enabled.store(true, Ordering::Relaxed);
            self.stop_assigning_new.store(false, Ordering::Relaxed);
            *self.last_disable.lock().expect("gate mutex poisoned") = None;
            self.metrics.set_enabled_state(true);
            return;
        }

        // Unhealthy: choose action based on reason and failover policy.
        match status.reason {
            Some(DisableReason::Down) => {
                match self.failover.on_down {
                    DownPolicy::DisableRedisTemporarily => {
                        self.set_disabled(Some(DisableReason::Down));
                    }
                    DownPolicy::PauseAndRetry => {
                        // future: producer would pause; for now, treat like disable.
                        self.set_disabled(Some(DisableReason::Down));
                    }
                }
            }

            Some(DisableReason::MaxMemory) => {
                // Treat as "saturated" class signal, but you may prefer full disable.
                self.apply_saturation(DisableReason::MaxMemory);
            }

            Some(DisableReason::MaxPending) => {
                self.apply_saturation(DisableReason::MaxPending);
            }

            Some(DisableReason::Latency) => {
                // Latency is usually "global" pain => disable publishing to protect app.
                self.set_disabled(Some(DisableReason::Latency));
            }

            Some(DisableReason::Saturated) => {
                self.apply_saturation(DisableReason::Saturated);
            }

            Some(DisableReason::Manual) => {
                // If status says manual (rare), keep disabled.
                self.set_disabled(Some(DisableReason::Manual));
            }

            None => {
                // Unhealthy but no reason: safest is disable.
                self.set_disabled(Some(DisableReason::Down));
            }
        }
    }

    fn apply_saturation(&self, reason: DisableReason) {
        match self.failover.on_saturated {
            SaturationPolicy::StopAssigningNew => {
                // Keep publishing for already-onboarded symbols (if still enabled),
                // but don't onboard new ones.
                self.stop_assigning_new.store(true, Ordering::Relaxed);

                // Record saturation event without disabling Redis entirely.
                // (We do not flip enabled=false.)
                // Still useful to surface the condition:
                self.metrics
                    .disable_events_total
                    .with_label_values(&[reason.as_str()])
                    .inc();
            }
            SaturationPolicy::ErrorNew => {
                // Equivalent: new symbols should error upstream.
                // Gate expresses this via can_assign_new_symbol() == false.
                self.stop_assigning_new.store(true, Ordering::Relaxed);
                self.metrics
                    .disable_events_total
                    .with_label_values(&[reason.as_str()])
                    .inc();
            }
            SaturationPolicy::SpilloverToOtherNode => {
                // future: would place new symbols on other nodes
                self.stop_assigning_new.store(true, Ordering::Relaxed);
                self.metrics
                    .disable_events_total
                    .with_label_values(&[reason.as_str()])
                    .inc();
            }
        }
    }

    fn set_disabled(&self, reason: Option<DisableReason>) {
        self.enabled.store(false, Ordering::Relaxed);
        self.stop_assigning_new.store(true, Ordering::Relaxed);

        if let Some(r) = reason {
            *self.last_disable.lock().expect("gate mutex poisoned") = Some(r);
            self.metrics.disable_with_reason(r.as_str());
        } else {
            *self.last_disable.lock().expect("gate mutex poisoned") = None;
            self.metrics.disable_with_reason("down");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::config::{FailoverConfig, SaturationPolicy};
    use crate::redis::health::types::{HealthStatus, RedisSnapshot};
    use std::time::SystemTime;

    fn gate() -> RedisGate {
        let failover = FailoverConfig {
            on_saturated: SaturationPolicy::StopAssigningNew,
            on_down: DownPolicy::DisableRedisTemporarily,
        };

        // Metrics are no-ops if feature=metrics is off.
        let metrics = RedisMetrics::new().unwrap();
        RedisGate::new(failover, metrics)
    }

    fn healthy() -> HealthStatus {
        HealthStatus::healthy(RedisSnapshot {
            ts: SystemTime::now(),
            is_up: true,
            ping_rtt_ms: Some(0.2),
            used_memory_bytes: Some(100),
            maxmemory_bytes: Some(1000),
            used_memory_pct: Some(10.0),
            pending_total: Some(0),
            p99_cmd_ms: Some(1.0),
        })
    }

    fn unhealthy(reason: DisableReason) -> HealthStatus {
        HealthStatus::unhealthy(
            reason,
            RedisSnapshot {
                ts: SystemTime::now(),
                is_up: false,
                ping_rtt_ms: None,
                used_memory_bytes: None,
                maxmemory_bytes: None,
                used_memory_pct: None,
                pending_total: None,
                p99_cmd_ms: None,
            },
        )
    }

    #[test]
    fn starts_enabled() {
        let g = gate();
        assert!(g.can_publish());
        assert!(g.can_assign_new_symbol());
        assert!(g.last_disable_reason().is_none());
    }

    #[test]
    fn down_disables_publishing() {
        let g = gate();
        g.apply_health(&unhealthy(DisableReason::Down));
        assert!(!g.can_publish());
        assert!(!g.can_assign_new_symbol());
        assert_eq!(g.last_disable_reason(), Some(DisableReason::Down));
    }

    #[test]
    fn healthy_reenables() {
        let g = gate();
        g.apply_health(&unhealthy(DisableReason::Down));
        assert!(!g.can_publish());

        g.apply_health(&healthy());
        assert!(g.can_publish());
        assert!(g.can_assign_new_symbol());
    }

    #[test]
    fn saturation_stops_assigning_new_only() {
        let g = gate();
        g.apply_health(&unhealthy(DisableReason::MaxPending));

        // With StopAssigningNew, we keep can_publish() true.
        // BUT new symbol assignment is blocked.
        // NOTE: in our current implementation, MaxPending routes to apply_saturation
        // and does not set enabled=false.
        assert!(g.can_publish());
        assert!(!g.can_assign_new_symbol());
    }
}
