use crate::error::AppResult;

#[cfg(feature = "metrics")]
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

#[derive(Clone, Debug)]
pub struct RedisMetrics {
    #[cfg(feature = "metrics")]
    registry: Registry,

    // --------------------------------------------
    // Throughput
    // --------------------------------------------
    #[cfg(feature = "metrics")]
    pub published_total: IntCounter,

    // --------------------------------------------
    // Latency
    // --------------------------------------------
    #[cfg(feature = "metrics")]
    pub publish_latency_seconds: Histogram,

    // --------------------------------------------
    // Failures / retries / drops
    // --------------------------------------------
    #[cfg(feature = "metrics")]
    pub publish_failures_total: IntCounter,

    // --------------------------------------------
    // Backpressure-ish (application-side)
    // --------------------------------------------
    #[cfg(feature = "metrics")]
    pub publish_queue_depth: IntGauge,

    // --------------------------------------------
    // Health gate / optional-Redis signals
    // --------------------------------------------
    /// 1 = Redis is currently used by the app; 0 = disabled (optional acceleration).
    #[cfg(feature = "metrics")]
    pub enabled_state: IntGauge,

    /// Counts transitions to disabled state, with a reason label.
    /// Example labels: "down", "saturated", "latency", "max_pending", "max_memory", "manual".
    #[cfg(feature = "metrics")]
    pub disable_events_total: IntCounterVec,

    #[cfg(not(feature = "metrics"))]
    _noop: (),
}

impl RedisMetrics {
    pub fn new() -> AppResult<Self> {
        #[cfg(feature = "metrics")]
        {
            let registry = Registry::new();

            let published_total = IntCounter::with_opts(Opts::new(
                "redis_stream_published_total",
                "Redis stream messages published total",
            ))?;

            // Better buckets for Redis publish: sub-ms to 1s range.
            let publish_latency_seconds = Histogram::with_opts(
                HistogramOpts::new(
                    "redis_publish_latency_seconds",
                    "Redis publish latency (seconds)",
                )
                .buckets(vec![
                    0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0,
                ]),
            )?;

            let publish_failures_total = IntCounter::with_opts(Opts::new(
                "redis_publish_failures_total",
                "Redis publish failures total",
            ))?;

            let publish_queue_depth = IntGauge::with_opts(Opts::new(
                "redis_publish_queue_depth",
                "Approx publish queue depth (application-side)",
            ))?;

            let enabled_state = IntGauge::with_opts(Opts::new(
                "redis_enabled_state",
                "Whether Redis is currently used by the app (1=yes, 0=no)",
            ))?;

            let disable_events_total = IntCounterVec::new(
                Opts::new(
                    "redis_disable_events_total",
                    "Number of times Redis was disabled by the app, labeled by reason",
                ),
                &["reason"],
            )?;

            registry.register(Box::new(published_total.clone()))?;
            registry.register(Box::new(publish_latency_seconds.clone()))?;
            registry.register(Box::new(publish_failures_total.clone()))?;
            registry.register(Box::new(publish_queue_depth.clone()))?;
            registry.register(Box::new(enabled_state.clone()))?;
            registry.register(Box::new(disable_events_total.clone()))?;

            // Default assumption: enabled (caller can override immediately)
            enabled_state.set(1);

            Ok(Self {
                registry,
                published_total,
                publish_latency_seconds,
                publish_failures_total,
                publish_queue_depth,
                enabled_state,
                disable_events_total,
            })
        }

        #[cfg(not(feature = "metrics"))]
        {
            Ok(Self { _noop: () })
        }
    }

    #[cfg(feature = "metrics")]
    pub fn encode_text(&self) -> AppResult<String> {
        let mf = self.registry.gather();
        let mut buf = Vec::new();
        TextEncoder::new().encode(&mf, &mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    // ------------------------------------------------------------
    // No-op helpers (compile away when metrics feature is off)
    // ------------------------------------------------------------

    #[inline]
    pub fn inc_published(&self, _n: u64) {
        #[cfg(feature = "metrics")]
        self.published_total.inc_by(_n);
    }

    #[inline]
    pub fn observe_publish_latency(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.publish_latency_seconds.observe(_secs);
    }

    #[inline]
    pub fn inc_publish_failure(&self) {
        #[cfg(feature = "metrics")]
        self.publish_failures_total.inc();
    }

    #[inline]
    pub fn set_queue_depth(&self, _depth: i64) {
        #[cfg(feature = "metrics")]
        self.publish_queue_depth.set(_depth);
    }

    // ------------------------------------------------------------
    // Optional-Redis / health-gate helpers
    // ------------------------------------------------------------

    /// Sets whether Redis is currently in use by the app.
    #[inline]
    pub fn set_enabled_state(&self, _enabled: bool) {
        #[cfg(feature = "metrics")]
        self.enabled_state.set(if _enabled { 1 } else { 0 });
    }

    /// Records an event that Redis was disabled (and why), and sets enabled_state=0.
    /// Suggested reasons: "down", "saturated", "latency", "max_pending", "max_memory", "manual".
    #[inline]
    pub fn disable_with_reason(&self, _reason: &str) {
        #[cfg(feature = "metrics")]
        {
            self.enabled_state.set(0);
            self.disable_events_total
                .with_label_values(&[_reason])
                .inc();
        }
    }
}
