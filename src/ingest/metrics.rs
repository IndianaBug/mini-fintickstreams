// src/ingest/metrics.rs
use crate::error::{AppError, AppResult};

#[cfg(feature = "metrics")]
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};

/// Minimal metrics for ingest pipelines (WS/HTTP -> queue -> process -> ack).
///
/// No labels by design (avoid high-cardinality early). If you later want per-exchange,
/// create one IngestMetrics instance per exchange/stream and serve them separately,
/// or add const labels at registration time.
#[derive(Clone, Debug)]
pub struct IngestMetrics {
    #[cfg(feature = "metrics")]
    registry: Registry,

    // --- Throughput
    #[cfg(feature = "metrics")]
    pub in_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub processed_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub acked_total: IntCounter,

    // --- Quality
    #[cfg(feature = "metrics")]
    pub errors_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub retried_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub duplicates_total: IntCounter,

    // --- Backpressure / lag
    #[cfg(feature = "metrics")]
    pub queue_depth: IntGauge,
    #[cfg(feature = "metrics")]
    pub lag_seconds: Histogram,

    // --- Rate limiting
    #[cfg(feature = "metrics")]
    pub rate_limited_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub rate_limit_wait_seconds: Histogram,

    // --- WS attempts (minimal)
    #[cfg(feature = "metrics")]
    pub ws_subscribe_attempts_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub ws_subscribe_rate_limited_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub ws_subscribe_wait_seconds: Histogram,

    #[cfg(feature = "metrics")]
    pub ws_reconnect_attempts_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub ws_reconnect_rate_limited_total: IntCounter,
    #[cfg(feature = "metrics")]
    pub ws_reconnect_wait_seconds: Histogram,

    // no-op fallback data (keeps struct non-empty without feature)
    #[cfg(not(feature = "metrics"))]
    _noop: (),
}

impl IngestMetrics {
    pub fn new() -> AppResult<Self> {
        #[cfg(feature = "metrics")]
        {
            let registry = Registry::new();

            // --- Throughput
            let in_total = IntCounter::with_opts(Opts::new(
                "ingest_in_total",
                "Messages received from ingest source (WS/HTTP)",
            ))?;

            let processed_total = IntCounter::with_opts(Opts::new(
                "ingest_processed_total",
                "Messages successfully processed/normalized",
            ))?;

            let acked_total = IntCounter::with_opts(Opts::new(
                "ingest_acked_total",
                "Messages acknowledged/committed (if applicable)",
            ))?;

            // --- Quality
            let errors_total = IntCounter::with_opts(Opts::new(
                "ingest_errors_total",
                "Ingest errors total (transport/parse/handler)",
            ))?;

            let retried_total = IntCounter::with_opts(Opts::new(
                "ingest_retried_total",
                "Retried operations total (any reason)",
            ))?;

            let duplicates_total = IntCounter::with_opts(Opts::new(
                "ingest_duplicates_total",
                "Duplicate deliveries detected total",
            ))?;

            // --- Backpressure / lag
            let queue_depth = IntGauge::with_opts(Opts::new(
                "ingest_queue_depth",
                "Current ingest queue depth / pending items (approx)",
            ))?;

            let lag_seconds = Histogram::with_opts(HistogramOpts::new(
                "ingest_lag_seconds",
                "End-to-end lag in seconds (now - message timestamp or enqueue time)",
            ))?;

            // --- Rate limiting
            let rate_limited_total = IntCounter::with_opts(Opts::new(
                "ingest_rate_limited_total",
                "Total times we were rate limited (blocked/dropped due to limiter/429)",
            ))?;

            let rate_limit_wait_seconds = Histogram::with_opts(HistogramOpts::new(
                "ingest_rate_limit_wait_seconds",
                "Time spent waiting for rate limiter permits (seconds)",
            ))?;

            // --- WS attempts (minimal)
            let ws_subscribe_attempts_total = IntCounter::with_opts(Opts::new(
                "ws_subscribe_attempts_total",
                "Total WS subscribe attempts performed",
            ))?;
            let ws_subscribe_rate_limited_total = IntCounter::with_opts(Opts::new(
                "ws_subscribe_rate_limited_total",
                "Total times WS subscribe attempts were throttled by limiter",
            ))?;
            let ws_subscribe_wait_seconds = Histogram::with_opts(HistogramOpts::new(
                "ws_subscribe_wait_seconds",
                "Time spent waiting to perform a WS subscribe attempt (seconds)",
            ))?;

            let ws_reconnect_attempts_total = IntCounter::with_opts(Opts::new(
                "ws_reconnect_attempts_total",
                "Total WS reconnect attempts performed",
            ))?;
            let ws_reconnect_rate_limited_total = IntCounter::with_opts(Opts::new(
                "ws_reconnect_rate_limited_total",
                "Total times WS reconnect attempts were throttled by limiter",
            ))?;
            let ws_reconnect_wait_seconds = Histogram::with_opts(HistogramOpts::new(
                "ws_reconnect_wait_seconds",
                "Time spent waiting to perform a WS reconnect attempt (seconds)",
            ))?;

            // Register everything
            registry.register(Box::new(in_total.clone()))?;
            registry.register(Box::new(processed_total.clone()))?;
            registry.register(Box::new(acked_total.clone()))?;
            registry.register(Box::new(errors_total.clone()))?;
            registry.register(Box::new(retried_total.clone()))?;
            registry.register(Box::new(duplicates_total.clone()))?;
            registry.register(Box::new(queue_depth.clone()))?;
            registry.register(Box::new(lag_seconds.clone()))?;
            registry.register(Box::new(rate_limited_total.clone()))?;
            registry.register(Box::new(rate_limit_wait_seconds.clone()))?;

            registry.register(Box::new(ws_subscribe_attempts_total.clone()))?;
            registry.register(Box::new(ws_subscribe_rate_limited_total.clone()))?;
            registry.register(Box::new(ws_subscribe_wait_seconds.clone()))?;
            registry.register(Box::new(ws_reconnect_attempts_total.clone()))?;
            registry.register(Box::new(ws_reconnect_rate_limited_total.clone()))?;
            registry.register(Box::new(ws_reconnect_wait_seconds.clone()))?;

            Ok(Self {
                registry,
                in_total,
                processed_total,
                acked_total,
                errors_total,
                retried_total,
                duplicates_total,
                queue_depth,
                lag_seconds,
                rate_limited_total,
                rate_limit_wait_seconds,
                ws_subscribe_attempts_total,
                ws_subscribe_rate_limited_total,
                ws_subscribe_wait_seconds,
                ws_reconnect_attempts_total,
                ws_reconnect_rate_limited_total,
                ws_reconnect_wait_seconds,
            })
        }

        #[cfg(not(feature = "metrics"))]
        {
            Ok(Self { _noop: () })
        }
    }

    /// Encode metrics to Prometheus text format.
    #[cfg(feature = "metrics")]
    pub fn encode_text(&self) -> AppResult<String> {
        use prometheus::{Encoder, TextEncoder};
        let mf = self.registry.gather();
        let mut buf = Vec::new();
        TextEncoder::new().encode(&mf, &mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    #[cfg(not(feature = "metrics"))]
    pub fn encode_text(&self) -> AppResult<String> {
        Err(AppError::InvalidConfig(
            "metrics feature is disabled".into(),
        ))
    }

    // --- Helpers (safe to call unconditionally)

    #[inline]
    pub fn inc_in(&self) {
        #[cfg(feature = "metrics")]
        self.in_total.inc();
    }

    #[inline]
    pub fn inc_processed(&self) {
        #[cfg(feature = "metrics")]
        self.processed_total.inc();
    }

    #[inline]
    pub fn inc_acked(&self) {
        #[cfg(feature = "metrics")]
        self.acked_total.inc();
    }

    #[inline]
    pub fn inc_error(&self) {
        #[cfg(feature = "metrics")]
        self.errors_total.inc();
    }

    #[inline]
    pub fn inc_retried(&self) {
        #[cfg(feature = "metrics")]
        self.retried_total.inc();
    }

    #[inline]
    pub fn inc_duplicate(&self) {
        #[cfg(feature = "metrics")]
        self.duplicates_total.inc();
    }

    #[inline]
    pub fn set_queue_depth(&self, _depth: i64) {
        #[cfg(feature = "metrics")]
        self.queue_depth.set(_depth);
    }

    #[inline]
    pub fn observe_lag(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.lag_seconds.observe(_secs);
    }

    #[inline]
    pub fn inc_rate_limited(&self) {
        #[cfg(feature = "metrics")]
        self.rate_limited_total.inc();
    }

    #[inline]
    pub fn observe_rate_limit_wait(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.rate_limit_wait_seconds.observe(_secs);
    }

    // --- WS helpers (safe to call unconditionally)

    #[inline]
    pub fn inc_ws_subscribe_attempt(&self) {
        #[cfg(feature = "metrics")]
        self.ws_subscribe_attempts_total.inc();
    }

    #[inline]
    pub fn inc_ws_subscribe_rate_limited(&self) {
        #[cfg(feature = "metrics")]
        self.ws_subscribe_rate_limited_total.inc();
    }

    #[inline]
    pub fn observe_ws_subscribe_wait(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.ws_subscribe_wait_seconds.observe(_secs);
    }

    #[inline]
    pub fn inc_ws_reconnect_attempt(&self) {
        #[cfg(feature = "metrics")]
        self.ws_reconnect_attempts_total.inc();
    }

    #[inline]
    pub fn inc_ws_reconnect_rate_limited(&self) {
        #[cfg(feature = "metrics")]
        self.ws_reconnect_rate_limited_total.inc();
    }

    #[inline]
    pub fn observe_ws_reconnect_wait(&self, _secs: f64) {
        #[cfg(feature = "metrics")]
        self.ws_reconnect_wait_seconds.observe(_secs);
    }
}
