use crate::error::{AppError, AppResult};
use crate::ingest::config::{ExchangeConfig, StringOrTable, WsStream};
use crate::ingest::metrics::IngestMetrics;
use crate::ingest::spec::{Ctx, resolve_ws_control, seed_ws_stream_ctx};
use crate::ingest::ws::limiter_registry::WsLimiterRegistry;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio::time::{Instant, interval};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub enum WsEvent {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Option<String>),
}

#[derive(Debug)]
pub struct WsClient {
    pub name: &'static str, // "binance_linear", "hyperliquid_perp"
    pub cfg: ExchangeConfig,
    pub metrics: Option<IngestMetrics>,
}

impl WsClient {
    pub fn new(name: &'static str, cfg: ExchangeConfig, metrics: Option<IngestMetrics>) -> Self {
        Self { name, cfg, metrics }
    }

    /// Run ONE stream per connection.
    ///
    /// ws_limiters is optional to make tests easier (no registry needed).
    /// test_hook is optional to allow terminating the reconnect loop deterministically in tests.
    pub async fn run_stream<F, Fut>(
        &self,
        ws_limiters: Option<&WsLimiterRegistry>,
        stream: &WsStream,
        mut ctx: Ctx,
        mut on_event: F,
        test_hook: Option<&mut WsTestHook>,
    ) -> AppResult<()>
    where
        F: FnMut(WsEvent) -> Fut,
        Fut: std::future::Future<Output = AppResult<()>>,
    {
        seed_ws_stream_ctx(stream, &mut ctx)?;

        ctx.entry("stream_id".to_string())
            .or_insert_with(|| "1".to_string());

        let control = resolve_ws_control(&self.cfg, &ctx)?;

        self.connect_loop(
            ws_limiters,
            control.subscribe,
            control.unsubscribe,
            on_event,
            test_hook,
        )
        .await
    }

    async fn connect_loop<F, Fut>(
        &self,
        ws_limiters: Option<&WsLimiterRegistry>,
        subscribe_msg: JsonValue,
        unsubscribe_msg: JsonValue,
        mut on_event: F,
        mut test_hook: Option<&mut WsTestHook>,
    ) -> AppResult<()>
    where
        F: FnMut(WsEvent) -> Fut,
        Fut: std::future::Future<Output = AppResult<()>>,
    {
        loop {
            // --- TEST HOOK: allow tests to stop before reconnecting again
            if let Some(h) = test_hook.as_deref_mut() {
                if !h.on_before_reconnect_attempt() {
                    return Ok(());
                }
            }

            // --- RECONNECT limiter: gate every connect attempt (optional)
            if let Some(lims) = ws_limiters {
                lims.acquire_reconnect(self.name).await?;
            }

            let url = self.cfg.ws_base_url.clone();
            info!(exchange = self.name, url = %url, "ws connecting");

            let (ws, _resp) = connect_async(url).await.map_err(|e| {
                AppError::Internal(format!("[ws:{}] connect error: {e}", self.name))
            })?;

            let (mut write, mut read) = ws.split();

            // --- SUBSCRIBE limiter: gate every subscribe send (optional)
            if let Some(lims) = ws_limiters {
                lims.acquire_subscribe(self.name).await?;
            }

            send_ws_payload(&mut write, &subscribe_msg).await?;

            // optional heartbeat loop driver
            let mut hb = self.heartbeat_sender();

            // optional connection timeout
            let timeout_secs = self.cfg.ws_connection_timeout_seconds;
            let deadline = if timeout_secs > 0 {
                Some(Instant::now() + Duration::from_secs(timeout_secs))
            } else {
                None
            };

            let mut close_reason: Option<String> = None;

            loop {
                if let Some(dl) = deadline {
                    if Instant::now() >= dl {
                        close_reason = Some("ws_connection_timeout_seconds reached".into());
                        break;
                    }
                }

                if let Some(hb_tick) = hb.as_mut() {
                    if hb_tick.tick().await.is_some() {
                        maybe_send_ws_heartbeat(&self.cfg, &mut write).await?;
                    }
                }

                let msg = match read.next().await {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        close_reason = Some(format!("read error: {e}"));
                        error!(exchange = self.name, error = %e, "ws read error");
                        break;
                    }
                    None => {
                        close_reason = Some("stream ended".into());
                        break;
                    }
                };

                match msg {
                    Message::Text(s) => {
                        if let Some(m) = &self.metrics {
                            m.inc_in();
                        }
                        on_event(WsEvent::Text(s.to_string())).await?;
                        if let Some(m) = &self.metrics {
                            m.inc_processed();
                        }
                    }
                    Message::Binary(b) => {
                        if let Some(m) = &self.metrics {
                            m.inc_in();
                        }
                        on_event(WsEvent::Binary(b.to_vec())).await?;
                        if let Some(m) = &self.metrics {
                            m.inc_processed();
                        }
                    }
                    Message::Ping(p) => {
                        on_event(WsEvent::Ping(p.clone().to_vec())).await?;
                        let _ = write.send(Message::Pong(p)).await;
                    }
                    Message::Pong(p) => {
                        on_event(WsEvent::Pong(p.to_vec())).await?;
                    }
                    Message::Close(frame) => {
                        close_reason = Some(format!("close: {:?}", frame));
                        let _ = on_event(WsEvent::Close(close_reason.clone())).await;
                        break;
                    }
                    _ => {}
                }
            }

            // best-effort unsubscribe
            let _ = send_ws_payload(&mut write, &unsubscribe_msg).await;

            warn!(
                exchange = self.name,
                reason = %close_reason.clone().unwrap_or_else(|| "unknown".into()),
                "ws reconnecting"
            );

            // --- TEST HOOK: allow tests to observe disconnect reasons / count cycles
            if let Some(h) = test_hook.as_deref_mut() {
                h.on_disconnected(close_reason.as_deref());
            }
        }
    }

    fn heartbeat_sender(&self) -> Option<HeartbeatDriver> {
        let hb_type = self.cfg.ws_heartbeat_type.as_ref()?.to_lowercase();
        if hb_type != "ping" {
            return None;
        }

        let timeout = self.cfg.ws_heartbeat_timeout_seconds.unwrap_or(30);
        let period = std::cmp::max(1, timeout / 2);

        Some(HeartbeatDriver {
            interval: interval(Duration::from_secs(period)),
            frame: self.cfg.ws_heartbeat_frame.clone(),
        })
    }
}

/// Optional test hook to make reconnect loops deterministic in tests.
#[derive(Debug, Default)]
pub struct WsTestHook {
    /// If Some(n): allow at most n reconnect attempts. After that, stop (return Ok(())).
    pub max_reconnect_attempts: Option<u32>,
    pub reconnect_attempts: u32,
    /// Optional callback-like storage for assertions.
    pub disconnects: Vec<Option<String>>,
}

impl WsTestHook {
    /// Called at the start of each outer loop iteration, before attempting connect/reconnect.
    /// Return false to stop the loop.
    pub fn on_before_reconnect_attempt(&mut self) -> bool {
        self.reconnect_attempts = self.reconnect_attempts.saturating_add(1);
        match self.max_reconnect_attempts {
            Some(max) => self.reconnect_attempts <= max,
            None => true,
        }
    }

    /// Called after a disconnect (inner loop ended) with the close reason (if any).
    pub fn on_disconnected(&mut self, reason: Option<&str>) {
        self.disconnects.push(reason.map(|s| s.to_string()));
    }
}

struct HeartbeatDriver {
    interval: tokio::time::Interval,
    frame: Option<StringOrTable>,
}

impl HeartbeatDriver {
    async fn tick(&mut self) -> Option<()> {
        self.interval.tick().await;
        Some(())
    }
}

async fn send_ws_payload<S>(write: &mut S, payload: &serde_json::Value) -> AppResult<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    write
        .send(Message::Text(payload.to_string().into()))
        .await
        .map_err(|e| AppError::Internal(format!("ws send json error: {e}")))?;
    Ok(())
}

async fn maybe_send_ws_heartbeat<S>(cfg: &ExchangeConfig, write: &mut S) -> AppResult<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let hb_type = match cfg.ws_heartbeat_type.as_deref() {
        Some(t) => t.to_lowercase(),
        None => return Ok(()),
    };

    if hb_type != "ping" {
        return Ok(());
    }

    match &cfg.ws_heartbeat_frame {
        Some(StringOrTable::String(s)) => {
            write
                .send(Message::Text(s.clone().into()))
                .await
                .map_err(|e| AppError::Internal(format!("ws heartbeat send text error: {e}")))?;
        }
        Some(StringOrTable::Table(v)) => {
            let json = serde_json::to_string(v)
                .map_err(|e| AppError::Internal(format!("ws heartbeat serialize error: {e}")))?;

            write
                .send(Message::Text(json.into()))
                .await
                .map_err(|e| AppError::Internal(format!("ws heartbeat send json error: {e}")))?;
        }
        None => {
            write
                .send(Message::Ping(Vec::new().into()))
                .await
                .map_err(|e| AppError::Internal(format!("ws heartbeat send ping error: {e}")))?;
        }
    }

    Ok(())
}

