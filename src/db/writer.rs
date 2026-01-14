//! db/handler.rs
//!
//! Main DB handler: routes -> acquires pool conn -> writes batch -> updates metrics.
//!
//! IMPORTANT CHANGE (batching behavior):
//! - `write_batch()` now takes `&mut Batch<T>`
//! - It will ONLY write when:
//!     (a) batch.rows.len() >= writer.batch_size
//!     OR
//!     (b) flush_interval_ms has elapsed since batch.enqueued_at
//! - On success it clears the batch (keeps same Batch object reusable).
//!
//! This makes `batch_size` act like the “transporter threshold” with minimal changes.
//!
//! Caller usage pattern:
//!     batch.rows.push(row);
//!     db.write_batch(&mut batch).await?;

use crate::app::StartStreamParams;
use crate::app::control::make_batch_key;
use crate::app::{ExchangeId, StreamId, StreamKnobs, StreamSpec};
use crate::app::{StreamKind, StreamTransport};
use crate::db::Batch;
use crate::db::config::WriterConfig;
use crate::db::metrics::DbMetrics;
use crate::db::pools::DbPools;
use crate::db::traits::BatchInsertRow;
use crate::error::{AppError, AppResult};
use sqlx::Row;
use sqlx::{Postgres, QueryBuilder};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Main DB handler: routes -> acquires pool conn -> writes batch -> updates metrics.
#[derive(Clone, Debug)]
pub struct DbHandler {
    pools: Arc<DbPools>,
    writer: WriterConfig,
    metrics: Arc<DbMetrics>,
    inflight: Arc<Semaphore>,
}

impl DbHandler {
    pub fn new(pools: Arc<DbPools>, writer: WriterConfig, metrics: Arc<DbMetrics>) -> Self {
        let inflight = Arc::new(Semaphore::new(writer.max_inflight_batches));
        Self {
            pools,
            writer,
            metrics,
            inflight,
        }
    }

    /// Write a batch using INSERT ... VALUES (...), (...), ...
    ///
    /// NEW batching behavior:
    /// - If batch is empty: returns Ok
    /// - If batch has fewer than batch_size rows AND flush_interval has NOT elapsed: returns Ok (keeps rows)
    /// - Otherwise: writes (in chunks of batch_size), then clears rows and resets enqueued_at
    pub async fn write_batch<T: BatchInsertRow>(&self, batch: &mut Batch<T>) -> AppResult<()> {
        if !batch.should_flush() {
            return Ok(());
        }

        // --- Backpressure: wait for a permit (queue wait time)
        let t0 = Instant::now();
        let permit = self
            .inflight
            .acquire()
            .await
            .map_err(|_| AppError::Shutdown)?;
        self.metrics.observe_queue_wait(t0.elapsed().as_secs_f64());

        // Approx inflight depth = max - available
        let depth =
            (self.writer.max_inflight_batches as i64) - (self.inflight.available_permits() as i64);
        self.metrics.set_queue_depth(depth);

        // --- Flush delay (how long it waited since enqueue)
        self.metrics
            .observe_flush_delay(batch.enqueued_at.elapsed().as_secs_f64());

        // --- Route shard + get pool
        let shard_id = self
            .pools
            .shard_id_for(&batch.key.exchange, &batch.key.stream, &batch.key.symbol)
            .await?;

        let pool = self.pools.pool_by_id(&shard_id).await?;

        // --- Pool max (from shard config) for health gauge
        let pool_max = self
            .pools
            .shards_snapshot()
            .await?
            .iter()
            .find(|s| s.id == shard_id)
            .map(|s| s.pool_max as i64)
            .unwrap_or(0);

        // --- Pool wait (explicit acquire to measure wait time)
        let acquire_t0 = Instant::now();
        let mut conn = pool.acquire().await.map_err(AppError::Sqlx)?;
        self.metrics
            .observe_pool_wait(acquire_t0.elapsed().as_secs_f64());

        // pool.size() includes idle+in-use; num_idle() is idle
        let size = pool.size() as i64;
        let idle = pool.num_idle() as i64;
        let in_use = (size - idle).max(0);
        self.metrics.set_pool_health(in_use, idle, pool_max);

        // --- Build & execute INSERT batches (chunked by batch_size)
        let write_t0 = Instant::now();

        // Table name is dynamic (depends on exchange). Compute once.
        let table_name = batch.rows[0].table(&batch.key.exchange);

        let mut total_written: u64 = 0;

        for chunk in batch.rows.chunks(batch.chunk_rows) {
            let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("INSERT INTO ");
            qb.push("\"");
            qb.push(&table_name.replace('.', "\".\""));
            qb.push("\"");

            qb.push(" (");

            for (i, col) in T::COLUMNS.iter().enumerate() {
                if i > 0 {
                    qb.push(", ");
                }
                qb.push("\"");
                qb.push(*col);
                qb.push("\"");
            }
            qb.push(") ");

            qb.push_values(chunk.iter(), |mut b, row| {
                row.push_binds(&mut b);
            });

            // Execute without capturing `permit` in a closure
            let res = qb.build().execute(&mut *conn).await;
            if let Err(e) = res {
                self.metrics.inc_failed_batch();
                drop(permit); // release before returning
                return Err(AppError::Sqlx(e));
            }

            total_written += chunk.len() as u64;
        }

        // release permit (drop) after successful writes
        drop(permit);

        // Success metrics
        self.metrics
            .observe_write_latency(write_t0.elapsed().as_secs_f64());
        self.metrics.inc_batches_written();
        self.metrics.add_rows_written(total_written);
        self.metrics.observe_rows_per_batch(total_written as f64);

        // Clear batch after successful write and reset timer
        batch.rows.clear();
        batch.enqueued_at = Instant::now();

        Ok(())
    }

    /// Simple retry helper (linear backoff).
    ///
    /// Note: No `T: Clone` needed now because we don't consume the batch.
    /// We also only clear rows on success inside `write_batch()`.
    pub async fn write_batch_with_retry<T: BatchInsertRow>(
        &self,
        batch: &mut Batch<T>,
        retries: usize,
        backoff: Duration,
    ) -> AppResult<()> {
        let mut attempt = 0usize;

        loop {
            match self.write_batch(batch).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < retries => {
                    self.metrics.inc_retried_batch();
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

// ----------------------------------------------------------------------------
// Instrument registry methods
// ----------------------------------------------------------------------------

impl DbHandler {
    /// Register (or update) a stream in mini_fintickstreams.stream_registry.
    ///
    /// - Inserts if missing
    /// - Updates knobs + enabled + updated_at if exists
    pub async fn upsert_stream_registry(
        &self,
        spec: &StreamSpec,
        knobs: &StreamKnobs,
        enabled: bool,
    ) -> AppResult<()> {
        // Build the canonical stream_id string (matches your StreamId::new)
        let stream_id = StreamId::new(spec.exchange, &spec.instrument, spec.kind, spec.transport);

        // Note: kind/transport are stored as TEXT in your table.
        // If you already have as_str()/Display impls, use those.
        let kind = spec.kind.to_string(); // e.g. "Trades"
        let transport = spec.transport.to_string(); // e.g. "Ws" or your lowercase mapping
        let exchange = spec.exchange;
        let exchange_id = ExchangeId::from_str(exchange)?;
        let instrument = &spec.instrument.clone();

        let batch_key = make_batch_key(
            exchange_id,
            spec.transport.clone(),
            spec.kind.clone(),
            &spec.instrument,
        )?;

        // --- Route shard + get pool
        let shard_id = self
            .pools
            .shard_id_for(&batch_key.exchange, &batch_key.stream, &batch_key.symbol)
            .await?;

        let pool = self.pools.pool_by_id(&shard_id).await?;
        let mut conn = pool.acquire().await.map_err(AppError::Sqlx)?;

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
            INSERT INTO mini_fintickstreams.stream_registry (
              stream_id, exchange, instrument, kind, transport, enabled,
              disable_db_writes, disable_redis_publishes,
              flush_rows, flush_interval_ms, chunk_rows, hard_cap_rows,
              created_at, updated_at
            )
            "#,
        );

        qb.push_values(std::iter::once(()), |mut b, _| {
            b.push_bind(stream_id.0.as_str());
            b.push_bind(exchange);
            b.push_bind(instrument);
            b.push_bind(kind.as_str());
            b.push_bind(transport.as_str());
            b.push_bind(enabled);

            b.push_bind(knobs.disable_db_writes);
            b.push_bind(knobs.disable_redis_publishes);

            b.push_bind(knobs.flush_rows as i32);
            b.push_bind(knobs.flush_interval_ms as i64);
            b.push_bind(knobs.chunk_rows as i32);
            b.push_bind(knobs.hard_cap_rows as i32);

            // timestamps
            b.push("now()");
            b.push("now()");
        });

        qb.push(
            r#"
            ON CONFLICT (stream_id) DO UPDATE SET
              exchange = EXCLUDED.exchange,
              instrument = EXCLUDED.instrument,
              kind = EXCLUDED.kind,
              transport = EXCLUDED.transport,
              enabled = EXCLUDED.enabled,

              disable_db_writes = EXCLUDED.disable_db_writes,
              disable_redis_publishes = EXCLUDED.disable_redis_publishes,
              flush_rows = EXCLUDED.flush_rows,
              flush_interval_ms = EXCLUDED.flush_interval_ms,
              chunk_rows = EXCLUDED.chunk_rows,
              hard_cap_rows = EXCLUDED.hard_cap_rows,

              updated_at = now()
            "#,
        );

        qb.build()
            .execute(&mut *conn)
            .await
            .map_err(AppError::Sqlx)?;

        Ok(())
    }

    pub async fn update_stream_knobs(
        &self,
        spec: &StreamSpec,
        knobs: &StreamKnobs,
    ) -> AppResult<()> {
        // route to the same shard as you do elsewhere
        let exchange_id = ExchangeId::from_str(spec.exchange)?;
        let batch_key = make_batch_key(
            exchange_id,
            spec.transport.clone(),
            spec.kind.clone(),
            &spec.instrument,
        )?;

        let shard_id = self
            .pools
            .shard_id_for(&batch_key.exchange, &batch_key.stream, &batch_key.symbol)
            .await?;
        let pool = self.pools.pool_by_id(&shard_id).await?;
        let mut conn = pool.acquire().await.map_err(AppError::Sqlx)?;

        let stream_id = StreamId::new(spec.exchange, &spec.instrument, spec.kind, spec.transport);

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
            UPDATE mini_fintickstreams.stream_registry
            SET
              disable_db_writes = "#,
        );

        qb.push_bind(knobs.disable_db_writes);
        qb.push(", disable_redis_publishes = ");
        qb.push_bind(knobs.disable_redis_publishes);

        qb.push(", flush_rows = ");
        qb.push_bind(knobs.flush_rows as i32);

        qb.push(", flush_interval_ms = ");
        qb.push_bind(knobs.flush_interval_ms as i64);

        qb.push(", chunk_rows = ");
        qb.push_bind(knobs.chunk_rows as i32);

        qb.push(", hard_cap_rows = ");
        qb.push_bind(knobs.hard_cap_rows as i32);

        qb.push(", updated_at = now() WHERE stream_id = ");
        qb.push_bind(stream_id.0.as_str());

        let res = qb
            .build()
            .execute(&mut *conn)
            .await
            .map_err(AppError::Sqlx)?;
        if res.rows_affected() == 0 {
            return Err(AppError::StreamNotFound(stream_id.0));
        }

        Ok(())
    }

    pub async fn remove_stream(&self, spec: &StreamSpec) -> AppResult<()> {
        let stream_id = StreamId::new(spec.exchange, &spec.instrument, spec.kind, spec.transport);

        let exchange_id = ExchangeId::from_str(spec.exchange)?;
        let batch_key = make_batch_key(
            exchange_id,
            spec.transport.clone(),
            spec.kind.clone(),
            &spec.instrument,
        )?;

        let shard_id = self
            .pools
            .shard_id_for(&batch_key.exchange, &batch_key.stream, &batch_key.symbol)
            .await?;

        let pool = self.pools.pool_by_id(&shard_id).await?;
        let mut conn = pool.acquire().await.map_err(AppError::Sqlx)?;

        let mut qb: QueryBuilder<Postgres> =
            QueryBuilder::new("DELETE FROM mini_fintickstreams.stream_registry WHERE stream_id = ");
        qb.push_bind(stream_id.0.as_str());

        let res = qb
            .build()
            .execute(&mut *conn)
            .await
            .map_err(AppError::Sqlx)?;
        if res.rows_affected() == 0 {
            return Err(AppError::StreamNotFound(stream_id.0));
        }

        Ok(())
    }
}

impl DbHandler {
    /// Load all enabled streams from mini_fintickstreams.stream_registry.
    pub async fn load_enabled_streams_from_registry(&self) -> AppResult<Vec<StartStreamParams>> {
        // Snapshot shard list once
        let shards = self.pools.shards_snapshot().await?;

        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<StartStreamParams> = Vec::new();

        for shard in shards {
            let pool = self.pools.pool_by_id(&shard.id).await?;
            let mut conn = pool.acquire().await.map_err(AppError::Sqlx)?;

            // Pull only what we need for restart
            let rows = sqlx::query(
                r#"
                SELECT stream_id, exchange, instrument, kind, transport
                FROM mini_fintickstreams.stream_registry
                WHERE enabled = true
                "#,
            )
            .fetch_all(&mut *conn)
            .await
            .map_err(AppError::Sqlx)?;

            for r in rows {
                let stream_id: String = r.try_get("stream_id").map_err(AppError::Sqlx)?;
                if !seen.insert(stream_id) {
                    // same row found on another shard (due to your sharding approach)
                    continue;
                }

                let exchange_s: String = r.try_get("exchange").map_err(AppError::Sqlx)?;
                let transport_s: String = r.try_get("transport").map_err(AppError::Sqlx)?;
                let kind_s: String = r.try_get("kind").map_err(AppError::Sqlx)?;
                let instrument: String = r.try_get("instrument").map_err(AppError::Sqlx)?;

                let exchange = ExchangeId::from_str(&exchange_s)?;
                let transport = StreamTransport::from_str(&transport_s)?;
                let kind = StreamKind::from_str(&kind_s)?;

                out.push(StartStreamParams {
                    exchange,
                    transport,
                    kind,
                    symbol: instrument,
                });
            }
        }

        // Optional: stable deterministic order on restart
        out.sort_by(|a, b| {
            (a.exchange as u8, a.transport as u8, a.kind as u8, &a.symbol).cmp(&(
                b.exchange as u8,
                b.transport as u8,
                b.kind as u8,
                &b.symbol,
            ))
        });

        Ok(out)
    }
}
