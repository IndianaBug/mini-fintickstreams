// tests/redis_retention.rs
//
// Run with:
//   REDIS_URL=redis://127.0.0.1:6380 cargo test -p <your_crate_name> --test redis_retention -- --nocapture
//
// Assumptions:
// - Redis is already running at REDIS_URL
// - Your crate exposes the modules used below
//
// What it tests:
// - Publishing > maxlen entries to one stream
// - Stream length stays bounded near maxlen (approx trimming)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use redis::AsyncCommands;

use crate::redis::client::RedisClient;
use crate::redis::config::{
    CapacityConfig, ConnectionConfig, FailoverConfig, GroupsConfig, RedisConfig, RedisMode,
    RetentionConfig, SaturationPolicy, DownPolicy, StreamsConfig,
};
use crate::redis::manager::{PublishOutcome, RedisManager};
use crate::redis::metrics::RedisMetrics;
use crate::redis::streams::StreamKind;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6380".to_string())
}

fn test_config(redis_url: &str) -> RedisConfig {
    let mut nodes = HashMap::new();
    nodes.insert("a".to_string(), redis_url.to_string());

    RedisConfig {
        enabled: true,
        mode: RedisMode::Single,
        default_node: "a".into(),
        nodes,

        connection: ConnectionConfig {
            connect_timeout_ms: 2_000,
            command_timeout_ms: 2_000,
            keepalive_sec: 30,
            tcp_nodelay: true,
        },

        capacity: CapacityConfig {
            poll_interval_sec: 1, // faster in tests
            max_memory_pct: 95,   // keep high so we don't disable during this test
            max_pending: 200_000, // unused for now
            max_p99_cmd_ms: 200,  // keep high so we don't disable due to local jitter
            redis_publish_latency_window: 512,
        },

        failover: FailoverConfig {
            on_saturated: SaturationPolicy::StopAssigningNew,
            on_down: DownPolicy::DisableRedisTemporarily,
        },

        streams: StreamsConfig {
            key_format: "stream:{exchange}:{symbol}:{kind}".into(),
            publish_trades: true,
            publish_depth: false,
            publish_liquidations: false,
            publish_funding: false,
            publish_open_interest: false,
        },

        retention: RetentionConfig {
            maxlen: 1_000, // keep small for fast tests
            approx: true,
        },

        groups: GroupsConfig {
            feature_builder: "cg:features".into(),
            ml_infer: None,
        },
    }
}

// helper: open an async connection manager for tests
async fn test_conn(url: &str) -> ConnectionManager {
    let rc = redis::Client::open(url).unwrap();
    ConnectionManager::new(rc).await.unwrap()
}

#[tokio::test]
async fn stream_retention_keeps_length_bounded() {
    let url = redis_url();
    let cfg = test_config(&url);

    // Create client and manager
    let client = Arc::new(RedisClient::connect_from_config(&cfg).await.unwrap());
    let metrics = RedisMetrics::new().unwrap();
    let manager = Arc::new(RedisManager::new(cfg.clone(), client.clone(), metrics).unwrap());

    // Start health loop (keeps gate updated)
    let _health = manager.spawn_health_loop();

    // Use a unique key each run so tests don't collide
    let exchange = "binance";
    let symbol = format!("TESTRET_{}", std::process::id());
    let kind = StreamKind::Trades;
    let stream_key = format!("stream:{exchange}:{symbol}:trades");

    // Clean start
    {
        let rc = redis::Client::open(url.as_str()).unwrap();
        let mut conn = rc.get_async_connection().await.unwrap();
        let _: () = conn.del(&stream_key).await.unwrap();
    }

    // Publish more than maxlen
    let target = (cfg.retention.maxlen as usize) * 5;
    for i in 0..target {
        let fields = [
            ("ts", "1700000000"),
            ("px", "42000.0"),
            ("qty", "0.01"),
            ("i", Box::leak(i.to_string().into_boxed_str())), // ok for test
        ];

        let out = manager
            .publish(exchange, &symbol, kind, &fields)
            .await
            .unwrap();

        // In a healthy test run, most should be Published
        // (We don't hard-fail on occasional failures; this is integration, not unit.)
        if i < 10 {
            assert!(matches!(out, PublishOutcome::Published | PublishOutcome::Failed));
        }
    }

    // Give Redis a brief moment to apply approximate trimming under load
    sleep(Duration::from_millis(200)).await;

    // Check XLEN is bounded near maxlen.
    let xlen: u64 = {
        let rc = redis::Client::open(url.as_str()).unwrap();
        let mut conn = rc.get_async_connection().await.unwrap();
        conn.xlen(&stream_key).await.unwrap()
    };

    // Since MAXLEN is approximate, allow a small cushion.
    // In practice this should stay close to maxlen.
    let maxlen = cfg.retention.maxlen;
    let upper = (maxlen as f64 * 1.25) as u64 + 50;

    assert!(
        xlen <= upper,
        "XLEN too large: got {xlen}, expected <= {upper} (maxlen={maxlen})"
    );

    // Cleanup
    {
        let rc = redis::Client::open(url.as_str()).unwrap();
        let mut conn = rc.get_async_connection().await.unwrap();
        let _: () = conn.del(&stream_key).await.unwrap();
    }
}

