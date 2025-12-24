// use crate::error::{AppError, AppResult};
// use redis::aio::ConnectionManager;
// use redis::{AsyncCommands, RedisResult, Value};
// use std::time::Duration;
// use tokio::time::timeout;
//
// /// Thin, "dumb" Redis client wrapper:
// /// - owns a ConnectionManager
// /// - provides only the primitives we need (XADD, INFO memory, XINFO GROUPS, PING)
// /// - enforces per-command timeouts at the wrapper boundary
// ///
// /// No health policy / gating / scheduling logic belongs in here.
// #[derive(Clone)]
// pub struct RedisClient {
//     manager: ConnectionManager,
//     connect_timeout: Duration,
//     command_timeout: Duration,
// }
//
// impl RedisClient {
//     /// Create a Redis client from a URI.
//     ///
//     /// Notes:
//     /// - redis::Client::open parses the URI.
//     /// - ConnectionManager will reconnect as needed.
//     pub async fn connect(
//         uri: &str,
//         connect_timeout: Duration,
//         command_timeout: Duration,
//     ) -> AppResult<Self> {
//         let client = redis::Client::open(uri)
//             .map_err(|e| AppError::InvalidConfig(format!("invalid redis uri '{uri}': {e}")))?;
//
//         // ConnectionManager creation is async; wrap in timeout.
//         let manager = timeout(connect_timeout, ConnectionManager::new(client))
//             .await
//             .map_err(|_| {
//                 AppError::Redis(format!("redis connect timeout after {:?}", connect_timeout))
//             })?
//             .map_err(|e| AppError::Redis(format!("redis connect error: {e}")))?;
//
//         Ok(Self {
//             manager,
//             connect_timeout,
//             command_timeout,
//         })
//     }
//
//     /// Basic liveness check.
//     pub async fn ping(&self) -> AppResult<()> {
//         self.with_timeout(async {
//             let mut conn = self.manager.clone();
//             let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
//             if pong != "PONG" {
//                 return Err(redis::RedisError::from((
//                     redis::ErrorKind::ResponseError,
//                     "PING did not return PONG",
//                 )));
//             }
//             Ok(())
//         })
//         .await
//     }
//
//     /// XADD wrapper with MAXLEN (~ optional).
//     ///
//     /// fields is an iterator of (&str, impl ToRedisArgs)
//     /// Keep this low-level; the publisher module should own event serialization.
//     pub async fn xadd_maxlen_approx(
//         &self,
//         stream_key: &str,
//         id: &str, // usually "*"
//         maxlen: u64,
//         approx: bool,
//         fields: &[(&str, &str)],
//     ) -> AppResult<String> {
//         // We build the exact XADD we want:
//         // XADD key MAXLEN [~] maxlen id field value [field value ...]
//         self.with_timeout(async {
//             let mut conn = self.manager.clone();
//
//             let mut cmd = redis::cmd("XADD");
//             cmd.arg(stream_key);
//
//             // Retention / trimming
//             cmd.arg("MAXLEN");
//             if approx {
//                 cmd.arg("~");
//             }
//             cmd.arg(maxlen);
//
//             // Entry ID
//             cmd.arg(id);
//
//             // Fields
//             for (k, v) in fields {
//                 cmd.arg(k).arg(v);
//             }
//
//             let entry_id: String = cmd.query_async(&mut conn).await?;
//             Ok(entry_id)
//         })
//         .await
//     }
//
//     /// INFO MEMORY parsed into a small struct.
//     pub async fn info_memory(&self) -> AppResult<RedisMemoryInfo> {
//         let raw = self
//             .with_timeout(async {
//                 let mut conn = self.manager.clone();
//                 let s: String = redis::cmd("INFO")
//                     .arg("memory")
//                     .query_async(&mut conn)
//                     .await?;
//                 Ok(s)
//             })
//             .await?;
//
//         Ok(RedisMemoryInfo::parse(&raw))
//     }
//
//     /// XINFO GROUPS <stream>
//     ///
//     /// Returns raw Redis Value for now; caller can parse or you can use the helpers below.
//     pub async fn xinfo_groups(&self, stream_key: &str) -> AppResult<Value> {
//         self.with_timeout(async {
//             let mut conn = self.manager.clone();
//             let v: Value = redis::cmd("XINFO")
//                 .arg("GROUPS")
//                 .arg(stream_key)
//                 .query_async(&mut conn)
//                 .await?;
//             Ok(v)
//         })
//         .await
//     }
//
//     /// Convenience: sum "pending" across all groups for a stream.
//     ///
//     /// This is useful for health polling, but be careful:
//     /// calling this across *many* stream keys every 2s will be expensive.
//     pub async fn pending_total_for_stream(&self, stream_key: &str) -> AppResult<u64> {
//         let v = self.xinfo_groups(stream_key).await?;
//         Ok(parse_xinfo_groups_pending_total(&v))
//     }
//
//     /// Internal: execute a future with the client command timeout.
//     async fn with_timeout<T>(
//         &self,
//         fut: impl std::future::Future<Output = RedisResult<T>>,
//     ) -> AppResult<T> {
//         timeout(self.command_timeout, fut)
//             .await
//             .map_err(|_| {
//                 AppError::Redis(format!(
//                     "redis command timeout after {:?}",
//                     self.command_timeout
//                 ))
//             })?
//             .map_err(|e| AppError::Redis(format!("{e}")))
//     }
// }
//
// /// Minimal memory info used by your health poller.
// #[derive(Debug, Clone)]
// pub struct RedisMemoryInfo {
//     pub used_memory_bytes: u64,
//     pub maxmemory_bytes: Option<u64>,
//     pub used_memory_pct: Option<f64>, // 0..=100
// }
//
// impl RedisMemoryInfo {
//     pub fn parse(info_memory: &str) -> Self {
//         // INFO memory is key:value lines. We only need:
//         // used_memory:<bytes>
//         // maxmemory:<bytes>
//         let mut used_memory_bytes: u64 = 0;
//         let mut maxmemory_bytes: Option<u64> = None;
//
//         for line in info_memory.lines() {
//             if line.starts_with('#') || line.trim().is_empty() {
//                 continue;
//             }
//             if let Some((k, v)) = line.split_once(':') {
//                 let k = k.trim();
//                 let v = v.trim();
//                 match k {
//                     "used_memory" => {
//                         if let Ok(n) = v.parse::<u64>() {
//                             used_memory_bytes = n;
//                         }
//                     }
//                     "maxmemory" => {
//                         if let Ok(n) = v.parse::<u64>() {
//                             // 0 means "no maxmemory configured"
//                             if n > 0 {
//                                 maxmemory_bytes = Some(n);
//                             }
//                         }
//                     }
//                     _ => {}
//                 }
//             }
//         }
//
//         let used_memory_pct = maxmemory_bytes.map(|m| {
//             if m == 0 {
//                 0.0
//             } else {
//                 (used_memory_bytes as f64) * 100.0 / (m as f64)
//             }
//         });
//
//         Self {
//             used_memory_bytes,
//             maxmemory_bytes,
//             used_memory_pct,
//         }
//     }
// }
//
// /// Parse XINFO GROUPS response and sum "pending".
// ///
// /// XINFO GROUPS returns an array of group entries.
// /// Each entry is an array like [ "name", <string>, "consumers", <int>, "pending", <int>, ... ].
// ///
// /// We treat missing/unknown formats as 0 (non-fatal).
// pub fn parse_xinfo_groups_pending_total(v: &Value) -> u64 {
//     match v {
//         Value::Bulk(groups) => groups.iter().map(parse_group_pending).sum(),
//         _ => 0,
//     }
// }
//
// fn parse_group_pending(group_entry: &Value) -> u64 {
//     // group_entry should be Bulk([ key, val, key, val, ... ])
//     let Value::Bulk(kvs) = group_entry else {
//         return 0;
//     };
//
//     // Iterate pairs
//     let mut i = 0;
//     while i + 1 < kvs.len() {
//         let key = value_to_string(&kvs[i]);
//         if key.as_deref() == Some("pending") {
//             return value_to_u64(&kvs[i + 1]).unwrap_or(0);
//         }
//         i += 2;
//     }
//     0
// }
//
// fn value_to_string(v: &Value) -> Option<String> {
//     match v {
//         Value::Data(bytes) => String::from_utf8(bytes.clone()).ok(),
//         Value::Status(s) => Some(s.clone()),
//         Value::Okay => Some("OK".into()),
//         _ => None,
//     }
// }
//
// fn value_to_u64(v: &Value) -> Option<u64> {
//     match v {
//         Value::Int(n) => {
//             if *n >= 0 {
//                 Some(*n as u64)
//             } else {
//                 None
//             }
//         }
//         Value::Data(bytes) => std::str::from_utf8(bytes).ok()?.parse::<u64>().ok(),
//         _ => None,
//     }
// }
