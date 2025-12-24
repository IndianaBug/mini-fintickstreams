use reqwest::Method;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// Runtime replacement context used for template rendering.
/// Example keys: "symbol", "coin", "stream_title", "stream_id", "subscription_type".
pub type Ctx = BTreeMap<String, String>;

/// Where endpoint `params` should be applied for HTTP requests.
/// - Query: appended as URL query string
/// - JsonBody: sent as JSON body
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamPlacement {
    Query,
    JsonBody,
}

/// A generic, fully-resolved HTTP request specification.
/// This is what your HTTP client should execute (reqwest).
#[derive(Debug, Clone)]
pub struct HttpRequestSpec {
    pub method: Method,
    pub path: String,
    /// Query parameters (already rendered).
    pub query: Vec<(String, String)>,
    /// Headers (already rendered). Exchange-specific auth can add to this later.
    pub headers: Vec<(String, String)>,
    /// Optional JSON body (already rendered).
    pub json_body: Option<JsonValue>,
    /// Rate-limit weight for your client-side throttle.
    pub weight: u32,
    pub interval_seconds: u64,
}

///// A generic WS subscription "payload" after rendering templates.
///// Some exchanges want plain stream strings, others want JSON messages.
//#[derive(Debug, Clone)]
//pub enum WsSubscriptionSpec {
//    Text(String),
//    Json(JsonValue),
//}

/// A resolved WS subscribe/unsubscribe message spec (already rendered).
#[derive(Debug, Clone)]
pub struct WsControlSpec {
    pub subscribe: JsonValue,
    pub unsubscribe: JsonValue,
}

/// Convenience helper: build a context from an iterator of pairs.
pub fn ctx_from_pairs<I, K, V>(pairs: I) -> Ctx
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    pairs
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
}
