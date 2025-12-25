use thiserror::Error;

/// Crate-wide result type.
pub type AppResult<T> = std::result::Result<T, AppError>;

use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    // =========
    // Config / startup
    // =========
    #[error("Configuration file IO error: {0}")]
    ConfigIo(#[from] std::io::Error),

    #[error("Failed to parse TOML config: {0}")]
    ConfigToml(#[from] toml::de::Error),

    #[error("Missing configuration field: {0}")]
    MissingConfig(&'static str),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    // =========
    // Networking / WebSocket / HTTP
    // =========
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("HTTP transport error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Invalid URL/URI: {0}")]
    Uri(#[from] http::uri::InvalidUri),

    /// Remote API returned a non-success HTTP status
    #[error("API error from {service}: status={status}, body={body}")]
    Api {
        service: String,
        status: StatusCode,
        body: String,
    },

    // =========
    // Serialization / deserialization
    // =========
    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    // =========
    // Database
    // =========
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    // =========
    // Metrics / Prometheus
    // =========
    #[error("Prometheus registry error: {0}")]
    Prometheus(#[from] prometheus::Error),

    // =========
    // Application-domain errors
    // =========
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    #[error("Stream already exists: {0}")]
    StreamAlreadyExists(String),

    #[error("Rate limit exceeded: {details}")]
    RateLimited { details: String },

    #[error("Failed to spawn task: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Shutdown requested")]
    Shutdown,

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Redis logic error: {0}")]
    RedisLogic(String),
}
