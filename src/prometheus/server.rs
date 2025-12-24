use crate::error::{AppError, AppResult};
use crate::prometheus::config::PrometheusConfig;

use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use std::{net::SocketAddr, sync::Arc};

type GatherFn = Arc<dyn Fn() -> AppResult<String> + Send + Sync>;

#[derive(Clone)]
struct AppState {
    gather: GatherFn,
}

pub async fn run_metrics_server<G>(cfg: PrometheusConfig, gather: G) -> AppResult<()>
where
    G: Fn() -> AppResult<String> + Send + Sync + 'static,
{
    let addr: SocketAddr = format!("{}:{}", cfg.bind_addr, cfg.port)
        .parse()
        .map_err(|e| AppError::InvalidConfig(format!("Invalid bind/port: {e}")))?;

    let state = AppState {
        gather: Arc::new(gather),
    };

    // Axum routes must be known at build time, so we build the router dynamically:
    // - The path is configurable, but Router::route takes a &str, so this is fine.
    let app = Router::new()
        .route(&cfg.metrics_path, get(metrics_handler))
        .with_state(state);

    tracing::info!(
        bind_addr = %cfg.bind_addr,
        port = cfg.port,
        path = %cfg.metrics_path,
        "prometheus metrics server starting (axum)"
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to bind metrics server: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| AppError::Internal(format!("Metrics server error: {e}")))?;

    Ok(())
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    match (state.gather)() {
        Ok(text) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
            );
            (StatusCode::OK, headers, text).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to gather metrics");
            (StatusCode::INTERNAL_SERVER_ERROR, "gather metrics failed\n").into_response()
        }
    }
}
