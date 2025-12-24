use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub id: String,
    pub env: String,
    pub config_version: u32,

    pub scales: ScalesConfig,

    pub exchange_toggles: ExchangeToggles,
    pub streams: StreamsConfig,
    pub limits: LimitsConfig,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
}

#[derive(Debug, Deserialize)]
pub struct ScalesConfig {
    pub price: i64,
    pub qty: i64,
    pub open_interest: i64,
    pub funding: i64,
}

#[derive(Debug, Deserialize)]
pub struct ExchangeToggles {
    pub binance_linear: bool,
    pub hyperliquid_perp: bool,
}

#[derive(Debug, Deserialize)]
pub struct StreamsConfig {
    pub assign_shard_on_create: bool,
    pub allow_reroute: bool,
    pub persist_assignments: bool,
}

#[derive(Debug, Deserialize)]
pub struct LimitsConfig {
    pub max_active_streams: u32,
    pub max_events_per_sec: u64,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

#[derive(Debug, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
}

fn validate_config(cfg: &AppConfig) -> AppResult<()> {
    if cfg.id.is_empty() {
        return Err(AppError::MissingConfig("id"));
    }

    if cfg.config_version == 0 {
        return Err(AppError::InvalidConfig(
            "config_version must be >= 1".into(),
        ));
    }

    if cfg.limits.max_active_streams == 0 {
        return Err(AppError::InvalidConfig(
            "max_active_streams must be > 0".into(),
        ));
    }

    if cfg.limits.max_events_per_sec == 0 {
        return Err(AppError::InvalidConfig(
            "max_events_per_sec must be > 0".into(),
        ));
    }

    // --------------------------------------------------
    // Fixed-point scale validation
    // --------------------------------------------------
    let scales = &cfg.scales;

    for (name, value) in [
        ("price", scales.price),
        ("qty", scales.qty),
        ("open_interest", scales.open_interest),
        ("funding", scales.funding),
    ] {
        if value <= 0 {
            return Err(AppError::InvalidConfig(format!(
                "scale '{name}' must be > 0"
            )));
        }

        if !is_power_of_ten(value) {
            return Err(AppError::InvalidConfig(format!(
                "scale '{name}' must be a power of 10 (got {value})"
            )));
        }
    }

    Ok(())
}

fn is_power_of_ten(mut v: i64) -> bool {
    if v <= 0 {
        return false;
    }
    while v % 10 == 0 {
        v /= 10;
    }
    v == 1
}

const APP_CONFIG_PATH: &str = "src/config/app.toml";

pub fn load_app_config() -> AppResult<AppConfig> {
    let contents = fs::read_to_string(APP_CONFIG_PATH)?; // AppError::ConfigIo
    let config: AppConfig = toml::from_str(&contents)?; // AppError::ConfigToml
    validate_config(&config)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_and_print_app_config() {
        let cfg = load_app_config().expect("failed to load app config");

        println!("id = {}", cfg.id);
        println!("env = {}", cfg.env);
        println!("config_version = {}", cfg.config_version);

        println!(
            "exchanges: binance_linear={}, hyperliquid_perp={}",
            cfg.exchange_toggles.binance_linear, cfg.exchange_toggles.hyperliquid_perp
        );

        println!(
            "limits: max_active_streams={}, max_events_per_sec={}",
            cfg.limits.max_active_streams, cfg.limits.max_events_per_sec
        );

        println!("logging.level = {}", cfg.logging.level);
        println!("metrics.enabled = {}", cfg.metrics.enabled);
    }
}
