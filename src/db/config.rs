use crate::error::{AppError, AppResult};
use serde::Deserialize;
use std::env;
use std::{collections::HashSet, fs, path::Path};

#[derive(Debug, Clone, Deserialize)]
pub struct TimescaleDbConfig {
    pub shards: Vec<ShardConfig>,
    pub writer: WriterConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardConfig {
    pub id: String,
    /// Name of the environment variable that holds the DSN
    pub dsn_env: String,
    // Connection pool
    pub pool_min: u32,
    pub pool_max: u32,
    pub connect_timeout_ms: u64,
    pub idle_timeout_sec: u64,

    // Routing rules
    #[serde(default)]
    pub rules: Vec<ShardRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShardRule {
    pub exchange: String,
    pub stream: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WriterConfig {
    pub batch_size: usize,
    pub flush_interval_ms: u64,
    pub max_inflight_batches: usize,
    pub use_copy: bool,
}

impl TimescaleDbConfig {
    pub fn load() -> AppResult<Self> {
        let path = std::env::var("TIMESCALE_DB_CONFIG")
            .unwrap_or_else(|_| "src/config/timescale_db.toml".to_string());

        let raw = fs::read_to_string(&path)?;
        let cfg: Self = toml::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> AppResult<()> {
        // ---- Top-level checks
        if self.shards.is_empty() {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: must define at least one [[shards]]".into(),
            ));
        }

        // ---- Shards checks
        let mut seen_ids = HashSet::new();

        for (i, shard) in self.shards.iter().enumerate() {
            let prefix = format!("timescale_db.toml: shards[{i}]");

            if shard.id.trim().is_empty() {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: id must not be empty"
                )));
            }
            if !seen_ids.insert(shard.id.clone()) {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: duplicate shard id '{}'",
                    shard.id
                )));
            }

            // dsn_env checks
            if shard.dsn_env.trim().is_empty() {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: dsn_env must not be empty"
                )));
            }

            // Fail fast if the env var is missing
            let dsn = env::var(&shard.dsn_env).map_err(|_| {
                AppError::InvalidConfig(format!(
                    "{prefix}: environment variable '{}' is not set",
                    shard.dsn_env
                ))
            })?;

            // Lightweight sanity check; sqlx will do real parsing later.
            if !dsn.starts_with("postgres://") && !dsn.starts_with("postgresql://") {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: DSN from env var '{}' must start with postgres:// or postgresql://",
                    shard.dsn_env
                )));
            }

            if shard.pool_min == 0 {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: pool_min must be >= 1"
                )));
            }
            if shard.pool_max == 0 {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: pool_max must be >= 1"
                )));
            }
            if shard.pool_min > shard.pool_max {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: pool_min ({}) must be <= pool_max ({})",
                    shard.pool_min, shard.pool_max
                )));
            }
            if shard.connect_timeout_ms == 0 {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: connect_timeout_ms must be > 0"
                )));
            }
            if shard.idle_timeout_sec == 0 {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: idle_timeout_sec must be > 0"
                )));
            }

            if shard.rules.is_empty() {
                return Err(AppError::InvalidConfig(format!(
                    "{prefix}: must define at least one [[shards.rules]]"
                )));
            }

            // Validate rules (allow "*" wildcard; otherwise require non-empty)
            for (r, rule) in shard.rules.iter().enumerate() {
                let rprefix = format!("{prefix}.rules[{r}]");
                validate_rule_field(&rprefix, "exchange", &rule.exchange)?;
                validate_rule_field(&rprefix, "stream", &rule.stream)?;
                validate_rule_field(&rprefix, "symbol", &rule.symbol)?;
            }
        }

        // ---- Writer checks
        if self.writer.batch_size == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: writer.batch_size must be > 0".into(),
            ));
        }
        if self.writer.flush_interval_ms == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: writer.flush_interval_ms must be > 0".into(),
            ));
        }
        if self.writer.max_inflight_batches == 0 {
            return Err(AppError::InvalidConfig(
                "timescale_db.toml: writer.max_inflight_batches must be > 0".into(),
            ));
        }

        Ok(())
    }
}

fn validate_rule_field(prefix: &str, field: &str, value: &str) -> AppResult<()> {
    let v = value.trim();
    if v.is_empty() {
        return Err(AppError::InvalidConfig(format!(
            "{prefix}: {field} must not be empty (use \"*\" for wildcard)"
        )));
    }
    // Currently we allow "*" or exact match strings.
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::db::config::TimescaleDbConfig;

    #[test]
    fn load_timescale_config_and_print() {
        let cfg = TimescaleDbConfig::load().expect("failed to load timescale_db.toml");
        println!("Loaded TimescaleDB config:\n{:#?}", cfg);
        // Minimal sanity assertions so the test actually verifies something
        assert!(!cfg.shards.is_empty());
        assert!(cfg.writer.batch_size > 0);
    }
}
