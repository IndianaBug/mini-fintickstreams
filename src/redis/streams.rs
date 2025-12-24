// src/redis/streams.rs

use crate::error::{AppError, AppResult};
use crate::redis::config::RedisConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    Trades,
    Depth,
    Liquidations,
    Funding,
    OpenInterest,
}

impl StreamKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamKind::Trades => "trades",
            StreamKind::Depth => "depth",
            StreamKind::Liquidations => "liquidations",
            StreamKind::Funding => "funding",
            StreamKind::OpenInterest => "open_interest",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamKeyBuilder {
    fmt: String,
}

impl StreamKeyBuilder {
    /// Build from validated RedisConfig.
    ///
    /// Assumes RedisConfig::validate() has already enforced:
    /// - non-empty format
    /// - presence of {exchange}, {symbol}, {kind}
    pub fn from_config(cfg: &RedisConfig) -> AppResult<Self> {
        Ok(Self {
            fmt: cfg.streams.key_format.clone(),
        })
    }

    #[inline]
    pub fn key(&self, exchange: &str, symbol: &str, kind: StreamKind) -> String {
        self.fmt
            .replace("{exchange}", exchange)
            .replace("{symbol}", symbol)
            .replace("{kind}", kind.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::config::RedisConfig;

    #[test]
    fn builds_stream_key_from_config() {
        let cfg = RedisConfig::load_default().unwrap();
        let builder = StreamKeyBuilder::from_config(&cfg).unwrap();

        let key = builder.key("binance", "BTCUSDT", StreamKind::Trades);
        assert_eq!(key, "stream:binance:BTCUSDT:trades");
    }

    #[test]
    fn builds_multiple_kinds() {
        let cfg = RedisConfig::load_default().unwrap();
        let builder = StreamKeyBuilder::from_config(&cfg).unwrap();

        assert_eq!(
            builder.key("bybit", "ETHUSDT", StreamKind::Depth),
            "stream:bybit:ETHUSDT:depth"
        );

        assert_eq!(
            builder.key("bybit", "ETHUSDT", StreamKind::OpenInterest),
            "stream:bybit:ETHUSDT:open_interest"
        );
    }
}
