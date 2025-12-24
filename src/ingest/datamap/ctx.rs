use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::appconfig::AppConfig;
use crate::error::{AppError, AppResult};
use crate::ingest::instruments::registry::InstrumentRegistry;
use crate::ingest::instruments::spec::InstrumentSpec;

/// Anything a mapper needs to normalize raw messages.
pub struct MapCtx<'a> {
    pub registry: &'a InstrumentRegistry,
    pub now: DateTime<Utc>, // ingest time

    // Global fixed-point scales (from config)
    pub price_scale: i64,
    pub qty_scale: i64,
    pub open_interest_scale: i64,
    pub funding_scale: i64,
}

impl<'a> MapCtx<'a> {
    pub fn new(registry: &'a InstrumentRegistry, cfg: &AppConfig) -> Self {
        Self {
            registry,
            now: Utc::now(),
            price_scale: cfg.scales.price,
            qty_scale: cfg.scales.qty,
            open_interest_scale: cfg.scales.open_interest,
            funding_scale: cfg.scales.funding,
        }
    }

    /// Resolve instrument spec (borrowed).
    pub fn instrument(&self, exchange: &str, symbol: &str) -> AppResult<&InstrumentSpec> {
        self.registry.get(exchange, symbol).ok_or_else(|| {
            AppError::Internal(format!(
                "unknown instrument: exchange='{exchange}' symbol='{symbol}'"
            ))
        })
    }

    /// Convenience: parse price string to Decimal (exact).
    #[inline]
    pub fn price_dec(&self, price_str: &str) -> AppResult<Decimal> {
        InstrumentSpec::dec_str(price_str)
    }

    /// Convenience: parse qty string to Decimal (exact).
    #[inline]
    pub fn qty_dec(&self, qty_str: &str) -> AppResult<Decimal> {
        InstrumentSpec::dec_str(qty_str)
    }

    /// Scale any decimal-string using a scale.
    #[inline]
    pub fn scale_str_i64(&self, s: &str, scale: i64) -> AppResult<i64> {
        let x = InstrumentSpec::dec_str(s)?;
        InstrumentSpec::scale_i64(x, scale)
    }

    #[inline]
    pub fn open_interest_str_to_i64(&self, oi_str: &str) -> AppResult<i64> {
        self.scale_str_i64(oi_str, self.open_interest_scale)
    }

    #[inline]
    pub fn funding_str_to_i64(&self, oi_str: &str) -> AppResult<i64> {
        self.scale_str_i64(oi_str, self.funding_scale)
    }

    /// Trade normalization: (price_str, qty_str) -> (price_i, qty_i_base).
    /// Uses instrument semantics for qty unit conversion.
    pub fn trade_to_scaled_i64(
        &self,
        exchange: &str,
        symbol: &str,
        price_str: &str,
        qty_str: &str,
    ) -> AppResult<(i64, i64)> {
        let inst = self.instrument(exchange, symbol)?;
        inst.trade_to_scaled_i64(price_str, qty_str, self.price_scale, self.qty_scale)
    }

    /// Convert qty string to BASE Decimal using price string.
    pub fn qty_str_to_base_dec(
        &self,
        exchange: &str,
        symbol: &str,
        qty_str: &str,
        price_str: &str,
    ) -> AppResult<Decimal> {
        let inst = self.instrument(exchange, symbol)?;
        inst.qty_str_to_base(qty_str, price_str)
    }

    /// Depth normalization helper:
    /// Convert a reported size to BASE-scaled i64, given a level price.
    ///
    /// Most exchanges report size in BASE for order book, but if you ever ingest
    /// a venue that reports quote/contract sizes, this stays correct.
    pub fn book_size_to_base_i64(
        &self,
        exchange: &str,
        symbol: &str,
        size_str: &str,
        price_str: &str,
    ) -> AppResult<i64> {
        let inst = self.instrument(exchange, symbol)?;
        let size_base_dec = inst.qty_str_to_base(size_str, price_str)?;
        InstrumentSpec::scale_i64(size_base_dec, self.qty_scale)
    }

    /// Just scale a price string to i64.
    pub fn price_str_to_i64(&self, price_str: &str) -> AppResult<i64> {
        self.scale_str_i64(price_str, self.price_scale)
    }
}

