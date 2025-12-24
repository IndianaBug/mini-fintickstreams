use crate::db::traits::BatchInsertRow;
use crate::ingest::datamap::event::{
    BookSide, DepthDeltaRow, FundingRow, LiquidationRow, OpenInterestRow, TradeRow, TradeSide,
};
use chrono::{DateTime, Utc};
use sqlx::Postgres;
use sqlx::query_builder::Separated;

#[derive(Debug, Clone)]
pub struct TradeDBRow {
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub side: i16,    // 0=buy, 1=sell
    pub price_i: i64, // scaled
    pub qty_i: i64,   // scaled
    pub trade_id: Option<i64>,
    pub is_maker: Option<bool>,
}

impl BatchInsertRow for TradeDBRow {
    const COLUMNS: &'static [&'static str] = &[
        "time", "symbol", "side", "price_i", "qty_i", "trade_id", "is_maker",
    ];

    fn table(&self, exchange: &str) -> String {
        format!("ex_{}.trades", exchange)
    }

    fn push_binds(&self, b: &mut Separated<'_, '_, Postgres, &'static str>) {
        b.push_bind(self.time.clone())
            .push_bind(self.symbol.clone())
            .push_bind(self.side)
            .push_bind(self.price_i)
            .push_bind(self.qty_i)
            .push_bind(self.trade_id)
            .push_bind(self.is_maker);
    }
}

// TradeRow -> TradeDBRow
impl From<TradeRow> for TradeDBRow {
    fn from(t: TradeRow) -> Self {
        TradeDBRow {
            time: t.time,
            symbol: t.symbol,
            side: t.side.as_i16(), // 0=buy, 1=sell
            price_i: t.price_i,
            qty_i: t.qty_i,
            trade_id: t.trade_id,
            is_maker: t.is_maker,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DepthDeltaDBRow {
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub side: i16, // 0=bid, 1=ask
    pub price_i: i64,
    pub size_i: i64, // 0 = delete
    pub seq: Option<i64>,
}

impl BatchInsertRow for DepthDeltaDBRow {
    const COLUMNS: &'static [&'static str] =
        &["time", "symbol", "side", "price_i", "size_i", "seq"];

    fn table(&self, exchange: &str) -> String {
        format!("ex_{}.depth_deltas", exchange)
    }

    fn push_binds(&self, b: &mut Separated<'_, '_, Postgres, &'static str>) {
        b.push_bind(self.time.clone())
            .push_bind(self.symbol.clone())
            .push_bind(self.side)
            .push_bind(self.price_i)
            .push_bind(self.size_i)
            .push_bind(self.seq);
    }
}

// DepthDeltaRow -> DepthDeltaDBRow
impl From<DepthDeltaRow> for DepthDeltaDBRow {
    fn from(d: DepthDeltaRow) -> Self {
        DepthDeltaDBRow {
            time: d.time,
            symbol: d.symbol,
            side: d.side.as_i16(), // 0=bid, 1=ask
            price_i: d.price_i,
            size_i: d.size_i, // 0 = delete
            seq: d.seq,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenInterestDBRow {
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub oi_i: i64,
}

impl BatchInsertRow for OpenInterestDBRow {
    const COLUMNS: &'static [&'static str] = &["time", "symbol", "oi_i"];

    fn table(&self, exchange: &str) -> String {
        format!("ex_{}.open_interest", exchange)
    }

    fn push_binds(&self, b: &mut Separated<'_, '_, Postgres, &'static str>) {
        b.push_bind(self.time.clone())
            .push_bind(self.symbol.clone())
            .push_bind(self.oi_i);
    }
}

// OpenInterestRow -> OpenInterestDBRow
impl From<OpenInterestRow> for OpenInterestDBRow {
    fn from(o: OpenInterestRow) -> Self {
        OpenInterestDBRow {
            time: o.time,
            symbol: o.symbol,
            oi_i: o.oi_i,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FundingDBRow {
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub funding_rate: i64,
    pub funding_time: Option<DateTime<Utc>>,
}

impl BatchInsertRow for FundingDBRow {
    const COLUMNS: &'static [&'static str] = &["time", "symbol", "funding_rate", "funding_time"];

    fn table(&self, exchange: &str) -> String {
        format!("ex_{}.funding", exchange)
    }

    fn push_binds(&self, b: &mut Separated<'_, '_, Postgres, &'static str>) {
        b.push_bind(self.time.clone())
            .push_bind(self.symbol.clone())
            .push_bind(self.funding_rate)
            .push_bind(self.funding_time.clone());
    }
}

// FundingRow -> FundingDBRow
impl From<FundingRow> for FundingDBRow {
    fn from(f: FundingRow) -> Self {
        FundingDBRow {
            time: f.time,
            symbol: f.symbol,
            funding_rate: f.funding_rate,
            funding_time: f.funding_time,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LiquidationDBRow {
    pub time: DateTime<Utc>,
    pub symbol: String,
    pub side: i16, // your convention
    pub price_i: Option<i64>,
    pub qty_i: i64,
    pub liq_id: Option<i64>,
}

impl BatchInsertRow for LiquidationDBRow {
    const COLUMNS: &'static [&'static str] =
        &["time", "symbol", "side", "price_i", "qty_i", "liq_id"];

    fn table(&self, exchange: &str) -> String {
        format!("ex_{}.liquidations", exchange)
    }

    fn push_binds(&self, b: &mut Separated<'_, '_, Postgres, &'static str>) {
        b.push_bind(self.time.clone())
            .push_bind(self.symbol.clone())
            .push_bind(self.side)
            .push_bind(self.price_i)
            .push_bind(self.qty_i)
            .push_bind(self.liq_id);
    }
}

// LiquidationRow -> LiquidationDBRow
impl From<LiquidationRow> for LiquidationDBRow {
    fn from(l: LiquidationRow) -> Self {
        LiquidationDBRow {
            time: l.time,
            symbol: l.symbol,
            side: l.side, // already i16 per your convention
            price_i: l.price_i,
            qty_i: l.qty_i,
            liq_id: l.liq_id,
        }
    }
}
