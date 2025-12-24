// ingest/exchanges/hyperliquid/types.rs
use crate::error::{AppError, AppResult};
use crate::ingest::traits::FromJsonStr;
use serde::Deserialize;

// ---------- Perp L2 book snapshot ----------

#[derive(Debug, Clone, Deserialize)]
pub struct Hyperliquid_book_level {
    pub px: String,
    pub sz: String,
    pub n: u32,
}

/// Hyperliquid returns `levels` as `[bids, asks]`.
pub type Hyperliquid_levels = [Vec<Hyperliquid_book_level>; 2];

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidPerpDepthSnapshot {
    pub coin: String,

    /// Event time (ms)
    pub time: u64,

    pub levels: Hyperliquid_levels,

    /// Present on some REST snapshots (string in API)
    pub spread: Option<String>,
}

impl FromJsonStr for HyperliquidPerpDepthSnapshot {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

// ---------- Perp info snapshot ----------

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidPerpInfoSnapshot {
    pub universe: Vec<Hyperliquid_perp_universe_entry>,

    /// API shape is `[[id, {..}], [id, {..}], ...]`
    #[serde(rename = "marginTables")]
    pub margin_tables: Vec<(u32, Hyperliquid_margin_table)>,

    #[serde(rename = "collateralToken")]
    pub collateral_token: u32,
}

impl FromJsonStr for HyperliquidPerpInfoSnapshot {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hyperliquid_perp_universe_entry {
    #[serde(rename = "szDecimals")]
    pub sz_decimals: u32,

    pub name: String,

    #[serde(rename = "maxLeverage")]
    pub max_leverage: u32,

    #[serde(rename = "marginTableId")]
    pub margin_table_id: u32,

    #[serde(default, rename = "isDelisted")]
    pub is_delisted: bool,

    #[serde(default, rename = "onlyIsolated")]
    pub only_isolated: bool,

    #[serde(default, rename = "marginMode")]
    pub margin_mode: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hyperliquid_margin_table {
    pub description: String,

    #[serde(rename = "marginTiers")]
    pub margin_tiers: Vec<Hyperliquid_margin_tier>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hyperliquid_margin_tier {
    #[serde(rename = "lowerBound")]
    pub lower_bound: String,

    #[serde(rename = "maxLeverage")]
    pub max_leverage: u32,
}

// ---------- WS: depth update ----------

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidPerpWsDepthUpdate {
    pub channel: String,
    pub data: Hyperliquid_perp_ws_depth_data,
}

impl FromJsonStr for HyperliquidPerpWsDepthUpdate {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hyperliquid_perp_ws_depth_data {
    pub coin: String,
    pub time: u64,
    pub levels: Hyperliquid_levels,
}

// ---------- WS: OI + funding update ----------

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidPerpWsOIFundingUpdate {
    pub channel: String,
    pub data: Hyperliquid_perp_ws_oi_funding_data,
}

impl FromJsonStr for HyperliquidPerpWsOIFundingUpdate {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hyperliquid_perp_ws_oi_funding_data {
    pub coin: String,
    pub ctx: Hyperliquid_active_asset_ctx,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hyperliquid_active_asset_ctx {
    pub funding: String,

    #[serde(rename = "openInterest")]
    pub open_interest: String,

    #[serde(rename = "prevDayPx")]
    pub prev_day_px: String,

    #[serde(rename = "dayNtlVlm")]
    pub day_ntl_vlm: String,

    pub premium: String,

    #[serde(rename = "oraclePx")]
    pub oracle_px: String,

    #[serde(rename = "markPx")]
    pub mark_px: String,

    #[serde(rename = "midPx")]
    pub mid_px: String,

    #[serde(rename = "impactPxs")]
    pub impact_pxs: [String; 2],

    #[serde(rename = "dayBaseVlm")]
    pub day_base_vlm: String,
}

// ---------- WS: trades ----------

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidPerpWsTrade {
    pub channel: String,
    pub data: Vec<Hyperliquid_trade>,
}

impl FromJsonStr for HyperliquidPerpWsTrade {
    fn from_json_str(s: &str) -> AppResult<Self> {
        serde_json::from_str(s).map_err(AppError::Json)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Hyperliquid_trade {
    pub coin: String,

    /// "B" or "S" in the payload
    pub side: String,

    pub px: String,
    pub sz: String,
    pub time: u64,
    pub hash: String,
    pub tid: u64,
    pub users: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;
    use std::fs;
    use std::path::PathBuf;

    fn testdata_path(file_name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("ingest")
            .join("datamap")
            .join("testdata")
            .join(file_name)
    }

    fn load_and_parse<T: DeserializeOwned>(struct_name: &str) -> bool {
        let file_name = format!("{struct_name}.json");
        let path = testdata_path(&file_name);

        println!("\n==> checking {}", path.display());

        let s = match fs::read_to_string(&path) {
            Ok(s) => {
                println!("    OK: file found");
                s
            }
            Err(e) => {
                println!("    ERROR: missing or unreadable file ({})", e);
                return false;
            }
        };

        match serde_json::from_str::<T>(&s) {
            Ok(_) => {
                println!("    OK: json parsed successfully");
                true
            }
            Err(e) => {
                println!("    ERROR: json parse failed: {}", e);
                false
            }
        }
    }

    #[test]
    fn hyperliquid_testdata_json_parses() {
        println!("\n=== Hyperliquid JSON testdata parse test ===");

        let mut ok = true;

        ok &= load_and_parse::<HyperliquidPerpDepthSnapshot>("HyperliquidPerpDepthSnapshot");
        ok &= load_and_parse::<HyperliquidPerpInfoSnapshot>("HyperliquidPerpInfoSnapshot");
        ok &= load_and_parse::<HyperliquidPerpWsDepthUpdate>("HyperliquidPerpWsDepthUpdate");
        ok &=
            load_and_parse::<HyperliquidPerpWsOIFundingUpdate>("HyperliquidPerpWsOIFundingUpdate");
        ok &= load_and_parse::<HyperliquidPerpWsTrade>("HyperliquidPerpWsTrade");

        if ok {
            println!("\n=== ALL Hyperliquid testdata JSON parsed successfully ===");
        } else {
            println!("\n=== Hyperliquid testdata JSON ERRORS detected ===");
            panic!("one or more Hyperliquid testdata files missing or failed to parse");
        }
    }
}
