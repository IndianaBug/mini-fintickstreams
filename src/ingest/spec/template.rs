use super::types::Ctx;
use crate::error::{AppError, AppResult};
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;
use toml::Value as TomlValue;

/// Render a string replacing any occurrences of `<key>` with ctx[key].
/// - Replaces multiple occurrences.
/// - Returns a nice error if a placeholder is present but missing in ctx.
pub fn render_string(input: &str, ctx: &Ctx) -> AppResult<String> {
    // Fast path: no templates
    if !input.contains('<') {
        return Ok(input.to_string());
    }

    // Collect placeholders of the form <...>
    let mut missing: BTreeSet<String> = BTreeSet::new();
    let mut out = input.to_string();

    // A simple, robust approach:
    // scan for `<` ... `>` segments and replace exact tokens.
    // This supports strings like "<symbol>@aggTrade" and JSON ids "<stream_id>".
    let mut i = 0usize;
    while let Some(start) = out[i..].find('<') {
        let start = i + start;
        if let Some(end_rel) = out[start..].find('>') {
            let end = start + end_rel;
            let key = &out[start + 1..end]; // inside <...>

            if let Some(val) = ctx.get(key) {
                // Replace this specific occurrence only
                out.replace_range(start..=end, val);
                // Continue scanning from start (val could contain '<', though unlikely)
                i = start;
            } else {
                missing.insert(key.to_string());
                // Skip past this '>' to continue scanning
                i = end + 1;
            }
        } else {
            break; // no closing '>', stop
        }
    }

    if !missing.is_empty() {
        return Err(AppError::InvalidConfig(format!(
            "Missing template replacements for: {}",
            missing.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }

    Ok(out)
}

/// Recursively render templates inside a TOML value.
/// - Strings are rendered via `render_string`.
/// - Tables/Arrays are traversed.
/// - Scalars are passed through.
pub fn render_toml(value: &TomlValue, ctx: &Ctx) -> AppResult<TomlValue> {
    Ok(match value {
        TomlValue::String(s) => TomlValue::String(render_string(s, ctx)?),

        TomlValue::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(render_toml(v, ctx)?);
            }
            TomlValue::Array(out)
        }

        TomlValue::Table(tbl) => {
            let mut out = toml::map::Map::new();
            for (k, v) in tbl {
                // keys are literal; values can be templated
                out.insert(k.clone(), render_toml(v, ctx)?);
            }
            TomlValue::Table(out)
        }

        // numbers/bools/datetime are not templated
        TomlValue::Integer(i) => TomlValue::Integer(*i),
        TomlValue::Float(f) => TomlValue::Float(*f),
        TomlValue::Boolean(b) => TomlValue::Boolean(*b),
        TomlValue::Datetime(dt) => TomlValue::Datetime(*dt),
    })
}

/// Convert a TOML scalar into a string suitable for query params.
/// Rejects arrays/tables (query params must be flat).
fn toml_scalar_to_string(v: &TomlValue, key: &str) -> AppResult<String> {
    match v {
        TomlValue::String(s) => Ok(s.clone()),
        TomlValue::Integer(i) => Ok(i.to_string()),
        TomlValue::Float(f) => Ok(f.to_string()),
        TomlValue::Boolean(b) => Ok(b.to_string()),
        TomlValue::Datetime(dt) => Ok(dt.to_string()),
        TomlValue::Array(_) | TomlValue::Table(_) => Err(AppError::InvalidConfig(format!(
            "Unsupported non-scalar TOML value for param '{}'",
            key
        ))),
    }
}

/// Render `params` (TOML table) into query params.
/// - Requires `params` to be a TOML table of scalars after rendering.
/// - Great for Binance-style `GET ...?symbol=...&limit=...`.
pub fn render_params_as_query(params: &TomlValue, ctx: &Ctx) -> AppResult<Vec<(String, String)>> {
    let rendered = render_toml(params, ctx)?;

    let tbl = rendered.as_table().ok_or_else(|| {
        AppError::InvalidConfig("params must be a TOML table for query params".into())
    })?;

    let mut out = Vec::with_capacity(tbl.len());
    for (k, v) in tbl {
        out.push((k.clone(), toml_scalar_to_string(v, k)?));
    }

    Ok(out)
}

/// Convert TOML into serde_json::Value recursively.
/// This is useful for JSON bodies and WS subscribe/unsubscribe payloads.
pub fn toml_to_json(value: &TomlValue) -> AppResult<JsonValue> {
    Ok(match value {
        TomlValue::String(s) => JsonValue::String(s.clone()),
        TomlValue::Integer(i) => JsonValue::Number((*i).into()),
        TomlValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .ok_or_else(|| AppError::InvalidConfig("Invalid float in TOML".into()))?,
        TomlValue::Boolean(b) => JsonValue::Bool(*b),
        TomlValue::Datetime(dt) => JsonValue::String(dt.to_string()),
        TomlValue::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(toml_to_json(v)?);
            }
            JsonValue::Array(out)
        }
        TomlValue::Table(tbl) => {
            let mut map = serde_json::Map::new();
            for (k, v) in tbl {
                map.insert(k.clone(), toml_to_json(v)?);
            }
            JsonValue::Object(map)
        }
    })
}

/// Render params into a JSON value (after template replacement).
/// - Great for Hyperliquid-style `{ type="l2Book", coin="<coin>" }` bodies.
/// - Also great for WS control messages like Binance's subscribe templates.
pub fn render_toml_as_json(value: &TomlValue, ctx: &Ctx) -> AppResult<JsonValue> {
    let rendered = render_toml(value, ctx)?;
    toml_to_json(&rendered)
}
