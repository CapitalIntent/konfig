//! Trading-specific config types for the Coinbase pod.
//!
//! `TradingRiskConfig` is the Rust representation of the risk-limit fields
//! in a `Config.konfig.io/v1` CRD whose `content` JSON object contains:
//!
//! ```json
//! {
//!   "max_order_size_usd": 1000.0,
//!   "max_position_usd": 10000.0,
//!   "max_daily_loss_usd": 500.0
//! }
//! ```
//!
//! Strategy params (not present here) can be added to `content` without
//! triggering the session-boundary apply gate — only risk-limit fields do.

use serde::Deserialize;

/// Hot-reloadable risk limits sourced from the Konfig CRD.
///
/// Deserialized from `ConfigSnapshot.content` via `serde_json::from_value`.
/// Any field absent in the JSON falls back to the compiled-in default.
#[derive(Debug, Clone, Deserialize)]
pub struct TradingRiskConfig {
    /// Maximum single-order notional in USD. Default: 1 000.
    #[serde(default = "default_max_order_size_usd")]
    pub max_order_size_usd: f64,

    /// Maximum absolute net position per product in USD. Default: 10 000.
    #[serde(default = "default_max_position_usd")]
    pub max_position_usd: f64,

    /// Daily net-of-fees loss limit. Default: 500.
    #[serde(default = "default_max_daily_loss_usd")]
    pub max_daily_loss_usd: f64,
}

fn default_max_order_size_usd() -> f64 {
    1_000.0
}
fn default_max_position_usd() -> f64 {
    10_000.0
}
fn default_max_daily_loss_usd() -> f64 {
    500.0
}

impl Default for TradingRiskConfig {
    fn default() -> Self {
        Self {
            max_order_size_usd: default_max_order_size_usd(),
            max_position_usd: default_max_position_usd(),
            max_daily_loss_usd: default_max_daily_loss_usd(),
        }
    }
}

impl TradingRiskConfig {
    /// Deserialize from the `content` field of a `ConfigSnapshot`.
    ///
    /// Returns `Default` when `content` is null or missing the risk fields.
    pub fn from_snapshot_content(content: &serde_json::Value) -> Self {
        serde_json::from_value(content.clone()).unwrap_or_default()
    }

    /// Returns `true` if any risk-limit field differs from `other`.
    ///
    /// Strategy fields (none yet in this struct) would NOT be checked here —
    /// they apply immediately without the session-boundary gate.
    pub fn risk_limits_changed(&self, other: &Self) -> bool {
        (self.max_order_size_usd - other.max_order_size_usd).abs() > f64::EPSILON
            || (self.max_position_usd - other.max_position_usd).abs() > f64::EPSILON
            || (self.max_daily_loss_usd - other.max_daily_loss_usd).abs() > f64::EPSILON
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_values_are_sane() {
        let cfg = TradingRiskConfig::default();
        assert!(cfg.max_order_size_usd > 0.0);
        assert!(cfg.max_position_usd > 0.0);
        assert!(cfg.max_daily_loss_usd > 0.0);
    }

    #[test]
    fn deserializes_from_full_json() {
        let content = json!({
            "max_order_size_usd": 2000.0,
            "max_position_usd": 20000.0,
            "max_daily_loss_usd": 1000.0
        });
        let cfg = TradingRiskConfig::from_snapshot_content(&content);
        assert_eq!(cfg.max_order_size_usd, 2000.0);
        assert_eq!(cfg.max_position_usd, 20000.0);
        assert_eq!(cfg.max_daily_loss_usd, 1000.0);
    }

    #[test]
    fn missing_fields_use_defaults() {
        let content = json!({ "max_order_size_usd": 500.0 });
        let cfg = TradingRiskConfig::from_snapshot_content(&content);
        assert_eq!(cfg.max_order_size_usd, 500.0);
        assert_eq!(cfg.max_position_usd, 10_000.0);
        assert_eq!(cfg.max_daily_loss_usd, 500.0);
    }

    #[test]
    fn null_content_returns_defaults() {
        let cfg = TradingRiskConfig::from_snapshot_content(&serde_json::Value::Null);
        let def = TradingRiskConfig::default();
        assert_eq!(cfg.max_order_size_usd, def.max_order_size_usd);
        assert_eq!(cfg.max_position_usd, def.max_position_usd);
        assert_eq!(cfg.max_daily_loss_usd, def.max_daily_loss_usd);
    }

    #[test]
    fn risk_limits_changed_detects_difference() {
        let a = TradingRiskConfig::default();
        let mut b = TradingRiskConfig::default();
        assert!(!a.risk_limits_changed(&b));
        b.max_position_usd = 99_999.0;
        assert!(a.risk_limits_changed(&b));
    }

    #[test]
    fn risk_limits_changed_same_values_returns_false() {
        let a = TradingRiskConfig::default();
        let b = TradingRiskConfig::default();
        assert!(!a.risk_limits_changed(&b));
    }
}
