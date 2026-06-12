//! Small string→`serde_json::Value` coercion shared by `import.rs` and
//! `configmap_watcher.rs`.
//!
//! Both modules pull values out of a `BTreeMap<String, String>` (raw
//! ConfigMap data) and need to convert each `String` into the most specific
//! JSON scalar that round-trips: i64 first (no fractional digits), then f64,
//! then bool, then the original `String` as a JSON string.  Keeping the
//! cascade in one place prevents the two call-sites from drifting (e.g.
//! one adding `null` support but not the other).

use serde_json::Value;

/// Coerce a raw `&str` value (from a ConfigMap `data:` map) into the most
/// specific `serde_json::Value` that round-trips it.
///
/// Order: `i64` → `f64` → `bool` → `String`.  `String` is the final fallback
/// and uses the original allocation (the caller owns the source `&str`;
/// this function does its own clone into `Value::String` only when needed).
pub fn scalar_value(s: &str) -> Value {
    s.parse::<i64>()
        .map(Value::from)
        .or_else(|_| s.parse::<f64>().map(|f| serde_json::json!(f)))
        .or_else(|_| s.parse::<bool>().map(Value::from))
        .unwrap_or_else(|_| Value::String(s.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn integer_wins_over_float_and_bool() {
        assert_eq!(scalar_value("42"), json!(42));
        assert_eq!(scalar_value("-1"), json!(-1));
        assert_eq!(scalar_value("0"), json!(0));
    }

    #[test]
    fn float_when_integer_does_not_match() {
        assert_eq!(scalar_value("1.5"), json!(1.5));
        assert_eq!(scalar_value("-0.5"), json!(-0.5));
    }

    #[test]
    fn bool_when_no_number_matches() {
        assert_eq!(scalar_value("true"), json!(true));
        assert_eq!(scalar_value("false"), json!(false));
    }

    #[test]
    fn fallback_string() {
        assert_eq!(scalar_value("hello"), json!("hello"));
        assert_eq!(scalar_value(""), json!(""));
        assert_eq!(scalar_value("Yes"), json!("Yes")); // not a Rust bool
    }
}
