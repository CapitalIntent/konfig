//! Subscribe request filtering: `names` + `label_selector`.
//!
//! Hand-rolled K8s label-selector parser (kube 0.98 ships the matcher but no
//! string parser) plus the [`SubscribeFilter`] predicate shared by the replay,
//! snapshot, and live paths. Split out of `subscribe.rs` (CU-86aj7k5rf).

use std::collections::{BTreeMap, BTreeSet, HashSet};

use kube::core::{Expression, Selector, SelectorExt};
use tonic::Status;

use crate::proto::ConfigEvent;

// ── Subscribe filtering: `names` + `label_selector` ─────────────────────────

/// Parse a K8s label-selector string into a [`Selector`].
///
/// kube 0.98 ships the selector *matcher* (`Selector` / `Expression` /
/// `SelectorExt`) but NO string→`Selector` parser, so we hand-roll the
/// equality + set-based grammar subset that K8s accepts on a `Subscribe`
/// request.  Kept a pure `&str → Result<Selector, Status>` fn so every branch
/// is unit-testable with no kube client or cache.
///
/// Grammar (comma-separated requirements, ANDed):
///
/// - `key in (v1, v2, ...)`    → [`Expression::In`]
/// - `key notin (v1, ...)`     → [`Expression::NotIn`]
/// - `key = value` / `key == value` → [`Expression::Equal`]
/// - `key != value`            → [`Expression::NotEqual`]
/// - `!key`                    → [`Expression::DoesNotExist`]
/// - bare `key`                → [`Expression::Exists`]
///
/// An empty / whitespace-only input yields an empty `Selector`, which matches
/// everything (the no-filter case).  Whitespace around keys, operators, and
/// values is trimmed.  Malformed input (unbalanced parens, empty key, empty
/// value where a value is required, unknown operator) returns
/// `Status::invalid_argument` so the RPC fails fast with a clear reason.
fn parse_label_selector(input: &str) -> Result<Selector, Status> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        // Empty Selector — `Selector::selects_all()` is true, matches all.
        return Ok(Selector::default());
    }

    let requirements = split_top_level_requirements(trimmed)?;
    let mut expressions: Vec<Expression> = Vec::with_capacity(requirements.len());
    for req in requirements {
        expressions.push(parse_requirement(req.trim())?);
    }
    // `impl FromIterator<Expression> for Selector` (kube 0.98) — collects the
    // expression list via `Selector::from_expressions`.
    Ok(expressions.into_iter().collect())
}

/// Split a selector string on top-level commas, treating commas inside
/// `( ... )` as value-list separators rather than requirement separators.
/// Errors on unbalanced parentheses.
fn split_top_level_requirements(input: &str) -> Result<Vec<&str>, Status> {
    let mut parts: Vec<&str> = Vec::new();
    let mut depth: i32 = 0;
    let mut start = 0usize;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err(Status::invalid_argument(
                        "invalid label_selector: unbalanced parentheses",
                    ));
                }
            }
            ',' if depth == 0 => {
                parts.push(&input[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(Status::invalid_argument(
            "invalid label_selector: unbalanced parentheses",
        ));
    }
    parts.push(&input[start..]);
    Ok(parts)
}

/// Parse one trimmed requirement into an [`Expression`].
fn parse_requirement(req: &str) -> Result<Expression, Status> {
    if req.is_empty() {
        return Err(Status::invalid_argument(
            "invalid label_selector: empty requirement",
        ));
    }

    // Set-based: `key in (...)` / `key notin (...)`. Detected by a top-level
    // `(` after an `in` / `notin` keyword token.
    if let Some(open) = req.find('(') {
        let head = req[..open].trim();
        let close = req.rfind(')').ok_or_else(|| {
            Status::invalid_argument("invalid label_selector: unbalanced parentheses")
        })?;
        if close < open {
            return Err(Status::invalid_argument(
                "invalid label_selector: unbalanced parentheses",
            ));
        }
        let values_str = &req[open + 1..close];
        // Split the head into `key` and the `in` / `notin` operator.
        let (key, op) = split_set_head(head)?;
        let values = parse_value_set(values_str)?;
        return match op {
            SetOp::In => Ok(Expression::In(key, values)),
            SetOp::NotIn => Ok(Expression::NotIn(key, values)),
        };
    }

    // Equality / inequality. Order matters: check the two-char operators
    // (`==`, `!=`) before the single-char `=`.
    if let Some((k, v)) = req.split_once("==") {
        return Ok(Expression::Equal(parse_key(k)?, parse_value(v)?));
    }
    if let Some((k, v)) = req.split_once("!=") {
        return Ok(Expression::NotEqual(parse_key(k)?, parse_value(v)?));
    }
    if let Some((k, v)) = req.split_once('=') {
        return Ok(Expression::Equal(parse_key(k)?, parse_value(v)?));
    }

    // Existence: `!key` (does-not-exist) or bare `key` (exists).
    if let Some(rest) = req.strip_prefix('!') {
        return Ok(Expression::DoesNotExist(parse_key(rest)?));
    }
    Ok(Expression::Exists(parse_key(req)?))
}

/// The two set-based operators.
enum SetOp {
    In,
    NotIn,
}

/// Split a set-requirement head (`"key in"` / `"key notin"`) into its key and
/// operator.  The operator is the final whitespace-delimited token.
fn split_set_head(head: &str) -> Result<(String, SetOp), Status> {
    let (key_part, op_token) = head.rsplit_once(char::is_whitespace).ok_or_else(|| {
        Status::invalid_argument("invalid label_selector: missing in/notin operator")
    })?;
    let op = match op_token.trim() {
        "in" => SetOp::In,
        "notin" => SetOp::NotIn,
        other => {
            return Err(Status::invalid_argument(format!(
                "invalid label_selector: unknown set operator '{other}'"
            )));
        }
    };
    Ok((parse_key(key_part)?, op))
}

/// Parse a comma-separated value list inside `( ... )` into a `BTreeSet`.
/// (kube 0.98 `Expression::In` / `NotIn` carry `BTreeSet<String>`.)
fn parse_value_set(values_str: &str) -> Result<BTreeSet<String>, Status> {
    let mut set = BTreeSet::new();
    for v in values_str.split(',') {
        let v = v.trim();
        if v.is_empty() {
            return Err(Status::invalid_argument(
                "invalid label_selector: empty value in set",
            ));
        }
        set.insert(v.to_owned());
    }
    if set.is_empty() {
        return Err(Status::invalid_argument(
            "invalid label_selector: empty value set",
        ));
    }
    Ok(set)
}

/// Trim and validate a label key — must be non-empty after trimming.
fn parse_key(key: &str) -> Result<String, Status> {
    let key = key.trim();
    if key.is_empty() {
        return Err(Status::invalid_argument(
            "invalid label_selector: empty key",
        ));
    }
    Ok(key.to_owned())
}

/// Trim and validate an equality value — must be non-empty after trimming.
fn parse_value(value: &str) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Err(Status::invalid_argument(
            "invalid label_selector: empty value",
        ));
    }
    Ok(value.to_owned())
}

/// Pure predicate combining the `Subscribe` request's `names` and
/// `label_selector` constraints, ANDed.  An event is delivered iff it passes
/// BOTH: its config name is in `names` (or `names` is unconstrained) AND its
/// labels match `selector` (an empty `Selector` matches everything).
///
/// Constructed once per `Subscribe` RPC and shared (`Arc`) across the
/// resume/snapshot/live paths, so the parse + `HashSet` build happen once, not
/// per event.
#[derive(Debug)]
pub(crate) struct SubscribeFilter {
    /// `None` ⇒ no name constraint (empty `names` request — historical
    /// behaviour).  `Some(set)` ⇒ event passes only if its config name is in
    /// the set.
    names: Option<HashSet<String>>,
    /// An empty `Selector` matches every label map.
    selector: Selector,
}

impl SubscribeFilter {
    /// Build a filter from a `Subscribe` request's `names` + `label_selector`.
    ///
    /// An empty `names` slice ⇒ no name constraint.  A malformed
    /// `label_selector` propagates as `INVALID_ARGUMENT` so the RPC fails fast
    /// before any stream is spawned.
    pub(crate) fn new(names: &[String], label_selector: &str) -> Result<Self, Status> {
        let names = if names.is_empty() {
            None
        } else {
            Some(names.iter().cloned().collect())
        };
        let selector = parse_label_selector(label_selector)?;
        Ok(Self { names, selector })
    }

    /// Returns true iff the event for `config_name` with `labels` passes BOTH
    /// the name and label-selector constraints.
    pub(crate) fn allow(&self, config_name: &str, labels: &BTreeMap<String, String>) -> bool {
        let name_ok = match &self.names {
            None => true,
            Some(set) => set.contains(config_name),
        };
        name_ok && self.selector.matches(labels)
    }
}

/// Extract the config name from a `ConfigEvent` for filtering.  Returns `None`
/// when the event carries no inner `Config` (defensive — every emitted event
/// sets `config: Some(..)`, but a `None` is treated as non-matching by the
/// caller so a malformed event is filtered out rather than leaked).
fn config_event_name(event: &ConfigEvent) -> Option<&str> {
    event.config.as_ref().map(|c| c.name.as_str())
}

/// Apply the [`SubscribeFilter`] to a `ConfigEvent` + its `labels`.  An event
/// with no inner `Config` name is treated as non-matching (filtered out)
/// rather than leaked.  Shared by the replay, post-snapshot, and live paths so
/// the name-extraction + AND logic lives in one unit-tested place.
pub(crate) fn filter_allows_event(
    filter: &SubscribeFilter,
    event: &ConfigEvent,
    labels: &BTreeMap<String, String>,
) -> bool {
    match config_event_name(event) {
        Some(name) => filter.allow(name, labels),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;
    use crate::grpc::subscribe::test_support::*;

    #[test]
    fn parse_label_selector_empty_matches_everything() {
        for input in ["", "   ", "\t\n"] {
            let sel = parse_label_selector(input).expect("empty selector is valid");
            assert!(
                sel.selects_all(),
                "empty/whitespace selector {input:?} must match everything"
            );
            assert!(sel.matches(&labels(&[("any", "thing")])));
            assert!(sel.matches(&BTreeMap::new()));
        }
    }

    #[test]
    fn parse_label_selector_equality_operators() {
        // `=` and `==` are both equality.
        for input in ["tier=critical", "tier == critical", "  tier  =  critical  "] {
            let sel = parse_label_selector(input).expect("valid equality selector");
            assert!(sel.matches(&labels(&[("tier", "critical")])), "{input:?}");
            assert!(!sel.matches(&labels(&[("tier", "normal")])), "{input:?}");
            assert!(!sel.matches(&BTreeMap::new()), "{input:?} on unlabeled");
        }
    }

    #[test]
    fn parse_label_selector_inequality_operator() {
        let sel = parse_label_selector("tier != normal").expect("valid != selector");
        // NotEqual matches when the key is absent OR holds a different value.
        assert!(sel.matches(&labels(&[("tier", "critical")])));
        assert!(sel.matches(&BTreeMap::new()));
        assert!(!sel.matches(&labels(&[("tier", "normal")])));
    }

    #[test]
    fn parse_label_selector_exists_and_does_not_exist() {
        let exists = parse_label_selector("tier").expect("bare key = Exists");
        assert!(exists.matches(&labels(&[("tier", "anything")])));
        assert!(!exists.matches(&BTreeMap::new()));

        let absent = parse_label_selector("!tier").expect("!key = DoesNotExist");
        assert!(absent.matches(&BTreeMap::new()));
        assert!(absent.matches(&labels(&[("other", "x")])));
        assert!(!absent.matches(&labels(&[("tier", "x")])));
    }

    #[test]
    fn parse_label_selector_set_based_in_and_notin() {
        let in_sel = parse_label_selector("env in (prod, staging)").expect("valid `in` selector");
        assert!(in_sel.matches(&labels(&[("env", "prod")])));
        assert!(in_sel.matches(&labels(&[("env", "staging")])));
        assert!(!in_sel.matches(&labels(&[("env", "dev")])));
        assert!(!in_sel.matches(&BTreeMap::new()), "absent key fails `in`");

        let notin_sel =
            parse_label_selector("env notin (dev, test)").expect("valid `notin` selector");
        assert!(notin_sel.matches(&labels(&[("env", "prod")])));
        assert!(
            notin_sel.matches(&BTreeMap::new()),
            "absent key passes `notin`"
        );
        assert!(!notin_sel.matches(&labels(&[("env", "dev")])));
    }

    #[test]
    fn parse_label_selector_multi_requirement_anded() {
        // Comma at top level = AND; comma inside parens = value-list separator.
        let sel = parse_label_selector("tier=critical, env in (prod,staging)")
            .expect("valid multi-requirement selector");
        assert!(sel.matches(&labels(&[("tier", "critical"), ("env", "prod")])));
        // Fails when one of the two ANDed requirements is unmet.
        assert!(!sel.matches(&labels(&[("tier", "critical"), ("env", "dev")])));
        assert!(!sel.matches(&labels(&[("tier", "normal"), ("env", "prod")])));
    }

    #[test]
    fn parse_label_selector_malformed_returns_invalid_argument() {
        let cases = [
            "tier in (prod",  // unbalanced paren
            "=value",         // empty key
            "tier=",          // missing value
            "tier in ()",     // empty value set
            "tier in (a,,b)", // empty value in set
            ",tier=critical", // empty requirement
            "tier foo (a)",   // unknown set operator
        ];
        for input in cases {
            let err =
                parse_label_selector(input).expect_err(&format!("{input:?} must be rejected"));
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "{input:?} must map to INVALID_ARGUMENT (got {err:?})"
            );
        }
    }

    // ── SubscribeFilter::allow: names × selector matrix ──────────────────────

    #[test]
    fn subscribe_filter_empty_allows_all() {
        let f = SubscribeFilter::new(&[], "").expect("empty filter is valid");
        assert!(f.allow("anything", &BTreeMap::new()));
        assert!(f.allow("cfg-a", &labels(&[("tier", "x")])));
    }

    #[test]
    fn subscribe_filter_names_only() {
        let names = vec!["cfg-a".to_string(), "cfg-b".to_string()];
        let f = SubscribeFilter::new(&names, "").expect("valid names filter");
        assert!(f.allow("cfg-a", &BTreeMap::new()));
        assert!(f.allow("cfg-b", &labels(&[("any", "label")])));
        assert!(!f.allow("cfg-c", &BTreeMap::new()), "name not in set");
    }

    #[test]
    fn subscribe_filter_selector_only() {
        let f = SubscribeFilter::new(&[], "tier=critical").expect("valid selector filter");
        // Any name passes; only the label constraint applies.
        assert!(f.allow("cfg-a", &labels(&[("tier", "critical")])));
        assert!(f.allow("cfg-z", &labels(&[("tier", "critical"), ("x", "y")])));
        assert!(!f.allow("cfg-a", &labels(&[("tier", "normal")])));
    }

    #[test]
    fn subscribe_filter_names_and_selector_anded() {
        let names = vec!["cfg-a".to_string()];
        let f = SubscribeFilter::new(&names, "tier=critical").expect("valid AND filter");
        // Passes both.
        assert!(f.allow("cfg-a", &labels(&[("tier", "critical")])));
        // Right name, wrong labels.
        assert!(!f.allow("cfg-a", &labels(&[("tier", "normal")])));
        // Right labels, wrong name.
        assert!(!f.allow("cfg-b", &labels(&[("tier", "critical")])));
    }

    #[test]
    fn subscribe_filter_invalid_selector_propagates() {
        let err = SubscribeFilter::new(&[], "tier in (oops")
            .expect_err("malformed selector must propagate");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ── Ticket acceptance: tier=critical selects only the critical config ────

    #[test]
    fn filter_tier_critical_selects_only_labeled_config() {
        let f = SubscribeFilter::new(&[], "tier=critical").expect("valid selector");
        // The critical config is delivered.
        assert!(f.allow("cfg-critical", &labels(&[("tier", "critical")])));
        // A normal-tier config is rejected.
        assert!(!f.allow("cfg-normal", &labels(&[("tier", "normal")])));
        // An unlabeled config is rejected.
        assert!(!f.allow("cfg-bare", &BTreeMap::new()));
    }

    // ── config_event_name / filter_allows_event ──────────────────────────────

    #[test]
    fn config_event_name_extracts_inner_config_name() {
        let ev = make_event("1", 1); // make_event sets config.name = "cfg"
        assert_eq!(config_event_name(&ev), Some("cfg"));

        let nameless = ConfigEvent {
            event_type: EventType::Modified as i32,
            config: None,
        };
        assert_eq!(config_event_name(&nameless), None);
    }

    #[test]
    fn filter_allows_event_treats_missing_config_as_non_matching() {
        let f = allow_all_filter();
        let nameless = ConfigEvent {
            event_type: EventType::Modified as i32,
            config: None,
        };
        // Even an allow-all filter rejects an event with no inner Config —
        // we never leak an unidentifiable event.
        assert!(!filter_allows_event(&f, &nameless, &BTreeMap::new()));
    }
}
