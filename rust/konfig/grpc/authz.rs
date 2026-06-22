//! Per-tenant authorization (Phase 8 PR2, CU-86ahrwd6f).
//!
//! Binds the mTLS client identity ([`crate::grpc::identity::ClientIdentity`])
//! to a cluster-scoped `ConfigACL.konfig.io/v1` so a valid certificate no
//! longer grants blanket read/write to every `(namespace, name)`. The actual
//! ACL table + its kube watcher live in [`crate::acl`]; this module is the pure
//! decision layer the per-RPC guard calls.
//!
//! # Why a per-RPC guard, not a tonic interceptor
//!
//! A transport interceptor sees the raw `Request` before routing — it has no
//! clean view of the decoded `(namespace, name)` or of the per-server
//! [`AclTable`]. The guard runs inside each handler (after `check_drain`)
//! where both are in scope, mirroring how `check_drain` / `log_rpc_entry` are
//! invoked.
//!
//! # Modes
//!
//! Selected by env `KONFIG_AUTHZ_MODE` (see [`Mode::from_env`]):
//!   - `disabled` (DEFAULT): short-circuit before any ACL/identity work — zero
//!     overhead, every RPC allowed. This is the safe pre-rollout default.
//!   - `permissive`: evaluate the policy and emit a would-deny audit line on a
//!     miss, but ALLOW regardless. Used to observe the blast radius before
//!     enforcing.
//!   - `enforce`: deny (PERMISSION_DENIED) on no matching rule.
//!
//! # Fail-safe
//!
//! In `enforce`, if the ACL cache has not finished its initial sync yet the
//! guard returns `UNAVAILABLE` (never allow, never panic) so the boot window
//! cannot serve un-authorized. `disabled` never consults the sync flag.

use tonic::Status;
use tracing::warn;

use crate::acl::AclTable;
use crate::grpc::identity::ClientIdentity;

/// Env var selecting the authz mode. Unset / unknown ⇒ [`Mode::Disabled`].
pub const MODE_ENV: &str = "KONFIG_AUTHZ_MODE";

/// The operation an RPC performs, matched against a rule's `verbs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    /// Read-family RPCs (`get`, `get_all`, `subscribe`, and the secret reads).
    Read,
    /// Mutating RPCs (`apply`, `apply_secret`, `revert`).
    Write,
}

impl Verb {
    /// Lower-case wire token used in the CRD `verbs` list and in deny messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Verb::Read => "read",
            Verb::Write => "write",
        }
    }

    /// Parse a CRD `verbs` entry. Unknown tokens ⇒ `None` (ignored on load).
    pub fn parse(s: &str) -> Option<Verb> {
        match s.trim().to_ascii_lowercase().as_str() {
            "read" => Some(Verb::Read),
            "write" => Some(Verb::Write),
            _ => None,
        }
    }
}

/// Enforcement mode. Default is [`Mode::Disabled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Authz off — every RPC allowed, no ACL/identity work performed.
    #[default]
    Disabled,
    /// Evaluate + log would-deny, but allow regardless.
    Permissive,
    /// Deny on no matching rule.
    Enforce,
}

impl Mode {
    /// Resolve the mode from `KONFIG_AUTHZ_MODE`.
    ///
    /// `permissive` / `enforce` (case-insensitive) select those modes;
    /// everything else — unset, empty, `disabled`, or an unrecognised value —
    /// resolves to [`Mode::Disabled`], the fail-open-but-quiet default the
    /// rollout starts from.
    pub fn from_env() -> Mode {
        match std::env::var(MODE_ENV) {
            Ok(v) => Mode::parse(&v),
            Err(_) => Mode::Disabled,
        }
    }

    /// Pure parse of a mode string (extracted for unit tests so they need not
    /// mutate the process env).
    pub fn parse(s: &str) -> Mode {
        match s.trim().to_ascii_lowercase().as_str() {
            "permissive" => Mode::Permissive,
            "enforce" => Mode::Enforce,
            // "disabled", "", and any unknown token all fail closed to the
            // zero-overhead default.
            _ => Mode::Disabled,
        }
    }
}

/// Does `pattern` (a `"<namespace>/<name>"` glob) authorize `(namespace, name)`?
///
/// Matching rules:
///   - The pattern is split on the first `/` into a namespace segment and a
///     name segment. A pattern with no `/` never matches (treated as malformed
///     and ignored — callers should reject such patterns at load time).
///   - Each segment matches literally, except the single token `*`, which
///     matches any value for that whole segment. `*` is a segment wildcard, not
///     a substring glob: `def*` is a literal three-char-plus-star name, not a
///     prefix match.
///
/// Examples: `default/*` matches `default/anything` but not `prod/x`; `*/*`
/// matches everything; `default/web` matches only that exact pair.
pub fn pattern_matches(pattern: &str, namespace: &str, name: &str) -> bool {
    let Some((pat_ns, pat_name)) = pattern.split_once('/') else {
        return false;
    };
    segment_matches(pat_ns, namespace) && segment_matches(pat_name, name)
}

/// One `"<ns>/<name>"` segment match: `*` wildcards the whole segment,
/// otherwise an exact string compare.
fn segment_matches(pat: &str, value: &str) -> bool {
    pat == "*" || pat == value
}

/// Authorize `identity` for `verb` on `(namespace, name)` under `mode`.
///
/// Contract:
///   - [`Mode::Disabled`] ⇒ always `Ok(())`, evaluated first so the disabled
///     path performs no ACL/identity work.
///   - [`Mode::Enforce`] + `synced == false` ⇒ `Err(UNAVAILABLE)` (fail-safe
///     boot window — never serve un-authorized before the ACL cache syncs).
///   - Otherwise evaluate the policy: a grant requires a rule under
///     `identity.id` whose `verbs` contains `verb` AND one of whose `patterns`
///     matches `(namespace, name)`. An anonymous identity has no `id` mapping,
///     so it never grants.
///   - On a miss: [`Mode::Permissive`] logs a would-deny audit line and returns
///     `Ok(())`; [`Mode::Enforce`] returns `Err(PERMISSION_DENIED)`.
///
/// The PERMISSION_DENIED message is intentionally generic — it names only the
/// identity / verb / target the caller already supplied, never which ACLs do or
/// do not exist, so the error cannot be used to enumerate the policy set.
pub fn check(
    mode: Mode,
    table: &AclTable,
    synced: bool,
    identity: &ClientIdentity,
    verb: Verb,
    namespace: &str,
    name: &str,
) -> Result<(), Status> {
    // Disabled: zero-overhead short-circuit before touching the ACL table or
    // even reading the identity.
    if mode == Mode::Disabled {
        return Ok(());
    }

    // Fail-safe: in enforce, refuse to decide until the cache has synced.
    if mode == Mode::Enforce && !synced {
        return Err(Status::unavailable(
            "authorization cache not yet synced — retry shortly",
        ));
    }

    let granted = !identity.anonymous && table.grants(&identity.id, verb, namespace, name);

    if granted {
        return Ok(());
    }

    // Miss. Build the generic, non-enumerating message once.
    let denial = format!(
        "identity '{}' not authorized for {} {}/{}",
        identity.id,
        verb.as_str(),
        namespace,
        name
    );

    match mode {
        Mode::Permissive => {
            // Audit path: structured would-deny line so operators can size the
            // blast radius before flipping to enforce. ALLOW regardless.
            warn!(
                target: "konfig::authz::audit",
                identity = %identity.id,
                verb = verb.as_str(),
                namespace = %namespace,
                name = %name,
                mode = "permissive",
                "authz would-deny (permissive — allowing)"
            );
            Ok(())
        }
        Mode::Enforce => Err(Status::permission_denied(denial)),
        // Unreachable: Disabled returned above.
        Mode::Disabled => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::Rule;
    use crate::grpc::identity::ClientIdentity;

    // ── pattern_matches ─────────────────────────────────────────────────────

    #[test]
    fn pattern_namespace_wildcard_matches_any_name_in_ns() {
        assert!(pattern_matches("default/*", "default", "x"));
        assert!(pattern_matches("default/*", "default", "anything"));
    }

    #[test]
    fn pattern_namespace_wildcard_rejects_other_ns() {
        assert!(!pattern_matches("default/*", "prod", "x"));
    }

    #[test]
    fn pattern_full_wildcard_matches_everything() {
        assert!(pattern_matches("*/*", "default", "x"));
        assert!(pattern_matches("*/*", "prod", "y"));
        assert!(pattern_matches("*/*", "", ""));
    }

    #[test]
    fn pattern_exact_matches_only_exact() {
        assert!(pattern_matches("default/web", "default", "web"));
        assert!(!pattern_matches("default/web", "default", "api"));
        assert!(!pattern_matches("default/web", "prod", "web"));
    }

    #[test]
    fn pattern_name_wildcard_with_exact_ns() {
        assert!(pattern_matches("prod/*", "prod", "db"));
        assert!(!pattern_matches("prod/*", "staging", "db"));
    }

    #[test]
    fn pattern_without_slash_never_matches() {
        assert!(!pattern_matches("default", "default", "x"));
        assert!(!pattern_matches("*", "default", "x"));
    }

    #[test]
    fn pattern_wildcard_is_segment_not_substring() {
        // `def*` is a literal name, not a prefix glob.
        assert!(!pattern_matches("default/def*", "default", "default"));
        assert!(pattern_matches("default/def*", "default", "def*"));
    }

    // ── Mode::from_env / parse ──────────────────────────────────────────────

    #[test]
    fn mode_parse_known_values() {
        assert_eq!(Mode::parse("permissive"), Mode::Permissive);
        assert_eq!(Mode::parse("PERMISSIVE"), Mode::Permissive);
        assert_eq!(Mode::parse("enforce"), Mode::Enforce);
        assert_eq!(Mode::parse(" Enforce "), Mode::Enforce);
        assert_eq!(Mode::parse("disabled"), Mode::Disabled);
    }

    #[test]
    fn mode_parse_unknown_and_empty_default_disabled() {
        assert_eq!(Mode::parse(""), Mode::Disabled);
        assert_eq!(Mode::parse("nonsense"), Mode::Disabled);
        assert_eq!(Mode::parse("enabled"), Mode::Disabled);
        assert_eq!(Mode::default(), Mode::Disabled);
    }

    #[test]
    fn mode_from_env_unset_is_disabled() {
        // SAFETY: single-threaded test target (RUST_TEST_THREADS=1, see
        // BUILD.bazel) so no concurrent reader observes the transient unset.
        unsafe {
            std::env::remove_var(MODE_ENV);
        }
        assert_eq!(Mode::from_env(), Mode::Disabled);
    }

    #[test]
    fn mode_from_env_reads_enforce() {
        unsafe {
            std::env::set_var(MODE_ENV, "enforce");
        }
        assert_eq!(Mode::from_env(), Mode::Enforce);
        unsafe {
            std::env::remove_var(MODE_ENV);
        }
    }

    // ── check() fixtures ────────────────────────────────────────────────────

    fn named(id: &str) -> ClientIdentity {
        ClientIdentity {
            id: id.to_string(),
            anonymous: false,
        }
    }

    fn anon() -> ClientIdentity {
        ClientIdentity {
            id: "anonymous".to_string(),
            anonymous: true,
        }
    }

    /// Fixture: identity `svc-a` may read `default/*`; identity `svc-w` may
    /// write `prod/web`.
    fn fixture_table() -> AclTable {
        let table = AclTable::new();
        table.replace_for_test(
            [
                (
                    "svc-a".to_string(),
                    vec![Rule {
                        verbs: vec![Verb::Read],
                        patterns: vec!["default/*".to_string()],
                    }],
                ),
                (
                    "svc-w".to_string(),
                    vec![Rule {
                        verbs: vec![Verb::Write],
                        patterns: vec!["prod/web".to_string()],
                    }],
                ),
            ]
            .into_iter()
            .collect(),
        );
        table
    }

    #[test]
    fn disabled_always_ok_even_anonymous_and_unsynced() {
        let table = AclTable::new(); // empty + not synced
        assert!(
            check(
                Mode::Disabled,
                &table,
                false,
                &anon(),
                Verb::Write,
                "any",
                "thing"
            )
            .is_ok()
        );
        assert!(
            check(
                Mode::Disabled,
                &table,
                false,
                &named("nobody"),
                Verb::Read,
                "x",
                "y"
            )
            .is_ok()
        );
    }

    #[test]
    fn enforce_grants_when_rule_matches_verb_and_pattern() {
        let table = fixture_table();
        assert!(
            check(
                Mode::Enforce,
                &table,
                true,
                &named("svc-a"),
                Verb::Read,
                "default",
                "web"
            )
            .is_ok()
        );
    }

    #[test]
    fn enforce_denies_anonymous() {
        let table = fixture_table();
        let err = check(
            Mode::Enforce,
            &table,
            true,
            &anon(),
            Verb::Read,
            "default",
            "web",
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn enforce_denies_identity_with_no_rule() {
        let table = fixture_table();
        let err = check(
            Mode::Enforce,
            &table,
            true,
            &named("ghost"),
            Verb::Read,
            "default",
            "web",
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn enforce_denies_wrong_verb() {
        let table = fixture_table();
        // svc-a may read default/*, but not write it.
        let err = check(
            Mode::Enforce,
            &table,
            true,
            &named("svc-a"),
            Verb::Write,
            "default",
            "web",
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn enforce_denies_wrong_namespace() {
        let table = fixture_table();
        // svc-a may read default/*, but not prod/*.
        let err = check(
            Mode::Enforce,
            &table,
            true,
            &named("svc-a"),
            Verb::Read,
            "prod",
            "web",
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn enforce_unsynced_is_unavailable_not_denied() {
        let table = fixture_table();
        let err = check(
            Mode::Enforce,
            &table,
            false,
            &named("svc-a"),
            Verb::Read,
            "default",
            "web",
        )
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn permissive_allows_on_miss() {
        let table = fixture_table();
        // No rule for "ghost", but permissive allows.
        assert!(
            check(
                Mode::Permissive,
                &table,
                true,
                &named("ghost"),
                Verb::Read,
                "default",
                "web"
            )
            .is_ok()
        );
        // Even anonymous is allowed in permissive.
        assert!(
            check(
                Mode::Permissive,
                &table,
                true,
                &anon(),
                Verb::Write,
                "prod",
                "web"
            )
            .is_ok()
        );
    }

    #[test]
    fn permissive_unsynced_still_allows() {
        let table = AclTable::new(); // not synced
        assert!(
            check(
                Mode::Permissive,
                &table,
                false,
                &named("ghost"),
                Verb::Read,
                "default",
                "web"
            )
            .is_ok()
        );
    }

    /// AC equivalent (no live-cluster harness exists — `rust/konfig/tests/` is
    /// absent, see PR report): cert A carries identity `svc-a` with
    /// `read:[default/*]`. It reads anything in `default`, and is denied for
    /// any other namespace or for write. This is the 2-cert acceptance test
    /// reduced to the decision layer.
    #[test]
    fn ac_cert_a_reads_default_denied_elsewhere() {
        let table = fixture_table();
        let cert_a = named("svc-a");

        // Reads default/<anything> → allowed.
        assert!(
            check(
                Mode::Enforce,
                &table,
                true,
                &cert_a,
                Verb::Read,
                "default",
                "alpha"
            )
            .is_ok()
        );
        assert!(
            check(
                Mode::Enforce,
                &table,
                true,
                &cert_a,
                Verb::Read,
                "default",
                "beta"
            )
            .is_ok()
        );

        // Reads another namespace → denied.
        assert_eq!(
            check(
                Mode::Enforce,
                &table,
                true,
                &cert_a,
                Verb::Read,
                "kube-system",
                "x"
            )
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );

        // Writes default → denied (read-only grant).
        assert_eq!(
            check(
                Mode::Enforce,
                &table,
                true,
                &cert_a,
                Verb::Write,
                "default",
                "alpha"
            )
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );

        // A different cert (cert B = svc-w, write prod/web) is denied reading
        // default — its grant does not cover cert A's namespace.
        let cert_b = named("svc-w");
        assert_eq!(
            check(
                Mode::Enforce,
                &table,
                true,
                &cert_b,
                Verb::Read,
                "default",
                "alpha"
            )
            .unwrap_err()
            .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn deny_message_does_not_leak_acl_contents() {
        let table = fixture_table();
        let err = check(
            Mode::Enforce,
            &table,
            true,
            &named("ghost"),
            Verb::Read,
            "default",
            "web",
        )
        .unwrap_err();
        let msg = err.message();
        // Mentions only what the caller supplied.
        assert!(msg.contains("ghost"));
        assert!(msg.contains("read"));
        assert!(msg.contains("default/web"));
        // Must NOT name any other identity or pattern from the table.
        assert!(!msg.contains("svc-a"));
        assert!(!msg.contains("svc-w"));
        assert!(!msg.contains("prod"));
    }
}
