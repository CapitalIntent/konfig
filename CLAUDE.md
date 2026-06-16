# konfig

Rust config-management service. Bazel-built monorepo; snmalloc allocator; deployed on Kubernetes.

## Build & test

- Tests: `bazel test //rust/konfig:test` — never `cargo test` directly (see `feedback_bazel_only_tests.md` in user memory; disk + cache reasons).
- Pre-PR: `cargo fmt`, `cargo clippy`, `cargo-crap` (see `feedback_pre_pr_checks.md`, `feedback_cargo_crap_before_pr.md`).
- Loadtest: scale Deployment to 1 replica first for accurate per-pod profiling (`feedback_loadtest_replicas.md`).

## Conventions

- **Branch workflow**: branch → PR → CI → Codex review → merge. Never push to main (`feedback_branch_workflow.md`).
- **Commit prefix**: `CU-<task-id>:` short description. No Claude/Co-Authored-By footers (`feedback_commit_messages.md`).
- **PR body**: caveman-TLDR format — What / Why / Cost / Evidence (`feedback_pr_description_format.md`).
- **Allocator**: snmalloc everywhere via `jayakasadev/snmalloc` Bazel dep (`feedback_rust_allocator.md`).
- **File access**: Bazel runfiles for repo file access — never `include_str!` or hardcoded relative paths (`feedback_bazel_runfiles.md`).

## Cleanup after a body of work

Bazel creates a separate **output_base** per unique workspace path. Every agent worktree under `.claude/worktrees/agent-*` spawns its own. Output_bases never auto-prune — they accumulate at ~1–3 GB each in `~/Library/Caches/bazel/_bazel_jayakasa/<hash>/`. Left unchecked they fill the disk (seen 170+ GB across 60 stale bases).

When a body of work completes (PRs merged, worktrees pruned per `feedback_worktree_cleanup.md`), reclaim disk:

**Surgical (keep active output_base + repo cache + install — recommended):**

```sh
# Active output_base hash for the current checkout
active=$(readlink bazel-bin | sed -E 's#.*/_bazel_[^/]+/([^/]+)/.*#\1#')

# Stop all bazel servers (worktrees own their own server processes)
pkill -9 -f bazel

# Drop every output_base except the active one. Keep install/ and cache/.
for d in ~/Library/Caches/bazel/_bazel_jayakasa/*/; do
  base=$(basename "$d")
  case "$base" in "$active"|install|cache) continue;; esac
  chmod -R u+w "$d" 2>/dev/null
  rm -rf "$d"
done

# /private/var/tmp/_bazel_jayakasa is bazel's default output_user_root; safe to
# nuke whole — only here if a stray invocation skipped --output_user_root.
chmod -R u+w /private/var/tmp/_bazel_jayakasa 2>/dev/null
rm -rf /private/var/tmp/_bazel_jayakasa
```

**Full reset (when disk is critical):**

```sh
pkill -9 -f bazel
chmod -R u+w ~/Library/Caches/bazel/_bazel_jayakasa /private/var/tmp/_bazel_jayakasa 2>/dev/null
rm -rf ~/Library/Caches/bazel/_bazel_jayakasa /private/var/tmp/_bazel_jayakasa
```

Trade-off: next build cold-rebuilds (~30 min for full `//...`). Surgical cleanup preserves the active workspace's incremental cache and the shared `cache/` (~60 GB of fetched http_archives / crates), so next build is fast.

Note: `bazel clean` only wipes one output_base's `bazel-out/`; it does NOT touch other output_bases. Use the loop above for cross-workspace cleanup.
