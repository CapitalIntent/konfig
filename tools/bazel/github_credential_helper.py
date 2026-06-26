#!/usr/bin/env python3
"""Bazel credential helper for fetching private GitHub release assets.

Wired in .bazelrc via:
    common --credential_helper=api.github.com=%workspace%/tools/bazel/github_credential_helper.py

The CapitalIntent repo is private, so the public releases/download/ URL 404s for
unauthenticated GETs. The cross-compile sysroot http_archive entries in
MODULE.bazel therefore point at the GitHub *API asset* endpoint
(https://api.github.com/repos/.../releases/assets/<id>). That endpoint needs two
headers Bazel's built-in netrc/auth_patterns cannot both set:

  * Authorization: Bearer <token>          (auth)
  * Accept: application/octet-stream        (return the binary, not JSON metadata)

This helper supplies both. It follows the EngFlow/Bazel credential-helper
protocol: read {"uri": "..."} on stdin for the `get` command, write
{"headers": {"Header": ["value"], ...}} on stdout.

Token resolution (first hit wins), so it works in CI and on a dev box with no
extra setup:
  1. $GH_TOKEN / $GITHUB_TOKEN   (GitHub Actions sets GITHUB_TOKEN)
  2. `gh auth token`            (local dev with the gh CLI logged in)

On any other host than api.github.com it returns an empty header set, so Bazel
falls back to its default credential resolution for all other downloads.
"""

import json
import os
import shutil
import subprocess
import sys
from urllib.parse import urlparse

_GITHUB_API_HOST = "api.github.com"


def _token() -> str:
    for var in ("GH_TOKEN", "GITHUB_TOKEN"):
        tok = os.environ.get(var)
        if tok:
            return tok.strip()
    gh = shutil.which("gh")
    if gh:
        try:
            out = subprocess.run(
                [gh, "auth", "token"],
                capture_output=True, text=True, check=True,
            )
            return out.stdout.strip()
        except subprocess.CalledProcessError:
            pass
    return ""


def _get(uri: str) -> dict:
    host = (urlparse(uri).hostname or "").lower()
    if host != _GITHUB_API_HOST:
        # Not ours — let Bazel's default resolution handle it.
        return {"headers": {}}
    tok = _token()
    if not tok:
        sys.stderr.write(
            "github_credential_helper: no GitHub token found "
            "(set GH_TOKEN/GITHUB_TOKEN or run `gh auth login`).\n"
        )
        # Still return Accept so a public asset (if any) can be fetched.
        return {"headers": {"Accept": ["application/octet-stream"]}}
    return {
        "headers": {
            "Authorization": ["Bearer " + tok],
            "Accept": ["application/octet-stream"],
        }
    }


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] != "get":
        sys.stderr.write("usage: github_credential_helper.py get  (reads JSON on stdin)\n")
        return 2
    try:
        req = json.load(sys.stdin)
    except json.JSONDecodeError:
        req = {}
    json.dump(_get(req.get("uri", "")), sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
