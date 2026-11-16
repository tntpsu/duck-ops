#!/usr/bin/env python3
"""
Tracked-file secret scanner for the duck stack.

Closes the 2026-05-26 coverage-matrix gap #5 (automated secret-leak
detection). Zero install — pure stdlib + git CLI. Scans only
tracked files (`git ls-files`) so `.env` and other gitignored content
is automatically excluded.

Usage:
    python scripts/secret_scan.py                # scan current repo
    python scripts/secret_scan.py path/to/repo   # scan another repo
    python scripts/secret_scan.py --all          # all 4 duck repos

Returns exit 0 when clean, 1 when any pattern matches. Findings are
reported as `file:line:pattern_name (excerpt redacted)` — the matched
secret is NEVER printed in full so the scan output is safe to share.

Designed to be invoked manually before commits and during periodic
security reviews. Does NOT install as a pre-commit hook (per user
CLAUDE.md: "no pre-commit hooks beyond what's in scripts/").

Upgrade path: if we hit false-positives or need git-history scanning,
swap in gitleaks (brew install gitleaks; gitleaks detect --source .).
"""
from __future__ import annotations

import argparse
import math
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

# Patterns curated from gitleaks' default ruleset. Each is conservative
# enough to keep false-positive rate low. If you add a pattern, prefer
# specificity (exact prefixes / length bounds) over greediness.
PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bsk-ant-[a-zA-Z0-9_\-]{20,}"), "Anthropic API key"),
    (re.compile(r"\bsk-proj-[a-zA-Z0-9_\-]{20,}"), "OpenAI project key"),
    (re.compile(r"\bsk-[a-zA-Z0-9]{40,}"), "OpenAI API key (generic)"),
    (re.compile(r"\bAIza[0-9A-Za-z_\-]{35}"), "Google API key"),
    (re.compile(r"\bghp_[a-zA-Z0-9]{36}"), "GitHub PAT (classic)"),
    (re.compile(r"\bghs_[a-zA-Z0-9]{36}"), "GitHub PAT (server)"),
    (re.compile(r"\bgho_[a-zA-Z0-9]{36}"), "GitHub OAuth token"),
    (re.compile(r"\bgithub_pat_[a-zA-Z0-9_]{60,}"), "GitHub fine-grained PAT"),
    (re.compile(r"\bAKIA[0-9A-Z]{16}"), "AWS access key"),
    (re.compile(r"\bEAA[A-Za-z0-9]{100,}"), "Meta long-lived token"),
    (re.compile(r"\bshpat_[a-f0-9]{32}"), "Shopify access token"),
    (re.compile(r"\bshppa_[a-f0-9]{32}"), "Shopify private app token"),
    (re.compile(r"\bshpss_[a-f0-9]{32}"), "Shopify shared secret"),
    (re.compile(r"\bxox[bpaorus]-[0-9]+-[0-9]+-[a-zA-Z0-9]+"), "Slack token"),
    (re.compile(r"\bSG\.[a-zA-Z0-9_\-]{20,}\.[a-zA-Z0-9_\-]{20,}"), "SendGrid API key"),
    (re.compile(r"\bAC[a-f0-9]{32}\b"), "Twilio Account SID"),
    (re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP |ENCRYPTED )?PRIVATE KEY-----"), "Private key block"),
    # Generic Bearer / Authorization — higher false-positive rate so
    # we require a real-looking token shape after the keyword.
    (re.compile(r"(?i)\b(?:authorization|bearer)[:=\s]+['\"]?[a-zA-Z0-9_\-]{30,}['\"]?"), "Bearer/Authorization header (verify)"),
]

# Files we deliberately skip even when tracked — typically binary
# blobs, generated artifacts, vendored dependencies, or test fixtures
# that legitimately reference key SHAPES.
SKIP_PATH_FRAGMENTS = (
    "node_modules/",
    ".venv/",
    "__pycache__/",
    "/dist/",
    "/build/",
    ".min.js",
    ".min.css",
    "secret_scan.py",  # don't match our own pattern strings
    "test_secret_scan",
)

# File extensions worth scanning (text content). Default binary skip.
TEXT_EXTENSIONS = {
    ".py", ".js", ".jsx", ".ts", ".tsx", ".mjs", ".cjs",
    ".json", ".yaml", ".yml", ".toml", ".ini", ".conf",
    ".md", ".markdown", ".rst", ".txt",
    ".sh", ".zsh", ".bash", ".fish",
    ".env",  # in case any sample.env is tracked
    ".html", ".css", ".scss",
    ".plist", ".xml",
    ".sql",
}


def _git_ls_files(repo: Path) -> list[Path]:
    """Return absolute paths to every tracked file in repo."""
    result = subprocess.run(
        ["git", "-C", str(repo), "ls-files"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    return [repo / line for line in result.stdout.splitlines() if line.strip()]


# Basenames that are config text whatever follows the first dot. `.env` is in
# TEXT_EXTENSIONS, which only ever matched a file whose SUFFIX is `.env`;
# `.env.clean` has suffix `.clean`, so tracked `.env.*` copies of a live
# environment file were skipped by this function and the scan reported clean on
# them for nine months, until 2026-11-13. A derived name is the normal way such
# a file gets made, so match on the basename.
SCAN_BASENAME_PREFIXES = (".env", "env.")


def _is_scannable(path: Path) -> bool:
    """Decide if a tracked file is worth running regex against."""
    s = str(path)
    if any(frag in s for frag in SKIP_PATH_FRAGMENTS):
        return False
    if path.name.lower().startswith(SCAN_BASENAME_PREFIXES):
        return True
    if path.suffix.lower() in TEXT_EXTENSIONS:
        return True
    # No extension — try anyway (might be a script or dotfile)
    return path.suffix == ""


# A credential with no distinctive prefix is invisible to PATTERNS. In the
# 2026-11-13 finding only two of the five credentials present carried a
# recognisable prefix; the other three matched nothing, so the scan reported a
# count well short of the real exposure.
#
# Entropy alone cannot close that, and measuring it said so loudly: in the real
# file a genuine 16-character password scores H=3.88 while a harmless prose
# prompt value scores 4.09 and 25 template-id strings outrank it. A bare
# H>=3.5 rule would have flagged ~25 non-secrets and been muted within a week.
#
# The NAME is the discriminator. Of the 97 assignments in that file exactly 6
# are keyed on a secret word, and all 6 are credentials; a tracked
# `.env.example`, whose values are placeholders, has none at all. So entropy is
# only a tiebreaker
# here, and the rule is scoped to env-shaped files because `TOKEN =
# os.getenv(...)` and `{"api_key_env": "OPENAI_API_KEY"}` are everywhere in
# .py/.json and carry no secret.
SECRET_KEY_NAME = re.compile(
    r"(?i)(?:^|_)(KEY|SECRET|TOKEN|PASS|PASSWORD|CREDENTIAL|CREDENTIALS|AUTH)\s*$")

#: Below this length a value cannot hold meaningful key material; the shortest
#: real credential in the known leak is SMTP_PASS at exactly 16.
MIN_SECRET_VALUE_LEN = 16

#: Shannon entropy floor. Deliberately low (3.0) because the NAME has already
#: done the filtering -- this only rejects a repetitive or obviously structured
#: value that slipped through on a secret-sounding key.
MIN_SECRET_VALUE_ENTROPY = 3.0

#: Values that are shaped like a credential but are not one. A placeholder is
#: the normal content of a tracked `.env.example`, which must stay quiet or
#: the rule gets switched off.
_PLACEHOLDER_MARKERS = (
    "your_", "your-", "yourkey", "changeme", "change_me", "change-me",
    "example", "placeholder", "redacted", "dummy", "sample", "insert",
    "todo", "fixme", "xxxx", "<", "replace", "notreal", "fake",
)


def _shannon_entropy(value: str) -> float:
    """Bits per character. Used only as a tiebreaker -- see SECRET_KEY_NAME."""
    if not value:
        return 0.0
    counts = Counter(value)
    length = len(value)
    return -sum((n / length) * math.log2(n / length) for n in counts.values())


def _looks_like_a_placeholder(value: str) -> bool:
    low = value.lower()
    if any(marker in low for marker in _PLACEHOLDER_MARKERS):
        return True
    # `${VAR}` / `$VAR` / `$(cmd)` is a reference to a secret, not a secret.
    if value.startswith("$"):
        return True
    # A single repeated character carries no key material.
    return len(set(value)) <= 2


def _is_env_shaped(path: Path) -> bool:
    """Is this a file whose lines are `NAME=value` environment assignments?"""
    name = path.name.lower()
    return name.startswith(SCAN_BASENAME_PREFIXES) or path.suffix.lower() == ".env"


def _secret_named_assignments(text: str) -> list[tuple[int, str, str]]:
    """Return `(lineno, key, value)` for every assignment whose KEY names a
    secret and whose VALUE is long and varied enough to be one."""
    hits: list[tuple[int, str, str]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        if stripped.lower().startswith("export "):
            stripped = stripped[len("export "):].lstrip()
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not SECRET_KEY_NAME.search(key):
            continue
        if len(value) < MIN_SECRET_VALUE_LEN:
            continue
        if _looks_like_a_placeholder(value):
            continue
        if _shannon_entropy(value) < MIN_SECRET_VALUE_ENTROPY:
            continue
        hits.append((lineno, key, value))
    return hits


def _redact(snippet: str) -> str:
    """Replace the middle of a matched secret with a placeholder so the
    scan output is safe to paste / log without re-leaking the value."""
    if len(snippet) <= 12:
        return "***redacted***"
    return f"{snippet[:4]}…{snippet[-4:]} ({len(snippet)} chars total)"


def scan_repo(repo: Path) -> list[dict]:
    """Run every pattern against every scannable tracked file in repo.
    Returns a list of finding dicts; empty list = clean."""
    findings: list[dict] = []
    repo = repo.resolve()
    for path in _git_ls_files(repo):
        if not path.exists() or path.is_dir():
            continue
        if not _is_scannable(path):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        matched_lines: set[int] = set()
        for lineno, line in enumerate(text.splitlines(), start=1):
            for pattern, name in PATTERNS:
                match = pattern.search(line)
                if not match:
                    continue
                matched_lines.add(lineno)
                findings.append({
                    "repo": str(repo),
                    "path": str(path.relative_to(repo)),
                    "line": lineno,
                    "pattern": name,
                    "redacted_excerpt": _redact(match.group(0)),
                })
        if _is_env_shaped(path):
            for lineno, key, value in _secret_named_assignments(text):
                # A prefix match on the same line is the more specific and more
                # actionable finding ("this is a Shopify token"), so the generic
                # rule yields to it rather than reporting the line twice.
                if lineno in matched_lines:
                    continue
                findings.append({
                    "repo": str(repo),
                    "path": str(path.relative_to(repo)),
                    "line": lineno,
                    "pattern": f"Secret-named key with a high-entropy value ({key})",
                    "redacted_excerpt": _redact(value),
                })
    return findings


# Default --all scans the three repos the operator owns. openclaw is
# a third-party engine (github.com/openclaw/openclaw) with intentional
# test fixtures that contain mock-token shapes; scanning it produces
# 27 known false-positives. Use --include-openclaw if you really want
# to scan it (e.g., before contributing upstream).
_OWNED_REPOS = (
    "/Users/philtullai/ai-agents/duck-ops",
    "/Users/philtullai/ai-agents/duckAgent",
    "/Users/philtullai/ai-agents/paint-to-print-3d",
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scan tracked files for committed secrets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("repos", nargs="*",
                        help="Repo paths to scan. Default: current dir.")
    parser.add_argument("--all", action="store_true",
                        help="Scan the three operator-owned duck repos (duck-ops, duckAgent, paint-to-print-3d).")
    parser.add_argument("--include-openclaw", action="store_true",
                        help="Also scan openclaw (expect known test-fixture false-positives).")
    args = parser.parse_args(argv)

    if args.all:
        repos = [Path(p) for p in _OWNED_REPOS]
        if args.include_openclaw:
            repos.append(Path("/Users/philtullai/ai-agents/openclaw"))
    elif args.repos:
        repos = [Path(p) for p in args.repos]
    else:
        repos = [Path.cwd()]

    all_findings: list[dict] = []
    for repo in repos:
        if not (repo / ".git").exists():
            print(f"SKIP {repo}: not a git repo", file=sys.stderr)
            continue
        findings = scan_repo(repo)
        if findings:
            print(f"\n=== {repo} — {len(findings)} potential finding(s) ===")
            for f in findings:
                print(f"  {f['path']}:{f['line']}  [{f['pattern']}]  {f['redacted_excerpt']}")
        else:
            file_count = len(_git_ls_files(repo))
            print(f"=== {repo} — clean ({file_count} tracked files scanned)")
        all_findings.extend(findings)

    print(f"\nTOTAL findings: {len(all_findings)}")
    return 1 if all_findings else 0


if __name__ == "__main__":
    sys.exit(main())
