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
import re
import subprocess
import sys
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


def _is_scannable(path: Path) -> bool:
    """Decide if a tracked file is worth running regex against."""
    s = str(path)
    if any(frag in s for frag in SKIP_PATH_FRAGMENTS):
        return False
    if path.suffix.lower() in TEXT_EXTENSIONS:
        return True
    # No extension — try anyway (might be a script or dotfile)
    return path.suffix == ""


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
        for lineno, line in enumerate(text.splitlines(), start=1):
            for pattern, name in PATTERNS:
                match = pattern.search(line)
                if not match:
                    continue
                findings.append({
                    "repo": str(repo),
                    "path": str(path.relative_to(repo)),
                    "line": lineno,
                    "pattern": name,
                    "redacted_excerpt": _redact(match.group(0)),
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
