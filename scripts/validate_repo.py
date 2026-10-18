"""Offline checks for the public portfolio repository.

The script intentionally avoids third-party imports so it can run in a fresh
GitHub Actions runner before Snowflake credentials are configured.
"""

from __future__ import annotations

import ast
from pathlib import Path
import re
import sys
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = (
    "README.md",
    "LICENSE",
    ".gitignore",
    ".env.example",
    "requirements.txt",
    "sql/01_setup_and_ingestion.sql",
    "sql/02_medallion_pipeline.sql",
    "sql/03_data_quality_and_profiling.sql",
    "sql/04_udfs.sql",
    "snowpark/rfm_and_sales_trends.py",
    "snowpark/worksheet_handler.py",
    "docs/architecture.png",
    "docs/architecture_detailed.png",
    "docs/case_study.md",
    "docs/execution_evidence.md",
    "docs/incremental_test.md",
    "assets/dashboard_executive_overview.png",
    "assets/dashboard_customer_rfm_analysis.png",
    "powerbi/tpch_analytics_dashboard.pbix",
    "powerbi/dashboard_report.pdf",
    "test_data/incremental_orders_900000001.tbl",
    "evidence/01_bronze_row_counts.png",
    "evidence/01_internal_stage_incremental_file.png",
    "evidence/01_roles.png",
    "evidence/01_developer_grants.png",
    "evidence/02_task_history.png",
    "evidence/02_incremental_silver_order.png",
    "evidence/02_incremental_customer_ltv.png",
    "evidence/02_incremental_daily_sales.png",
    "evidence/03_layer_reconciliation.png",
    "evidence/03_query_profile.png",
    "evidence/04_snowpark_success.png",
    "evidence/04_udf_tests.png",
)

TEXT_SUFFIXES = {".md", ".py", ".sql", ".txt", ".yml", ".yaml", ".example"}

SECRET_PATTERNS = {
    "private key block": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "AWS access key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    "Snowflake password assignment": re.compile(
        r"SNOWFLAKE_PASSWORD\s*=\s*(?!your_password\b)(?!<password>\b)\S+",
        re.IGNORECASE,
    ),
}


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def check_required_files() -> None:
    missing = [name for name in REQUIRED_FILES if not (ROOT / name).is_file()]
    if missing:
        fail("missing required files: " + ", ".join(missing))


def check_python_syntax() -> None:
    for path in sorted(ROOT.rglob("*.py")):
        if any(part.startswith(".") for part in path.relative_to(ROOT).parts):
            continue
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def check_binary_headers() -> None:
    signatures = {
        ROOT / "docs/architecture.png": b"\x89PNG\r\n\x1a\n",
        ROOT / "assets/dashboard_executive_overview.png": b"\x89PNG\r\n\x1a\n",
        ROOT / "powerbi/dashboard_report.pdf": b"%PDF-",
    }
    for path, signature in signatures.items():
        with path.open("rb") as handle:
            if handle.read(len(signature)) != signature:
                fail(f"unexpected binary header: {path.relative_to(ROOT)}")


def check_for_secrets() -> None:
    findings: list[str] = []
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or ".git" in path.parts:
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES and path.name != ".env.example":
            continue
        content = path.read_text(encoding="utf-8", errors="ignore")
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(content):
                findings.append(f"{path.relative_to(ROOT)}: {label}")
    if findings:
        fail("possible secrets detected:\n  " + "\n  ".join(findings))


def check_markdown_links() -> None:
    link_pattern = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
    broken: list[str] = []
    for path in sorted(ROOT.rglob("*.md")):
        content = path.read_text(encoding="utf-8")
        for raw_target in link_pattern.findall(content):
            target = raw_target.strip().split(" ", 1)[0].strip("<>")
            if not target or target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            relative_target = unquote(target.split("#", 1)[0])
            if not (path.parent / relative_target).resolve().exists():
                broken.append(f"{path.relative_to(ROOT)} -> {target}")
    if broken:
        fail("broken Markdown links:\n  " + "\n  ".join(broken))


def main() -> None:
    check_required_files()
    check_python_syntax()
    check_binary_headers()
    check_for_secrets()
    check_markdown_links()
    print("Repository validation passed.")


if __name__ == "__main__":
    main()
