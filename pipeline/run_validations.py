#!/usr/bin/env python3
"""
Data Quality Framework — Main Validation Pipeline
==================================================
Runs all expectation suites against the transactions dataset,
generates an HTML report, and triggers alerts on failures.

Usage:
    python pipeline/run_validations.py
    python pipeline/run_validations.py --data data/transactions.csv
    python pipeline/run_validations.py --suite suite_01_nulls
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import great_expectations as gx
import pandas as pd

from alerts.alerting import send_alert, build_summary

# ─── Logging Setup ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("reports/validation.log", mode="a"),
    ],
)
logger = logging.getLogger("dq_framework")

# ─── Constants ────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SUITES_DIR = PROJECT_ROOT / "expectations" / "suites"
REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

SUITE_NAMES = [
    "suite_01_nulls",
    "suite_02_data_types",
    "suite_03_value_ranges",
    "suite_04_referential_integrity",
]

# ─── GX Context Bootstrap ─────────────────────────────────────────────────────


def build_context() -> gx.DataContext:
    """Create an in-memory GX DataContext (no filesystem state required)."""
    context = gx.get_context(mode="ephemeral")
    return context


def load_dataframe(data_path: str) -> pd.DataFrame:
    """Load the transactions CSV into a typed DataFrame."""
    logger.info(f"Loading data from: {data_path}")
    df = pd.read_csv(
        data_path,
        dtype={
            "transaction_id": str,
            "customer_id": str,
            "product_id": str,
            "product_category": str,
            "transaction_date": str,
            "amount": float,
            "quantity": int,
            "payment_method": str,
            "status": str,
            "region": str,
            "sales_rep_id": str,
        },
    )
    logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


# ─── Suite Loader ─────────────────────────────────────────────────────────────


def load_suite(suite_name: str) -> gx.core.ExpectationSuite:
    """Read a suite JSON file and return a GX ExpectationSuite object."""
    suite_path = SUITES_DIR / f"{suite_name}.json"
    if not suite_path.exists():
        raise FileNotFoundError(f"Suite not found: {suite_path}")

    with open(suite_path) as f:
        raw = json.load(f)

    suite = gx.core.ExpectationSuite(expectation_suite_name=suite_name)
    for exp in raw.get("expectations", []):
        config = gx.core.ExpectationConfiguration(
            expectation_type=exp["expectation_type"],
            kwargs=exp["kwargs"],
            meta=exp.get("meta", {}),
        )
        suite.add_expectation(config)

    logger.info(f"Loaded suite '{suite_name}' ({len(suite.expectations)} expectations)")
    return suite


# ─── Validator Runner ─────────────────────────────────────────────────────────


def run_suite(
    context: gx.DataContext,
    df: pd.DataFrame,
    suite: gx.core.ExpectationSuite,
) -> gx.core.ExpectationValidationResult:
    """Run a single expectation suite against the DataFrame."""
    datasource = context.sources.add_or_update_pandas(name="transactions_source")
    data_asset = datasource.add_dataframe_asset(name=suite.expectation_suite_name)
    batch_request = data_asset.build_batch_request(dataframe=df)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    result = validator.validate()
    return result


# ─── HTML Report Generator ────────────────────────────────────────────────────


def generate_html_report(all_results: list[dict], output_path: Path) -> None:
    """
    Build a self-contained HTML report from validation results.
    Styled for readability — no external dependencies.
    """
    run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    total_suites = len(all_results)
    passed_suites = sum(1 for r in all_results if r["success"])
    failed_suites = total_suites - passed_suites

    suite_rows = ""
    detail_sections = ""

    for r in all_results:
        status_badge = (
            '<span class="badge pass">PASS</span>'
            if r["success"]
            else '<span class="badge fail">FAIL</span>'
        )
        suite_rows += f"""
        <tr>
            <td>{r['suite_name']}</td>
            <td>{status_badge}</td>
            <td>{r['passed_count']}</td>
            <td>{r['failed_count']}</td>
            <td>{r['total_count']}</td>
            <td>{r['success_percent']:.1f}%</td>
        </tr>"""

        # Build per-expectation breakdown
        exp_rows = ""
        for exp in r["expectation_results"]:
            exp_status = "✅" if exp["success"] else "❌"
            exp_rows += f"""
            <tr class="{'exp-fail' if not exp['success'] else ''}">
                <td>{exp_status}</td>
                <td><code>{exp['expectation_type']}</code></td>
                <td><code>{exp['column']}</code></td>
                <td>{exp['result_detail']}</td>
            </tr>"""

        detail_sections += f"""
        <div class="suite-detail">
            <h3>{status_badge} {r['suite_name']}</h3>
            <p class="suite-desc">{r.get('description', '')}</p>
            <table class="exp-table">
                <thead>
                    <tr>
                        <th>Status</th>
                        <th>Expectation</th>
                        <th>Column / Target</th>
                        <th>Detail</th>
                    </tr>
                </thead>
                <tbody>{exp_rows}</tbody>
            </table>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Report — {run_time}</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background: #f0f2f5; color: #1a1a2e; line-height: 1.6; }}
        header {{ background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                  color: white; padding: 32px 40px; }}
        header h1 {{ font-size: 1.8rem; font-weight: 700; margin-bottom: 4px; }}
        header p {{ opacity: 0.7; font-size: 0.9rem; }}
        .container {{ max-width: 1100px; margin: 0 auto; padding: 32px 20px; }}
        .summary-cards {{ display: grid; grid-template-columns: repeat(4, 1fr);
                           gap: 16px; margin-bottom: 32px; }}
        .card {{ background: white; border-radius: 12px; padding: 20px 24px;
                 box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
        .card .label {{ font-size: 0.75rem; text-transform: uppercase;
                        letter-spacing: .05em; color: #6b7280; margin-bottom: 6px; }}
        .card .value {{ font-size: 2rem; font-weight: 700; }}
        .card.green .value {{ color: #059669; }}
        .card.red .value {{ color: #dc2626; }}
        .card.blue .value {{ color: #2563eb; }}
        .card.amber .value {{ color: #d97706; }}
        section {{ background: white; border-radius: 12px; padding: 24px 28px;
                   margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
        section h2 {{ font-size: 1.1rem; font-weight: 600; margin-bottom: 16px;
                      padding-bottom: 12px; border-bottom: 1px solid #e5e7eb; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.875rem; }}
        thead th {{ background: #f9fafb; padding: 10px 12px; text-align: left;
                    font-weight: 600; color: #374151; border-bottom: 2px solid #e5e7eb; }}
        tbody td {{ padding: 10px 12px; border-bottom: 1px solid #f3f4f6; }}
        tbody tr:hover {{ background: #f9fafb; }}
        .badge {{ display: inline-block; padding: 2px 10px; border-radius: 20px;
                  font-size: 0.75rem; font-weight: 700; letter-spacing: .03em; }}
        .badge.pass {{ background: #d1fae5; color: #065f46; }}
        .badge.fail {{ background: #fee2e2; color: #991b1b; }}
        .suite-detail {{ margin-bottom: 28px; }}
        .suite-detail h3 {{ font-size: 1rem; font-weight: 600; margin-bottom: 6px; }}
        .suite-desc {{ font-size: 0.85rem; color: #6b7280; margin-bottom: 12px; }}
        .exp-table {{ font-size: 0.8rem; }}
        .exp-fail td {{ background: #fff5f5; }}
        code {{ background: #f3f4f6; padding: 1px 5px; border-radius: 4px;
                font-size: 0.8em; font-family: 'Fira Code', monospace; }}
        footer {{ text-align: center; color: #9ca3af; font-size: 0.8rem; padding: 24px; }}
    </style>
</head>
<body>
    <header>
        <h1>📊 Data Quality Report</h1>
        <p>Generated: {run_time} &nbsp;·&nbsp; Dataset: transactions.csv</p>
    </header>
    <div class="container">
        <div class="summary-cards">
            <div class="card blue">
                <div class="label">Total Suites</div>
                <div class="value">{total_suites}</div>
            </div>
            <div class="card green">
                <div class="label">Suites Passed</div>
                <div class="value">{passed_suites}</div>
            </div>
            <div class="card {'red' if failed_suites > 0 else 'green'}">
                <div class="label">Suites Failed</div>
                <div class="value">{failed_suites}</div>
            </div>
            <div class="card {'green' if failed_suites == 0 else 'amber'}">
                <div class="label">Overall Health</div>
                <div class="value">{'✅ OK' if failed_suites == 0 else '⚠️ WARN'}</div>
            </div>
        </div>

        <section>
            <h2>Suite Summary</h2>
            <table>
                <thead>
                    <tr>
                        <th>Suite Name</th>
                        <th>Status</th>
                        <th>Passed</th>
                        <th>Failed</th>
                        <th>Total</th>
                        <th>Pass Rate</th>
                    </tr>
                </thead>
                <tbody>{suite_rows}</tbody>
            </table>
        </section>

        <section>
            <h2>Expectation Details by Suite</h2>
            {detail_sections}
        </section>
    </div>
    <footer>Data Quality Framework · Great Expectations {gx.__version__}</footer>
</body>
</html>"""

    with open(output_path, "w") as f:
        f.write(html)

    logger.info(f"HTML report written to: {output_path}")


# ─── Result Parser ────────────────────────────────────────────────────────────


def parse_result(suite_name: str, result, suite: gx.core.ExpectationSuite) -> dict:
    """Extract structured data from a GX ValidationResult."""
    stats = result.statistics
    total = stats.get("evaluated_expectations", 0)
    passed = stats.get("successful_expectations", 0)
    failed = stats.get("unsuccessful_expectations", 0)
    success_pct = (passed / total * 100) if total > 0 else 0.0

    exp_results = []
    for er in result.results:
        exp_type = er.expectation_config.expectation_type
        kwargs = er.expectation_config.kwargs
        column = kwargs.get("column", kwargs.get("column_list", "table-level"))
        if isinstance(column, list):
            column = ", ".join(column)

        # Build human-readable detail
        detail = ""
        if not er.success:
            res = er.result or {}
            if "unexpected_count" in res:
                detail = f"{res['unexpected_count']} unexpected value(s)"
                if "unexpected_percent" in res:
                    detail += f" ({res['unexpected_percent']:.1f}%)"
            elif "observed_value" in res:
                detail = f"observed: {res['observed_value']}"
            else:
                detail = "expectation failed"
        else:
            res = er.result or {}
            if "observed_value" in res:
                detail = f"observed: {res['observed_value']}"
            else:
                detail = "ok"

        exp_results.append(
            {
                "success": er.success,
                "expectation_type": exp_type,
                "column": str(column),
                "result_detail": detail,
            }
        )

    # Load suite description from JSON meta
    description = ""
    suite_path = SUITES_DIR / f"{suite_name}.json"
    if suite_path.exists():
        with open(suite_path) as f:
            raw = json.load(f)
        description = raw.get("meta", {}).get("suite_description", "")

    return {
        "suite_name": suite_name,
        "success": bool(result.success),
        "passed_count": passed,
        "failed_count": failed,
        "total_count": total,
        "success_percent": success_pct,
        "expectation_results": exp_results,
        "description": description,
    }


# ─── Main Entry Point ─────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Run data quality validations")
    parser.add_argument(
        "--data",
        default=str(DATA_DIR / "transactions.csv"),
        help="Path to input CSV file",
    )
    parser.add_argument(
        "--suite",
        default=None,
        help="Run a single named suite instead of all suites",
    )
    parser.add_argument(
        "--report",
        default=str(REPORTS_DIR / "dq_report.html"),
        help="Output path for the HTML report",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Data Quality Framework — Validation Run Starting")
    logger.info("=" * 60)

    # Load data
    df = load_dataframe(args.data)

    # Decide which suites to run
    suites_to_run = [args.suite] if args.suite else SUITE_NAMES

    # Build GX context
    context = build_context()

    all_results = []
    any_failure = False

    for suite_name in suites_to_run:
        logger.info(f"▶ Running suite: {suite_name}")
        try:
            suite = load_suite(suite_name)
            result = run_suite(context, df, suite)
            parsed = parse_result(suite_name, result, suite)
            all_results.append(parsed)

            if parsed["success"]:
                logger.info(
                    f"  ✅ PASS — {parsed['passed_count']}/{parsed['total_count']} expectations passed"
                )
            else:
                any_failure = True
                logger.warning(
                    f"  ❌ FAIL — {parsed['failed_count']}/{parsed['total_count']} expectations failed"
                )
                # Trigger alerting stub
                send_alert(suite_name, parsed)

        except Exception as exc:
            logger.error(f"  💥 ERROR running suite '{suite_name}': {exc}", exc_info=True)
            any_failure = True

    # Generate HTML report
    report_path = Path(args.report)
    generate_html_report(all_results, report_path)

    # Print overall summary
    summary = build_summary(all_results)
    print("\n" + summary)
    logger.info("Validation run complete.")

    # Exit with non-zero code so CI fails on data quality issues
    sys.exit(1 if any_failure else 0)


if __name__ == "__main__":
    main()
