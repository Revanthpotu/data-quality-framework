"""
tests/test_pipeline.py
======================
Unit tests for the Data Quality Framework pipeline.
Run with: pytest tests/ -v
"""

import json
from pathlib import Path

import pandas as pd
import pytest

# ─── Fixtures ─────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SUITES_DIR = PROJECT_ROOT / "expectations" / "suites"
DATA_PATH = PROJECT_ROOT / "data" / "transactions.csv"
CUSTOMERS_PATH = PROJECT_ROOT / "data" / "customers.csv"


@pytest.fixture
def transactions_df():
    return pd.read_csv(
        DATA_PATH,
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


@pytest.fixture
def customers_df():
    return pd.read_csv(CUSTOMERS_PATH)


# ─── Data File Tests ──────────────────────────────────────────────────────────


class TestDataFiles:
    def test_transactions_csv_exists(self):
        assert DATA_PATH.exists(), "transactions.csv not found"

    def test_customers_csv_exists(self):
        assert CUSTOMERS_PATH.exists(), "customers.csv not found"

    def test_transactions_has_expected_columns(self, transactions_df):
        expected_cols = [
            "transaction_id", "customer_id", "product_id", "product_category",
            "transaction_date", "amount", "quantity", "payment_method",
            "status", "region", "sales_rep_id",
        ]
        assert list(transactions_df.columns) == expected_cols

    def test_transactions_not_empty(self, transactions_df):
        assert len(transactions_df) > 0

    def test_transactions_no_null_primary_key(self, transactions_df):
        assert transactions_df["transaction_id"].notna().all()

    def test_transaction_ids_unique(self, transactions_df):
        assert transactions_df["transaction_id"].is_unique

    def test_amounts_positive(self, transactions_df):
        assert (transactions_df["amount"] > 0).all()

    def test_quantities_positive_integer(self, transactions_df):
        assert (transactions_df["quantity"] >= 1).all()


# ─── Suite JSON Tests ─────────────────────────────────────────────────────────


class TestExpectationSuites:
    EXPECTED_SUITES = [
        "suite_01_nulls",
        "suite_02_data_types",
        "suite_03_value_ranges",
        "suite_04_referential_integrity",
    ]

    def test_all_suite_files_exist(self):
        for name in self.EXPECTED_SUITES:
            path = SUITES_DIR / f"{name}.json"
            assert path.exists(), f"Missing suite file: {name}.json"

    def test_suites_are_valid_json(self):
        for name in self.EXPECTED_SUITES:
            path = SUITES_DIR / f"{name}.json"
            with open(path) as f:
                data = json.load(f)
            assert "expectations" in data
            assert "expectation_suite_name" in data

    def test_suites_have_expectations(self):
        for name in self.EXPECTED_SUITES:
            path = SUITES_DIR / f"{name}.json"
            with open(path) as f:
                data = json.load(f)
            assert len(data["expectations"]) > 0, f"{name} has no expectations"

    def test_each_expectation_has_type_and_kwargs(self):
        for name in self.EXPECTED_SUITES:
            path = SUITES_DIR / f"{name}.json"
            with open(path) as f:
                data = json.load(f)
            for i, exp in enumerate(data["expectations"]):
                assert "expectation_type" in exp, f"{name}[{i}] missing expectation_type"
                assert "kwargs" in exp, f"{name}[{i}] missing kwargs"


# ─── Referential Integrity Tests ──────────────────────────────────────────────


class TestReferentialIntegrity:
    def test_all_customer_ids_in_master(self, transactions_df, customers_df):
        valid_ids = set(customers_df["customer_id"])
        txn_ids = set(transactions_df["customer_id"])
        orphaned = txn_ids - valid_ids
        assert len(orphaned) == 0, f"Orphaned customer_ids: {orphaned}"

    def test_status_values_valid(self, transactions_df):
        valid_statuses = {"completed", "pending", "refunded", "failed", "cancelled"}
        found = set(transactions_df["status"].dropna())
        invalid = found - valid_statuses
        assert len(invalid) == 0, f"Invalid status values: {invalid}"

    def test_payment_methods_valid(self, transactions_df):
        valid_methods = {"credit_card", "debit_card", "paypal", "bank_transfer", "cash"}
        found = set(transactions_df["payment_method"].dropna())
        invalid = found - valid_methods
        assert len(invalid) == 0, f"Invalid payment methods: {invalid}"


# ─── Alerting Tests ───────────────────────────────────────────────────────────


class TestAlerting:
    def test_build_summary_returns_string(self):
        from alerts.alerting import build_summary

        mock_results = [
            {
                "suite_name": "suite_01_nulls",
                "success": True,
                "passed_count": 5,
                "failed_count": 0,
                "total_count": 5,
                "success_percent": 100.0,
                "expectation_results": [],
            }
        ]
        summary = build_summary(mock_results)
        assert isinstance(summary, str)
        assert "suite_01_nulls" in summary

    def test_send_alert_logs_warning(self, caplog):
        import logging
        from alerts.alerting import send_alert

        mock_result = {
            "suite_name": "suite_test",
            "success": False,
            "passed_count": 3,
            "failed_count": 2,
            "total_count": 5,
            "success_percent": 60.0,
            "expectation_results": [
                {
                    "success": False,
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": "amount",
                    "result_detail": "2 unexpected value(s)",
                }
            ],
        }

        with caplog.at_level(logging.WARNING, logger="dq_framework.alerts"):
            send_alert("suite_test", mock_result)

        assert any("ALERT" in record.message for record in caplog.records)
