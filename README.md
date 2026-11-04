# 📊 Data Quality Framework

> A data-quality validation framework built with Great Expectations: four expectation suites, HTML reports, alerting stubs for Slack, PagerDuty and email, Docker packaging and GitHub Actions CI.

[![Data Quality CI](https://github.com/Revanthpotu/data-quality-framework/actions/workflows/dq_validation.yml/badge.svg)](https://github.com/Revanthpotu/data-quality-framework/actions/workflows/dq_validation.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Great Expectations](https://img.shields.io/badge/great--expectations-0.18-orange.svg)](https://greatexpectations.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Table of Contents

- [What Is Data Quality?](#what-is-data-quality)
- [Why This Matters in Production](#why-this-matters-in-production)
- [Project Structure](#project-structure)
- [Dataset](#dataset)
- [Expectation Suites](#expectation-suites)

---

## What Is Data Quality?

**Data quality** is the degree to which data is fit for its intended purpose. In data engineering, bad data doesn't just produce wrong answers — it silently corrupts dashboards, trains flawed ML models, and triggers incorrect business decisions before anyone realises something went wrong.

The five dimensions this framework validates:

| Dimension | Question | Example |
|-----------|----------|---------|
| **Completeness** | Are required fields present? | `transaction_id` is never null |
| **Validity** | Do values conform to defined rules? | `payment_method` ∈ {credit_card, debit_card, paypal} |
| **Accuracy** | Are values in sensible ranges? | `amount` between $0.01 and $50,000 |
| **Consistency** | Do values follow a stable format? | `transaction_date` always `YYYY-MM-DD` |
| **Integrity** | Do foreign keys reference valid records? | Every `customer_id` exists in `customers.csv` |

---

## Why This Matters in Production

Without automated data quality checks:

- **Silent failures**: A pipeline loads 0 rows or null amounts with no error raised
- **Schema drift**: An upstream team renames a column; your downstream query silently returns `NULL`
- **Type mismatches**: Dates stored as strings break date-range filters
- **Fraud blind spots**: `amount = -500` passes into revenue reports unchallenged
- **Duplicate keys**: `transaction_id` duplication double-counts revenue in aggregations

Great Expectations solves this by treating data quality as **code** — versionable, testable, and runnable in CI just like unit tests.

---

## Project Structure

```
data-quality-framework/
│
├── data/
│   ├── transactions.csv          # 50-row seed dataset (customer transactions)
│   └── customers.csv             # 42-row customer master (for referential integrity)
│
├── expectations/
│   └── suites/
│       ├── suite_01_nulls.json              # Completeness checks
│       ├── suite_02_data_types.json         # Type/format/enum validation
│       ├── suite_03_value_ranges.json       # Business rule & range checks
│       └── suite_04_referential_integrity.json  # Schema contract & FK integrity
│
├── pipeline/
│   ├── __init__.py
│   └── run_validations.py        # Main orchestrator — runs all suites
│
├── alerts/
│   ├── __init__.py
│   └── alerting.py               # Alert dispatcher (Slack/PagerDuty/Email stubs)
│
├── tests/
│   └── test_pipeline.py          # pytest unit tests
│
├── reports/                      # Generated HTML report lands here (gitignored)
│
├── .github/
│   └── workflows/
│       └── dq_validation.yml     # GitHub Actions CI pipeline
│
├── Dockerfile                    # Multi-stage Docker build
├── docker-compose.yml            # Docker Compose for local runs + report server
├── requirements.txt
└── README.md
```

---

## Dataset

The seed dataset (`data/transactions.csv`) simulates a real-world e-commerce transaction feed:

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | string | Primary key, format `TXN-NNNNN` |
| `customer_id` | string | FK to customers master, format `CUST-NNN` |
| `product_id` | string | Product reference, format `PROD-XNN` |
| `product_category` | string | Taxonomy category (Electronics, Clothing, etc.) |
| `transaction_date` | string | ISO 8601 date `YYYY-MM-DD` |
| `amount` | float | Transaction value in USD |
| `quantity` | int | Units purchased |
| `payment_method` | string | Enum: credit_card, debit_card, paypal, etc. |
| `status` | string | Lifecycle state: completed, pending, refunded, etc. |
| `region` | string | Sales territory: North, South, East, West |
| `sales_rep_id` | string | Attributed rep, format `REP-NN` |

The customer master (`data/customers.csv`) provides referential targets for `customer_id` foreign-key validation.

The seed data is 50 sample transactions so suites run in seconds. The suites are written against column rules, not row counts, except the minimum-row check in Suite 04.

---

## Expectation Suites

### Suite 01 — Completeness & Nulls (`suite_01_nulls.json`)

Ensures critical columns are fully populated. A null `transaction_id` or `amount` is a pipeline failure, not a data issue.

| Expectation | Column | Rationale |
|-------------|--------|-----------|
| Not null | `transaction_id` | Primary key — null = catastrophic |
| Not null | `customer_id` | Breaks customer-level aggregations |
| Not null | `transaction_date` | Null dates corrupt time-series |
| Not null | `amount` | Revenue figure cannot be absent |
| Not null | `status` | Required for fulfillment tracking |
| Not null | `payment_method` | Required for reconciliation |
| >99% unique | `transaction_id` | Uniqueness proxy for deduplication |

---

### Suite 02 — Data Types & Formats (`suite_02_data_types.json`)

Validates that values conform to expected patterns and enum sets — the first line of defense against upstream schema drift.

| Expectation | Column | Rule |
|-------------|--------|------|
| Regex match | `transaction_id` | `^TXN-\d{5}$` |
| Regex match | `customer_id` | `^CUST-\d{3}$` |
| Regex match | `product_id` | `^PROD-[A-Z]\d{2}$` |
| Regex match | `transaction_date` | `^\d{4}-\d{2}-\d{2}$` |
| Type = float | `amount` | Numeric type for arithmetic |
| Type = int | `quantity` | No fractional units |
| In set | `payment_method` | Approved payment channel enum |
| In set | `status` | Valid lifecycle states |
| In set | `region` | Defined sales territories |

---

### Suite 03 — Value Ranges & Business Rules (`suite_03_value_ranges.json`)

Catches values that are technically valid types but violate business logic. This is where data quality goes beyond "is it a number" to "is it a *believable* number."

| Expectation | Column | Rule | Rationale |
|-------------|--------|------|-----------|
| Between | `amount` | $0.01 – $50,000 | Fraud threshold ceiling |
| Between | `quantity` | 1 – 500 | Bulk-order sanity check |
| Mean between | `amount` | $10 – $2,000 | Distribution health check |
| Median between | `amount` | $10 – $1,000 | Outlier-robust sanity |
| Stdev between | `amount` | $1 – $5,000 | Catches suspiciously flat or exploded data |
| Date between | `transaction_date` | 2020-01-01 – 2030-12-31 | Valid business window |
| In set | `product_category` | Master taxonomy list | Prevents unmapped categories |

---

### Suite 04 — Referential Integrity & Schema Contract (`suite_04_referential_integrity.json`)

The structural layer — validates the shape of the dataset and ensures foreign-key relationships hold. Schema drift is the #1 silent killer in data pipelines.

| Expectation | Target | Rule |
|-------------|--------|------|
| Row count between | Table | Min 10 rows (empty load detection) |
| Column count = 11 | Table | Schema contract — no additions or removals |
| Columns match ordered list | Table | Exact column names and order |
| Values unique | `transaction_id` | Primary key constraint |
| Regex match | `sales_rep_id` | `^REP-\d{2}$` (valid HR reference) |
| Column A > Column B | `amount` > `quantity` | Unit price sanity (catches currency mix-ups) |
| Not null | `sales_rep_id` | Commission attribution required |

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*Built to demonstrate production data engineering practices: framework design, testability, CI/CD integration, and operational alerting.*
