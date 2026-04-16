# 📊 Data Quality Framework

> A production-grade data quality validation system built with **Great Expectations**, featuring automated expectation suites, HTML reporting, alerting stubs, Docker packaging, and GitHub Actions CI.

[![Data Quality CI](https://github.com/YOUR_USERNAME/data-quality-framework/actions/workflows/dq_validation.yml/badge.svg)](https://github.com/YOUR_USERNAME/data-quality-framework/actions/workflows/dq_validation.yml)
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
- [Quick Start](#quick-start)
- [Running with Docker](#running-with-docker)
- [GitHub Actions CI](#github-actions-ci)
- [Alerting](#alerting)
- [Extending the Framework](#extending-the-framework)
- [Push to GitHub](#push-to-github)

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

## Quick Start

### Prerequisites

- Python 3.11+
- `git`

### 1. Clone and set up

```bash
git clone https://github.com/YOUR_USERNAME/data-quality-framework.git
cd data-quality-framework

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Run all validation suites

```bash
python pipeline/run_validations.py
```

Output:
- Console summary table with pass/fail per suite
- `reports/dq_report.html` — open in any browser
- `reports/validation.log` — full run log
- `reports/alerts.log` — alert events (created only on failures)

### 3. Run a single suite

```bash
python pipeline/run_validations.py --suite suite_01_nulls
python pipeline/run_validations.py --suite suite_03_value_ranges
```

### 4. Run tests

```bash
pytest tests/ -v
pytest tests/ -v --cov=pipeline --cov=alerts --cov-report=term-missing
```

---

## Running with Docker

### Build and run

```bash
# Build the image
docker build -t data-quality-framework .

# Run validations (report mounts to ./reports/)
docker run --rm -v "$(pwd)/reports:/app/reports" data-quality-framework
```

### Docker Compose

```bash
# Run validations
docker compose up dq-validator

# Run validations + serve the HTML report at http://localhost:8080
docker compose --profile serve up
```

### Pass alerting secrets

```bash
# Create a .env file (never commit this)
cat > .env << EOF
DQ_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
DQ_ALERT_EMAIL=oncall@yourcompany.com
EOF

docker compose up dq-validator
```

---

## GitHub Actions CI

Every `git push` triggers the CI workflow (`.github/workflows/dq_validation.yml`):

1. **Install dependencies** — caches pip packages for speed
2. **Lint** — runs `ruff` on the pipeline and alerting modules
3. **Run each suite individually** — individual pass/fail visibility per suite
4. **Full run + HTML report** — generates the complete report
5. **Upload artifacts** — HTML report and alerts log downloadable from the Actions tab
6. **Post summary** — table of suite results in the GitHub Step Summary
7. **Docker smoke test** (main branch only) — builds the image and runs inside container

The pipeline exits with code `1` on any suite failure, so CI fails and blocks merges.

**Viewing the report:**
1. Go to the Actions tab in your GitHub repo
2. Click any workflow run
3. Download the `dq-report-NNN` artifact
4. Open `dq_report.html` in your browser

---

## Alerting

The `alerts/alerting.py` module is designed for production extensibility. On any suite failure, the framework:

1. **Always logs** a `WARNING`-level entry to `reports/alerts.log` with structured JSON
2. **Prints** a formatted alert block to stdout (visible in CI logs)
3. **Stubs** out Slack, PagerDuty, and Email dispatchers — activate by setting environment variables

### Activating Slack alerts

```bash
# Set the webhook URL
export DQ_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."

# Uncomment the requests block in alerts/alerting.py → _dispatch_slack()
```

### Activating PagerDuty

```bash
export DQ_PAGERDUTY_KEY="your-routing-key"
# Uncomment the requests block in alerts/alerting.py → _dispatch_pagerduty()
```

### Severity levels

| Pass Rate | Severity |
|-----------|----------|
| < 50% | CRITICAL |
| 50–74% | HIGH |
| 75–89% | MEDIUM |
| 90–99% | LOW |

---

## Extending the Framework

### Add a new expectation suite

1. Create `expectations/suites/suite_05_YOUR_SUITE.json` following the existing JSON schema
2. Add the suite name to `SUITE_NAMES` in `pipeline/run_validations.py`
3. Add a step in `.github/workflows/dq_validation.yml`

### Add a new data source

```python
# In pipeline/run_validations.py, modify load_dataframe() or add a new loader:
def load_from_postgres(connection_string: str) -> pd.DataFrame:
    from sqlalchemy import create_engine
    engine = create_engine(connection_string)
    return pd.read_sql("SELECT * FROM transactions", engine)
```

### Add a new alert channel

Add a new `_dispatch_*` function in `alerts/alerting.py` and call it from `send_alert()`.

### Integrate with Airflow

```python
# In your DAG:
from airflow.operators.bash import BashOperator

dq_check = BashOperator(
    task_id="run_data_quality",
    bash_command="python /opt/airflow/dags/dq/pipeline/run_validations.py",
)
```

---

## Push to GitHub

```bash
# 1. Initialize the repo
cd data-quality-framework
git init
git add .
git commit -m "feat: initial data quality framework with GE suites, CI, and Docker"

# 2. Create the GitHub repo (requires GitHub CLI)
gh repo create data-quality-framework --public --description "Production-grade data quality framework using Great Expectations"

# 3. Push
git remote add origin https://github.com/YOUR_USERNAME/data-quality-framework.git
git branch -M main
git push -u origin main
```

### GitHub Topics to add

Go to your repo → ⚙️ (gear icon next to "About") → add these topics:

```
data-quality  great-expectations  data-engineering  python
data-validation  etl  data-pipeline  pytest  docker  github-actions
pandas  data-observability  portfolio
```

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*Built to demonstrate production data engineering practices: framework design, testability, CI/CD integration, and operational alerting.*
