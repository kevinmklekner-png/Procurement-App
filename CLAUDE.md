# Procurement App - Claude Code Instructions

## Overview
Federal procurement intelligence tool tracking 5 small financial regulatory agencies: **FHFA, SEC, OCC, CFPB, FCA**. Pulls solicitations from SAM.gov API, parses documents (SOWs, evaluation criteria, pricing), and serves a Flask dashboard.

## Directory Layout
```
├── app.py              # Flask app (port 5000)
├── config.py           # SAM_API_KEY from env, base URL
├── database.py         # ProcurementDatabase class (SQLite)
├── test_api.py         # unittest suite
├── requirements.txt    # requests, pdfplumber, beautifulsoup4, python-docx, flask
├── pipeline/           # Data collection
│   ├── sam_api.py          # SAMApiClient with retry/backoff
│   ├── collect_agencies.py # Targeted agency collection
│   ├── collect_data.py     # General collection (daily/backfill)
│   ├── collect_documents.py# Download & parse attachments
│   ├── import_local_docs.py# Import local PDF/DOCX/Excel
│   └── run_reports.py      # Report generation CLI
├── analysis/           # Analytics & parsing
│   ├── analytics.py        # ProcurementAnalytics (market reports)
│   ├── doc_parser.py       # Text extraction, SOW/eval parsing
│   ├── sow_review.py       # SOWReviewer (reviews, comparisons)
│   └── report_generator.py # HTML report builder
├── data/               # Local files: Forecasts/, Solicitations/{FHFA,SEC,OCC}/, pricing/
├── db/                 # federal_procurement.db (SQLite)
├── templates/          # Jinja2: dashboard, agency, opportunity, forecast, search, reports
├── reports/            # Generated HTML reports
└── ops/                # cron_setup.sh, launchd plist (daily 6 AM)
```

## Data Flow
```
SAM.gov API → collect_agencies.py → solicitations table
                                  → collect_documents.py → opportunity_documents
                                                         → doc_parser → sow_analysis, evaluation_criteria
Local files → import_local_docs.py → solicitations + documents + labor_categories
Database → analytics.py / sow_review.py → report_generator.py → reports/*.html
Database → app.py → templates → Web UI
```

## Commands
```bash
# Run app
python3 app.py --debug              # Dev mode with auto-reload
python3 app.py --port 8080          # Custom port

# Tests
python3 test_api.py                 # unittest (uses in-memory SQLite)

# Pipeline (run from project root)
python3 pipeline/collect_agencies.py 365         # Collect last N days
python3 pipeline/collect_data.py daily           # Last 2 days
python3 pipeline/collect_documents.py --agency FHFA
python3 pipeline/collect_documents.py --all
python3 pipeline/import_local_docs.py --dry-run
python3 pipeline/run_reports.py daily
```

## Coding Conventions

### Naming
- **Files**: snake_case (`collect_agencies.py`)
- **Classes**: PascalCase (`ProcurementDatabase`, `SAMApiClient`)
- **Functions**: snake_case (`insert_solicitation`, `get_agency_stats`)
- **Constants**: UPPER_SNAKE_CASE (`SAM_API_KEY`, `NOTICE_TYPE_MAP`)
- **Private helpers**: leading underscore (`_get_db`, `_parse_factor_number`)
- **DB tables**: snake_case plural (`solicitations`, `opportunity_documents`)
- **DB columns**: snake_case (`notice_id`, `posted_date`)

### Imports
Subdirectory modules use `sys.path.insert` to reach root and sibling dirs:
```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from database import ProcurementDatabase
```
Analysis modules from pipeline:
```python
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'analysis'))
from doc_parser import extract_text, classify_document
```

### Database Access
- Use `ProcurementDatabase()` with no args for default path (`db/federal_procurement.db`)
- Row factory returns dict-like `sqlite3.Row` objects
- Always `db.close()` when done
- Flask: fresh connection per request via `_get_db()` (SQLite thread safety)
- Error pattern: try/except with `conn.rollback()` on failure, print error, return None

### Flask Patterns
- Templates extend `base.html` with `{% block content %}`
- Dark theme: `#16213e`, `#0f3460`, `#e94560`
- Reports are self-contained HTML with inline CSS (no external deps)
- Use `python3 -u` for unbuffered output in long-running scripts

## Critical Constraints

### SAM.gov API
- **NEVER hardcode the API key** — always use `os.environ.get("SAM_API_KEY")`
- Call `config.validate_config()` before any API operations
- Rate limit: exponential backoff on 429 (10s, 20s, 40s, 80s, 160s)
- Date ranges must be split into ≤180-day chunks
- 3-second delay between requests in collection scripts

### Security
- API key via environment variable only, never in source files
- The launchd plist has a hardcoded key (known issue)
- No `.gitignore` exists yet — be careful not to commit `*.db`, `data/`, `.env`

### Database Schema (key tables)
- `solicitations` — unique on `notice_id`
- `opportunity_documents` — unique on `file_url`, tracks download/parse status
- `sow_analysis` — structured SOW data with JSON fields (key_tasks, labor_categories, etc.)
- `evaluation_criteria` — one row per factor with subfactors as JSON
- `forecast_opportunities` — deduped on (agency, project_description, fiscal_year)
- `labor_categories` — deduped on (notice_id, category_name, period_number, site_type)

## Environment
- Python 3.9 (macOS system Python at `/Library/Developer/CommandLineTools`)
- pip packages: `~/Library/Python/3.9/lib/`
- Flask not on PATH — use `python3 app.py` directly
- Git remote: `https://github.com/kevinmklekner-png/Procurement-App.git` (branch: main)
