# 📈 Google Trends ETL Pipeline — Azure Serverless

An automated ETL pipeline that fetches daily Google Trends data for Germany, classifies each search term via the OpenAI API, and persists the results in Azure SQL — fully serverless, scheduled, and production-ready.

---

## Architecture Overview

```
Google Trends RSS Feed
        │
        ▼
┌─────────────────────────┐
│   Azure Function App    │  ← Timer Trigger (daily, 06:00 UTC)
│   (Python 3.x)          │
│                         │
│  1. Fetch RSS Feed       │
│  2. Parse & clean data   │
│  3. Classify via OpenAI  │
│  4. Write to Azure SQL   │
└─────────────────────────┘
        │
        ▼
┌─────────────────┐
│   Azure SQL DB  │  ← Serverless tier (auto-pause enabled)
└─────────────────┘
        │
        ▼
┌─────────────────┐
│    Power BI     │  ← Dashboard / Reporting
└─────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Python 3.x |
| Data Processing | Pandas |
| Orchestration | Azure Functions (Timer Trigger) |
| Classification | OpenAI API (`gpt-4o-mini`) |
| Database | Azure SQL (Serverless) |
| Visualization | Power BI |
| Feed Parsing | `feedparser` |

---

## Features

- **Scheduled Execution** — Timer Trigger fires daily at 06:00, no manual intervention required
- **Serverless DB Handling** — Custom wake-up logic with retry mechanism handles Azure SQL auto-pause gracefully
- **AI-powered Classification** — Each search term is assigned a category (Sport, Politik, Wirtschaft, etc.) via GPT-4o-mini
- **Robust Error Handling** — Exponential-style retry logic for both DB connection and SQL inserts
- **Clean Data Model** — Numeric traffic normalization (K/M suffix parsing), Berlin timezone, ranked output

---

## Pipeline Flow

### 1. `wecke_datenbank()` — DB Wake-up
Azure SQL Serverless pauses after inactivity. This function sends a `SELECT 1` probe up to 10 times with increasing wait intervals (20s → 40s) before the main ETL begins.

### 2. `lade_trends()` — Data Extraction & Transformation
- Fetches the top 20 entries from the Google Trends RSS feed (`geo=DE`)
- Cleans and normalizes traffic strings (`1.2K` → `1200`, `3M` → `3000000`)
- Assigns a timestamp in `Europe/Berlin` timezone
- Sorts by traffic volume descending, adds rank

### 3. `kategorisiere_mit_ki()` — AI Classification
- Sends all search terms in a single batch prompt to OpenAI
- Returns a JSON mapping of `{term: category}` in zero-shot fashion
- Falls back to `"Sonstiges"` if the API call fails

### 4. `speichere_in_azure_sql()` — Load
- Inserts all rows into the `google_trends` table
- Retry logic: up to 5 attempts with wait times of 10 / 30 / 60 / 90 / 120 seconds

---

## Database Schema

```sql
CREATE TABLE google_trends (
    rang              INT,
    suchbegriff       NVARCHAR(255),
    kategorie         NVARCHAR(100),
    traffic           NVARCHAR(50),
    traffic_numerisch FLOAT,
    zeitpunkt         DATETIME
);
```

---

## Configuration

All secrets are managed via **Azure Function App Environment Variables** (never hardcoded):

| Variable | Description |
|---|---|
| `SQL_USERNAME` | Azure SQL login username |
| `SQL_PASSWORD` | Azure SQL login password |
| `OPENAI_API_KEY` | OpenAI API key |

---

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export SQL_USERNAME=...
export SQL_PASSWORD=...
export OPENAI_API_KEY=...

# Run via Azure Functions Core Tools
func start
```

---

## Project Structure

```
├── function_app.py     # Main ETL logic & Azure Function definition
├── host.json           # Azure Functions host configuration
├── requirements.txt    # Python dependencies
└── .funcignore         # Files excluded from deployment
```

---

## Key Design Decisions

**Single-file architecture** — All ETL logic lives in `function_app.py` to keep the deployment unit minimal and the codebase easy to audit.

**Batch classification** — Instead of one API call per search term, all terms are sent in a single prompt, reducing latency and API cost by ~95%.

**Serverless-aware DB connection** — Cold-start latency of Azure SQL Serverless (up to 60s) is handled explicitly rather than relying on generic timeout settings.
