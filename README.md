# Zomato Data & AI Pipeline

An end-to-end batch pipeline for a food-delivery dataset — raw CSVs to AI-powered analytics, fully orchestrated.

**Flow:** Kaggle Dataset → Amazon S3 → Snowflake → dbt → Airflow → AI (Gemini)

Raw data lands in an S3 data lake, flows into Snowflake through a storage integration, and moves through medallion layers with dbt: **Bronze** (raw tables loaded via `COPY INTO`), **Silver** (cleaned staging views), and **Gold** (business-ready dimensions, incremental facts, and marts). Apache Airflow orchestrates the entire pipeline — data transforms and AI enrichment — as one scheduled DAG. On top sits an AI layer built on Google's Gemini API: LLM enrichment turns free-text reviews into structured, queryable data; a RAG app lets you chat with your reviews; and a text-to-SQL app lets you query the warehouse in plain English. Streamlit serves both AI apps.

---

## Architecture

| Layer | Where | What |
|---|---|---|
| Source | Kaggle dataset | Restaurant, customer, order, and review data |
| Lake | Amazon S3 | Raw CSVs staged per table |
| Bronze | Snowflake `ZOMATO.RAW` | Loaded via `COPY INTO` from an S3 storage integration |
| Silver | Snowflake `ZOMATO.STAGING` | dbt staging views — cleaned, typed, renamed |
| Gold | Snowflake `ZOMATO.MARTS` | Dimensions, incremental facts, business marts |
| AI | Snowflake `ZOMATO.AI` | LLM-enriched reviews, RAG chat, text-to-SQL |
| Orchestration | Airflow (Docker) | One DAG: load → transform → enrich → AI mart |

## Tech stack

Python · Pandas · NumPy · Amazon S3 · Snowflake · dbt (`dbt-snowflake`) · Apache Airflow 3 (Docker) · Google Gemini API (`gemini-3.6-flash`) · Sentence-Transformers (`all-MiniLM-L6-v2`) · Streamlit

---

## Repository structure

```
├── airflow/                  # Airflow 3 on Docker
│   ├── Dockerfile            #   Snowflake provider + Gemini + dbt in its own venv
│   ├── docker-compose.yaml   #   postgres + apiserver + scheduler + dag-processor
│   ├── .env                  #   SNOWFLAKE_* / GEMINI_API_KEY (not committed)
│   └── dags/zomato_batch.py  #   the pipeline DAG
├── zomato/                   # dbt project
│   ├── models/staging/       #   staging views (Silver) + sources + tests
│   ├── models/marts/         #   dims, incremental facts, business marts (Gold)
│   └── macros/                #   custom schema-name macro
├── ai/                        # AI layer
│   ├── enrich_reviews.py     #   LLM enrichment → ZOMATO.AI.REVIEW_ENRICHED
│   ├── rag_chat.py           #   RAG — "chat with your reviews" (Streamlit)
│   ├── text_to_sql.py        #   text-to-SQL — "chat with your warehouse" (Streamlit)
│   └── .env                   #   AI credentials (not committed)
├── snowflake/                 # Snowflake setup SQL
└── aws/iam/                   # IAM policy + trust policies for S3 ↔ Snowflake
```

---

## How the pipeline works

### 1 · Data lands in S3
CSVs are uploaded to `s3://<BUCKET>/raw/<table>/`, one folder per source table.

### 2 · S3 → Snowflake — keyless integration
Snowflake reads the bucket using a storage integration + IAM role, no stored access keys. Setup order matters: create the AWS policy and role → create the Snowflake `STORAGE INTEGRATION` → `DESC INTEGRATION` to get Snowflake's IAM user ARN and external ID → paste both into the role's trust policy.

### 3 · Load — `COPY INTO`
Raw table DDL matches each CSV's column order; `COPY INTO` pulls each file from the stage into `ZOMATO.RAW`.

### 4 · Transform — dbt (medallion)
- **Staging (Silver)** — one view per source, cleaning and typing raw fields
- **Dimensions (Gold)** — restaurant, customer, and calendar dimensions
- **Facts (Gold, incremental)** — order-level facts using `materialized='incremental'` with a merge strategy, so reruns only process new rows
- **Marts (Gold)** — daily city revenue, restaurant performance, delivery SLA, review insights
- **Tests** — `unique` / `not_null` / `relationships` / `accepted_values`, run via `dbt build`

### 5 · Orchestrate — Airflow
A single DAG, `zomato_batch`, runs the whole thing as one dependency graph:

```
reload_raw  →  dbt_build_core  →  enrich_reviews  →  dbt_build_ai
(COPY from S3)  (dbt build + tests)  (Gemini enrichment)  (AI mart)
```

Both the data ETL steps and the AI enrichment step run as Airflow tasks in the same pipeline — the AI layer isn't a separate, manually-run process. Credentials are injected via environment variables at the container level, never hardcoded.

### 6 · AI layer — three capabilities

**LLM enrichment** (`ai/enrich_reviews.py`)
Runs as a DAG task alongside the data ETL. Reads new review text, prompts `gemini-3.6-flash` for structured JSON output (sentiment label, sentiment score, topic, key issue), and writes it back into `ZOMATO.AI.REVIEW_ENRICHED` — which dbt then models into a mart like any other table. Only processes reviews not already enriched, so it never reprocesses the same review twice.

**RAG chat** (`ai/rag_chat.py`)
A Streamlit app for asking natural-language questions about customer reviews. Reviews are embedded locally using `sentence-transformers` (`all-MiniLM-L6-v2`) — free, no API calls, no rate limits — and cached to disk. The user's question is embedded the same way, cosine similarity retrieves the most relevant reviews, and Gemini generates an answer grounded strictly in that retrieved context, with the source reviews shown alongside the answer.

**Text-to-SQL** (`ai/text_to_sql.py`)
A Streamlit app for querying the warehouse in plain English. Gemini is given the marts' schema and writes a single Snowflake `SELECT` query for the question asked; a safety layer rejects anything that isn't a read-only query before it's ever executed against Snowflake.

---

## Running it

```bash
# Snowflake objects (warehouse, database, schemas, role) + S3 storage integration
# — run the snowflake/ SQL scripts in order in Snowsight

# dbt
cd zomato
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai

# Airflow
cd airflow
cp example.env .env          # fill SNOWFLAKE_*, GEMINI_API_KEY
docker compose build && docker compose up -d
# http://localhost:8080 → un-pause zomato_batch → Trigger

# AI apps (standalone)
export GEMINI_API_KEY=...
python ai/enrich_reviews.py
streamlit run ai/rag_chat.py      # chat with reviews
streamlit run ai/text_to_sql.py   # chat with the warehouse
```