# EDC Data Ingestion Platform
### Databricks Apps · Lakebase PostgreSQL · Lakeflow Connect · Unity Catalog

A reference implementation of the EDC (Electronic Data Capture) clinical-trial
data platform described in the project requirement doc. Operational trial data
is entered through a Databricks App, stored transactionally in Lakebase
PostgreSQL, replicated into Unity Catalog via Lakeflow Connect, and refined
through a Bronze → Silver → Gold medallion pipeline.

```
┌──────────────────┐     ┌──────────────────────┐     ┌───────────────────────┐     ┌───────────────┐     ┌──────────┐
│  Databricks App   │────▶│  Lakebase PostgreSQL │────▶│   Lakeflow Connect     │────▶│  Unity Catalog │────▶│  Silver /│
│ (Streamlit CRUD,  │     │   (OLTP, schema edc) │     │ (gateway + ingestion   │     │  Bronze layer  │     │   Gold   │
│  search, dash-    │     │                      │     │  pipeline, CDC)        │     │ digital_cro.   │     │  layers  │
│  boards, trigger) │     │                      │     │                        │     │ bronze_edc     │     │          │
└──────────────────┘     └──────────────────────┘     └───────────────────────┘     └───────────────┘     └──────────┘
```

---

## Table of contents

1. [What's in the zip](#whats-in-the-zip)
2. [Data model](#data-model)
3. [Prerequisites](#prerequisites)
4. [Step-by-step deployment](#step-by-step-deployment)
5. [File-by-file reference](#file-by-file-reference)
6. [Running the Databricks App locally](#running-the-databricks-app-locally)
7. [Security notes](#security-notes)
8. [Troubleshooting](#troubleshooting)
9. [Assumptions made / things to verify](#assumptions-made--things-to-verify)
10. [Future enhancements](#future-enhancements)

---

## What's in the zip

```
edc_databricks_platform/
├── README.md                                 ← this file
├── databricks.yml                             ← Databricks Asset Bundle root config
├── sql/
│   └── 01_schema_ddl.sql                      ← Phase 1: Postgres DDL (15 tables)
├── synthetic_data/
│   ├── generate_synthetic_data.py             ← Phase 2: synthetic data generator
│   └── requirements.txt
├── databricks_app/
│   ├── app.py                                 ← Phase 3: Streamlit app (CRUD/search/dashboard/trigger)
│   ├── db.py                                  ← Postgres connection helper used by app.py
│   ├── app.yaml                                ← Databricks App runtime config
│   └── requirements.txt
├── lakeflow_connect/
│   ├── postgres_source_setup.sql               ← Phase 4: enable logical replication on Lakebase
│   ├── create_pipeline_rest_api.py             ← Phase 4: create connection+gateway+pipeline via SDK/REST
│   ├── monitor_pipeline.py                     ← Phase 4: CLI to check pipeline health/events
│   ├── requirements.txt
│   └── resources/
│       ├── postgresql_pipeline.yml             ← Asset Bundle: gateway + ingestion pipeline definitions
│       └── postgresql_job.yml                  ← Asset Bundle: scheduled job to run the pipeline
└── unity_catalog/
    ├── silver_transformations.sql              ← Phase 5: cleansed/deduplicated Silver tables
    └── gold_aggregations.sql                   ← Phase 5: business-level Gold aggregates
```

Every `.py` file has been checked with `python -m py_compile` for syntax
correctness. The SQL and YAML have been manually reviewed for consistency
(matching table/column names, FK order, etc.), but none of it has been run
against a live Lakebase instance or Databricks workspace — see
[Troubleshooting](#troubleshooting) for what to double check on first run.

---

## Data model

The requirement doc names 15 source tables but doesn't specify their columns,
so a standard clinical-trial/CDMS (Clinical Data Management System) schema was
designed for them. Entity relationships (all in the `edc` Postgres schema):

```
o_study ─┬─< o_protocol_derivation
         ├─< o_site ─< o_subject ─┬─< o_subject_enf
         │                        ├─< o_subject_event >─ o_study_event ─< o_study_crf ─< o_study_crf_data_item ─ o_codelist_item
         │                        │         │
         │                        │         └─< o_subject_crf_data_item >──┘ (also FK's to o_study_crf_data_item)
         │                        ├─< o_query ─< o_query_crf_data_items_rel >─ o_subject_crf_data_item
         │                        └─< o_subject_audit_history
         └─< o_study_event
o_metadata   (standalone key/value config table)
```

| Table | Role |
|---|---|
| `o_study` | Study master record (protocol number, sponsor, phase, status) |
| `o_protocol_derivation` | Protocol versions / amendments per study |
| `o_site` | Investigational sites for a study |
| `o_subject` | Enrolled subjects, linked to a study and site |
| `o_subject_enf` | Subject eligibility / screening outcome |
| `o_study_event` | Visit/event schedule definitions for a study |
| `o_study_crf` | CRF (form) definitions attached to a study event |
| `o_study_crf_data_item` | Field-level metadata for a CRF (name, type, codelist, validation) |
| `o_subject_event` | A subject's actual occurrence of a study event/visit |
| `o_subject_crf_data_item` | **Core transactional table** — the actual captured data values |
| `o_codelist_item` | Controlled terminology / dictionary values (e.g., race, severity) |
| `o_query` | Data-management queries raised against captured data |
| `o_query_crf_data_items_rel` | Many-to-many: which data item(s) a query concerns |
| `o_subject_audit_history` | Generic audit trail (21 CFR Part 11 style change log) |
| `o_metadata` | Generic key/value app + reference configuration store |

If your organization already has a fixed EDC schema (e.g., exported from
Medidata Rave, Oracle Clinical, or another CDMS), swap the column definitions
in `sql/01_schema_ddl.sql` to match — the rest of the pipeline (synthetic data
generator, app, Lakeflow config, Silver/Gold SQL) will need corresponding
column-name updates in that case.

---

## Prerequisites

- A Databricks workspace with **Unity Catalog** enabled and **serverless
  compute** available.
- A **Lakebase PostgreSQL** database/instance already created (per your note —
  this guide assumes it exists and you have its host/port/database/credentials).
- Databricks CLI ≥ 0.276.0 installed and authenticated (`databricks auth login`),
  if you plan to use the Asset Bundle deployment path.
- Python 3.10+ locally for running the synthetic data generator and/or the
  REST API scripts.
- Privileges: `CREATE CONNECTION` on the metastore, `USE CATALOG` / `CREATE
  SCHEMA` on the target catalog (`digital_cro` in the examples), and superuser
  or equivalent access on the Lakebase database to run the DDL and replication
  setup.

---

## Step-by-step deployment

### 1. Create the schema in Lakebase
```bash
psql "host=<lakebase-host> port=5432 dbname=<db> user=<user> sslmode=require" \
  -f sql/01_schema_ddl.sql
```
This creates the `edc` schema, all 15 tables with PK/FK/check constraints,
supporting indexes, and `updated_date` triggers on the four mutable tables.

### 2. Generate synthetic data
```bash
cd synthetic_data
pip install -r requirements.txt
export LAKEBASE_HOST=<host>
export LAKEBASE_PORT=5432
export LAKEBASE_DB=<db>
export LAKEBASE_USER=<user>
export LAKEBASE_PASSWORD=<password>
python generate_synthetic_data.py --studies 3 --sites-per-study 4 --subjects-per-site 30
```
This inserts, in FK-safe order: studies → protocol versions → study events →
CRFs/data-item definitions → codelists → sites → subjects → eligibility
records → subject visits → captured CRF data → queries → audit entries.
Re-running is safe for codelists/metadata (uses `ON CONFLICT DO NOTHING`), but
will create additional studies/subjects each time — use a fresh database or
truncate tables first if you want a clean reset.

### 3. Set up PostgreSQL for Lakeflow Connect replication
Run **`lakeflow_connect/postgres_source_setup.sql`** as an admin against
Lakebase. It:
- Verifies `wal_level = logical` (required for logical replication)
- Creates a least-privilege `databricks_replication` user (read-only + `REPLICATION`)
- Creates a `PUBLICATION` covering every table in the `edc` schema

**Change the placeholder password** in that script before running it. Then
store the real password in a Databricks secret scope so it's never hardcoded:
```bash
databricks secrets create-scope edc-app-secrets
databricks secrets put-secret edc-app-secrets pg-replication-password
```

### 4. Create the Lakeflow Connect connection, gateway, and pipeline
Two equivalent options — pick one:

**Option A — Databricks Asset Bundle (declarative, recommended for CI/CD):**
```bash
databricks bundle deploy -t dev
databricks bundle run edc_ingestion_pipeline -t dev
```
Edit the `variables:` block in `lakeflow_connect/resources/postgresql_pipeline.yml`
first if your connection name / target catalog / schema differ from the defaults.
Note: you still need to create the Unity Catalog **connection** itself (the
credential object) before the bundle can reference it by name — either via the
Databricks UI (Catalog Explorer → Connections → Create connection) or via
`create_pipeline_rest_api.py` below, which creates it for you.

**Option B — Python script against the REST API (via the Databricks SDK):**
```bash
cd lakeflow_connect
pip install -r requirements.txt
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<pat-or-service-principal-token>
python create_pipeline_rest_api.py \
  --pg-host <lakebase-host> --pg-database <db> \
  --pg-password-secret-scope edc-app-secrets --pg-password-secret-key pg-replication-password \
  --target-catalog digital_cro --target-schema bronze_edc
```
This creates (or reuses, if already present) the UC connection, the
continuous **gateway** pipeline, and the **ingestion** pipeline, then starts
both and polls until the first run completes. It prints an
`ingestion_pipeline_id` at the end — copy this.

**Either way**, the gateway must be left running continuously (this is a
PostgreSQL replication requirement, not optional) to avoid WAL bloat on the
source database.

### 5. Deploy the Silver + Gold Lakeflow Declarative Pipeline
In the workspace, create a new Lakeflow Declarative Pipeline (Delta Live
Tables) using `unity_catalog/silver_transformations.sql` and
`unity_catalog/gold_aggregations.sql` as its source files, targeting the
`digital_cro` catalog. Run it after the Bronze ingestion pipeline has
completed at least one update, since Silver reads `STREAM(digital_cro.bronze_edc.*)`.

### 6. Deploy the Databricks App
```bash
cd databricks_app
databricks apps create edc-ops-app
databricks sync . /Workspace/Users/<you>/edc-ops-app
databricks apps deploy edc-ops-app --source-code-path /Workspace/Users/<you>/edc-ops-app
```
Then in the workspace UI:
- Open the app → **Resources** → **Add resource** → **Database instance** →
  select your Lakebase instance. This auto-injects `PGHOST`, `PGPORT`,
  `PGDATABASE`, `PGUSER`, `PGPASSWORD` into the app's environment.
- Edit `app.yaml`'s `LAKEFLOW_PIPELINE_ID` to the ingestion pipeline ID from
  step 4, and point the `DATABRICKS_TOKEN` env entry at a secret scope
  containing a token/service-principal credential with permission to call the
  Pipelines API.
- Redeploy after these changes.

Open the app URL — you should see the Dashboard, Studies, Sites, Subjects,
Queries, and Ingestion pages.

---

## File-by-file reference

**`sql/01_schema_ddl.sql`** — Idempotent-ish DDL (uses `CREATE SCHEMA IF NOT
EXISTS`, but `CREATE TABLE` will fail if tables already exist — drop the
schema with `DROP SCHEMA edc CASCADE;` first if you need to rerun it from
scratch). Includes a `set_updated_date()` trigger function reused across
`o_study`, `o_subject`, `o_subject_event`, and `o_subject_crf_data_item`.

**`synthetic_data/generate_synthetic_data.py`** — CLI flags: `--studies`,
`--sites-per-study`, `--subjects-per-site`. Uses `Faker` with a fixed seed
(42) for reproducible runs. Reads connection details from
`LAKEBASE_HOST/PORT/DB/USER/PASSWORD` env vars (defaults to `localhost` /
`postgres` for quick local testing against a throwaway Postgres).

**`databricks_app/db.py`** — Thin `psycopg2` + `pandas` wrapper (`query_df`,
`execute`) used by every page in `app.py`. Reads `PGHOST` etc. first (the
names Databricks Apps injects for an attached Lakebase resource), falling
back to `LAKEBASE_*` for local development.

**`databricks_app/app.py`** — Six Streamlit pages via a sidebar radio:
- *Dashboard* — subject/site/query counts, enrollment-by-site bar chart,
  subject-status and query-status pie charts (Plotly).
- *Studies* / *Sites* / *Subjects* — create forms + editable/deletable tables
  (CRUD), with a free-text search box on the Subjects page.
- *Queries* — filterable table + a status-update control.
- *Ingestion* — a button that `POST`s to
  `/api/2.0/pipelines/{id}/updates` to trigger a Lakeflow Connect run, and a
  panel that `GET`s `/api/2.0/pipelines/{id}` to show current state.

**`lakeflow_connect/postgres_source_setup.sql`** — Run once per database,
as an admin, before creating the gateway/pipeline.

**`lakeflow_connect/create_pipeline_rest_api.py`** — Idempotent
(`get_or_create_*` helpers check for existing objects by name first). Uses
`databricks.sdk.WorkspaceClient`, which resolves auth from
`DATABRICKS_HOST`/`DATABRICKS_TOKEN` env vars, a CLI profile, or a service
principal — whichever is configured.

**`lakeflow_connect/monitor_pipeline.py`** — Standalone CLI (`--pipeline-id`)
for checking pipeline state, recent updates, and recent event-log
warnings/errors. Suitable for a scheduled Databricks Job or a CI health check.

**`lakeflow_connect/resources/*.yml`** — Databricks Asset Bundle resource
definitions consumed by `databricks.yml`. `postgresql_pipeline.yml` defines
the gateway (`continuous: true`) and ingestion pipeline (`continuous: false`,
run on demand or via the job); `postgresql_job.yml` schedules the ingestion
pipeline hourly.

**`unity_catalog/silver_transformations.sql`** — One `STREAMING TABLE` per
Bronze source table, using `QUALIFY ROW_NUMBER() OVER (PARTITION BY <pk>
ORDER BY <updated/created date> DESC) = 1` to collapse CDC history down to
the latest row per primary key, plus a couple of `EXPECT ... ON VIOLATION
DROP ROW` data-quality constraints.

**`unity_catalog/gold_aggregations.sql`** — Four materialized views:
`enrollment_summary` (subjects by study/site/status), `query_aging` (open
query counts and average/max age in days), `data_entry_completeness`
(defined vs. captured CRF items, verification/SDV counts), and
`subject_audit_rollup` (daily change counts by table/action).

---

## Running the Databricks App locally

You can test `app.py` outside of Databricks Apps against any reachable
Postgres (e.g., a local Docker Postgres you've loaded the schema and
synthetic data into):

```bash
cd databricks_app
pip install -r requirements.txt
export PGHOST=localhost PGPORT=5432 PGDATABASE=postgres PGUSER=postgres PGPASSWORD=postgres PGSSLMODE=disable
streamlit run app.py
```
The Ingestion page will show a warning instead of erroring if
`DATABRICKS_HOST`/`DATABRICKS_TOKEN`/`LAKEFLOW_PIPELINE_ID` aren't set — every
other page works fully offline against just Postgres.

---

## Security notes

- No password or token is hardcoded anywhere in this project — Postgres
  credentials come from env vars / Databricks App resources, and the
  Databricks API token comes from a secret scope reference in `app.yaml`.
- The replication user created in `postgres_source_setup.sql` is
  intentionally scoped to `SELECT` + `REPLICATION` only, not superuser.
- Change the placeholder password in `postgres_source_setup.sql` before
  running it — it is not a real credential and must not be used as-is.
- The Streamlit CRUD forms build parameterized queries (`%s` placeholders)
  throughout `db.py`/`app.py`, not string-interpolated SQL, to avoid SQL
  injection.

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `psql: FATAL: SSL required` | Add `sslmode=require` to the connection string (Lakebase typically requires TLS). |
| DDL fails with "relation already exists" | Schema was already created on a previous run — `DROP SCHEMA edc CASCADE;` first, or skip tables you don't need to recreate. |
| Synthetic data script errors on FK violation | Make sure step 1 (DDL) fully succeeded and no tables were partially created; rerun DDL from a clean schema. |
| Gateway pipeline immediately fails | Check `wal_level` is actually `logical` (may require a Lakebase instance restart) and that the `databricks_replication` user's password matches what's stored in the UC connection. |
| App can't connect to Postgres | Confirm the Lakebase resource is attached to the app in **Resources**, and that `PGSSLMODE` isn't set to `disable` in a context that requires TLS. |
| Ingestion trigger button fails with 403 | The `DATABRICKS_TOKEN` secret's principal needs `CAN_MANAGE`/`CAN_RUN` on the pipeline. |
| Silver pipeline has no data | Confirm the Bronze ingestion pipeline (step 4) has completed at least one successful update before starting Silver. |

---

## Assumptions made / things to verify

- **Table columns**: invented from a standard CDMS domain model since the
  requirement doc only listed table names. Cross-check against your actual
  source system if one exists.
- **Lakeflow Connect for PostgreSQL** is a managed CDC connector (gateway +
  ingestion pipeline pair), not a single REST call — hence both a declarative
  (Asset Bundle) and scripted (SDK) path are provided, matching how Databricks
  documents this connector today.
- **App framework**: built with Streamlit since it's a common, well-supported
  choice for Databricks Apps; `db.py` has no Streamlit-specific code, so
  swapping to Dash/Flask/Gradio only requires rewriting `app.py`.
- **Catalog/schema names** (`digital_cro.bronze_edc`, `digital_cro.silver_edc`,
  `digital_cro.gold_edc`) follow the requirement doc's Bronze naming; Silver/Gold
  schema names are a convention used here and can be renamed freely.

---

## Future enhancements

Per the requirement doc's "Future Enhancements" section:
- **CDC** — already partially addressed via Lakeflow Connect's logical
  replication; extending Silver from snapshot-style dedup to true incremental
  `MERGE` would reduce reprocessing cost further.
- **Data Quality** — expand the `EXPECT` constraints in
  `silver_transformations.sql` and route violations to a quarantine table.
- **Unity Catalog Lineage** — no action needed to enable it, but worth
  reviewing the auto-generated lineage graph once Bronze/Silver/Gold are live.
- **AI/BI Dashboards** — the Gold materialized views are designed to be
  queried directly from Databricks AI/BI (formerly Databricks SQL dashboards).
- **Monitoring & Alerting** — `monitor_pipeline.py` is a starting point; wire
  it into a scheduled Job with failure notifications, or extend
  `postgresql_job.yml`'s `email_notifications` block.
