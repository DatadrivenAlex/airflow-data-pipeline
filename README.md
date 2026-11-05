# USGS earthquake ELT

Orchestrated extract, load, and transform for the public [USGS earthquake catalog](https://earthquake.usgs.gov/fdsnws/event/1/). Apache Airflow schedules a daily TaskFlow DAG. The warehouse is a local DuckDB file, or Postgres when `WAREHOUSE_URL` is set. Staging and marts are plain SQL files, run by Python, laid out the way a dbt project splits `staging/` and `marts/`.

This is a portfolio project by Alexandru Cojocari for junior Python and data engineer roles.

The pipeline does not need an API key. A committed CSV of January 2024 magnitude 4.5+ events is the offline dataset, and it is also the fallback when the USGS feed cannot be reached.

## Architecture

```mermaid
flowchart LR
  subgraph sources [Sources]
    API["USGS FDSN event API"]
    Sample["Bundled January 2024 CSV"]
  end
  subgraph orch [Orchestration]
    DAG["Airflow DAG usgs_earthquakes_elt"]
  end
  subgraph warehouse [Warehouse]
    Raw["raw.earthquakes"]
    Stg["staging.stg_earthquakes"]
    Daily["marts.mart_daily_seismicity"]
    Region["marts.mart_region_summary"]
    Sig["marts.mart_significant_events"]
  end
  API --> DAG
  Sample --> DAG
  DAG --> Raw
  Raw --> Stg
  Stg --> Daily
  Stg --> Region
  Stg --> Sig
  Daily --> Checks{"Quality checks"}
  Region --> Checks
  Sig --> Checks
  Checks -->|pass| Done["DAG success"]
  Checks -->|fail| Stop["DAG failed"]
```

Raw keeps the USGS columns as text, plus a parsed UTC `event_time` and load metadata. Staging keeps one earthquake per event id, using the latest `updated` revision, and drops non-earthquake types such as quarry blasts. Marts are rebuilt from the full staging table on every run: daily seismicity, a region summary, and events at magnitude 5 and above.

```mermaid
flowchart LR
  extract --> load_raw --> build_staging --> build_marts --> run_quality_checks
```

`extract` and `load_raw` retry twice, with exponential backoff starting at 5 minutes. `run_quality_checks` does not retry. Every task has a one-hour SLA and a 30-minute execution timeout. The DAG run itself times out after two hours. `catchup` is off and `max_active_runs` is 1, so turning the DAG on does not replay history and two runs cannot write the warehouse at once.

## Layout

```text
dags/usgs_earthquakes_elt.py          TaskFlow DAG
pipeline/                             extract, load, SQL runner, quality checks, CLI
pipeline/transforms/staging/          one SELECT per model
pipeline/transforms/marts/
pipeline/transforms/schema.yml        row, null, unique, freshness, and mart checks
data/samples/usgs_earthquakes.csv     committed USGS extract, used offline
tests/                                offline pytest suite
docker-compose.yml                    Airflow 2.10 LocalExecutor
.github/workflows/ci.yml              ruff and pytest
```

## Quickstart

Python 3.12. No API key.

```bash
python3 -m venv .venv
source .venv/bin/activate
make install
make test
make elt
```

`make install` installs Airflow 2.10.5 with the official constraint file for Python 3.12, plus DuckDB and psycopg2. `make test` does not call USGS. `make elt` is `python -m pipeline run --offline`.

The CLI defaults to the bundled sample. To request a live window instead:

```bash
python -m pipeline run --online --start 2024-01-01 --end 2024-01-02
```

`--end` is exclusive. Add `--no-sample-fallback` if a failed download should fail the command instead of loading the sample.

### Airflow on your machine

Docker Compose v2. The metadata database is Postgres. The warehouse stays DuckDB unless you opt into the warehouse profile.

```bash
cp .env.example .env
make up
```

The first build installs DuckDB into the Airflow image and migrates the metadata database. Then open <http://localhost:8080> and sign in with `airflow` / `airflow`. That password is for this local demo only. Unpause `usgs_earthquakes_elt`.

`.env` ships with `ELT_OFFLINE=1`, so the scheduled run replays the sample and does not need network access. Set `ELT_OFFLINE=0` and restart the stack to query USGS. `ELT_ALLOW_SAMPLE_FALLBACK=0` makes a failed download fail the extract task instead.

Optional Postgres warehouse, on port 5433 so it does not collide with the Airflow metadata database on 5432:

```bash
docker compose --profile warehouse-postgres up -d
```

Then set `WAREHOUSE_URL=postgresql://warehouse:warehouse@warehouse-postgres:5432/warehouse` in `.env` and recreate the Airflow containers. `warehouse` / `warehouse` is a local demo password.

`make down` stops the stack and keeps the metadata volume. `make logs` follows the scheduler and webserver.

## Sample output

This is the stdout of `python -m pipeline run --offline`, captured on 2026-11-05 against the committed CSV. `load_id` and the freshness lag are from that process. A later run gets a new load id and a new lag. The catalog counts stay the same while `data/samples/usgs_earthquakes.csv` is unchanged.

```text
USGS earthquake ELT
run_source: sample
overwrite_window: false
window_start: 2024-01-01 00:00:00
window_end: 2024-02-01 00:00:00
load_id: af331070e147483e9ca013e5be9d3a87
rows_in_file: 623
rows_landed: 623
raw_rows: 623
staging_rows: 623
daily_rows: 31
region_rows: 91
significant_rows: 133
max_magnitude: 7.5
audit_status: success
quality: pass (13 checks)
- staging_min_rows: rows=623 min_rows=1
- staging_event_id_not_null: nulls=0
- staging_event_time_not_null: nulls=0
- staging_latitude_not_null: nulls=0
- staging_longitude_not_null: nulls=0
- staging_magnitude_not_null: nulls=0
- staging_event_id_unique: duplicate_keys=0
- daily_event_date_unique: duplicate_keys=0
- region_name_unique: duplicate_keys=0
- staging_freshness: max_extracted_at=2026-11-05 22:00:00 lag_seconds=1 max_lag_hours=36
- daily_mart_reconciles: sum=623 count=623
- region_mart_reconciles: sum=623 count=623
- significant_mart_reconciles: rows=133 filtered=133

mart_daily_seismicity
event_date | event_count | avg_magnitude | max_magnitude | avg_depth_km | significant_event_count
-----------+-------------+---------------+---------------+--------------+------------------------
2024-01-01 | 49          | 4.86          | 7.5           | 45.72        | 13                     
2024-01-02 | 15          | 4.75          | 5.4           | 38.47        | 3                      
2024-01-03 | 28          | 4.75          | 5.5           | 47.20        | 6                      
2024-01-04 | 29          | 4.64          | 5.7           | 128.18       | 2                      
2024-01-05 | 18          | 4.69          | 5.1           | 66.03        | 2                      
2024-01-06 | 17          | 4.74          | 5.6           | 30.23        | 2                      
2024-01-07 | 22          | 4.79          | 5.6           | 28.60        | 5                      
2024-01-08 | 15          | 4.81          | 6.7           | 29.58        | 3                      
2024-01-09 | 15          | 4.92          | 5.9           | 65.74        | 5                      
2024-01-10 | 11          | 4.74          | 5             | 75.24        | 3                      
2024-01-11 | 14          | 4.79          | 6.4           | 106.06       | 2                      
2024-01-12 | 26          | 4.79          | 5.9           | 60.28        | 6                      
2024-01-13 | 13          | 4.67          | 5.2           | 82.49        | 1                      
2024-01-14 | 11          | 4.67          | 5.1           | 219.62       | 1                      
2024-01-15 | 19          | 4.82          | 5.2           | 65.25        | 7                      
2024-01-16 | 15          | 4.56          | 4.8           | 117.41       | 0                      
2024-01-17 | 12          | 4.77          | 5.6           | 56.31        | 4                      
2024-01-18 | 23          | 4.85          | 6.4           | 102.24       | 5                      
2024-01-19 | 19          | 4.78          | 5.6           | 38.33        | 4                      
2024-01-20 | 19          | 4.97          | 6.6           | 66.26        | 6                      
2024-01-21 | 13          | 4.68          | 5.3           | 106.54       | 2                      
2024-01-22 | 39          | 4.87          | 7             | 31.79        | 11                     
2024-01-23 | 33          | 4.76          | 6.3           | 78.99        | 7                      
2024-01-24 | 26          | 4.73          | 5.3           | 41.02        | 9                      
2024-01-25 | 26          | 4.82          | 5.6           | 50.39        | 7                      
2024-01-26 | 13          | 4.72          | 5.4           | 43.66        | 2                      
2024-01-27 | 22          | 4.86          | 6.1           | 71.81        | 8                      
2024-01-28 | 20          | 4.76          | 6.5           | 115.61       | 2                      
2024-01-29 | 15          | 4.68          | 5.7           | 90.45        | 2                      
2024-01-30 | 11          | 4.73          | 5.1           | 132.17       | 2                      
2024-01-31 | 15          | 4.71          | 5.8           | 60.73        | 1                      

mart_region_summary (top 10 by event_count)
region                        | event_count | avg_magnitude | max_magnitude
------------------------------+-------------+---------------+--------------
Japan                         | 72          | 4.81          | 6.2          
Indonesia                     | 50          | 4.75          | 5.6          
China                         | 46          | 4.83          | 7            
Philippines                   | 38          | 4.74          | 6.7          
Tonga                         | 31          | 4.95          | 6.4          
Papua New Guinea              | 27          | 4.75          | 5.4          
Chile                         | 19          | 4.76          | 5.4          
Vanuatu                       | 19          | 4.84          | 6.3          
South Sandwich Islands region | 18          | 4.78          | 5.6          
Northern Mariana Islands      | 16          | 4.79          | 6.1          

mart_significant_events (top 8 by magnitude)
event_time              | magnitude | region           | place                                
------------------------+-----------+------------------+--------------------------------------
2024-01-01 07:10:09.476 | 7.5       | Japan Earthquake | 2024 Noto Peninsula, Japan Earthquake
2024-01-22 18:09:04.340 | 7         | China            | 128 km WNW of Aykol, China           
2024-01-08 20:48:42.361 | 6.7       | Philippines      | 93 km SE of Sarangani, Philippines   
2024-01-20 21:31:05.092 | 6.6       | Brazil           | 123 km NW of Tarauacá, Brazil        
2024-01-28 09:38:56.538 | 6.5       | Brazil           | 70 km W of Tarauacá, Brazil          
2024-01-11 09:20:26.337 | 6.4       | Afghanistan      | 45 km SSW of Jurm, Afghanistan       
2024-01-18 22:12:21.755 | 6.4       | Tonga            | 148 km NW of Fangale’ounga, Tonga    
2024-01-23 14:33:45.022 | 6.3       | Vanuatu          | 39 km SW of Port-Vila, Vanuatu       
```

The sample file is the USGS CSV for `starttime=2024-01-01`, `endtime=2024-02-01` (exclusive), `minmagnitude=4.5`, `orderby=time-asc`, saved on 2026-11-05.

```text
sha256  38d2ad3737f679210fd7a0fc6828857f51818528a8157f29680c105b1538853d
bytes   117640
```

USGS sometimes revises an event months later, so downloading that URL again can change magnitudes or the `updated` column. `make refresh-sample` replaces the file. Re-run the tests after a refresh: they compare the SQL output with an independent aggregation of whatever CSV is committed, and they do not hard-code the January counts.

USGS earthquake data is in the public domain. This repository's code is MIT. The license does not relicense the sample.

## Runbook

`catchup` is false. History is loaded only when you backfill or test a logical date. For this daily timetable, logical date `2024-01-15` is the half-open data interval `[2024-01-15, 2024-01-16)`. `airflow dags backfill` treats `--start-date` and `--end-date` as inclusive logical dates.

### Backfill

Inside the Compose scheduler, for 1–7 January 2024:

```bash
make backfill START=2024-01-01 END=2024-01-07
```

That runs:

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill usgs_earthquakes_elt \
  --start-date 2024-01-01 --end-date 2024-01-07 --reset-dagruns -y
```

With `ELT_OFFLINE=1`, every logical date upserts the full January sample by event id and does not delete a day partition. Replays stay idempotent, and they do not pretend each logical date was that day's USGS extract. With `ELT_OFFLINE=0`, each logical date downloads that interval and replaces rows whose `event_time` falls in it.

To practice the partition overwrite without Airflow or USGS:

```bash
python -m pipeline run \
  --from-csv data/samples/usgs_earthquakes.csv \
  --start 2024-01-01 --end 2024-01-02 \
  --min-magnitude 0
```

A second run of the same window replaces that day. A run for `--start 2024-01-02 --end 2024-01-03` leaves 1 January in place.

### Rerun one logical date

`airflow dags test` runs the DAG in the container process, which is the quick way to see task logs:

```bash
make rerun DATE=2024-01-15
```

To have the scheduler run it, with the configured retries, clear the task instances and let the scheduler create the work again:

```bash
make clear DATE=2024-01-15
```

Clearing is safe to repeat. The load deletes the incoming event ids before insert, and a live window also deletes that time partition before insert.

### When a run fails

Quality checks run after the warehouse commit. A failure raises `DataQualityError`, lists every failed check, and sets `meta.load_audit.status` to `quality_failed`. The quality task is not retried, because the same rows would fail again. The rows stay in DuckDB so you can look at them:

```bash
python -c "import duckdb; con = duckdb.connect('data/warehouse/earthquakes.duckdb'); print(con.sql('select status, rows_landed, record_source, loaded_at from meta.load_audit order by loaded_at desc limit 5'))"
```

Read the check id in the task log, fix the extract or the threshold in `pipeline/transforms/schema.yml`, then clear the DAG run. `staging_min_rows` rejects an empty staging table, including a window whose events were all non-earthquakes. Lower `min_rows` only if an empty but valid window should pass.

Extract and load retry twice. A USGS timeout with `ELT_ALLOW_SAMPLE_FALLBACK=1` does not use those retries: the extract task logs a warning and loads the bundled sample without deleting the requested partition, so an outage does not wipe a day that already loaded. Set `ELT_ALLOW_SAMPLE_FALLBACK=0` when a download failure should fail the DAG after the retries. Downstream tasks are skipped when extract fails outright.

An SLA miss is logged by `log_sla_miss`. On Airflow 2 that callback does not mark the task failed. The execution timeout and the DAG-run timeout do fail the run.

## Tests

GitHub Actions runs ruff and pytest on Python 3.12. The job starts Postgres and sets `POSTGRES_TEST_URL`, so the two warehouse tests run there. Without that variable they skip, and the rest of the suite still runs on DuckDB.

The suite covers:

- CSV parsing, revision collapse, and the half-open window filter
- offline extract that does not open a socket, live-download success, and sample fallback
- idempotent reloads, magnitude revisions, and partition overwrite that leaves other days alone
- staging and mart SQL against an independent aggregation of the committed CSV
- quality failures for nulls, duplicate ids, empty tables, stale `extracted_at`, and mart totals that do not match staging
- DAG import, task order, no cycles, retries, SLA, and the quality task's `retries=0`

Locally, with Postgres on `localhost:5432`:

```bash
POSTGRES_TEST_URL=postgresql://warehouse:warehouse@127.0.0.1:5432/warehouse make test
```

## Limitations

- Airflow 2.10 LocalExecutor. This is not Airflow 3, Celery, or Kubernetes.
- Staging and marts are full refreshes of the current raw table. Incremental behavior is only on the raw load: event-id upsert, plus a time-partition delete for API and `--from-csv` windows.
- Offline mode and the API fallback replay one static month. They do not invent "today's" earthquakes, and they do not delete the logical date's partition.
- Freshness is ingestion freshness on `extracted_at`, compared with the check clock, default 36 hours. It is not "the newest earthquake is recent," because the bundled file is historical. A load that did not run inside that lag fails the DAG.
- Region is the text after the first comma in the USGS `place` string. `128 km WNW of Aykol, China` becomes `China`. The M7.5 on 1 January 2024 is stored by USGS as `2024 Noto Peninsula, Japan Earthquake`, so the mart labels that row `Japan Earthquake` rather than `Japan`. Places with no comma, such as `South of the Fiji Islands`, stay whole.
- The Postgres path uses the same SQL as DuckDB and is covered when `POSTGRES_TEST_URL` is set. DuckDB is the default and the path the Airflow container uses unless you set `WAREHOUSE_URL`.
- CI does not boot the Compose stack. The tests call the Python functions the tasks call, and they import the DAG to check that it parses and is acyclic.
- There is no paging, alerting, or data catalog beyond the Airflow task log and `meta.load_audit`. `email_on_failure` is off because the demo has no SMTP server.
- The local Fernet key and the `airflow` / `warehouse` passwords in `.env.example` are for a laptop demo. Do not reuse them anywhere else.

## License

[MIT](LICENSE). Copyright 2026 Alexandru Cojocari.
