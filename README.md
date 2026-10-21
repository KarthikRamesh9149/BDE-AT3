# BDE-AT3

Airbnb and Census ELT pipeline for Big Data Engineering Assignment 3.

Part 1 is complete in PostgreSQL database `BDEAT3`. The May 2020 cloud load succeeded on 21 October 2026; independent DBeaver checks confirmed all five source counts and contents. The dbt warehouse and validation controls are implemented and locally tested. Hosted May execution remains pending GitHub connection authorisation and final deployment configuration. Part 3 cloud loads and Part 4 analysis have not started.

## Files

| File | Purpose |
|---|---|
| `part_1.sql` | Guarded setup and separately labelled post-May verification SQL, run in DBeaver |
| `dags/airbnb_census_monthly_rebuild.py` | Unscheduled monthly load and exact source reconciliation |
| `source_manifest.json` | SHA-256 hashes and row counts of the sixteen supplied CSVs |
| `tests/test_part1.py` | Local PostgreSQL integration and failure tests |
| `dbt_project.yml`, `models/`, `snapshots/`, `macros/` | Silver, timestamp snapshots, Gold, datamart views and monthly validation controls |
| `tests/test_part2.py`, `tests/test_full_year.py` | Local dbt failure/retry tests and twelve-month rehearsal |
| `tests/independent_metrics.py` | Independent calculations from source CSVs for all datamart metrics |
| `verification.sql` | Read-only post-job checks for DBeaver |
| `docs/part_1_setup.md` | Connection, input, execution and recovery instructions |
| `docs/part_2_setup.md` | Development/production targets and the single-command monthly job |
| `docs/part_3_runbook.md` | Remaining monthly sequence, counts and recovery checklist |
| `docs/brief_coverage.md` | Mapping between requirements and project files |
| `docs/validation.md` | Local test scope, actual cloud results and remaining work |

The saved manual deployment job is `BDEAT3 monthly build` in `BDEAT3 Production`. Its single May command is:

```sh
dbt build --threads 1 --fail-fast --vars '{"observation_month":"2020-05-01"}'
```

The cloud completion ledger remains empty until the hosted job passes. Each later Airflow month requires the preceding month's verified Bronze load and recorded successful dbt deployment job. After each monthly load, change the job's month variable, manually run the job and wait for Success before the next load.

## Design

```mermaid
flowchart LR
  A[Five validated source objects] --> B[Choose and pin one month]
  B --> C[Stage and reconcile raw values]
  C --> D[Commit Bronze and load records together]
  D --> E[Verify committed Bronze]
  E --> F[Manually run saved dbt job]
  F --> G[Verified completion record permits next month]
  G --> B
```

All source fields remain text in Bronze. Filename months establish observation dates; the supplied scrape dates remain unchanged. Silver performs cleaning and typing. Host and Property use timestamp snapshots. Static geographic and Census references do not require SCD history. The four Gold dimensions are Host, Property, LGA and Month. The fact has one row per observed listing-month; the three datamart views join the dimension versions valid for that month.

The database and source prefix are separate from earlier work. Database identity, SSL, table structure, source hashes, object generations and chronological history are checked before loading. Identical retries verify existing rows without changing their load timestamps. A failed write rolls back all raw-table and load-ledger writes in that transaction.
