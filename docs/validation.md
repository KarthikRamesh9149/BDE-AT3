# Part 1 validation

## Local checks, 21 October 2026

The BDEAT3 candidate passed 15 tests in 133.47 seconds against PostgreSQL 16.15 with SSL. The tests imported the actual Airflow DAG using Python 3.11.15, Airflow 2.11.0, PostgreSQL provider 6.2.0, Google provider 15.1.0 and psycopg2-binary 2.9.10.

The tests cover:

- Manual scheduling, paused initial state and task dependency order.
- Empty initial tables, refusal to reset an existing workload and rejection of the wrong database.
- May source counts, unchanged retries and rejection of June before the required dbt completion.
- A fixed month for each Airflow run and rejection of an older run after advancement.
- Rollback after an injected failure following multiple table writes.
- Rejection of an object replaced after validation and of changed stored source values.
- Preservation of empty strings, literal backslash-N, quotes, newlines, Unicode and duplicate rows.
- Invalid CSV headers, row widths, zero bytes, row counts and UTF-8.
- All twelve source files and a full Bronze rehearsal totalling 412,122 rows across twelve months; a thirteenth month is rejected.

The full rehearsal uses the actual supplied CSVs and PostgreSQL SQL. Storage access is a local object-interface fixture, and dbt success markers are explicitly test-only fixtures. These checks do not establish that a GCS download, Composer task or dbt Cloud job has succeeded.

## Run the integration tests

Use a disposable local PostgreSQL 16.15 server on `127.0.0.1:55433`, SSL enabled, with the `postgres` account permitted to connect. The tests drop and recreate the local database `BDEAT3`. Never forward this port to a real assignment or production database.

Install the test requirements in a Python 3.11 environment using the published Airflow 2.11.0 constraints for Python 3.11. Set `AT3_SOURCE_ROOT` to the local directory containing `airbnb/`, `census/` and `lga/`, then run:

```sh
python -m pytest tests/test_part1.py -q
```

## Cloud preparation checked, 21 October 2026

The whole `part_1.sql` script was executed through DBeaver against Cloud SQL database `BDEAT3`. Its results showed `ssl_active = true`, all five Bronze row counts equal to zero, and zero dbt completion records. The original `bigdataat3` database remains a separate database.

The saved Airflow connection `postgres_bdeat3` targets `BDEAT3` at the Cloud SQL private endpoint, with `sslmode=require` and a 10-second connection timeout. Airflow reports encrypted password and Extra fields. Composer connectivity will be established by the first actual task run.

Cloud Storage lists only May 2020 in the new listings prefix, plus the two Census and two LGA reference files in their own subfolders. The five prepared upload files match the committed source manifest. Cloud byte hashes will be checked by `validate_sources` before any raw-table writes.

The new DAG was uploaded to the Composer bucket and parsed by Airflow 2.11.1+composer at 23:37:59 AEDT. Its Details view reports no import errors, paused state true, catchup false, one maximum active run and external triggers only. The graph has exactly `validate_sources -> load_bronze -> verify_bronze`. The run grid is empty.

The complete code text displayed by Airflow matches the tested local DAG exactly (39,671 bytes; SHA-256 `7da23c541fa68d432ebd7c2a1cdee69a951336791722dc71dcecdbad8b5527fe`).

## Monthly execution still required

The May DAG has not been triggered. After triggering it in the next checkpoint, retain the actual task results and independent DBeaver counts. The local rehearsal does not replace those checks. The new dbt project contents, deployment configuration and saved job have not yet been implemented or run.

The cloud environment uses Airflow 2.11.1 with its Composer provider set. A local import confirms compatibility with the tested environment only; the deployed DAG must also parse in Composer.
