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

The setup SQL, now labelled Section A in `part_1.sql`, was executed through DBeaver against Cloud SQL database `BDEAT3`. Its results showed `ssl_active = true`, all five Bronze row counts equal to zero, and zero dbt completion records. The original `bigdataat3` database remains a separate database.

The saved Airflow connection `postgres_bdeat3` targets `BDEAT3` at the Cloud SQL private endpoint, with `sslmode=require` and a 10-second connection timeout. Airflow reports encrypted password and Extra fields. Connectivity was subsequently established by the successful May tasks below.

Cloud Storage listed only May 2020 in the new listings prefix, plus the two Census and two LGA reference files in their own subfolders. The five prepared upload files match the committed source manifest. The May `validate_sources` task subsequently confirmed all five cloud byte hashes before writing raw tables.

The new DAG was uploaded to the Composer bucket and parsed by Airflow 2.11.1+composer at 23:37:59 AEDT. Before execution, its Details view reported no import errors, paused state true, catchup false, one maximum active run and external triggers only. The graph has exactly `validate_sources -> load_bronze -> verify_bronze`. Its run grid was empty at that preparation checkpoint.

The complete code text displayed by Airflow matches the tested local DAG exactly (39,671 bytes; SHA-256 `7da23c541fa68d432ebd7c2a1cdee69a951336791722dc71dcecdbad8b5527fe`).

## May cloud execution, 21 October 2026

Run `manual__2026-10-21T23:56:47+11:00` was manually triggered with `mode=next`. The run began at 23:57:41 AEDT and finished successfully at 23:58:04 AEDT. All three tasks succeeded on the first attempt. The logical date in the run ID precedes the actual start time.

`validate_sources` downloaded the five actual GCS objects and matched their SHA-256 hashes and row counts to `source_manifest.json`. Their object generations were recorded in the run manifest. `load_bronze` and `verify_bronze` both reported that exact source comparisons passed. Verification's `raw retry=True` log field means it compared already committed rows without replacing them; it does not indicate a second Airflow run or a failed task attempt.

Independent DBeaver queries on 22 October checked the committed source values against fingerprints calculated directly from the prepared CSVs, including duplicate multiplicities. Every count and content comparison passed:

| Table | Rows | Count and source values |
|---|---:|---|
| `bronze.raw_airbnb` | 37,562 | Match |
| `bronze.raw_census_g01` | 132 | Match |
| `bronze.raw_census_g02` | 132 | Match |
| `bronze.raw_lga_code` | 129 | Match |
| `bronze.raw_lga_suburb` | 4,470 | Match |

Nine further checks returned true: database `BDEAT3`, active SSL, only May listings, one verified month, one verified run, five source records with object generations, an empty dbt completion ledger, no Silver/Gold schemas yet, and DATE-typed source-month metadata. The exact content and state queries are retained in Section B of `part_1.sql` for the Part 1 checkpoint.

The original database was checked before and after the load. Counts and whole-row fingerprints remained identical for its five Bronze tables, Host and Property snapshots, and listing-month fact table. This verifies the eight checked relations; it is not a claim that every object or shared server resource was independently audited.

The deployed DAG and setup statements were unchanged during this checkpoint. The existing 15 local test results still apply to them. The added read-only verification queries were executed successfully against Cloud SQL through DBeaver.

## Next checkpoint

Part 1 is complete. Part 2 must implement and test the new dbt project, link the GitHub repository, verify development and deployment destinations, and successfully run the saved May job. No dbt completion record has been inserted. June must not be loaded before that job succeeds. Part 3 and Part 4 remain pending.
