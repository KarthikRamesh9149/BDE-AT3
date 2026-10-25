# Validation record

These entries describe the checkpoints on their stated dates. See the [README completion record](../README.md#recorded-completion) for the final project status.

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

## Part 1 completion checkpoint

At the end of Part 1, the cloud contained only the verified May raw data. The dbt completion ledger was empty. The following records describe the subsequent Part 2 work.

## Part 2 local failure and recovery tests, 22 October 2026

The final May/June integration test passed in 209.66 seconds using dbt-core 1.12.6, dbt-postgres 1.11.0 and the disposable local PostgreSQL 16.15 instance with SSL. Every complete build passed 47 data tests across 19 models and two snapshots. Its CLI total of 70 includes two project hooks.

The test executed the real DAG loading logic and the new single-command dbt build. It confirmed:

- May counts of 37,562 facts, 37,562 Property versions and 27,273 Host versions.
- 9,002 independent May metric comparisons calculated directly from CSV data.
- Actual execution ordering: Silver business tests precede the snapshot preparation model, snapshot tests precede the Gold preparation model, and all Gold branches follow that shared dependency.
- Unchanged snapshot/fact fingerprints after same-month retries.
- Rejection of an excluded required test before model writes.
- Rejection of changed Bronze values without changing snapshots or Gold.
- An injected Property snapshot write failure leaves Gold at May and records no June completion. A retry after removing the injected failure correctly completes June.
- 74,463 facts after June and 18,212 independent metric comparisons across May/June, followed by an unchanged June retry.

These are local tests. Storage is a local object-interface fixture, and the cloud job/run IDs in the fixture are deliberately simulated. They do not prove hosted dbt execution. Genuine failed test attempts were retained with the later passing evidence.

## Twelve-month local rehearsal with explicit month arguments, 22 October 2026

`tests/test_full_year.py` passed in 1,074.40 seconds. It alternated the actual DAG loading code and the full dbt build for every month from May 2020 through April 2021. All twelve builds passed their 47 data tests and recorded local fixture completions. The final fact count was 412,122.

The independent CSV calculations matched 186,552 datamart metric values. The completed views contained 348 neighbourhood groups, 8,870 property-type groups and 361 host-neighbourhood groups. The test specifically confirmed that the 4,780 July rows with a September scrape date remained July observations.

All seven statements in `verification.sql` were then executed read-only against that local database. Each month's Bronze and fact counts matched the source ledger. All twelve completion records matched their manifests and verification timing. Host-version, Property-version, stored-key and duplicate-listing-month error counts were all zero. Hashes of all 40 dbt source/configuration files were unchanged across the verified final candidate.

This rehearsal does not insert data or completion records in Cloud SQL. The deployed cloud database still has only May Bronze data at this checkpoint.

## Hosted configuration, 22 October 2026

The `BDEAT3` connection targets the new database. The Development environment uses that connection, v1 Latest, SSL required and `connect_timeout: 10`. Its saved user credentials retain schema `dbt_kramesh`, target `dev` and one thread. The dbt Cloud connection test completed successfully. Production uses separate canonical schemas selected by profile schema `bdeat3`.

`BDEAT3 Production` (environment `70506183164733`) was saved on v1 Latest with connection profile `default`. The manual deployment job `BDEAT3 monthly build` (job `70506183143171`) was saved with one May `dbt build` command, one thread, target `prod`, no schedule, no job-completion trigger, no deferral, and a 3,600-second timeout.

The previous managed repository was disconnected after its source and starter archive had been backed up. GitHub account authorisation and the dbt Cloud app installation were completed, with repository access limited to `KarthikRamesh9149/BDE-AT3`. That private repository is linked to the dbt project. The reviewed deployment revision was set to `859be2d9178bf3bac904942fb9814ef45cf9aeb9` and independently matched to the production run's actual checkout.

## Hosted May builds and independent checks, 22 October 2026

The isolated Studio build succeeded in 3 minutes 57 seconds, invocation `c9e955bb-437c-4b7c-9d15-0ecbd7d27c12`. It used target `dev` and the separate development schemas; it did not record a production completion.

Saved production run [`70506184016163`](https://lk965.us1.dbt.com/deploy/70506183170770/projects/70506183174422/runs/70506184016163) succeeded in 4 minutes 6 seconds. Its build invocation was `e529c0fd-8fb4-49c9-bfc1-89337dce3ab5`. The actual v1 Latest runtime reported `2026.10.14+638bb1a`. The retained `run_results.json` contains 19 successful models, two successful snapshots, 47 passing tests and two successful project hooks, totalling 70 resources with no errors, warnings or skipped resources. This is 47 tests, not 70 tests.

Read-only checks ran against the canonical Cloud SQL schemas through DBeaver. After its CSV export stalled, the same checks and the complete datamart values were retrieved through dbt Studio with `dbt show`; that command did not rebuild models or alter the production completion ledger. The stalled read-only export connection was closed, and DBeaver was restarted.

| Check | Actual Cloud SQL result |
|---|---|
| Database and encryption | `BDEAT3`, SSL active |
| Loaded and completed months | May 2020 only |
| May source / Bronze / fact rows | 37,562 / 37,562 / 37,562 |
| Host / Property snapshot versions | 27,273 / 37,562 |
| Host-version, Property-version, stored-key and duplicate-listing-month errors | Zero for all four checks |
| Completion record | Real saved job/run IDs, 47 tests, matching manifest and reviewed revision; completion after Airflow verification |
| Datamart groups | 29 listing neighbourhoods, 412 property-type groups, 30 host-neighbourhood groups |
| Independent source-CSV comparison | All 9,002 May datamart values matched; numeric tolerance below 0.00000001 |

These hosted results establish the Part 2 May checkpoint. Later documentation-only commits do not change the tested implementation; any final rerun must still match its actual checkout to the declared deployment revision and preserve the May facts and snapshot histories.

## Part 3 handover

The Part 3 runbook is prepared. The eleven remaining CSVs were checked locally but have not been uploaded to the new prefix or loaded into `BDEAT3`. For each month, alternate one chronological Airflow load with one successful saved dbt job and verification. The twelve-month local rehearsal is separate evidence and does not mean the cloud months have run. Part 4 is deferred.

## Part 3 June load, 22 October 2026

The later Part 3 checkpoint supersedes the upload status above. All twelve listing CSVs are now present in `data/at3_rebuild/listings/`. Airflow run `manual__2026-10-22T15:21:50+11:00` succeeded for June. All three tasks succeeded and the verification log reported 36,901 source rows with exact comparisons passed. June's saved dbt job and its independent database checks remain pending. Do not trigger July before June is complete.

## Automatic month selection: local evidence, 22 October 2026

The change is confined to the month resolver in `macros/silver_helpers.sql` and its use in `macros/build_control.sql`. The DAG, models, snapshots, initial setup SQL, source manifest and project hooks are unchanged. Omitting the month variable selects the latest committed Bronze month, requires its Airflow verification, and records that month for reuse throughout the build. Explicit month arguments still work subject to the existing chronology checks.

The two targeted integration tests passed in 187.36 seconds against a disposable PostgreSQL 16.15 instance with SSL, using dbt-core 1.12.6 and dbt-postgres 1.11.0. They built May using the prior revision `c7cf2fcd1e71d939b808e6bf9c5e75a12cc98610`, then applied the new command. They verified:

- An automatic May retry preserved raw values, snapshot and fact contents, and snapshot table identities.
- Empty, unverified or mismatched load records were rejected. An unverified June load could not fall back to May.
- Stale explicit months, skipped months, excluded required tests, full refresh and multiple threads were rejected.
- An injected June snapshot failure prevented a completion record and blocked the next Airflow month. A June retry completed successfully.
- Earlier fact and dimension attributes remained unchanged as June closed the appropriate historical validity intervals. Repeating June preserved the complete snapshot and fact contents.
- All 18,212 independently calculated May/June datamart values matched. Complete builds passed 47 tests across 19 models and two snapshots, plus two project hooks. Dependency-order assertions also passed.

A subsequent regression run finished with 16 tests passed and one failed in 1,009.47 seconds. The failure was in the automatic full-year rehearsal: May through December completed successfully, ending with 278,682 facts and eight verified monthly completions in the disposable database. January's datamart reconciliation failed because the Docker virtual disk ran out of space while PostgreSQL wrote a temporary file. January did not receive a successful completion record. This is not a completed January or twelve-month pass.

A replacement rehearsal used a separate host-backed data directory with more space. The replacement rehearsal stopped after 15 tests passed; the active test was interrupted. No further full-year result is claimed. The earlier twelve-month result in this document used explicit month arguments and remains distinct from the automatic-month change.

The local tests used the supplied CSV files and real PostgreSQL execution, with simulated object storage and test-only cloud identities. Cloud SQL, the Airflow DAG and dbt Cloud jobs were not changed or run by these tests. Hosted verification of the new selector remains pending. The existing saved job must use the new repository revision and its matching `DBT_DEPLOYMENT_GIT_SHA` before removing the explicit month argument.

After stopping the rehearsal, the original local PostgreSQL container and volume were restarted. Read-only counts and whole-row fingerprints matched the saved pre-test values for both snapshot tables and the fact table: 304,937 Host versions, 412,122 Property versions and 412,122 facts. The temporary rehearsal container is stopped.
