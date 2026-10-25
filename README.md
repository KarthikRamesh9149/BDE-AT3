# Big Data Engineering Assignment 3

This submission combines twelve monthly Sydney Airbnb files, from May 2020 to April 2021, with Census and geographic references. The completed warehouse contains 412,122 listing-month records.

## Where to start

- Read the handover report (`Handover_Report_Karthik_Ramesh.pdf`) in the submission package for the approach, findings, limitations and supporting evidence.
- Use `part_4.sql` from the submission package to review or rerun the five business analyses against the completed database. It is read-only and returns thirteen result sets, including the supporting comparisons.
- Use [verification.sql](verification.sql) to check monthly loads, historical records and warehouse consistency.

All twelve monthly cycles are complete. The setup instructions below explain how the pipeline was deployed; the completed database does not need another load or reset.

## File guide

This guide covers the submission package. The report, `part_4.sql` and `screenshots/` are supplied there; this repository contains the pipeline and dbt project.

| File or folder | Purpose |
| --- | --- |
| `part_1.sql` | Creates the Bronze and pipeline tables; includes separate checks for the first May load. |
| `dags/airbnb_census_monthly_rebuild.py` | Combined Airflow DAG for the initial load and the remaining eleven months. |
| `dbt_project.yml` | Project settings and monthly build hooks. |
| `models/` | Nineteen models, with source definitions and YAML tests. |
| `snapshots/` | Host and Property timestamp snapshots and their definitions. |
| `macros/` | Shared transformation, schema, source-validation and build-control logic. |
| `tests/` | Six standalone SQL tests; together with the YAML definitions and generic test macros, these produce 47 dbt data tests. |
| `source_manifest.json` | Expected row counts and SHA-256 hashes for the sixteen input CSVs. |
| `requirements-dbt.txt` | Pinned dbt Core and PostgreSQL adapter dependencies. |
| `screenshots/` | 26 supporting captures grouped by Parts 1–4. |

Keep the dbt folders and `dbt_project.yml` together at the project root. The models depend on the supplied macros and YAML files. The original assignment CSVs are required for reproduction and are not included in this ZIP. The dbt project repository is [BDE-AT3](https://github.com/KarthikRamesh9149/BDE-AT3).

## Environment settings

| Setting | Value used |
| --- | --- |
| Orchestration | Cloud Composer 3; Airflow `2.11.1+composer`; no automatic schedule |
| Database | Cloud SQL PostgreSQL `16.15`, database `BDEAT3`, SSL required |
| Airflow connection | `postgres_bdeat3`; Extra: `{"sslmode":"require","connect_timeout":10}` |
| DAG | `airbnb_census_monthly_rebuild` |
| Storage | Bucket `australia-southeast1-bigdat-bc43ec38-bucket`; prefix `data/at3_rebuild/` |
| dbt production | Profile `default`, target `prod`, schema setting `bdeat3` |
| dbt development | Schema setting `dbt_kramesh`, keeping development outputs separate |
| Deployment job | `BDEAT3 monthly build`, in `BDEAT3 Production`; one thread, no schedule, 3,600-second timeout |

Store credentials in the platform connection settings. The Airflow connection uses the Cloud SQL private endpoint. Composer needs the Google Cloud Storage and PostgreSQL providers. PostgreSQL 16 or later is required by the validation function `pg_input_is_valid`.

The schema macro uses the production schema setting to write to `silver`, `gold` and `control`. Production builds require genuine dbt Cloud job/run metadata and the production invocation context. Set `DBT_DEPLOYMENT_GIT_SHA` to the reviewed deployment commit and compare it with the checkout revision shown in the run. The completed June–April builds used `f5f0a8dfa9f0280821608ea3a0967883b0c01c34`.

The source checks and target guards are tied to this assignment's database, source files and date range. A different environment or dataset requires coordinated configuration changes and validation.

## Run order

1. **Prepare the empty database.** Connect DBeaver to the authorised `BDEAT3` database using SSL and run Section A of `part_1.sql` once. It stops if the assignment schemas already exist; it is not a reset script.
2. **Upload the first month's inputs.** Under `data/at3_rebuild/`, place `05_2020.csv` in `listings/`, the two `2016Census_G01_NSW_LGA.csv` and `2016Census_G02_NSW_LGA.csv` files in `census/`, and `NSW_LGA_CODE.csv` and `NSW_LGA_SUBURB.csv` in `lga/`. Keep the original files unchanged so their hashes match the manifest.
3. **Load May through Airflow.** Upload the combined DAG to the Composer bucket's `dags/` folder and trigger it with `mode=next`. Wait for `validate_sources`, `load_bronze` and `verify_bronze` to succeed. Run Section B of `part_1.sql` at this May-only checkpoint.
4. **Build the warehouse.** After the verified load, manually run the saved dbt Cloud deployment job with the command below. It selects the latest loaded Bronze month, checks its Airflow verification and keeps that month fixed during the build.

   ```sh
   dbt build --threads 1 --fail-fast
   ```

5. **Check the result.** Confirm that dbt succeeded, then run `verification.sql` in DBeaver. A complete build reports 19 models, two snapshots, 47 data tests and two hooks: 70 successful resources.
6. **Process June through April.** Upload the remaining eleven files to `data/at3_rebuild/listings/`. For each month, complete one Airflow load, one manual dbt build and verification before advancing. Leave the four reference files unchanged and avoid overlapping deployment jobs.
7. **Run the annual analysis.** Once all twelve monthly completion records are present, execute `part_4.sql` in DBeaver. Question (c) groups verified equivalent property names for annual reporting while keeping room type and capacity separate. The warehouse retains the original labels.

## If a monthly cycle fails

- Keep the error details, resolve the cause and retry the same cycle before advancing.
- If the latest month was loaded but needs Airflow verification again, use `mode=retry_latest`. This rechecks that month without advancing. If no month has been committed yet, retry the failed Airflow run.
- If dbt failed after a verified load, rerun the saved dbt job for that month, then repeat the database checks.
- Preserve the snapshots and completion history. Do not manually create completion records, drop snapshots or use `--full-refresh` as a routine recovery step.

Airflow blocks the next month until the previous load is verified and its dbt completion record matches. This keeps monthly facts and historical versions in step.

## Recorded completion

The original April dbt run, `70506184020273`, completed with 70 successful resources; its DBeaver verification passed 29 checks. Run `70506184033463` on 24 October 2026 confirmed the same analytical results. The revised question (c) results were checked on the hosted database on 25 October 2026. That reporting change did not alter the warehouse or the other business analyses.
