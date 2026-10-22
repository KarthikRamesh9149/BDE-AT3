# Brief coverage

This map uses the visible assignment-brief photographs and the supplied teaching-team clarification screenshots. A source file or local test is not evidence of a successful hosted run. Actual cloud status is recorded in `validation.md`.

Current checkpoint: Parts 1 and 2 are complete through May 2020. The GitHub repository is linked, the isolated Studio build and saved production job passed, and the cloud results were independently checked. Part 3 has started with the verified June Airflow load; June's dbt build is still pending. The automatic-month command passed local upgrade, failure/recovery and retry checks, with monthly cycles through December. Its full-year rehearsal is incomplete, as recorded in `validation.md`; hosted execution of this change is still pending. Part 4 and final submission remain deferred.

| Brief requirement | Project implementation / evidence |
|---|---|
| Part 0: twelve Sydney Airbnb months, Census G01/G02 and LGA mappings | `source_manifest.json` records all sixteen supplied CSVs and their expected counts/hashes. |
| Part 1: Composer/Airflow, Cloud SQL PostgreSQL, DBeaver setup | `part_1.sql`, the saved `postgres_bdeat3` connection, and the verified May run in `validation.md`. |
| Part 1: initially upload only May plus references | Five files were present under the new prefix at the May checkpoint. Remaining files are a Part 3 upload. |
| Part 1: manual DAG with no schedule | One combined DAG for Parts 1 and 3; `schedule_interval=None`, `catchup=False`, one active run. |
| Bronze: preserve raw tables | Five text source tables plus separate operational metadata. Source content is checked before and after loading. |
| Silver: clean/type/standardise data | Seven models in `models/silver/`, with date, flag, geography and business-rule tests. Source values and observation lineage remain available. |
| Dimension entities and timestamp snapshots | `snap_host.sql` and `snap_property.sql` use timestamp strategy, stable entity keys and filename-month timestamps. Snapshot inputs contain one row per entity for the selected month. |
| Static reference clarification | LGA and calendar dimensions, suburb mappings and Census references are not snapshotted. |
| At least four Gold dimensions | `dim_host`, `dim_property`, `dim_lga`, `dim_month`. |
| Gold fact contains IDs and metrics | `fact_listing_month`, one row per observed listing-month. |
| Two Census reference tables in Gold | `ref_census_g01`, `ref_census_g02`. |
| Datamarts are views with SCD2 joins | Three `dm_*.sql` view models use version keys and half-open historical validity ranges. |
| Neighbourhood and property-type metrics | `listing_metrics` calculates all nine requested metric groups, including distinct-host superhost rate, numeric median, calendar-month changes, stays and revenue. |
| Host-neighbourhood LGA metrics | `dm_host_neighbourhood`: distinct hosts, average revenue per active listing, and total estimated revenue divided by distinct hosts. |
| Metric definitions and ordering | `macros/gold_helpers.sql` implements the brief's formulas and specified group/month ordering. `gold_edge_cases.sql` checks zero denominators, NULL review ratings, missing groups, inactive listings and historical attribution. |
| Part 3: chronological Airflow then manual dbt job after each month | The DAG refuses a new month without the preceding successful saved-job completion. The automatic selector uses the latest verified Bronze month; the operator still triggers dbt manually after each load. The runbook requires final cloud Success and revision checks before the next Airflow trigger. |
| Part 4: five business questions with SQL/screenshots | Deferred. No Part 4 result is claimed in this checkpoint. |
| Submission SQL, DAG, models, snapshots, project config | `part_1.sql`, combined DAG, `models/`, `snapshots/`, `dbt_project.yml`; include required `macros/`, YAML tests and supporting verification files so the project remains executable. `part_4.sql` is still to be written. |
| Handover report, evidence, 3,000-word limit, named ZIP | Deferred until analysis and cloud execution evidence are complete. The report must explain decisions, issues/fixes and business answers. Figures/tables are excluded from the word count stated in the brief. |

The sequence is an initial May load, warehouse setup, then eleven Airflow → saved dbt job cycles. This DAG validates, loads and verifies the raw tables in three tasks. It compares unchanged static reference data on later runs. The brief does not prescribe a particular task count or monthly reference-table truncation.

The existing GCP infrastructure is reused, while the database and source prefix are separate. The dbt repository and connection settings are explicitly changed to the new project. Original source backups and actual run history are retained. Isolation checks reduce accidental mixing; they do not constitute a guarantee about every possible future edit or execution.
