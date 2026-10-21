# Part 3: eleven remaining monthly cycles

This is an execution checklist, not evidence that the cloud cycles have run. Start only after the May production job is successful and independently verified.

## Once, before June

1. Check the target is `BDEAT3`, the DAG is `airbnb_census_monthly_rebuild`, and the dbt environment is `BDEAT3 Production`.
2. Confirm May is the only loaded month, has a successful verified Airflow run and a matching saved-job completion, and appears in both snapshots and the Gold fact.
3. Upload `06_2020.csv` through `04_2021.csv` to `data/at3_rebuild/listings/` in the existing Composer bucket. Do not replace May or the static files. The DAG chooses only the oldest eligible next month from the supplied manifest.
4. Confirm the saved dbt job uses the reviewed code revision, one `dbt build` command, one thread and no schedule or automatic trigger. Do not queue multiple runs.

## Repeat for each month

1. In Airflow, trigger the combined DAG once using its default `mode=next`.
2. Wait for `validate_sources`, `load_bronze` and `verify_bronze` to succeed. Read the selected month and loaded row count. If any task fails, repair/retry that run before proceeding.
3. In the saved dbt job, set the single command's `observation_month` to the month just loaded. Save and review the command before selecting **Run now**.
4. Wait for the job's final **Success**. Check the actual checkout revision, all 47 passing tests, no skipped required resources, and its completion hook. A green Airflow run alone does not complete the cycle.
5. Run `verification.sql` in DBeaver. Confirm the latest month's completion matches the verified source manifest and run, fact rows equal cumulative source rows, and each fact has exactly one valid Host and Property version.
6. Record the actual Airflow run ID, dbt run ID, revision, counts and status in the evidence table. Capture the real run/result screens. Then move to the next month.

Never trigger two new monthly loads without a successful saved dbt job between them. Never change observation month to skip a failed cycle.

| Month variable | Source file | Rows | Cumulative facts |
|---|---|---:|---:|
| `2020-05-01` | `05_2020.csv` | 37,562 | 37,562 |
| `2020-06-01` | `06_2020.csv` | 36,901 | 74,463 |
| `2020-07-01` | `07_2020.csv` | 36,057 | 110,520 |
| `2020-08-01` | `08_2020.csv` | 31,391 | 141,911 |
| `2020-09-01` | `09_2020.csv` | 34,829 | 176,740 |
| `2020-10-01` | `10_2020.csv` | 34,276 | 211,016 |
| `2020-11-01` | `11_2020.csv` | 33,795 | 244,811 |
| `2020-12-01` | `12_2020.csv` | 33,871 | 278,682 |
| `2021-01-01` | `01_2021.csv` | 33,902 | 312,584 |
| `2021-02-01` | `02_2021.csv` | 33,630 | 346,214 |
| `2021-03-01` | `03_2021.csv` | 33,229 | 379,443 |
| `2021-04-01` | `04_2021.csv` | 32,679 | 412,122 |

## Completion criteria

All twelve months must be present once at listing-month grain, with 412,122 facts, twelve verified monthly loads and twelve corresponding successful deployment completions. Each observed fact must have exactly one historical Host and Property match. Preserve unsuccessful attempts as well as later successful recovery runs. Finish the final dbt tests and capture evidence before starting Part 4.

Part 4 business analysis, report and submission are a later unit. Do not delete or stop the cloud resources until their evidence and any further access required for marking have been considered.
