# Part 1 setup and monthly operation

## Target configuration

| Setting | Value |
|---|---|
| Cloud SQL instance | `utsbigdataat3` |
| Database | `BDEAT3` (case-sensitive) |
| Airflow connection | `postgres_bdeat3` |
| DAG | `airbnb_census_monthly_rebuild` |
| Bucket | `australia-southeast1-bigdat-bc43ec38-bucket` |
| Source prefix | `data/at3_rebuild/` |
| Schedule | `None`; manual trigger only |

Use the existing PostgreSQL account password directly in DBeaver and Airflow. Credentials are not stored in this repository. DBeaver connects to the Cloud SQL public endpoint through its authorised network. Composer uses the private endpoint. Both connections must require SSL.

## Prepare the database in DBeaver

1. Connect to database `BDEAT3` using the standard PostgreSQL driver. Verify the database name and SSL before proceeding.
2. Open `part_1.sql` and execute the whole script. It creates five raw tables in `bronze` and separate operational tables in `pipeline`.
3. Confirm all five final raw-table row counts are zero and `pipeline.dbt_completions` is empty.
4. Keep the SQL script for submission. It is a one-time setup, not a monthly reset. If it reports existing schemas, stop and inspect the selected database rather than dropping or truncating existing work.

The source has 22 Airbnb, 109 G01, 9 G02, 2 LGA code and 27 suburb-mapping fields. All 180 positions are preserved. The additional `_source_file`, `_loaded_at` and Airbnb `_source_month` columns record provenance. Unnamed mapping columns have positional names and are retained.

## Upload Part 1 inputs

Upload only these files under the configured prefix:

| Object suffix | Expected rows |
|---|---:|
| `listings/05_2020.csv` | 37,562 |
| `census/2016Census_G01_NSW_LGA.csv` | 132 |
| `census/2016Census_G02_NSW_LGA.csv` | 132 |
| `lga/NSW_LGA_CODE.csv` | 129 |
| `lga/NSW_LGA_SUBURB.csv` | 4,470 |

The other eleven Airbnb files are uploaded in Part 3. The manifest lists their expected hashes in advance, but that does not load or publish their data.

## Configure and deploy Airflow

Create a Postgres connection with ID `postgres_bdeat3`, database `BDEAT3`, the Cloud SQL private host, port `5432` and the existing login. Set Extra to:

```json
{"sslmode":"require","connect_timeout":10}
```

Upload the combined DAG file to the Composer bucket's `dags/` folder. Confirm that Airflow parses it successfully and initially shows it paused. Check the three tasks in order: `validate_sources`, `load_bronze`, `verify_bronze`. Preparation ends here; the May run is a separate checkpoint.

## May load and subsequent monthly cycle

When ready to execute Part 1, unpause and manually trigger the DAG with its default `mode=next`. Wait for all three tasks to succeed. Check source row counts, the selected month, the verified load record and that no second month has been inserted.

After Part 2 is ready, manually run the saved dbt deployment job for May and wait for Success before proceeding. Part 3 repeats the same sequence for June 2020 through April 2021. The DAG does not trigger dbt itself.

| Month | Airbnb rows |
|---|---:|
| May 2020 | 37,562 |
| June 2020 | 36,901 |
| July 2020 | 36,057 |
| August 2020 | 31,391 |
| September 2020 | 34,829 |
| October 2020 | 34,276 |
| November 2020 | 33,795 |
| December 2020 | 33,871 |
| January 2021 | 33,902 |
| February 2021 | 33,630 |
| March 2021 | 33,229 |
| April 2021 | 32,679 |
| Total | 412,122 |

## Recovery behaviour

- Retrying a task in the same Airflow run preserves its originally selected month and source manifest.
- `retry_latest` checks the last loaded month without replacing raw rows or advancing the month.
- Changed source bytes or object generations require investigation. Do not overwrite objects after a run has pinned them.
- A failure before commit leaves no partial raw load. If loading committed but verification failed, retry verification for that run.
- The static reference tables are loaded once and compared exactly on later runs. They are not truncated each month.
- Never insert manual rows into `pipeline.dbt_completions`. Part 2 must write a completion only after checking the required deployment job results, invocation, source manifest and code revision.

## Technical references

- [Airflow task and retry guidance](https://airflow.apache.org/docs/apache-airflow/2.11.0/best-practices.html)
- [Cloud Storage object generation preconditions](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob)
- [PostgreSQL 16 COPY and CSV semantics](https://www.postgresql.org/docs/16/sql-copy.html)
