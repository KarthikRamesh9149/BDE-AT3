# Part 2: warehouse and monthly dbt job

## Destination and execution

The repository is `KarthikRamesh9149/BDE-AT3`. Both environments use the PostgreSQL connection `BDEAT3`, database `BDEAT3`, port 5432, and SSL. The production environment is `BDEAT3 Production`, on dbt v1 Latest. Its connection profile is `default`, with schema `bdeat3`. `generate_schema_name` maps this production profile to the explicit `silver`, `gold` and `control` schemas.

Development must use a separate profile schema, `dbt_kramesh`. This creates `dbt_kramesh_silver`, `dbt_kramesh_gold` and `dbt_kramesh_control`; it reads the same verified Bronze inputs but cannot write the production completion ledger. Do not use the production schema for Studio commands.

After publishing the automatic-month revision and updating the deployment revision setting, use one unchanged execution command for every monthly cycle:

```sh
dbt build --threads 1 --fail-fast
```

Use the manual deployment job `BDEAT3 monthly build` (ID `70506183143171`), with no schedule, no other automatic trigger, target name `prod`, one thread and a 3,600-second timeout. The dbt scheduler serialises runs of the same deployment job. Never create or run a second job against the canonical schemas while this job is running. Do not use `--full-refresh`, resource exclusions, deferred state, or a stand-alone snapshot command for the monthly cycle.

The automatic month selector must be present in the deployed revision before switching to this command. Earlier revisions required an explicit `observation_month` variable. The current code reads the latest committed month from `pipeline.monthly_loads` and requires both its load record and linked Airflow run to be verified. It rejects an empty or unverified latest load instead of falling back to an older month. The start hook records the selected month in the existing attempt ledger; subsequent resources reuse that month for the same invocation. The selector performs only reads and does not load data or advance the Airflow DAG.

The same command supports a retry of the latest month. An explicit `--vars '{"observation_month":"2020-06-01"}'` remains available for diagnosis, but must still match the latest loaded month and pass every existing check. Neither the current date nor `scraped_date` determines the observation month.

Set `DBT_DEPLOYMENT_GIT_SHA` in the deployment environment to the reviewed 40-character commit. This is a declared revision, not an automatically attested checkout value. Verify the actual revision shown in each cloud run against it. Keep the deployment revision fixed throughout the twelve-month cycle; investigate and retest any code change before continuing. Cloud supplies its real job and run IDs. Local test fixtures are never entered in the hosted environment.

## Nine validation responsibilities in one command

| Responsibility | Implementation and stop condition |
|---|---|
| 1. Bronze check | The start hook checks database, SSL, source configuration, verified Airflow history, exact CSV values, hashes, counts and contiguous months before model writes. |
| 2. Silver build | Typed and cleaned models retain filename-month, original scrape dates and source metadata. Their tests run before the shared `silver_ready` dependency. |
| 3. Snapshot preflight | `silver_ready` checks one current-month row per entity and validates both existing snapshot histories before either snapshot starts. |
| 4. Update snapshots | Host and Property use the timestamp strategy with month-start `updated_at`. Their hooks check inputs and resulting history inside each snapshot transaction. |
| 5. Snapshot tests | Uniqueness, required fields and full snapshot integrity tests must pass before `snapshot_ready`. |
| 6. Verify snapshot history | The shared pre-Gold check compares all observed entity states to Silver, including duplicate multiplicities, NULL attributes, keys and validity intervals. |
| 7. Gold preflight | Every observed fact must match exactly one Host version and one Property version. All Gold branches depend on this check. |
| 8. Gold build | Four dimensions, one fact, two Census reference tables and three datamart views are built. Views use historical values valid for each observation month. |
| 9. Final tests | All required resources must have actual passing results. Source and snapshot checks run again before the end hook records completion. The operator also checks the cloud run's final Success status and revision. |

There are 19 models, 2 snapshots and 47 data tests. A complete local build reports 70 successful entries including its two project hooks; this does not mean 70 tests.

The start transaction withdraws the latest month's readiness marker while preserving its attempt record. The end transaction restores readiness only after success. These transactions share the Airflow advisory lock. The lock does not span all dbt model transactions, so the single-job concurrency setting and manual wait between runs are required. A failure or cancellation can leave some models committed; the partial-failure rehearsal verifies recovery by rerunning the same month without advancing the DAG.

## Warehouse choices

- Bronze preserves all source values as text. Silver casts types, trims labels, normalises geography keys and removes the Census `LGA` prefix from join keys.
- The filename supplies the observation month. The 4,780 July rows whose supplied scrape date is 5 September remain July observations; their original scrape dates remain available.
- Host and Property use SCD2. Static LGA, calendar and Census data do not use snapshots. Absent monthly observations are not invented or treated as confirmed deletions.
- Host locations retain explicit `Unknown`, `Overseas` and `Unmatched` groups. A listing's location is never substituted for an unknown host location.
- The fact contains IDs and measures. Its grain is one listing per observed month. Half-open validity joins use `observed_at >= valid_from` and `observed_at < valid_to`, with an open-ended NULL `valid_to`.
- Property-type labels are trimmed, with genuine source category changes retained. Any broader category harmonisation needs an explicit justified mapping rather than silently merging unlike categories.
- Active listings use `has_availability = true`. Prices and review-rating averages use active listings. Superhost rate uses distinct hosts. Stays equal `30 - availability_30` for active listings. Revenue equals stays multiplied by price.
- Month-on-month changes compare adjacent calendar months. Missing prior groups and zero prior denominators produce NULL. Unknown reviews and undefined averages are not replaced with zero.

## Verification and recovery

After May, expect 37,562 facts, 37,562 Property versions and 27,273 Host versions. Check the three views, point-in-time matches, all 47 test results and the May completion record. `verification.sql` contains read-only checks for DBeaver.

If any step fails, retain the run and error evidence. Resolve the cause and rerun the same month's saved job. Do not load the next month, manually insert a completion record, drop snapshots or reset source tables. An excluded resource is rejected before model writes. A changed Bronze value is rejected before snapshots or Gold writes. A same-month retry leaves the snapshot and fact contents unchanged when its inputs and code are unchanged.

## Technical references

- [dbt build and dependency tests](https://docs.getdbt.com/reference/commands/build)
- [dbt end-hook execution results](https://docs.getdbt.com/reference/dbt-jinja-functions/on-run-end-context)
- [dbt Cloud environment variables](https://docs.getdbt.com/docs/build/environment-variables)
- [Deployment connection profiles](https://docs.getdbt.com/docs/deploy/deploy-environments)
- [Deployment job queue and serial execution](https://docs.getdbt.com/docs/deploy/job-scheduler)
