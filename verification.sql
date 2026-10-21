-- Read-only checks after the saved monthly dbt job reports Success.
-- Connect to BDEAT3 in DBeaver. Do not run the Part 1 setup again.

SELECT current_database() AS database_name,
       (SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()) AS ssl_active,
       (SELECT max(observation_month) FROM pipeline.monthly_loads) AS latest_loaded_month,
       (SELECT max(observation_month) FROM pipeline.dbt_completions) AS latest_completed_month;

SELECT m.observation_month, r.run_id AS airflow_run_id, r.verified_at,
       c.dbt_cloud_job_id, c.dbt_cloud_run_id, c.git_sha, c.dbt_version,
       c.passed_tests, c.completed_at,
       c.manifest_sha256 = m.manifest_sha256 AS manifest_matches,
       c.completed_at >= m.verified_at AS completed_after_verification
FROM pipeline.monthly_loads m
JOIN pipeline.load_runs r USING (run_id)
LEFT JOIN pipeline.dbt_completions c ON c.observation_month = m.observation_month
ORDER BY m.observation_month;

WITH bronze_counts AS (
    SELECT _source_month AS month, count(*) AS rows
    FROM bronze.raw_airbnb GROUP BY _source_month
), fact_counts AS (
    SELECT month_id AS month, count(*) AS rows
    FROM gold.fact_listing_month GROUP BY month_id
), expected AS (
    SELECT observation_month AS month, sum(row_count) AS rows
    FROM pipeline.source_files WHERE target_table = 'raw_airbnb'
    GROUP BY observation_month
)
SELECT coalesce(b.month, f.month, e.month) AS month,
       e.rows AS source_rows, b.rows AS bronze_rows, f.rows AS fact_rows,
       b.rows = e.rows AND f.rows = e.rows AS counts_match
FROM bronze_counts b FULL JOIN fact_counts f USING (month)
FULL JOIN expected e USING (month)
ORDER BY month;

WITH host_matches AS (
    SELECT f.listing_id, f.month_id, count(h.host_version_id) AS matches
    FROM gold.fact_listing_month f LEFT JOIN gold.dim_host h
      ON h.host_id = f.host_id AND f.month_id >= h.valid_from
     AND (f.month_id < h.valid_to OR h.valid_to IS NULL)
    GROUP BY f.listing_id, f.month_id
), property_matches AS (
    SELECT f.listing_id, f.month_id, count(p.property_version_id) AS matches
    FROM gold.fact_listing_month f LEFT JOIN gold.dim_property p
      ON p.listing_id = f.listing_id AND p.host_id = f.host_id
     AND f.month_id >= p.valid_from
     AND (f.month_id < p.valid_to OR p.valid_to IS NULL)
    GROUP BY f.listing_id, f.month_id
)
SELECT 'host_version_match_errors' AS check_name, count(*) AS errors
FROM host_matches WHERE matches <> 1
UNION ALL
SELECT 'property_version_match_errors', count(*)
FROM property_matches WHERE matches <> 1
UNION ALL
SELECT 'duplicate_listing_months', count(*) FROM (
    SELECT listing_id, month_id FROM gold.fact_listing_month
    GROUP BY listing_id, month_id HAVING count(*) <> 1
) duplicates
UNION ALL
SELECT 'stored_version_key_errors', count(*)
FROM gold.fact_listing_month f
LEFT JOIN gold.dim_host h ON f.host_version_id = h.host_version_id
LEFT JOIN gold.dim_property p ON f.property_version_id = p.property_version_id
WHERE h.host_id IS DISTINCT FROM f.host_id
   OR p.listing_id IS DISTINCT FROM f.listing_id
   OR p.host_id IS DISTINCT FROM f.host_id
   OR NOT (f.month_id >= h.valid_from AND (f.month_id < h.valid_to OR h.valid_to IS NULL))
   OR NOT (f.month_id >= p.valid_from AND (f.month_id < p.valid_to OR p.valid_to IS NULL));

SELECT 'host' AS entity, observation_month, count(*) AS versions
FROM silver.snap_host GROUP BY observation_month
UNION ALL
SELECT 'property', observation_month, count(*)
FROM silver.snap_property GROUP BY observation_month
ORDER BY entity, observation_month;

SELECT 'listing_neighbourhood' AS datamart, count(*) AS groups
FROM gold.dm_listing_neighbourhood
UNION ALL SELECT 'property_type', count(*) FROM gold.dm_property_type
UNION ALL SELECT 'host_neighbourhood', count(*) FROM gold.dm_host_neighbourhood;

SELECT observation_month, invocation_id, job_id, run_id, git_sha,
       started_at, finished_at, status
FROM pipeline.dbt_attempts ORDER BY started_at;
