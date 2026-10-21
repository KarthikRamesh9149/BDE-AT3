"""Real local PostgreSQL/dbt rehearsal. Cloud identity values are test fixtures only.

This file uses the disposable 127.0.0.1:55433 database from test_part1. It never
connects to Cloud SQL or represents a real hosted job as having passed.
"""
import json
import os
from pathlib import Path
import subprocess

from test_part1 import database, query, run_month, loader, ROOT, SOURCE
from independent_metrics import verify_marts


def build(tmp_path, month, label, succeeds=True, extra=()):
    profile = tmp_path / 'profiles.yml'
    profile.write_text('''default:
  target: qa
  outputs:
    qa:
      type: postgres
      host: 127.0.0.1
      port: 55433
      user: postgres
      password: ''
      dbname: BDEAT3
      schema: bdeat3
      threads: 1
      sslmode: require
''')
    env = dict(os.environ, DBT_CLOUD_JOB_ID='1', DBT_CLOUD_RUN_ID=str(build.counter),
               DBT_CLOUD_INVOCATION_CONTEXT='prod', DBT_DEPLOYMENT_GIT_SHA='1'*40)
    build.counter += 1
    command = [str(ROOT/'.venv-dbt/bin/dbt'), 'build', '--threads', '1', '--fail-fast',
               '--no-use-colors', '--profiles-dir', str(tmp_path),
               '--vars', json.dumps({'observation_month': month}), *extra]
    result = subprocess.run(command, cwd=ROOT, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)
    (tmp_path/(label+'.log')).write_text(result.stdout)
    print(label, result.returncode, result.stdout[-1200:], flush=True)
    assert (result.returncode == 0) == succeeds, result.stdout[-7000:]
    artifact = ROOT/'target/run_results.json'
    if artifact.exists():
        (tmp_path/(label+'.json')).write_bytes(artifact.read_bytes())
        return json.loads(artifact.read_text())


build.counter = 100


def fingerprints(conn):
    rows = query(conn, """select 'host',count(*),md5(string_agg(to_jsonb(t)::text,'' order by dbt_scd_id))
        from silver.snap_host t union all
        select 'property',count(*),md5(string_agg(to_jsonb(t)::text,'' order by dbt_scd_id))
        from silver.snap_property t union all
        select 'fact',count(*),md5(string_agg(to_jsonb(t)::text,'' order by listing_id,month_id))
        from gold.fact_listing_month t""")
    return {name: (count, digest) for name, count, digest in rows}


def test_monthly_build_and_failure_recovery(database, tmp_path):
    conn, _ = database
    run_month('local-part2-may')
    result = build(tmp_path, '2020-05-01', 'may')
    assert query(conn, 'select count(*) from gold.fact_listing_month') == [(37562,)]
    assert query(conn, 'select count(*) from silver.snap_host') == [(27273,)]
    assert query(conn, 'select count(*) from silver.snap_property') == [(37562,)]
    assert query(conn, 'select count(*) from pipeline.dbt_completions') == [(1,)]
    assert all(r['status'] in ('success','pass') for r in result['results'])
    print('May independent metric checks:', verify_marts(conn, SOURCE, ['2020-05-01']), flush=True)
    execution = {r['unique_id']: r for r in result['results']}
    def finished(uid):
        return max(t['completed_at'] for t in execution[uid]['timing'])
    def started(uid):
        return min(t['started_at'] for t in execution[uid]['timing'])
    assert finished('test.bde_at3.silver_business_rules') < started('model.bde_at3.silver_ready')
    assert finished('test.bde_at3.snapshot_integrity') < started('model.bde_at3.snapshot_ready')
    assert all(finished('model.bde_at3.snapshot_ready') < started('model.bde_at3.'+name)
               for name in ['dim_host','dim_property','dim_lga','dim_month','ref_census_g01','ref_census_g02'])
    before = fingerprints(conn)
    build(tmp_path, '2020-05-01', 'may_retry')
    assert fingerprints(conn) == before
    assert query(conn, 'select count(*) from pipeline.dbt_attempts') == [(2,)]

    build(tmp_path, '2020-05-01', 'excluded_test', False,
          ['--exclude','gold_datamart_reconciliation'])
    assert fingerprints(conn) == before
    query(conn, "update bronze.raw_airbnb set price='invalid' where listing_id=(select min(listing_id) from bronze.raw_airbnb)")
    build(tmp_path, '2020-05-01', 'tampered_bronze', False)
    assert fingerprints(conn) == before
    # Restore from the exact file through the fixture reset, then rehearse the next month.
    source_file = next(p for p in __import__('test_part1').SOURCE.rglob('05_2020.csv'))
    import csv
    with source_file.open(encoding='utf-8-sig', newline='') as f:
        rows = list(csv.DictReader(f))
    minimum = min(r['LISTING_ID'] for r in rows)
    price = next(r['PRICE'] for r in rows if r['LISTING_ID'] == minimum)
    query(conn, 'update bronze.raw_airbnb set price=%s where listing_id=%s', (price, minimum))
    run_month('local-part2-june')
    query(conn, """create function silver.fail_property_fixture() returns trigger language plpgsql as $$
        begin raise exception 'Local QA injected property failure'; end $$;
        create trigger property_failure_fixture before insert on silver.snap_property
        for each row execute function silver.fail_property_fixture();""")
    build(tmp_path, '2020-06-01', 'june_partial_failure', False)
    assert query(conn, "select count(*) from pipeline.dbt_completions where observation_month='2020-06-01'") == [(0,)]
    assert fingerprints(conn)['fact'] == before['fact']
    assert query(conn, "select max(observation_month)::text from silver.snap_property") == [('2020-05-01',)]
    query(conn, 'drop trigger property_failure_fixture on silver.snap_property; drop function silver.fail_property_fixture()')
    build(tmp_path, '2020-06-01', 'june_recovery')
    assert query(conn, 'select count(*) from gold.fact_listing_month') == [(74463,)]
    assert query(conn, 'select count(*) from pipeline.dbt_completions') == [(2,)]
    print('Two-month independent metric checks:', verify_marts(conn, SOURCE, ['2020-05-01','2020-06-01']), flush=True)
    june = fingerprints(conn)
    build(tmp_path, '2020-06-01', 'june_retry')
    assert fingerprints(conn) == june
    assert query(conn, "select count(*) from gold.dm_listing_neighbourhood where month_year='2020-05-01' and percentage_change_active_listings is not null") == [(0,)]
    print('Evidence directory:', tmp_path, flush=True)
