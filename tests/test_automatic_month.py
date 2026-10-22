"""Automatic month selection against the disposable local PostgreSQL fixture."""
import os
from pathlib import Path

import pytest

from test_part1 import database, query, run_month, loader, context, raw_state, SOURCE
from test_part2 import build, fingerprints, historical_fingerprints, assert_complete_build
from independent_metrics import verify_marts


def test_no_verified_month_is_rejected(database, tmp_path):
    conn, _ = database
    build(tmp_path, None, 'empty_bronze', False,
          error='Automatic month selection requires the latest Bronze load to be verified')
    run_month('local-auto-may')
    for table in ('monthly_loads', 'load_runs'):
        saved = query(conn, f'select verified_at from pipeline.{table}')[0][0]
        query(conn, f'update pipeline.{table} set verified_at=null')
        build(tmp_path, None, 'unverified_'+table, False,
              error='Automatic month selection requires the latest Bronze load to be verified')
        query(conn, f'update pipeline.{table} set verified_at=%s', (saved,))
    query(conn, "update pipeline.load_runs set observation_month='2020-06-01'")
    build(tmp_path, None, 'mismatched_airflow_month', False,
          error='Automatic month selection requires the latest Bronze load to be verified')
    assert query(conn, 'select count(*) from pipeline.dbt_completions') == [(0,)]
    assert query(conn, "select to_regclass('silver.snap_host'),to_regclass('silver.snap_property')") == [(None, None)]


def test_existing_may_upgrade_and_automatic_june_recovery(database, tmp_path):
    conn, _ = database
    baseline = os.environ.get('AT3_BASELINE_PROJECT')
    assert baseline, 'Set AT3_BASELINE_PROJECT to the exported prior deployment revision.'
    baseline = Path(baseline)
    assert baseline.is_dir()
    run_month('local-upgrade-may')
    assert_complete_build(build(tmp_path, '2020-05-01', 'legacy_may', project_dir=baseline))
    before = fingerprints(conn)
    raw_before = raw_state(conn)
    snapshot_oids = query(conn, "select 'silver.snap_host'::regclass::oid,'silver.snap_property'::regclass::oid")
    assert_complete_build(build(tmp_path, None, 'automatic_may_retry'))
    assert fingerprints(conn) == before
    assert raw_state(conn) == raw_before
    assert query(conn, "select 'silver.snap_host'::regclass::oid,'silver.snap_property'::regclass::oid") == snapshot_oids

    for label, extra, error in (
        ('excluded_test', ['--exclude', 'gold_datamart_reconciliation'], 'cannot exclude a required resource'),
        ('full_refresh', ['--full-refresh'], 'Monthly builds require --threads 1 and no --full-refresh'),
        ('multiple_threads', ['--threads', '2'], 'Monthly builds require --threads 1 and no --full-refresh'),
    ):
        build(tmp_path, None, label, False, extra, error=error)
        assert fingerprints(conn) == before

    # A bogus later load must not allow the automatic selector to skip June.
    query(conn, """insert into pipeline.load_runs(run_id,observation_month,verified_at)
        values('local-gap-fixture','2020-07-01',clock_timestamp());
        insert into pipeline.monthly_loads(observation_month,run_id,manifest_sha256,verified_at)
        values('2020-07-01','local-gap-fixture',repeat('0',64),clock_timestamp());""")
    build(tmp_path, None, 'skipped_month', False, error='Bronze month history has a gap')
    assert fingerprints(conn) == before
    query(conn, """delete from pipeline.monthly_loads where run_id='local-gap-fixture';
        delete from pipeline.load_runs where run_id='local-gap-fixture';""")

    june_context = context('local-upgrade-june')
    loader.validate_sources(**june_context)
    loader.load_bronze(**june_context)
    build(tmp_path, None, 'june_not_yet_verified', False,
          error='Automatic month selection requires the latest Bronze load to be verified')
    assert fingerprints(conn) == before
    loader.verify_bronze(**june_context)
    build(tmp_path, '2020-05-01', 'stale_explicit_month', False,
          error='Requested month must be the latest loaded Bronze month')
    assert fingerprints(conn) == before

    history_before = historical_fingerprints(conn, '2020-06-01')
    query(conn, """create function silver.fail_property_fixture() returns trigger language plpgsql as $$
        begin raise exception 'Local QA injected property failure'; end $$;
        create trigger property_failure_fixture before insert on silver.snap_property
        for each row execute function silver.fail_property_fixture();""")
    build(tmp_path, None, 'automatic_june_partial_failure', False,
          error='Local QA injected property failure')
    assert fingerprints(conn)['fact'] == before['fact']
    assert query(conn, "select count(*) from pipeline.dbt_completions where observation_month='2020-06-01'") == [(0,)]
    with pytest.raises(loader.AirflowException, match='successful saved dbt job'):
        loader.validate_sources(**context('local-july-too-soon'))
    query(conn, 'drop trigger property_failure_fixture on silver.snap_property; drop function silver.fail_property_fixture()')
    assert_complete_build(build(tmp_path, None, 'automatic_june_recovery'))
    assert historical_fingerprints(conn, '2020-06-01') == history_before
    assert query(conn, 'select count(*) from gold.fact_listing_month') == [(74463,)]
    assert query(conn, 'select max(observation_month)::text from pipeline.dbt_completions') == [('2020-06-01',)]
    june = fingerprints(conn)
    assert_complete_build(build(tmp_path, None, 'automatic_june_retry'))
    assert fingerprints(conn) == june
    assert query(conn, "select 'silver.snap_host'::regclass::oid,'silver.snap_property'::regclass::oid") == snapshot_oids
    print('Upgrade and automatic recovery: preserved May history and unchanged June retry.', flush=True)
    print('Independent May/June metrics:', verify_marts(conn, SOURCE, ['2020-05-01', '2020-06-01']), flush=True)
