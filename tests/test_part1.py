"""Local PostgreSQL integration checks. Storage and dbt completion are explicit test fixtures.
These tests never connect to Cloud SQL and do not prove a cloud job succeeded.
"""
import csv
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace

import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path(os.environ['AT3_SOURCE_ROOT'])
DSN = dict(host='127.0.0.1', port=55433, user='postgres', sslmode='require', connect_timeout=5)

spec = importlib.util.spec_from_file_location('monthly_loader', ROOT / 'dags/airbnb_census_monthly_rebuild.py')
loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loader)


class LocalBlob:
    """Only the object interface is simulated; parsing and SQL execute unmodified."""
    def __init__(self, bucket, name):
        self.bucket, self.name = bucket, name
        relative = name.removeprefix(loader.PREFIX)
        assert relative != name, 'Unexpected storage prefix'
        if relative.startswith('listings/'):
            relative = 'airbnb/' + relative.split('/', 1)[1]
        self.path = bucket.root / relative
        self.generation = bucket.generations.get(name, 1)
        self.size = None

    def reload(self, timeout):
        self.size = self.path.stat().st_size

    def download_to_filename(self, target, if_generation_match, timeout):
        assert if_generation_match == self.generation
        shutil.copyfile(self.path, target)


class LocalBucket:
    def __init__(self, root):
        self.root, self.generations = root, {}

    def blob(self, name):
        return LocalBlob(self, name)


@pytest.fixture
def database(monkeypatch):
    admin = psycopg2.connect(dbname='postgres', **DSN)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute('DROP DATABASE IF EXISTS "BDEAT3" WITH (FORCE)')
        cur.execute('CREATE DATABASE "BDEAT3"')
    admin.close()
    conn = psycopg2.connect(dbname='BDEAT3', **DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute((ROOT / 'part_1.sql').read_text())
    bucket = LocalBucket(SOURCE)

    class LocalPostgresHook:
        def __init__(self, **kwargs):
            assert kwargs['postgres_conn_id'] == 'postgres_bdeat3'

        def get_connection(self, connection_id):
            return SimpleNamespace(schema='BDEAT3', extra_dejson={'sslmode': 'require', 'connect_timeout': 5})

        def get_conn(self):
            return psycopg2.connect(dbname='BDEAT3', **DSN)

    class LocalGCSHook:
        def __init__(self, **kwargs):
            pass

        def get_conn(self):
            return self

        def bucket(self, name):
            assert name == loader.BUCKET
            return bucket

    assert not loader.USE_PSYCOPG3, 'Run psycopg2 tests with the Airflow 2.11 constraints environment'
    monkeypatch.setattr(loader, 'PostgresHook', LocalPostgresHook)
    monkeypatch.setattr(loader, 'GCSHook', LocalGCSHook)
    yield conn, bucket
    conn.close()


def query(conn, sql, args=()):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else None


def context(run_id, mode='next'):
    return {'run_id': run_id, 'params': {'mode': mode}}


def run_month(run_id, mode='next'):
    ctx = context(run_id, mode)
    loader.validate_sources(**ctx)
    loader.load_bronze(**ctx)
    return loader.verify_bronze(**ctx)


def mark_local_dbt_fixture(conn, month):
    # Deliberately test-only. The real ledger stays empty until a verified deployment job writes it.
    query(conn, '''INSERT INTO pipeline.dbt_completions
        (observation_month,manifest_sha256,dbt_invocation_id,dbt_cloud_job_id,dbt_cloud_run_id,git_sha,dbt_version,passed_tests)
        SELECT observation_month,manifest_sha256,%s,1,%s,%s,'TEST FIXTURE',1
        FROM pipeline.monthly_loads WHERE observation_month=%s''',
        ('local-fixture-' + month, loader.MONTHS.index(month) + 1, '0' * 40, loader.month_date(month)))


def raw_state(conn):
    return {table: query(conn, f'SELECT count(*), min(_loaded_at), max(_loaded_at) FROM bronze.{table}')[0]
            for table in loader.SOURCE_SPECS}


def test_dag_contract():
    assert loader.dag.schedule_interval is None
    assert loader.dag.catchup is False
    assert loader.dag.max_active_runs == 1
    assert loader.dag.is_paused_upon_creation is True
    assert set(loader.dag.task_ids) == {'validate_sources', 'load_bronze', 'verify_bronze'}
    assert loader.validate.downstream_task_ids == {'load_bronze'}
    assert loader.load.downstream_task_ids == {'verify_bronze'}
    assert loader.DATABASE == 'BDEAT3'
    assert loader.PREFIX == 'data/at3_rebuild/'


def test_initial_schema_and_safe_reexecution(database):
    conn, _ = database
    assert all(row[0] == 0 for row in raw_state(conn).values())
    assert query(conn, 'SELECT count(*) FROM pipeline.dbt_completions') == [(0,)]
    with pytest.raises(psycopg2.Error, match='not a reset script'):
        query(conn, (ROOT / 'part_1.sql').read_text())
    query(conn, 'ROLLBACK')
    assert all(row[0] == 0 for row in raw_state(conn).values())


def test_sql_wrong_database_guard(database):
    admin = psycopg2.connect(dbname='postgres', **DSN)
    admin.autocommit = True
    with pytest.raises(psycopg2.Error, match='Wrong database'):
        query(admin, (ROOT / 'part_1.sql').read_text())
    query(admin, 'ROLLBACK')
    assert query(admin, "SELECT count(*) FROM pg_namespace WHERE nspname='bronze'") == [(0,)]
    admin.close()


def test_may_counts_retry_gate_and_pinned_june(database):
    conn, _ = database
    result = run_month('may')
    assert result == {'month': '2020-05', 'verified': True}
    assert {k: v[0] for k, v in raw_state(conn).items()} == {
        'raw_airbnb': 37562, 'raw_census_g01': 132, 'raw_census_g02': 132, 'raw_lga_code': 129, 'raw_lga_suburb': 4470}
    before = raw_state(conn)
    run_month('may')
    run_month('may-retry', 'retry_latest')
    assert raw_state(conn) == before
    with pytest.raises(loader.AirflowException, match='successful saved dbt job'):
        loader.validate_sources(**context('june-too-soon'))
    assert query(conn, 'SELECT count(*) FROM pipeline.monthly_loads') == [(1,)]
    mark_local_dbt_fixture(conn, '2020-05')
    run_month('june')
    # A cleared/retried first task is still tied to June after the month has committed.
    assert loader.validate_sources(**context('june'))['month'] == '2020-06'
    assert query(conn, 'SELECT count(*) FROM bronze.raw_airbnb') == [(74463,)]
    with pytest.raises(loader.AirflowException, match='older or skipped'):
        loader.validate_sources(**context('may'))


def test_atomic_failure_rolls_back_every_raw_table(database, monkeypatch):
    conn, _ = database
    ctx = context('failed-may')
    loader.validate_sources(**ctx)
    original = loader.assert_identical

    def fail_after_second_insert(cursor, spec, stage, month):
        if spec['table'] == 'raw_census_g01':
            raise loader.AirflowException('Injected failure after multiple writes')
        return original(cursor, spec, stage, month)

    monkeypatch.setattr(loader, 'assert_identical', fail_after_second_insert)
    with pytest.raises(loader.AirflowException, match='Injected failure'):
        loader.load_bronze(**ctx)
    assert all(row[0] == 0 for row in raw_state(conn).values())
    assert query(conn, 'SELECT count(*) FROM pipeline.source_files') == [(0,)]
    assert query(conn, 'SELECT count(*) FROM pipeline.monthly_loads') == [(0,)]
    assert query(conn, 'SELECT observation_month FROM pipeline.load_runs')[0][0] == loader.month_date('2020-05')
    monkeypatch.setattr(loader, 'assert_identical', original)
    run_month('failed-may')
    assert query(conn, 'SELECT count(*) FROM bronze.raw_airbnb') == [(37562,)]


def test_replaced_object_is_rejected_before_loading(database):
    conn, bucket = database
    ctx = context('changed-object')
    loader.validate_sources(**ctx)
    bucket.generations[loader.specs_for('2020-05')[0]['object_name']] = 2
    with pytest.raises(loader.AirflowException, match='replaced after validation'):
        loader.load_bronze(**ctx)
    assert query(conn, 'SELECT count(*) FROM bronze.raw_airbnb') == [(0,)]


def test_raw_tampering_blocks_progress(database):
    conn, _ = database
    run_month('may')
    mark_local_dbt_fixture(conn, '2020-05')
    query(conn, "UPDATE bronze.raw_census_g02 SET median_age_persons='999' WHERE ctid IN (SELECT ctid FROM bronze.raw_census_g02 LIMIT 1)")
    with pytest.raises(loader.AirflowException, match='source values changed'):
        loader.validate_sources(**context('june'))
    assert query(conn, 'SELECT count(*) FROM pipeline.monthly_loads') == [(1,)]


def test_staging_preserves_empty_literal_null_and_duplicates(database, tmp_path):
    values = [['a,b', '', '\\N', 'quote"', 'line\nbreak', 'é'],
              ['a,b', '', '\\N', 'quote"', 'line\nbreak', 'é']]
    spec = {'table': 'raw_fixture', 'filename': 'fixture.csv', 'headers': list('abcdef'),
            'columns': list('abcdef'), 'rows': 2}
    path = tmp_path / 'fixture.csv'
    with path.open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(spec['headers'])
        writer.writerows(values)
    assert loader.inspect_csv(path, spec) == 2
    with loader.database_transaction() as cur:
        cur.execute('CREATE TABLE bronze.raw_fixture (a text,b text,c text,d text,e text,f text,_source_file text NOT NULL,_loaded_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP)')
        stage = loader.stage_file(cur, spec, path, tmp_path, '2020-05')
        loader.assert_source_values(cur, spec, stage, path, tmp_path)
        cur.execute('SELECT a,b,c,d,e,f FROM stage_raw_fixture')
        assert cur.fetchall() == [tuple(v) for v in values]
        cur.execute('DROP TABLE bronze.raw_fixture')


@pytest.mark.parametrize('kind', ['header','width','zero_byte','row_count','invalid_utf8'])
def test_bad_csv_fails(kind, tmp_path):
    spec = {'filename':'bad.csv','headers':['a','b'],'rows':1}
    content = {'header':b'x,b\n1,2\n', 'width':b'a,b\n1,2,3\n',
               'zero_byte':b'a,b\n1,\x00\n', 'row_count':b'a,b\n', 'invalid_utf8':b'a,b\n\xff,2\n'}[kind]
    path = tmp_path / 'bad.csv'
    path.write_bytes(content)
    with pytest.raises(loader.AirflowException):
        loader.inspect_csv(path,spec)


def test_all_supplied_files_and_july_observation_month():
    total = 0
    for month in loader.MONTHS:
        spec = loader.specs_for(month)[0]
        path = SOURCE / 'airbnb' / spec['filename']
        assert loader.file_sha256(path) == spec['sha256']
        total += loader.inspect_csv(path, spec)
    assert total == 412122
    july = list(csv.DictReader((SOURCE/'airbnb/07_2020.csv').open(newline='')))
    # Actual CSV date spelling is profiled separately; the filename establishes the month.
    assert loader.specs_for('2020-07')[0]['filename'] == '07_2020.csv'
    assert len(july) == 36057


def test_twelve_month_rehearsal_with_test_only_completion_markers(database):
    conn, _ = database
    for month in loader.MONTHS:
        assert run_month('rehearsal-' + month)['month'] == month
        mark_local_dbt_fixture(conn, month)
    assert query(conn, 'SELECT count(*), count(DISTINCT _source_month) FROM bronze.raw_airbnb') == [(412122,12)]
    assert query(conn, 'SELECT count(*) FROM pipeline.monthly_loads WHERE verified_at IS NOT NULL') == [(12,)]
    with pytest.raises(loader.AirflowException, match='All twelve'):
        loader.validate_sources(**context('thirteenth'))
