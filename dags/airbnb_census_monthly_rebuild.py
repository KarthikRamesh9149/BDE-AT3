"""Load one source month, then wait for its manually triggered dbt Cloud job.
Raw rows and load records commit together. Identical retries change no raw rows.
"""

import csv
import hashlib
import json
import logging
import re
import sqlite3
from contextlib import closing, contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks import postgres as postgres_provider

PostgresHook = postgres_provider.PostgresHook
# Earlier compatible providers use psycopg2 and do not expose this flag.
USE_PSYCOPG3 = getattr(postgres_provider, 'USE_PSYCOPG3', False)

if USE_PSYCOPG3:
    from psycopg import sql
else:
    from psycopg2 import sql

LOG = logging.getLogger(__name__)
DATABASE = 'BDEAT3'
DAG_ID = 'airbnb_census_monthly_rebuild'
POSTGRES_CONN_ID = 'postgres_bdeat3'
BUCKET = 'australia-southeast1-bigdat-bc43ec38-bucket'
PREFIX = 'data/at3_rebuild/'
CONTRACT_VERSION = 'at3-rebuild-v1'
LOCK_ID = 70506183174422
MAX_SOURCE_BYTES = 32 * 1024 * 1024
MONTHS = tuple(f'{year}-{month:02d}' for year, months in
               [(2020, range(5, 13)), (2021, range(1, 5))] for month in months)

SOURCE_SPECS = {'raw_airbnb': {'filename': '05_2020.csv',
                'headers': ['LISTING_ID',
                            'SCRAPE_ID',
                            'SCRAPED_DATE',
                            'HOST_ID',
                            'HOST_NAME',
                            'HOST_SINCE',
                            'HOST_IS_SUPERHOST',
                            'HOST_NEIGHBOURHOOD',
                            'LISTING_NEIGHBOURHOOD',
                            'PROPERTY_TYPE',
                            'ROOM_TYPE',
                            'ACCOMMODATES',
                            'PRICE',
                            'HAS_AVAILABILITY',
                            'AVAILABILITY_30',
                            'NUMBER_OF_REVIEWS',
                            'REVIEW_SCORES_RATING',
                            'REVIEW_SCORES_ACCURACY',
                            'REVIEW_SCORES_CLEANLINESS',
                            'REVIEW_SCORES_CHECKIN',
                            'REVIEW_SCORES_COMMUNICATION',
                            'REVIEW_SCORES_VALUE'],
                'rows': 37562},
 'raw_census_g01': {'filename': '2016Census_G01_NSW_LGA.csv',
                    'headers': ['LGA_CODE_2016',
                                'Tot_P_M',
                                'Tot_P_F',
                                'Tot_P_P',
                                'Age_0_4_yr_M',
                                'Age_0_4_yr_F',
                                'Age_0_4_yr_P',
                                'Age_5_14_yr_M',
                                'Age_5_14_yr_F',
                                'Age_5_14_yr_P',
                                'Age_15_19_yr_M',
                                'Age_15_19_yr_F',
                                'Age_15_19_yr_P',
                                'Age_20_24_yr_M',
                                'Age_20_24_yr_F',
                                'Age_20_24_yr_P',
                                'Age_25_34_yr_M',
                                'Age_25_34_yr_F',
                                'Age_25_34_yr_P',
                                'Age_35_44_yr_M',
                                'Age_35_44_yr_F',
                                'Age_35_44_yr_P',
                                'Age_45_54_yr_M',
                                'Age_45_54_yr_F',
                                'Age_45_54_yr_P',
                                'Age_55_64_yr_M',
                                'Age_55_64_yr_F',
                                'Age_55_64_yr_P',
                                'Age_65_74_yr_M',
                                'Age_65_74_yr_F',
                                'Age_65_74_yr_P',
                                'Age_75_84_yr_M',
                                'Age_75_84_yr_F',
                                'Age_75_84_yr_P',
                                'Age_85ov_M',
                                'Age_85ov_F',
                                'Age_85ov_P',
                                'Counted_Census_Night_home_M',
                                'Counted_Census_Night_home_F',
                                'Counted_Census_Night_home_P',
                                'Count_Census_Nt_Ewhere_Aust_M',
                                'Count_Census_Nt_Ewhere_Aust_F',
                                'Count_Census_Nt_Ewhere_Aust_P',
                                'Indigenous_psns_Aboriginal_M',
                                'Indigenous_psns_Aboriginal_F',
                                'Indigenous_psns_Aboriginal_P',
                                'Indig_psns_Torres_Strait_Is_M',
                                'Indig_psns_Torres_Strait_Is_F',
                                'Indig_psns_Torres_Strait_Is_P',
                                'Indig_Bth_Abor_Torres_St_Is_M',
                                'Indig_Bth_Abor_Torres_St_Is_F',
                                'Indig_Bth_Abor_Torres_St_Is_P',
                                'Indigenous_P_Tot_M',
                                'Indigenous_P_Tot_F',
                                'Indigenous_P_Tot_P',
                                'Birthplace_Australia_M',
                                'Birthplace_Australia_F',
                                'Birthplace_Australia_P',
                                'Birthplace_Elsewhere_M',
                                'Birthplace_Elsewhere_F',
                                'Birthplace_Elsewhere_P',
                                'Lang_spoken_home_Eng_only_M',
                                'Lang_spoken_home_Eng_only_F',
                                'Lang_spoken_home_Eng_only_P',
                                'Lang_spoken_home_Oth_Lang_M',
                                'Lang_spoken_home_Oth_Lang_F',
                                'Lang_spoken_home_Oth_Lang_P',
                                'Australian_citizen_M',
                                'Australian_citizen_F',
                                'Australian_citizen_P',
                                'Age_psns_att_educ_inst_0_4_M',
                                'Age_psns_att_educ_inst_0_4_F',
                                'Age_psns_att_educ_inst_0_4_P',
                                'Age_psns_att_educ_inst_5_14_M',
                                'Age_psns_att_educ_inst_5_14_F',
                                'Age_psns_att_educ_inst_5_14_P',
                                'Age_psns_att_edu_inst_15_19_M',
                                'Age_psns_att_edu_inst_15_19_F',
                                'Age_psns_att_edu_inst_15_19_P',
                                'Age_psns_att_edu_inst_20_24_M',
                                'Age_psns_att_edu_inst_20_24_F',
                                'Age_psns_att_edu_inst_20_24_P',
                                'Age_psns_att_edu_inst_25_ov_M',
                                'Age_psns_att_edu_inst_25_ov_F',
                                'Age_psns_att_edu_inst_25_ov_P',
                                'High_yr_schl_comp_Yr_12_eq_M',
                                'High_yr_schl_comp_Yr_12_eq_F',
                                'High_yr_schl_comp_Yr_12_eq_P',
                                'High_yr_schl_comp_Yr_11_eq_M',
                                'High_yr_schl_comp_Yr_11_eq_F',
                                'High_yr_schl_comp_Yr_11_eq_P',
                                'High_yr_schl_comp_Yr_10_eq_M',
                                'High_yr_schl_comp_Yr_10_eq_F',
                                'High_yr_schl_comp_Yr_10_eq_P',
                                'High_yr_schl_comp_Yr_9_eq_M',
                                'High_yr_schl_comp_Yr_9_eq_F',
                                'High_yr_schl_comp_Yr_9_eq_P',
                                'High_yr_schl_comp_Yr_8_belw_M',
                                'High_yr_schl_comp_Yr_8_belw_F',
                                'High_yr_schl_comp_Yr_8_belw_P',
                                'High_yr_schl_comp_D_n_g_sch_M',
                                'High_yr_schl_comp_D_n_g_sch_F',
                                'High_yr_schl_comp_D_n_g_sch_P',
                                'Count_psns_occ_priv_dwgs_M',
                                'Count_psns_occ_priv_dwgs_F',
                                'Count_psns_occ_priv_dwgs_P',
                                'Count_Persons_other_dwgs_M',
                                'Count_Persons_other_dwgs_F',
                                'Count_Persons_other_dwgs_P'],
                    'rows': 132},
 'raw_census_g02': {'filename': '2016Census_G02_NSW_LGA.csv',
                    'headers': ['LGA_CODE_2016',
                                'Median_age_persons',
                                'Median_mortgage_repay_monthly',
                                'Median_tot_prsnl_inc_weekly',
                                'Median_rent_weekly',
                                'Median_tot_fam_inc_weekly',
                                'Average_num_psns_per_bedroom',
                                'Median_tot_hhd_inc_weekly',
                                'Average_household_size'],
                    'rows': 132},
 'raw_lga_code': {'filename': 'NSW_LGA_CODE.csv', 'headers': ['LGA_CODE', 'LGA_NAME'], 'rows': 129},
 'raw_lga_suburb': {'filename': 'NSW_LGA_SUBURB.csv',
                    'headers': ['LGA_NAME',
                                'SUBURB_NAME',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                '',
                                ''],
                    'rows': 4470}}

SOURCE_MANIFEST = {'01_2021.csv': {'rows': 33902, 'sha256': '7dd40f2e2365e16dcc6e5a0cfa192cf31937d811ed74cad33bae18431718a8af'},
 '02_2021.csv': {'rows': 33630, 'sha256': 'a1afdf2e404ddd1d2026ec17e966a80ca49ff12898ff987c723abb745606be96'},
 '03_2021.csv': {'rows': 33229, 'sha256': '8bcc107d871590e35372e1a78aa943c118eeddca35631d29defcca6e0f21e7be'},
 '04_2021.csv': {'rows': 32679, 'sha256': '8e1860b7b201c6e2e159edf6d2a102b878792a801c5e7499cb354feeb9a3951f'},
 '05_2020.csv': {'rows': 37562, 'sha256': '30ac7a2f7c9e2cea9b9bf7ff313b820945edfd1e566e9cb4c34b9b92a5a31600'},
 '06_2020.csv': {'rows': 36901, 'sha256': 'ac3a91c6ef8d5e0720e1e749bd6b5fb7acb215bb8aa994e5701ccddaaf8b5428'},
 '07_2020.csv': {'rows': 36057, 'sha256': '8f67e8def491d099957531ed2f650f2ef3e1ab3258a44af152de2177d7125193'},
 '08_2020.csv': {'rows': 31391, 'sha256': '3bdba346e2ff5219b1a262a13500893780dae7e7b13d0d4477d1e39e1a07616b'},
 '09_2020.csv': {'rows': 34829, 'sha256': '1fab1cc6018a55489c1bd95a3a3ab96ddadf226eae572b6529c21a00585c28f2'},
 '10_2020.csv': {'rows': 34276, 'sha256': '6dcd0d09d4f94473aa12bf4dd9e926859f44470cd50a3541ca2547b0ba46609b'},
 '11_2020.csv': {'rows': 33795, 'sha256': 'b745f434a5c614a1e53d16fd0fba6b264f3f76b1d74c8d027a12ece657d4e186'},
 '12_2020.csv': {'rows': 33871, 'sha256': '8a945954d81f3e69c6ec5365bfefbd98dfe401a5c7e72714f82665418e850802'},
 '2016Census_G01_NSW_LGA.csv': {'rows': 132,
                                'sha256': 'f9db8fb29c9bc034be1cb31296bf9e85b1daa8368044ac11a8eeff76c7fd4dd4'},
 '2016Census_G02_NSW_LGA.csv': {'rows': 132,
                                'sha256': '1415ed6aebe4726921be823ff8ef910cff01151ed3a0cc438326de477b1a7df7'},
 'NSW_LGA_CODE.csv': {'rows': 129,
                      'sha256': 'd4b0d2238588ffc5c2c1007d5307d83ac048ea849392d7ff9c2504b0a1f8dcfe'},
 'NSW_LGA_SUBURB.csv': {'rows': 4470,
                        'sha256': '0c2f2b3b764b924011a4a85d8547f4276cc2a4c69d24190a6a14a77021c33580'}}

for _table, _spec in SOURCE_SPECS.items():
    _spec['columns'] = [name.lower() if name else f'source_column_{index:02d}'
                        for index, name in enumerate(_spec['headers'], 1)]
    _spec['folder'] = 'listings' if _table == 'raw_airbnb' else ('census' if 'census' in _table else 'lga')


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()

def inspect_csv(path, spec):
    # csv.reader preserves strings, empty fields and duplicates; no pandas NA inference.
    count = 0
    try:
        with open(path, encoding='utf-8-sig', newline='') as handle:
            rows = csv.reader(handle, strict=True)
            if next(rows, None) != spec['headers']:
                raise AirflowException(f"Header mismatch: {spec['filename']}")
            for row in rows:
                if len(row) != len(spec['headers']):
                    raise AirflowException(f"Incorrect field count: {spec['filename']}, CSV line {rows.line_num}")
                if any('\x00' in value for value in row):
                    raise AirflowException(f"Unsupported zero byte: {spec['filename']}, CSV line {rows.line_num}")
                count += 1
    except (UnicodeError, csv.Error) as error:
        raise AirflowException(f"Invalid CSV encoding or syntax: {spec['filename']}") from None
    if count == 0 or (spec['rows'] is not None and count != spec['rows']):
        raise AirflowException(f"Unexpected row count: {spec['filename']}: {count}")
    return count

def expected_columns(spec):
    columns = [(c, 'text', True, None) for c in spec['columns']]
    columns += [('_source_file', 'text', False, None),
                ('_loaded_at', 'timestamp with time zone', False, 'CURRENT_TIMESTAMP')]
    if spec['table'] == 'raw_airbnb':
        columns.append(('_source_month', 'date', False, None))
    return columns

def check_database(cursor):
    cursor.execute("SET LOCAL statement_timeout = '120s'")
    cursor.execute("SET LOCAL lock_timeout = '15s'")
    cursor.execute("SELECT current_database(), (SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid())")
    database, ssl_active = cursor.fetchone()
    if database != DATABASE or ssl_active is not True:
        raise AirflowException('The expected database and an SSL connection are required.')
    cursor.execute('SELECT contract_version, bucket_name, source_prefix FROM pipeline.configuration WHERE singleton')
    if cursor.fetchone() != (CONTRACT_VERSION, BUCKET, PREFIX):
        raise AirflowException('The database source contract does not match this DAG.')
    for table, original in SOURCE_SPECS.items():
        spec = dict(original, table=table)
        cursor.execute("""SELECT column_name, data_type, is_nullable = 'YES', column_default
                          FROM information_schema.columns
                          WHERE table_schema = 'bronze' AND table_name = %s
                          ORDER BY ordinal_position""", (table,))
        if cursor.fetchall() != expected_columns(spec):
            raise AirflowException(f'Unexpected Bronze table structure: {table}')
        cursor.execute('''SELECT
            (SELECT COUNT(*) FROM pg_index WHERE indrelid = to_regclass(%s) AND indisunique),
            (SELECT COUNT(*) FROM pg_trigger WHERE tgrelid = to_regclass(%s) AND NOT tgisinternal),
            (SELECT COUNT(*) FROM pg_rewrite WHERE ev_class = to_regclass(%s) AND rulename <> '_RETURN')''',
            (f'bronze.{table}',) * 3)
        if any(cursor.fetchone()):
            raise AirflowException(f'Unexpected unique index, trigger or rule on {table}; review before loading.')


    cursor.execute("SELECT convalidated, pg_get_expr(conbin,conrelid) FROM pg_constraint WHERE conrelid = 'bronze.raw_airbnb'::regclass AND contype = 'c'")
    constraint_matches = False
    for validated, expression in cursor.fetchall():
        normalised = re.sub(r'[\s()]', '', expression).lower().replace('::numeric', '')
        if validated and normalised in ('extractdayfrom_source_month=1', "extractdayfrom_source_month='1'"):
            constraint_matches = True
    if not constraint_matches:
        raise AirflowException('The validated month-start constraint expression differs from part_1.sql.')
    cursor.execute('CREATE TEMP TABLE month_constraint_probe (LIKE bronze.raw_airbnb INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP')
    cursor.execute("INSERT INTO month_constraint_probe (_source_file,_source_month) VALUES ('constraint_probe',DATE '2020-05-01')")
    cursor.execute('SAVEPOINT month_constraint_guard')
    rejected = False
    try:
        cursor.execute("INSERT INTO month_constraint_probe (_source_file,_source_month) VALUES ('constraint_probe',DATE '2020-05-02')")
    except Exception as error:
        rejected = (getattr(error, 'sqlstate', None) or getattr(error, 'pgcode', None)) == '23514'
        cursor.execute('ROLLBACK TO SAVEPOINT month_constraint_guard')
        if not rejected:
            raise AirflowException('Could not verify the month-start constraint.') from None
    else:
        cursor.execute('ROLLBACK TO SAVEPOINT month_constraint_guard')
    cursor.execute('RELEASE SAVEPOINT month_constraint_guard')
    if not rejected:
        raise AirflowException('The Airbnb constraint did not reject a non-first-of-month date.')

def insert_columns(spec):
    return spec['columns'] + ['_source_file'] + (['_source_month'] if spec['table'] == 'raw_airbnb' else [])

def stage_file(cursor, spec, source_path, directory, month):
    stage_name = 'stage_' + spec['table']
    target = sql.Identifier('bronze', spec['table'])
    stage = sql.Identifier(stage_name)
    cursor.execute(sql.SQL('CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP')
                   .format(stage, target))
    normalised = Path(directory) / ('copy_' + spec['filename'])
    with open(source_path, encoding='utf-8-sig', newline='') as source, \
            open(normalised, 'w', encoding='utf-8', newline='') as output:
        reader = csv.reader(source, strict=True)
        next(reader)
        # Quote every value: even literal backslash-N and empty strings stay literal strings.
        writer = csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator='\n')
        extra = [spec['filename']] + ([month + '-01'] if spec['table'] == 'raw_airbnb' else [])
        for row in reader:
            writer.writerow(row + extra)
    columns = sql.SQL(', ').join(map(sql.Identifier, insert_columns(spec)))
    copy_statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '\\N', ENCODING 'UTF8')").format(stage, columns)
    with open(normalised, encoding='utf-8', newline='') as handle:
        if USE_PSYCOPG3:
            with cursor.copy(copy_statement) as copy:
                for chunk in iter(lambda: handle.read(1024 * 1024), ''):
                    copy.write(chunk)
        else:
            cursor.copy_expert(copy_statement.as_string(cursor), handle)
    return stage

def assert_source_values(cursor, spec, stage, path, directory):
    # Independently compare parsed original CSV records with PostgreSQL staging values.
    # Disk-backed signed counts retain duplicates without keeping all records in RAM.
    counts_path = Path(directory) / (spec['table'] + '_counts.sqlite')
    upsert = ('INSERT INTO counts(value,balance) VALUES (?,?) '
              'ON CONFLICT(value) DO UPDATE SET balance = balance + excluded.balance')
    canonical = lambda values: json.dumps(list(values), ensure_ascii=False, separators=(',', ':'))
    with closing(sqlite3.connect(str(counts_path))) as comparison:
        comparison.execute('CREATE TABLE counts(value TEXT PRIMARY KEY, balance INTEGER NOT NULL)')
        with open(path, encoding='utf-8-sig', newline='') as source:
            reader = csv.reader(source, strict=True)
            next(reader)
            comparison.executemany(upsert, ((canonical(row), 1) for row in reader))
        columns = sql.SQL(', ').join(map(sql.Identifier, spec['columns']))
        cursor.execute(sql.SQL('SELECT {} FROM {}').format(columns, stage))
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            comparison.executemany(upsert, ((canonical(row), -1) for row in rows))
        if comparison.execute('SELECT COUNT(*) FROM counts WHERE balance <> 0').fetchone()[0]:
            raise AirflowException(f"Original CSV values differ from staging: {spec['table']}")


def month_date(month):
    if month not in MONTHS:
        raise AirflowException('Observation month is outside May 2020 to April 2021.')
    return date.fromisoformat(month + '-01')


def specs_for(month):
    month_date(month)
    result = []
    for table, original in SOURCE_SPECS.items():
        spec = dict(original, table=table)
        if table == 'raw_airbnb':
            year, number = month.split('-')
            spec['filename'] = f'{number}_{year}.csv'
        spec.update(SOURCE_MANIFEST[spec['filename']])
        spec['object_name'] = PREFIX + spec['folder'] + '/' + spec['filename']
        result.append(spec)
    return result


def manifest_digest(manifest):
    value = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(value).hexdigest()


def content_digest(rows):
    # Sort canonical records: independent of row order, sensitive to duplicates and NULL vs ''.
    values = sorted(json.dumps(list(row), ensure_ascii=False, separators=(',', ':')) for row in rows)
    digest = hashlib.sha256()
    for value in values:
        record = value.encode('utf-8')
        digest.update(len(record).to_bytes(8, 'big'))
        digest.update(record)
    return digest.hexdigest()


def download_checked(bucket, spec, path, expected_generation=None):
    blob = bucket.blob(spec['object_name'])
    blob.reload(timeout=60)
    if blob.size is None or not 0 < blob.size <= MAX_SOURCE_BYTES:
        raise AirflowException('Source object size is outside the assignment-file limit.')
    generation = int(blob.generation)
    if expected_generation is not None and generation != int(expected_generation):
        raise AirflowException('A source object was replaced after validation; review it before retrying.')
    blob.download_to_filename(str(path), if_generation_match=generation, timeout=60)
    if file_sha256(path) != spec['sha256']:
        raise AirflowException(f"Source SHA256 differs from the supplied file: {spec['filename']}")
    count = inspect_csv(path, spec)
    with open(path, encoding='utf-8-sig', newline='') as handle:
        rows = csv.reader(handle, strict=True)
        next(rows)
        digest = content_digest(rows)
    return {'table': spec['table'], 'filename': spec['filename'], 'object_name': spec['object_name'],
            'generation': generation, 'sha256': spec['sha256'], 'content_sha256': digest, 'rows': count}


@contextmanager
def database_transaction():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID,
                        options='-c client_encoding=UTF8 -c statement_timeout=120000 -c lock_timeout=15000')
    settings = hook.get_connection(POSTGRES_CONN_ID)
    extras = settings.extra_dejson
    try:
        timeout = int(extras.get('connect_timeout', 0))
    except (TypeError, ValueError):
        timeout = 0
    if settings.schema != DATABASE or not 1 <= timeout <= 15:
        raise AirflowException('Configure the exact restart database and connect_timeout of 1 to 15 seconds.')
    if extras.get('sslmode') not in ('require', 'verify-ca', 'verify-full'):
        raise AirflowException('The PostgreSQL connection must require SSL.')
    with closing(hook.get_conn()) as connection:
        connection.autocommit = False
        try:
            with connection.cursor() as cursor:
                check_database(cursor)
                cursor.execute('SELECT pg_advisory_xact_lock(%s)', (LOCK_ID,))
                yield cursor
            connection.commit()
        except Exception as error:
            connection.rollback()
            if isinstance(error, AirflowException):
                raise
            state = getattr(error, 'sqlstate', None) or getattr(error, 'pgcode', None) or 'unavailable'
            if not re.fullmatch(r'[A-Z0-9]{5}', state):
                state = 'unavailable'
            raise AirflowException(f'Database step failed; error_type={type(error).__name__}; SQLSTATE={state}') from None


def loaded_history(cursor):
    cursor.execute('SELECT observation_month FROM pipeline.monthly_loads ORDER BY observation_month')
    months = [value.isoformat()[:7] for (value,) in cursor.fetchall()]
    if months != list(MONTHS[:len(months)]):
        raise AirflowException('Load history must be a consecutive prefix starting in May.')
    cursor.execute('SELECT DISTINCT _source_month FROM bronze.raw_airbnb ORDER BY _source_month')
    actual = [value.isoformat()[:7] for (value,) in cursor.fetchall()]
    if actual != months:
        raise AirflowException('Raw months and committed load records disagree.')
    return months


def require_completed(cursor, months):
    if not months:
        return
    cursor.execute('''SELECT m.observation_month
        FROM pipeline.monthly_loads m LEFT JOIN pipeline.dbt_completions d USING (observation_month)
        WHERE m.observation_month <= %s AND
            (m.verified_at IS NULL OR d.observation_month IS NULL
             OR d.manifest_sha256 IS DISTINCT FROM m.manifest_sha256
             OR d.completed_at < m.verified_at)
        ORDER BY m.observation_month''', (month_date(months[-1]),))
    if cursor.fetchall():
        raise AirflowException('The preceding month needs verified Bronze and a successful saved dbt job before advancing.')


def select_month(cursor, run_id, mode):
    history = loaded_history(cursor)
    cursor.execute('SELECT observation_month FROM pipeline.load_runs WHERE run_id = %s', (run_id,))
    existing = cursor.fetchone()
    if existing:
        month = existing[0].isoformat()[:7]
        check_selected_month(cursor, month, history)
        return month
    if mode == 'retry_latest':
        if not history:
            raise AirflowException('There is no loaded month to retry.')
        month = history[-1]
    elif mode == 'next':
        if len(history) == len(MONTHS):
            raise AirflowException('All twelve source months have already been loaded.')
        require_completed(cursor, history)
        month = MONTHS[len(history)]
    else:
        raise AirflowException('Use next or retry_latest.')
    cursor.execute('INSERT INTO pipeline.load_runs (run_id, observation_month) VALUES (%s,%s)',
                   (run_id, month_date(month)))
    return month


def check_selected_month(cursor, month, history):
    if history and month == history[-1]:
        require_completed(cursor, history[:-1])
        return
    if len(history) < len(MONTHS) and month == MONTHS[len(history)]:
        require_completed(cursor, history)
        return
    raise AirflowException('This run is for an older or skipped month; it cannot advance or rewrite history.')


def read_manifest(cursor, run_id):
    cursor.execute('SELECT observation_month, source_manifest FROM pipeline.load_runs WHERE run_id = %s', (run_id,))
    record = cursor.fetchone()
    if not record or not isinstance(record[1], dict):
        raise AirflowException('This run has no validated source manifest.')
    month, manifest = record[0].isoformat()[:7], record[1]
    if manifest.get('month') != month or manifest.get('contract_version') != CONTRACT_VERSION:
        raise AirflowException('Run manifest has an incorrect month or source contract.')
    specs = specs_for(month)
    files = manifest.get('files', [])
    if len(files) != len(specs):
        raise AirflowException('The run manifest must contain five source files.')
    for spec, item in zip(specs, files):
        for field in ('table', 'filename', 'object_name', 'rows', 'sha256'):
            if item.get(field) != spec[field]:
                raise AirflowException('The run manifest differs from the fixed source contract.')
    return month, manifest


def assert_history_contents(cursor, history):
    cursor.execute('''SELECT source_file, target_table, observation_month, generation, sha256,
                             content_sha256, row_count FROM pipeline.source_files ORDER BY source_file''')
    files = cursor.fetchall()
    expected_files = {spec['object_name'] for month in history for spec in specs_for(month)}
    if {row[0] for row in files} != expected_files:
        raise AirflowException('Stored source inventory does not match the loaded month history.')
    for name, table, observed, generation, sha, digest, count in files:
        filename = Path(name).name
        expected = SOURCE_MANIFEST.get(filename)
        if not expected or (sha, count) != (expected['sha256'], expected['rows']):
            raise AirflowException('A stored source record differs from the input baseline.')
        month = observed.isoformat()[:7] if observed else MONTHS[0]
        spec = next((s for s in specs_for(month) if s['object_name'] == name), None)
        if spec is None or spec['table'] != table or generation <= 0:
            raise AirflowException('A stored source record has an incorrect table or month.')
        columns = sql.SQL(', ').join(map(sql.Identifier, spec['columns']))
        condition = sql.SQL('WHERE _source_month = %s') if observed else sql.SQL('')
        args = (observed,) if observed else ()
        cursor.execute(sql.SQL('SELECT {}, _source_file, _loaded_at FROM {} {}')
                       .format(columns, sql.Identifier('bronze', table), condition), args)
        records = cursor.fetchall()
        if len(records) != count or any(row[-2] != filename or row[-1] is None for row in records):
            raise AirflowException('Stored raw row counts or source metadata changed.')
        if content_digest(row[:-2] for row in records) != digest:
            raise AirflowException('Stored raw source values changed; stop before advancing.')
    if not history:
        for table in SOURCE_SPECS:
            cursor.execute(sql.SQL('SELECT count(*) FROM {}').format(sql.Identifier('bronze', table)))
            if cursor.fetchone()[0]:
                raise AirflowException('Initial raw tables must all be empty.')


def validate_sources(**context):
    run_id = context['run_id']
    # Persist the selection before downloads. XCom is not the source of retry identity.
    with database_transaction() as cursor:
        month = select_month(cursor, run_id, context['params']['mode'])
        assert_history_contents(cursor, loaded_history(cursor))
    bucket = GCSHook(gcp_conn_id='google_cloud_default').get_conn().bucket(BUCKET)
    manifest = {'contract_version': CONTRACT_VERSION, 'month': month, 'files': []}
    with TemporaryDirectory(prefix='bronze_validate_') as directory:
        for spec in specs_for(month):
            item = download_checked(bucket, spec, Path(directory) / spec['filename'])
            manifest['files'].append(item)
            LOG.info('Validated %s: rows=%s; generation=%s; SHA256=%s',
                     item['filename'], item['rows'], item['generation'], item['sha256'])
    with database_transaction() as cursor:
        check_selected_month(cursor, month, loaded_history(cursor))
        cursor.execute('SELECT source_manifest FROM pipeline.load_runs WHERE run_id = %s', (run_id,))
        saved = cursor.fetchone()[0]
        if saved is not None and saved != manifest:
            raise AirflowException('The same Airflow run cannot change its source manifest.')
        cursor.execute('UPDATE pipeline.load_runs SET source_manifest = %s::jsonb WHERE run_id = %s',
                       (json.dumps(manifest), run_id))
    return {'month': month, 'manifest_sha256': manifest_digest(manifest)}


def assert_identical(cursor, spec, stage, month):
    columns = sql.SQL(', ').join(map(sql.Identifier, insert_columns(spec)))
    condition = sql.SQL('WHERE _source_month = %s') if spec['table'] == 'raw_airbnb' else sql.SQL('')
    args = (month_date(month),) if spec['table'] == 'raw_airbnb' else ()
    target_rows = sql.SQL('SELECT {} FROM {} {}').format(columns, sql.Identifier('bronze', spec['table']), condition)
    source_rows = sql.SQL('SELECT {} FROM {}').format(columns, stage)
    cursor.execute(sql.SQL('SELECT count(*) FROM (({} EXCEPT ALL {}) UNION ALL ({} EXCEPT ALL {})) differences')
                   .format(target_rows, source_rows, source_rows, target_rows), args + args)
    if cursor.fetchone()[0]:
        raise AirflowException(f"Exact source comparison failed: {spec['table']}")


def process_manifest(cursor, run_id, month, manifest, directory, verify_only=False):
    # Hold table locks through staging, checking and commit. No partial file or month is visible.
    tables = sql.SQL(', ').join(sql.Identifier('bronze', table) for table in SOURCE_SPECS)
    cursor.execute(sql.SQL('LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE').format(tables))
    history = loaded_history(cursor)
    check_selected_month(cursor, month, history)
    assert_history_contents(cursor, history)
    is_retry = month in history
    if verify_only and not is_retry:
        raise AirflowException('Cannot verify a month that has not committed.')
    for spec, item in zip(specs_for(month), manifest['files']):
        path = Path(directory) / spec['filename']
        stage = stage_file(cursor, spec, path, directory, month)
        assert_source_values(cursor, spec, stage, path, directory)
        cursor.execute('SELECT generation, sha256, content_sha256, row_count FROM pipeline.source_files WHERE source_file = %s',
                       (spec['object_name'],))
        stored = cursor.fetchone()
        expected = (item['generation'], item['sha256'], item['content_sha256'], item['rows'])
        if stored is not None and stored != expected:
            raise AirflowException('Loaded source identity differs from the validated object.')
        must_exist = is_retry or spec['table'] != 'raw_airbnb' and month != MONTHS[0]
        if must_exist and stored is None:
            raise AirflowException('An expected committed source record is missing.')
        if stored is None:
            if verify_only:
                raise AirflowException('Verification cannot insert raw rows.')
            columns = sql.SQL(', ').join(map(sql.Identifier, insert_columns(spec)))
            cursor.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {}')
                           .format(sql.Identifier('bronze', spec['table']), columns, columns, stage))
            cursor.execute('''INSERT INTO pipeline.source_files
                (source_file, target_table, observation_month, generation, sha256, content_sha256, row_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                (spec['object_name'], spec['table'], month_date(month) if spec['table'] == 'raw_airbnb' else None, *expected))
        assert_identical(cursor, spec, stage, month)
    digest = manifest_digest(manifest)
    if is_retry:
        cursor.execute('SELECT manifest_sha256 FROM pipeline.monthly_loads WHERE observation_month = %s', (month_date(month),))
        if cursor.fetchone()[0] != digest:
            raise AirflowException('Retry manifest differs from the committed monthly manifest.')
    else:
        cursor.execute('''INSERT INTO pipeline.monthly_loads (observation_month, run_id, manifest_sha256)
                          VALUES (%s,%s,%s)''', (month_date(month), run_id, digest))
    if verify_only:
        cursor.execute('''UPDATE pipeline.monthly_loads SET verified_at = COALESCE(verified_at, CURRENT_TIMESTAMP)
                          WHERE observation_month = %s''', (month_date(month),))
        cursor.execute('''UPDATE pipeline.load_runs SET verified_at = COALESCE(verified_at, CURRENT_TIMESTAMP)
                          WHERE run_id = %s''', (run_id,))
    LOG.info('%s %s; source rows=%s; exact comparisons passed; raw retry=%s',
             'Verified' if verify_only else 'Loaded', month,
             SOURCE_MANIFEST[specs_for(month)[0]['filename']]['rows'], is_retry)


def run_database_step(context, verify_only):
    run_id = context['run_id']
    with database_transaction() as cursor:
        month, manifest = read_manifest(cursor, run_id)
    bucket = GCSHook(gcp_conn_id='google_cloud_default').get_conn().bucket(BUCKET)
    with TemporaryDirectory(prefix='bronze_month_') as directory:
        for spec, item in zip(specs_for(month), manifest['files']):
            actual = download_checked(bucket, spec, Path(directory) / spec['filename'], item['generation'])
            if actual != item:
                raise AirflowException('Downloaded source differs from this run manifest.')
        with database_transaction() as cursor:
            checked_month, checked_manifest = read_manifest(cursor, run_id)
            if (checked_month, checked_manifest) != (month, manifest):
                raise AirflowException('Run selection changed during processing.')
            process_manifest(cursor, run_id, month, manifest, directory, verify_only=verify_only)
    return {'month': month, 'verified': verify_only}


def load_bronze(**context):
    return run_database_step(context, verify_only=False)


def verify_bronze(**context):
    result = run_database_step(context, verify_only=True)
    LOG.info('Bronze month %s verified. Manually run its saved dbt Cloud job before the next month.', result['month'])
    return result


with DAG(
    dag_id=DAG_ID,
    description='Load and verify one monthly Airbnb file, with fixed Census and LGA references.',
    start_date=datetime(2020, 5, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'karthik_ramesh', 'retries': 1, 'retry_delay': timedelta(minutes=1)},
    dagrun_timeout=timedelta(minutes=30),
    params={'mode': Param('next', type='string', enum=['next', 'retry_latest'],
                          description='Next chronological month, or verify the unchanged latest month again.')},
    tags=['assignment3', 'bronze'],
    is_paused_upon_creation=True,
) as dag:
    validate = PythonOperator(task_id='validate_sources', python_callable=validate_sources,
                              execution_timeout=timedelta(minutes=8), show_return_value_in_logs=False)
    load = PythonOperator(task_id='load_bronze', python_callable=load_bronze,
                          execution_timeout=timedelta(minutes=10), show_return_value_in_logs=False)
    verify = PythonOperator(task_id='verify_bronze', python_callable=verify_bronze,
                            execution_timeout=timedelta(minutes=10), show_return_value_in_logs=False)
    validate >> load >> verify
