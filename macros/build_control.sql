{% macro required_build_nodes() %}
  {% set nodes = [] %}
  {% if execute %}
    {% for n in graph.nodes.values() %}
      {% if n.package_name == project_name and n.resource_type in ['model','snapshot','test','seed'] %}
        {% do nodes.append(n.unique_id) %}
      {% endif %}
    {% endfor %}
  {% endif %}
  {{ return(nodes) }}
{% endmacro %}

{% macro bronze_checks() %}
{% set selected_month = observation_month_value() %}
do $bronze_guard$
declare actual_count bigint; actual_hash text;
begin
  if current_database() <> 'BDEAT3' or not exists
      (select 1 from pg_stat_ssl where pid=pg_backend_pid() and ssl) then
    raise exception 'BDEAT3 and an SSL database connection are required';
  end if;
  if not exists (select 1 from pipeline.configuration where singleton
      and contract_version='at3-rebuild-v1'
      and bucket_name='australia-southeast1-bigdat-bc43ec38-bucket'
      and source_prefix='data/at3_rebuild/') then
    raise exception 'The Bronze source configuration does not match this project';
  end if;
  if (select max(observation_month) from pipeline.monthly_loads)
      is distinct from {{ observation_month() }} then
    raise exception 'Requested month must be the latest loaded Bronze month';
  end if;
  if exists (select 1 from pipeline.monthly_loads m
      left join pipeline.load_runs r using(run_id)
      where m.verified_at is null or r.verified_at is null
         or r.observation_month is distinct from m.observation_month) then
    raise exception 'Every loaded month needs a verified Airflow load';
  end if;
  if exists (select 1 from generate_series(date '2020-05-01',
      {{ observation_month() }},interval '1 month') d
      where not exists(select 1 from pipeline.monthly_loads m where m.observation_month=d::date)) then
    raise exception 'Bronze month history has a gap';
  end if;
  {% set expected = [] %}
  {% for s in source_contract() %}
    {% if s.month is none or s.month <= selected_month %}
      {% do expected.append(s) %}
      if not exists (select 1 from pipeline.source_files where
          source_file='data/at3_rebuild/{{ "listings" if s.month else ("census" if "census" in s.table else "lga") }}/{{ s.file }}'
          and target_table='{{ s.table }}' and generation > 0
          and observation_month is not distinct from {{ "date '" ~ s.month ~ "'" if s.month else 'null::date' }}
          and row_count={{ s.rows }} and sha256='{{ s.sha256 }}'
          and content_sha256='{{ s.content_sha256 }}') then
        raise exception 'Source ledger differs from the checked CSV contract: {{ s.file }}';
      end if;
      select count(*), encode(sha256(string_agg(
          int8send(octet_length(convert_to(v,'UTF8'))::bigint)||convert_to(v,'UTF8'),
          ''::bytea order by v collate "C")),'hex')
      into actual_count, actual_hash
      from (select array_to_json(array[{{ s.columns|join(', ') }}])::text as v
            from bronze.{{ s.table }}
            {% if s.month %}where _source_month=date '{{ s.month }}'{% endif %}) q;
      if actual_count <> {{ s.rows }} or actual_hash is distinct from '{{ s.content_sha256 }}' then
        raise exception 'Bronze rows differ from source values: {{ s.file }}';
      end if;
      if exists(select 1 from bronze.{{ s.table }}
          where {% if s.month %}_source_month=date '{{ s.month }}' and{% endif %}
          (_source_file is distinct from '{{ s.file }}' or _loaded_at is null)) then
        raise exception 'Invalid source metadata: {{ s.file }}';
      end if;
    {% endif %}
  {% endfor %}
  if (select count(*) from pipeline.source_files) <> {{ expected|length }}
      or exists(select 1 from bronze.raw_airbnb where _source_month is null
          or _source_month>{{ observation_month() }} or _source_month<date '2020-05-01'
          or extract(day from _source_month)<>1) then
    raise exception 'Unexpected source file or observation month';
  end if;
  if exists ({{ bronze_business_issues() }}) then
    raise exception 'Bronze business validation failed; inspect silver_preflight';
  end if;
end $bronze_guard$;
{% endmacro %}

{% macro begin_monthly_build() %}
  {% if execute and flags.WHICH == 'build' %}
    {% if target.type != 'postgres' or target.database != 'BDEAT3' %}
      {{ exceptions.raise_compiler_error('Use PostgreSQL database BDEAT3.') }}
    {% endif %}
    {% if flags.FULL_REFRESH or target.threads|int != 1 %}
      {{ exceptions.raise_compiler_error('Monthly builds require --threads 1 and no --full-refresh.') }}
    {% endif %}
    {% for uid in required_build_nodes() %}
      {% if uid not in selected_resources %}
        {{ exceptions.raise_compiler_error('Monthly build cannot exclude a required resource: ' ~ uid) }}
      {% endif %}
    {% endfor %}
    {% if target.schema == 'bdeat3' %}
      {% if not ((target.host == '34.151.182.59') or
                 (target.name == 'qa' and target.host == '127.0.0.1' and target.port == 55433)) %}
        {{ exceptions.raise_compiler_error('Unrecognised canonical deployment target.') }}
      {% endif %}
      {% for key in ['DBT_CLOUD_JOB_ID','DBT_CLOUD_RUN_ID'] %}
        {% if env_var(key,'')|int <= 0 %}
          {{ exceptions.raise_compiler_error('Canonical schemas require a saved dbt Cloud job: ' ~ key) }}
        {% endif %}
      {% endfor %}
      {% if env_var('DBT_CLOUD_INVOCATION_CONTEXT','') != 'prod' %}
        {{ exceptions.raise_compiler_error('Canonical builds require the production deployment environment.') }}
      {% endif %}
      {% if not modules.re.fullmatch('[0-9a-f]{40}',env_var('DBT_DEPLOYMENT_GIT_SHA','')) %}
        {{ exceptions.raise_compiler_error('Set DBT_DEPLOYMENT_GIT_SHA to the reviewed deployment commit.') }}
      {% endif %}
    {% endif %}
    -- Atomically check Bronze and withdraw readiness before model writes.
    -- dbt closes hook connections, so cross-build concurrency is controlled by
    -- the single saved job. This transaction shares the Airflow loader lock.
    begin;
    do $build_lock$ begin
      if not pg_try_advisory_xact_lock(70506183174422::bigint) then
        raise exception 'Another load or dbt build is in progress';
      end if;
    end $build_lock$;
    {{ bronze_checks() }}
    {% if target.schema == 'bdeat3' %}
      create table if not exists pipeline.dbt_attempts (
        invocation_id text primary key,
        observation_month date not null references pipeline.monthly_loads(observation_month),
        job_id bigint not null, run_id bigint not null,
        git_sha text not null, started_at timestamptz not null default clock_timestamp(),
        finished_at timestamptz, status text not null check(status in ('started','succeeded','failed'))
      );
      insert into pipeline.dbt_attempts(invocation_id,observation_month,job_id,run_id,git_sha,status)
        values('{{ invocation_id }}',{{ observation_month() }},
        {{ env_var('DBT_CLOUD_JOB_ID')|int }},{{ env_var('DBT_CLOUD_RUN_ID')|int }},
        '{{ env_var('DBT_DEPLOYMENT_GIT_SHA') }}','started');
      -- This is current readiness state. The attempt history remains append-only per invocation.
      delete from pipeline.dbt_completions where observation_month={{ observation_month() }};
    {% endif %}
    commit;
  {% else %}
    select 1;
  {% endif %}
{% endmacro %}

{% macro complete_monthly_build(results) %}
  -- depends_on: {{ ref('slv_listings') }}
  -- depends_on: {{ ref('host_snapshot_input') }}
  -- depends_on: {{ ref('property_snapshot_input') }}
  {% if execute and flags.WHICH == 'build' %}
    {% set expected = required_build_nodes() %}
    {% set seen = [] %}
    {% set passed_tests = [] %}
    {% set failures = [] %}
    {% for r in results %}
      {% if r.node.unique_id in expected %}
        {% do seen.append(r.node.unique_id) %}
        {% if r.status|string not in ['success','pass'] %}
          {% do failures.append(r.node.unique_id) %}
        {% elif r.node.resource_type == 'test' %}
          {% do passed_tests.append(r.node.unique_id) %}
        {% endif %}
      {% endif %}
    {% endfor %}
    {% for uid in expected %}
      {% if uid not in seen %}{% do failures.append(uid) %}{% endif %}
    {% endfor %}
    {% if failures or not passed_tests %}
      {% if target.schema == 'bdeat3' %}
        {% do run_query("update pipeline.dbt_attempts set status='failed',finished_at=clock_timestamp() where invocation_id='" ~ invocation_id ~ "'") %}
      {% endif %}
      {{ exceptions.raise_compiler_error('No completion recorded: required resources did not all pass: ' ~ failures|join(', ')) }}
    {% endif %}
    -- Verify source values and complete histories again before releasing the next month.
    begin;
    select pg_advisory_xact_lock(70506183174422::bigint);
    {{ bronze_checks() }}
    {{ snapshot_checks('both') }}
    {% if target.schema == 'bdeat3' %}
      insert into pipeline.dbt_completions
        (observation_month,manifest_sha256,dbt_invocation_id,dbt_cloud_job_id,
         dbt_cloud_run_id,git_sha,dbt_version,passed_tests)
      select observation_month,manifest_sha256,'{{ invocation_id }}',
        {{ env_var('DBT_CLOUD_JOB_ID')|int }},{{ env_var('DBT_CLOUD_RUN_ID')|int }},
        '{{ env_var('DBT_DEPLOYMENT_GIT_SHA') }}','{{ dbt_version }}',{{ passed_tests|length }}
      from pipeline.monthly_loads where observation_month={{ observation_month() }};
      update pipeline.dbt_attempts set status='succeeded',finished_at=clock_timestamp()
        where invocation_id='{{ invocation_id }}';
    {% endif %}
    commit;
    {{ log('All ' ~ expected|length ~ ' required resources completed; ' ~ passed_tests|length ~ ' tests passed.', info=true) }}
  {% else %}
    select 1;
  {% endif %}
{% endmacro %}
