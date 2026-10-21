{% macro entity_columns(kind) -%}
  {%- if kind == 'host' -%}
    host_id, host_name, host_since, host_is_superhost,
    host_neighbourhood, host_lga_code, host_lga_name, host_location_status,
    observation_month, updated_at
  {%- elif kind == 'property' -%}
    listing_id, host_id, listing_neighbourhood, listing_lga_code, listing_lga_name,
    property_type, room_type, accommodates, observation_month, updated_at
  {%- else -%}
    {{ exceptions.raise_compiler_error('Unknown snapshot entity.') }}
  {%- endif -%}
{%- endmacro %}

{% macro expected_entity(kind, predicate) -%}
  select {% if kind == 'host' %}distinct{% endif %}
  {% if kind == 'host' %}
    host_id, host_name, host_since, host_is_superhost,
    host_neighbourhood, host_lga_code, host_lga_name, host_location_status,
  {% else %}
    listing_id, host_id, listing_neighbourhood, listing_lga_code, listing_lga_name,
    property_type, room_type, accommodates,
  {% endif %}
    observation_month, observed_at as updated_at
  from {{ ref('slv_listings') }}
  where {{ predicate }}
{%- endmacro %}

{% macro snapshot_checks(completed='none') %}
  {% if target.type != 'postgres' or target.database != 'BDEAT3' %}
    {{ exceptions.raise_compiler_error('Snapshots must target PostgreSQL database BDEAT3.') }}
  {% endif %}
  {% if completed not in ['none', 'host', 'property', 'both'] %}
    {{ exceptions.raise_compiler_error('Invalid snapshot completion check.') }}
  {% endif %}
-- Hooks run inside each PostgreSQL snapshot transaction. Both entities share the lock.
do $snapshot_guard$
declare
    requested date := {{ observation_month() }};
    first_month date;
    latest date;
    periods integer;
    bad boolean;
begin
    perform pg_advisory_xact_lock(470, 3);
    lock table {{ ref('slv_listings') }}, {{ ref('host_snapshot_input') }},
               {{ ref('property_snapshot_input') }} in share mode;

    {% for kind, key in [('host', 'host_id'), ('property', 'listing_id')] %}
    -- Do not filter away a stale month or silently accept an empty input.
    if not exists (select 1 from {{ ref(kind ~ '_snapshot_input') }}) then
        raise exception '{{ kind }} snapshot input is empty';
    end if;
    if exists (
        select 1 from {{ ref(kind ~ '_snapshot_input') }}
        where {{ key }} is null
           or observation_month is distinct from requested
           or updated_at is distinct from requested::timestamp
    ) then
        raise exception '{{ kind }} input key, month or timestamp does not match requested month %', requested;
    end if;
    if exists (
        select 1 from {{ ref(kind ~ '_snapshot_input') }}
        group by {{ key }} having count(*) <> 1
    ) then
        raise exception '{{ kind }} input has duplicate entity keys';
    end if;
    if exists (
        with expected as ({{ expected_entity(kind, 'observation_month = requested') }}),
        actual as (select * from {{ ref(kind ~ '_snapshot_input') }}),
        difference as (
            (select to_jsonb(e) from expected e except all select to_jsonb(a) from actual a)
            union all
            (select to_jsonb(a) from actual a except all select to_jsonb(e) from expected e)
        ) select 1 from difference
    ) then
        raise exception '{{ kind }} input does not exactly match selected-month Silver entities';
    end if;

    if to_regclass('{{ snapshot_relation(kind) }}') is null then
        if requested <> date '2020-05-01' then
            raise exception '{{ kind }} snapshot must start with May 2020';
        end if;
        {% if completed in [kind, 'both'] %}
        raise exception '{{ kind }} snapshot is not complete for requested month';
        {% endif %}
    else
        execute 'select min(observation_month), max(observation_month),
                        count(distinct observation_month) from {{ snapshot_relation(kind) }}'
            into first_month, latest, periods;
        if first_month is distinct from date '2020-05-01' or latest is null then
            raise exception '{{ kind }} snapshot history is empty or does not start with May';
        end if;
        if periods <> (select count(*) from generate_series(first_month, latest, interval '1 month')) then
            raise exception '{{ kind }} snapshot history has a missing month';
        end if;
        if requested <> latest and requested <> (latest + interval '1 month')::date then
            raise exception '{{ kind }} snapshot rejects skipped or backwards month % (latest %)', requested, latest;
        end if;
        {% if completed in [kind, 'both'] %}
        if latest <> requested then
            raise exception '{{ kind }} snapshot is not complete for requested month %', requested;
        end if;
        {% endif %}

        -- Compare ALL observed history, including a same-month retry, with Silver.
        execute $history_{{ kind }}$
            with expected as ({{ expected_entity(kind, 'observation_month <= $1') }}),
            actual as (
                select to_jsonb(s) - array['dbt_scd_id','dbt_updated_at',
                       'dbt_valid_from','dbt_valid_to'] as state
                from {{ snapshot_relation(kind) }} s
            ), difference as (
                (select to_jsonb(e) from expected e except all select state from actual)
                union all
                (select state from actual except all select to_jsonb(e) from expected e)
            ) select exists(select 1 from difference)
        $history_{{ kind }}$ into bad using latest;
        if bad then
            raise exception '{{ kind }} stored history differs from Silver; explicit repair is required';
        end if;

        execute $interval_{{ kind }}$
            with history as (
                select *, lead(updated_at) over (partition by {{ key }} order by updated_at) as next_time
                from {{ snapshot_relation(kind) }}
            )
            select exists (
                select 1 from history
                where {{ key }} is null or dbt_scd_id is null
                   or extract(day from observation_month) <> 1
                   or updated_at is distinct from observation_month::timestamp
                   or dbt_valid_from is distinct from updated_at
                   or dbt_updated_at is distinct from updated_at
                   or dbt_valid_to is distinct from next_time
            ) or exists (
                select 1 from history group by dbt_scd_id having count(*) <> 1
            ) or exists (
                select 1 from history group by {{ key }}, observation_month having count(*) <> 1
            )
        $interval_{{ kind }}$ into bad;
        if bad then
            raise exception '{{ kind }} snapshot has invalid version keys or validity intervals';
        end if;
    end if;
    {% endfor %}
end
$snapshot_guard$;
{% endmacro %}

{% macro snapshot_preflight() %}
    {% if execute %}
        {% do run_query(snapshot_checks()) %}
        {{ log('Snapshot input and chronology checks passed.', info=true) }}
    {% endif %}
{% endmacro %}

{% macro verify_entity_snapshots() %}
  {% if execute %}
    {% do run_query(snapshot_checks('both')) %}
    {{ log('Both snapshots match every observed Silver entity and validity interval.', info=true) }}
  {% endif %}
{% endmacro %}
