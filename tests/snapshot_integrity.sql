{{ config(tags=['snapshot_validation']) }}
-- Every stored entity observation must match Silver, including NULL attributes.
with checks as (
{% for kind, key in [('host','host_id'),('property','listing_id')] %}
    {% if not loop.first %}union all{% endif %}
    select '{{ kind }}' as entity, 'history_or_interval_mismatch' as issue
    where exists (
        with expected as ({{ expected_entity(kind, 'observation_month <= ' ~ observation_month()) }}),
        actual as (
            select to_jsonb(s) - array['dbt_scd_id','dbt_updated_at','dbt_valid_from','dbt_valid_to'] as state
            from {{ ref('snap_' ~ kind) }} s
        ), difference as (
            (select to_jsonb(e) from expected e except all select state from actual)
            union all
            (select state from actual except all select to_jsonb(e) from expected e)
        ) select 1 from difference
    ) or exists (
        select 1 from (
            select *, lead(updated_at) over (partition by {{ key }} order by updated_at) as next_time
            from {{ ref('snap_' ~ kind) }}
        ) h
        where dbt_valid_from is distinct from updated_at
           or dbt_updated_at is distinct from updated_at
           or dbt_valid_to is distinct from next_time
    )
{% endfor %}
    union all
    select 'host', 'initial_may_count_mismatch'
    where {{ observation_month() }} = date '2020-05-01'
      and (select count(*) from {{ ref('snap_host') }}) <> 27273
    union all
    select 'property', 'initial_may_count_mismatch'
    where {{ observation_month() }} = date '2020-05-01'
      and (select count(*) from {{ ref('snap_property') }}) <> 37562
)
select * from checks
