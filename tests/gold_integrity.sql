{{ config(tags=['gold']) }}
-- Metadata checks must run after all three reporting views have been built.
-- depends_on: {{ ref('dm_listing_neighbourhood') }}
-- depends_on: {{ ref('dm_property_type') }}
-- depends_on: {{ ref('dm_host_neighbourhood') }}
with expected_measures as (
    select listing_id,observation_month as month_id,host_id,listing_lga_code,host_lga_code,
           price,has_availability,availability_30,number_of_reviews,
           review_scores_rating,review_scores_accuracy,review_scores_cleanliness,
           review_scores_checkin,review_scores_communication,review_scores_value,
           case when has_availability then 30-availability_30 else 0 end as number_of_stays,
           case when has_availability then (30-availability_30)*price else 0::numeric end as estimated_revenue
    from {{ ref('slv_listings') }} where observation_month <= {{ observation_month() }}
), fact_measures as (
    select to_jsonb(f)-array['host_version_id','property_version_id'] as state
    from {{ ref('fact_listing_month') }} f
), differences as (
    (select to_jsonb(e) from expected_measures e except all select state from fact_measures)
    union all
    (select state from fact_measures except all select to_jsonb(e) from expected_measures e)
), failures as (
    select 'fact_source_conservation' as issue where exists(select 1 from differences)
    union all
    select 'historical_version_or_owner' where exists (
        select 1 from {{ ref('fact_listing_month') }} f
        where (select count(*) from {{ ref('dim_host') }} h
               where h.host_id=f.host_id and h.host_version_id=f.host_version_id
                 and h.host_lga_code is not distinct from f.host_lga_code
                 and f.month_id::timestamp>=h.valid_from
                 and (f.month_id::timestamp<h.valid_to or h.valid_to is null))<>1
           or (select count(*) from {{ ref('dim_property') }} p
               where p.listing_id=f.listing_id and p.property_version_id=f.property_version_id
                 and p.host_id=f.host_id and p.listing_lga_code=f.listing_lga_code
                 and f.month_id::timestamp>=p.valid_from
                 and (f.month_id::timestamp<p.valid_to or p.valid_to is null))<>1
           or not exists(select 1 from {{ ref('dim_month') }} m where m.month_id=f.month_id)
           or not exists(select 1 from {{ ref('dim_lga') }} l where l.lga_code=f.listing_lga_code)
           or (f.host_lga_code is not null and not exists(
               select 1 from {{ ref('dim_lga') }} l where l.lga_code=f.host_lga_code))
    )
    union all
    select 'datamart_join_fanout_or_loss' where
        exists (
          with joined as ({{ historical_fact_rows() }}),
          counts as (select listing_id,month_id,count(*) as n from joined group by listing_id,month_id)
          select 1 from {{ ref('fact_listing_month') }} f full join counts c using(listing_id,month_id)
          where f.listing_id is null or c.n is distinct from 1::bigint
        )
    union all
    select 'host_dimension_history' where exists (
        with expected as (select dbt_scd_id as host_version_id, host_id, host_name, host_since,
       host_is_superhost, host_neighbourhood, host_lga_code, host_lga_name,
       host_location_status, dbt_valid_from as valid_from, dbt_valid_to as valid_to
from {{ ref('snap_host') }}
where observation_month <= {{ observation_month() }}
)
        (select to_jsonb(e) from expected e except all select to_jsonb(a) from {{ ref('dim_host') }} a)
        union all
        (select to_jsonb(a) from {{ ref('dim_host') }} a except all select to_jsonb(e) from expected e)
    )
    union all
    select 'property_dimension_history' where exists (
        with expected as (select dbt_scd_id as property_version_id, listing_id, host_id,
       listing_neighbourhood, listing_lga_code, listing_lga_name,
       property_type, room_type, accommodates,
       dbt_valid_from as valid_from, dbt_valid_to as valid_to
from {{ ref('snap_property') }}
where observation_month <= {{ observation_month() }}
)
        (select to_jsonb(e) from expected e except all select to_jsonb(a) from {{ ref('dim_property') }} a)
        union all
        (select to_jsonb(a) from {{ ref('dim_property') }} a except all select to_jsonb(e) from expected e)
    )
    {% for kind in ['g01','g02'] %}
    union all
    select 'census_{{ kind }}_preservation' where exists (
        (select to_jsonb(s)-array['_source_file','_loaded_at'] from {{ ref('slv_census_' ~ kind) }} s
         except all select to_jsonb(g) from {{ ref('ref_census_' ~ kind) }} g)
        union all
        (select to_jsonb(g) from {{ ref('ref_census_' ~ kind) }} g except all
         select to_jsonb(s)-array['_source_file','_loaded_at'] from {{ ref('slv_census_' ~ kind) }} s)
    )
    {% endfor %}
    union all
    select 'lga_preservation' where exists (
        (select lga_code,lga_name from {{ ref('slv_lga') }} except all select * from {{ ref('dim_lga') }})
        union all
        (select * from {{ ref('dim_lga') }} except all select lga_code,lga_name from {{ ref('slv_lga') }})
    )
    union all
    select 'month_calendar' where exists(
        select 1 from {{ ref('dim_month') }}
        where month_id<date '2020-05-01' or month_id>{{ observation_month() }}
           or extract(day from month_id)<>1
           or calendar_year<>extract(year from month_id) or month_number<>extract(month from month_id)
    ) or (select count(*) from {{ ref('dim_month') }})<>
         ((extract(year from {{ observation_month() }})-2020)*12+extract(month from {{ observation_month() }})-4)
    union all
    select 'first_may_counts' where {{ observation_month() }}=date '2020-05-01'
      and ((select count(*) from {{ ref('fact_listing_month') }})<>37562
        or (select count(*) from {{ ref('dim_host') }})<>27273
        or (select count(*) from {{ ref('dim_property') }})<>37562)
    union all
    select 'view_materialization' where
      (select count(*) from information_schema.views where table_schema='{{ warehouse_schema('gold') }}'
       and table_name in ('dm_listing_neighbourhood','dm_property_type','dm_host_neighbourhood'))<>3
)
select * from failures
