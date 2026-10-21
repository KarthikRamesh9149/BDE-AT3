{{ config(tags=['silver']) }}
-- Returning any row fails this test. No source record is deleted to make it pass.
with listings as (select * from {{ ref('slv_listings') }}),
bronze_counts as (
    select _source_month as observation_month, count(*) as row_count
    from {{ source('bronze', 'raw_airbnb') }}
    where _source_month <= {{ observation_month() }}
    group by _source_month
),
silver_counts as (
    select observation_month, count(*) as row_count from listings group by observation_month
),
checks as (
    select 'listing_ranges_or_required_text' as check_name, count(*) as failures
    from listings where listing_id <= 0 or host_id <= 0 or accommodates <= 0
       or availability_30 not between 0 and 30 or number_of_reviews < 0
       or price < 0 or price::text in ('NaN', 'Infinity', '-Infinity')
       or property_type = '' or room_type = '' or listing_neighbourhood = ''
       or observation_month < date '2020-05-01'
       or extract(day from observation_month) <> 1
       or observed_at <> observation_month::timestamp
       or host_since > (observation_month + interval '1 month - 1 day')::date
    union all
    select 'review_score_range_or_nonfinite_value', count(*) from listings l
    where (review_scores_rating is not null and
           (review_scores_rating::text in ('NaN', 'Infinity', '-Infinity')
            or review_scores_rating not between 0 and 100))
       or exists (
           select 1 from (values (l.review_scores_accuracy), (l.review_scores_cleanliness),
                                (l.review_scores_checkin), (l.review_scores_communication),
                                (l.review_scores_value)) scores(score)
           where score is not null and
                 (score::text in ('NaN', 'Infinity', '-Infinity') or score not between 0 and 10))
    union all
    select 'nonfinite_census_g02_value', count(*) from {{ ref('slv_census_g02') }} g
    where exists (
        select 1 from (values (g.median_age_persons), (g.median_mortgage_repay_monthly),
                             (g.median_tot_prsnl_inc_weekly), (g.median_rent_weekly),
                             (g.median_tot_fam_inc_weekly), (g.average_num_psns_per_bedroom),
                             (g.median_tot_hhd_inc_weekly), (g.average_household_size)) measures(value)
        where value::text in ('NaN', 'Infinity', '-Infinity'))
    union all
    select 'inconsistent_host_location_status', count(*) from listings
    where host_location_status not in ('Matched', 'Unknown', 'Overseas', 'Unmatched')
       or (host_location_status = 'Matched' and host_lga_code is null)
       or (host_location_status in ('Unknown', 'Overseas', 'Unmatched') and host_lga_code is not null)
    union all
    select 'property_census_join_coverage', count(*) from listings l
    left join {{ ref('slv_census_g01') }} g1 on l.listing_lga_code = g1.lga_code
    left join {{ ref('slv_census_g02') }} g2 on l.listing_lga_code = g2.lga_code
    where g1.lga_code is null or g2.lga_code is null
    union all
    select 'nonconsecutive_or_missing_loaded_month', count(*)
    from generate_series(date '2020-05-01', {{ observation_month() }}, interval '1 month') m(month)
    where not exists (select 1 from listings l where l.observation_month = m.month::date)
    union all
    select 'airbnb_row_conservation', abs(
        (select count(*) from listings) -
        (select count(*) from {{ source('bronze', 'raw_airbnb') }}
         where _source_month <= {{ observation_month() }}))
    union all
    select 'future_month_in_silver', count(*) from listings
    where observation_month > {{ observation_month() }}
    union all
    select 'airbnb_row_conservation_per_month', count(*)
    from bronze_counts b full join silver_counts s using (observation_month)
    where b.row_count is distinct from s.row_count
    union all
    select 'host_input_month_or_timestamp', count(*) from {{ ref('host_snapshot_input') }}
    where observation_month <> {{ observation_month() }} or updated_at <> observation_month::timestamp
    union all
    select 'property_input_month_or_timestamp', count(*) from {{ ref('property_snapshot_input') }}
    where observation_month <> {{ observation_month() }} or updated_at <> observation_month::timestamp
    union all
    select 'host_input_count', abs(
        (select count(*) from {{ ref('host_snapshot_input') }}) -
        (select count(distinct host_id) from listings where observation_month = {{ observation_month() }}))
    union all
    select 'property_input_count', abs(
        (select count(*) from {{ ref('property_snapshot_input') }}) -
        (select count(*) from listings where observation_month = {{ observation_month() }}))
    union all
    select 'lga_code_format', count(*) from {{ ref('slv_lga') }}
    where lga_code !~ '^[0-9]{5}$'
    union all
    select 'suburb_unnamed_source_fields', count(*)
    from {{ source('bronze', 'raw_lga_suburb') }}
    where concat_ws('', source_column_03, source_column_04, source_column_05, source_column_06, source_column_07, source_column_08, source_column_09, source_column_10, source_column_11, source_column_12, source_column_13, source_column_14, source_column_15, source_column_16, source_column_17, source_column_18, source_column_19, source_column_20, source_column_21, source_column_22, source_column_23, source_column_24, source_column_25, source_column_26, source_column_27) <> ''
    union all
    select 'census_g01_row_conservation', abs(
        (select count(*) from {{ ref('slv_census_g01') }}) -
        (select count(*) from {{ source('bronze', 'raw_census_g01') }}))
    union all
    select 'census_g02_row_conservation', abs(
        (select count(*) from {{ ref('slv_census_g02') }}) -
        (select count(*) from {{ source('bronze', 'raw_census_g02') }}))
    union all
    select 'lga_row_conservation', abs(
        (select count(*) from {{ ref('slv_lga') }}) -
        (select count(*) from {{ source('bronze', 'raw_lga_code') }}))
    union all
    select 'suburb_row_conservation', abs(
        (select count(*) from {{ ref('slv_suburb') }}) -
        (select count(*) from {{ source('bronze', 'raw_lga_suburb') }}))
)
select * from checks where failures <> 0
