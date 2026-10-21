{% macro bronze_business_issues() %}
-- Read Bronze only. Run this before building Silver; any returned row fails.
with a as (
    select * from {{ source('bronze', 'raw_airbnb') }}
    where _source_month <= {{ observation_month() }}
), issues as (
    select 'invalid_required_' || v.field as issue from a
    cross join lateral (values
        ('listing_id', a.listing_id, 'bigint'),
        ('host_id', a.host_id, 'bigint'),
        ('accommodates', a.accommodates, 'integer'),
        ('availability_30', a.availability_30, 'integer'),
        ('number_of_reviews', a.number_of_reviews, 'integer')
    ) v(field, value, sql_type)
    where coalesce(btrim(v.value), '') !~ '^[0-9]+$'
       or not coalesce(pg_input_is_valid(btrim(v.value), v.sql_type), false)
    union all
    select 'invalid_price' from a
    where coalesce(btrim(price), '') !~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$'
       or not coalesce(pg_input_is_valid(btrim(price), 'numeric'), false)
    union all
    select 'invalid_optional_review_' || v.field from a
    cross join lateral (values
        ('rating', a.review_scores_rating), ('accuracy', a.review_scores_accuracy),
        ('cleanliness', a.review_scores_cleanliness), ('checkin', a.review_scores_checkin),
        ('communication', a.review_scores_communication), ('value', a.review_scores_value)
    ) v(field, value)
    where nullif(btrim(v.value), '') is not null and
          (btrim(v.value) !~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$'
           or not coalesce(pg_input_is_valid(btrim(v.value), 'numeric'), false))
    union all
    select 'invalid_availability_flag' from a
    where coalesce(btrim(has_availability), '') not in ('t', 'f')
    union all
    select 'invalid_optional_superhost_flag' from a
    where nullif(btrim(host_is_superhost), '') is not null
      and btrim(host_is_superhost) not in ('t', 'f')
    union all
    select 'invalid_scraped_date' from a
    where coalesce(btrim(scraped_date), '') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
       or not coalesce(pg_input_is_valid(btrim(scraped_date), 'date'), false)
    union all
    select 'invalid_optional_host_registration_date' from a
    where nullif(btrim(host_since), '') is not null and
          (btrim(host_since) !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
           or not coalesce(pg_input_is_valid(
               split_part(btrim(host_since), '/', 3) || '-' ||
               lpad(split_part(btrim(host_since), '/', 2), 2, '0') || '-' ||
               lpad(split_part(btrim(host_since), '/', 1), 2, '0'), 'date'), false))
    union all
    select 'missing_required_listing_text' from a
    where nullif(btrim(scrape_id), '') is null
       or nullif(btrim(property_type), '') is null
       or nullif(btrim(room_type), '') is null
       or nullif(btrim(listing_neighbourhood), '') is null
    union all
    select 'source_filename_month_mismatch' from a
    where _source_file is distinct from to_char(_source_month, 'MM_YYYY') || '.csv'
       or _loaded_at is null
       or extract(day from _source_month) <> 1
       or _source_month < date '2020-05-01'
    union all
    select 'duplicate_listing_month' from a
    group by listing_id, _source_month having count(*) > 1
    union all
    select 'conflicting_host_attributes_within_month' from a
    group by host_id, _source_month
    having count(distinct row(host_name, host_since, host_is_superhost, host_neighbourhood)) > 1
    union all
    select 'missing_observation_month'
    from generate_series(date '2020-05-01', {{ observation_month() }}, interval '1 month') m(month)
    where not exists (select 1 from a where a._source_month = m.month::date)
    union all
    select 'invalid_g01_measure' from {{ source('bronze', 'raw_census_g01') }} r
    cross join lateral jsonb_each_text(to_jsonb(r) - 'lga_code_2016' - '_source_file' - '_loaded_at') v(field, value)
    where coalesce(btrim(v.value), '') !~ '^[0-9]+$'
       or not coalesce(pg_input_is_valid(btrim(v.value), 'bigint'), false)
    union all
    select 'invalid_g02_measure' from {{ source('bronze', 'raw_census_g02') }} r
    cross join lateral jsonb_each_text(to_jsonb(r) - 'lga_code_2016' - '_source_file' - '_loaded_at') v(field, value)
    where coalesce(btrim(v.value), '') !~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$'
       or not coalesce(pg_input_is_valid(btrim(v.value), 'numeric'), false)
    union all
    select 'invalid_g01_code' from {{ source('bronze', 'raw_census_g01') }}
    where coalesce(btrim(lga_code_2016), '') !~ '^LGA[0-9]{5}$'
    union all
    select 'invalid_g02_code' from {{ source('bronze', 'raw_census_g02') }}
    where coalesce(btrim(lga_code_2016), '') !~ '^LGA[0-9]{5}$'
    union all
    select 'duplicate_g01_code' from {{ source('bronze', 'raw_census_g01') }}
    group by btrim(lga_code_2016) having count(*) > 1
    union all
    select 'duplicate_g02_code' from {{ source('bronze', 'raw_census_g02') }}
    group by btrim(lga_code_2016) having count(*) > 1
    union all
    select 'invalid_lga_code_or_name' from {{ source('bronze', 'raw_lga_code') }}
    where coalesce(btrim(lga_code), '') !~ '^[0-9]{5}$' or nullif(btrim(lga_name), '') is null
    union all
    select 'duplicate_lga_code' from {{ source('bronze', 'raw_lga_code') }}
    group by btrim(lga_code) having count(*) > 1
    union all
    select 'duplicate_lga_name' from {{ source('bronze', 'raw_lga_code') }}
    group by {{ geography_key('lga_name') }} having count(*) > 1
    union all
    select 'duplicate_normalised_suburb' from {{ source('bronze', 'raw_lga_suburb') }}
    group by {{ geography_key('suburb_name') }} having count(*) > 1
    union all
    select 'missing_suburb_or_unmatched_reference_lga' from {{ source('bronze', 'raw_lga_suburb') }} s
    left join {{ source('bronze', 'raw_lga_code') }} l
      on {{ geography_key('s.lga_name') }} = {{ geography_key('l.lga_name') }}
    where nullif(btrim(s.suburb_name), '') is null or l.lga_code is null
)
select issue, count(*) as failures from issues group by issue

{% endmacro %}
