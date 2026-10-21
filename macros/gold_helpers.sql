{% macro gold_preflight() %}
    {% if execute %}
        {% do run_query(snapshot_checks('both')) %}
        {{ log('Gold preflight passed: both snapshots match the selected source month.', info=true) }}
    {% endif %}
{% endmacro %}

{% macro historical_fact_rows() %}
-- Version key AND half-open validity range prevent latest-version attribution.
select f.*, h.host_is_superhost, h.host_location_status,
       coalesce(hl.lga_name, h.host_location_status) as host_neighbourhood_lga,
       lg.lga_name as listing_neighbourhood, p.property_type, p.room_type, p.accommodates
from {{ ref('fact_listing_month') }} f
join {{ ref('dim_month') }} m on m.month_id=f.month_id
join {{ ref('dim_host') }} h
  on h.host_version_id=f.host_version_id and h.host_id=f.host_id
 and f.month_id::timestamp >= h.valid_from
 and (f.month_id::timestamp < h.valid_to or h.valid_to is null)
join {{ ref('dim_property') }} p
  on p.property_version_id=f.property_version_id and p.listing_id=f.listing_id
 and p.host_id=f.host_id
 and f.month_id::timestamp >= p.valid_from
 and (f.month_id::timestamp < p.valid_to or p.valid_to is null)
join {{ ref('dim_lga') }} lg on lg.lga_code=f.listing_lga_code
left join {{ ref('dim_lga') }} hl on hl.lga_code=f.host_lga_code
{% endmacro %}

{% macro listing_metrics(group_fields, observed_sql=none, calendar_sql=none) %}
{% set groups=group_fields|join(', ') %}
with observed as ({% if observed_sql is none %}{{ historical_fact_rows() }}{% else %}{{ observed_sql }}{% endif %}),
summary as (
    select {{ groups }}, month_id as month_year,
           count(*) as total_listings,
           count(*) filter(where has_availability) as active_listings,
           count(*) filter(where not has_availability) as inactive_listings,
           min(price) filter(where has_availability) as minimum_price,
           max(price) filter(where has_availability) as maximum_price,
           avg(price) filter(where has_availability) as average_price,
           count(distinct host_id) as number_of_distinct_hosts,
           count(distinct host_id) filter(where host_is_superhost) as superhosts,
           avg(review_scores_rating) filter(where has_availability) as average_review_scores_rating,
           sum(number_of_stays) as total_number_of_stays,
           sum(estimated_revenue) as total_estimated_revenue,
           avg(estimated_revenue) filter(where has_availability) as average_estimated_revenue_per_active_listing
    from observed group by {{ groups }}, month_id
), first_seen as (
    select {{ groups }},min(month_year) as first_month from summary group by {{ groups }}
), calendar as (
    select {% for field in group_fields %}g.{{ field }}, {% endfor %}m.month_id as month_year
    from first_seen g join ({% if calendar_sql is none %}select month_id from {{ ref('dim_month') }}{% else %}{{ calendar_sql }}{% endif %}) m on m.month_id>=g.first_month
), complete_counts as (
    -- Zero reporting counts do not create synthetic listing facts or inactive rows.
    select {% for field in group_fields %}cal.{{ field }}, {% endfor %}cal.month_year,
           coalesce(s.total_listings,0) as total_listings,
           coalesce(s.active_listings,0) as active_listings,
           coalesce(s.inactive_listings,0) as inactive_listings,
           s.minimum_price,s.maximum_price,s.average_price,
           coalesce(s.number_of_distinct_hosts,0) as number_of_distinct_hosts,
           coalesce(s.superhosts,0) as superhosts,s.average_review_scores_rating,
           coalesce(s.total_number_of_stays,0) as total_number_of_stays,
           coalesce(s.total_estimated_revenue,0) as total_estimated_revenue,
           s.average_estimated_revenue_per_active_listing
    from calendar cal left join summary s on s.month_year=cal.month_year
    {% for field in group_fields %}and s.{{ field }}=cal.{{ field }} {% endfor %}
), active_prices as (
    -- Numeric median avoids converting source prices to floating point.
    select {{ groups }}, month_id as month_year, price,
           row_number() over(partition by {{ groups }},month_id order by price,listing_id) as rn,
           count(*) over(partition by {{ groups }},month_id) as n
    from observed where has_availability
), medians as (
    select {{ groups }},month_year,avg(price) as median_price
    from active_prices where rn in ((n+1)/2,(n+2)/2)
    group by {{ groups }},month_year
)
select {% for field in group_fields %}c.{{ field }}, {% endfor %}c.month_year,
       c.total_listings,c.active_listings,c.inactive_listings,
       100.0*c.active_listings/nullif(c.total_listings,0) as active_listings_rate,
       c.minimum_price,c.maximum_price,md.median_price,c.average_price,
       c.number_of_distinct_hosts,
       100.0*c.superhosts/nullif(c.number_of_distinct_hosts,0) as superhost_rate,
       c.average_review_scores_rating,
       100.0*(c.active_listings-pr.active_listings)/nullif(pr.active_listings,0) as percentage_change_active_listings,
       100.0*(c.inactive_listings-pr.inactive_listings)/nullif(pr.inactive_listings,0) as percentage_change_inactive_listings,
       c.total_number_of_stays,c.total_estimated_revenue,
       c.average_estimated_revenue_per_active_listing
from complete_counts c
left join medians md on md.month_year=c.month_year
{% for field in group_fields %} and md.{{ field }}=c.{{ field }}{% endfor %}
-- Compare the immediately preceding calendar month, never the last nonempty row.
-- First observed month or a zero denominator gives NULL, not invented growth.
left join complete_counts pr on pr.month_year=(c.month_year-interval '1 month')::date
{% for field in group_fields %} and pr.{{ field }}=c.{{ field }}{% endfor %}
order by {% for field in group_fields %}c.{{ field }}, {% endfor %}c.month_year
{% endmacro %}
