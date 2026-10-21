{{ config(tags=['gold']) }}
{% set specs=[('dm_listing_neighbourhood',['listing_neighbourhood']),
              ('dm_property_type',['property_type','room_type','accommodates']),
              ('dm_host_neighbourhood',['host_neighbourhood_lga','host_location_status'])] %}
with source_rows as (
    select s.*,s.listing_lga_name as canonical_listing_neighbourhood,
           coalesce(s.host_lga_name,s.host_location_status) as host_neighbourhood_lga
    from {{ ref('slv_listings') }} s where observation_month <= {{ observation_month() }}
)
{% for name,fields in specs %}
, aggregate_{{ loop.index }} as (
    select {% for f in fields %}{% if f=='listing_neighbourhood' %}canonical_listing_neighbourhood as listing_neighbourhood{% else %}{{ f }}{% endif %}, {% endfor %}
           observation_month as month_year,
           count(distinct host_id) as number_of_distinct_hosts,
           sum(case when has_availability then 30-availability_30 else 0 end) as total_number_of_stays,
           sum(case when has_availability then (30-availability_30)*price else 0 end) as total_estimated_revenue,
           avg((30-availability_30)*price) filter(where has_availability) as average_estimated_revenue_per_active_listing
    {% if name!='dm_host_neighbourhood' %}
         , count(*) as total_listings,
           count(*) filter(where has_availability) as active_listings,
           count(*) filter(where not has_availability) as inactive_listings,
           100.0*count(*) filter(where has_availability)/count(*) as active_listings_rate,
           min(price) filter(where has_availability) as minimum_price,
           max(price) filter(where has_availability) as maximum_price,
           ((array_agg(price order by price) filter(where has_availability))[((count(*) filter(where has_availability)+1)/2)::integer]
            +(array_agg(price order by price) filter(where has_availability))[((count(*) filter(where has_availability)+2)/2)::integer])/2.0 as median_price,
           avg(price) filter(where has_availability) as average_price,
           100.0*count(distinct host_id) filter(where host_is_superhost)/count(distinct host_id) as superhost_rate,
           avg(review_scores_rating) filter(where has_availability) as average_review_scores_rating
    {% else %}
         , sum(case when has_availability then (30-availability_30)*price else 0 end)/count(distinct host_id) as estimated_revenue_per_host
    {% endif %}
    from source_rows group by {% for f in fields %}{% if f=='listing_neighbourhood' %}canonical_listing_neighbourhood{% else %}{{ f }}{% endif %}, {% endfor %}observation_month
)
, expected_{{ loop.index }} as (
    {% if name=='dm_host_neighbourhood' %}
    select * from aggregate_{{ loop.index }}
    {% else %}
    select {% for f in fields %}g.{{ f }}, {% endfor %}m.month_start::date as month_year,
           coalesce(a.number_of_distinct_hosts,0) as number_of_distinct_hosts,
           coalesce(a.total_number_of_stays,0) as total_number_of_stays,
           coalesce(a.total_estimated_revenue,0) as total_estimated_revenue,
           a.average_estimated_revenue_per_active_listing,
           coalesce(a.total_listings,0) as total_listings,
           coalesce(a.active_listings,0) as active_listings,
           coalesce(a.inactive_listings,0) as inactive_listings,
           a.active_listings_rate,a.minimum_price,a.maximum_price,a.median_price,
           a.average_price,a.superhost_rate,a.average_review_scores_rating
    from (select {{ fields|join(', ') }},min(month_year) as first_month
          from aggregate_{{ loop.index }} group by {{ fields|join(', ') }}) g
    cross join lateral generate_series(g.first_month,{{ observation_month() }},interval '1 month') m(month_start)
    left join aggregate_{{ loop.index }} a on a.month_year=m.month_start::date
    {% for f in fields %}and a.{{ f }}=g.{{ f }} {% endfor %}
    {% endif %}
)
, compared_{{ loop.index }} as (
    select 'row_or_metric_{{ name }}'::text as issue
    from expected_{{ loop.index }} e full join {{ ref(name) }} a using ({{ fields|join(', ') }},month_year)
    where e.month_year is null or a.month_year is null
       or e.number_of_distinct_hosts is distinct from a.number_of_distinct_hosts
       or e.total_estimated_revenue is distinct from a.total_estimated_revenue
    {% set metrics=['average_estimated_revenue_per_active_listing'] %}
    {% if name!='dm_host_neighbourhood' %}
       or e.total_number_of_stays is distinct from a.total_number_of_stays
       or e.total_listings is distinct from a.total_listings
       or e.active_listings is distinct from a.active_listings
       or e.inactive_listings is distinct from a.inactive_listings
    {% set metrics=metrics+['active_listings_rate','minimum_price','maximum_price','median_price','average_price','superhost_rate','average_review_scores_rating'] %}
    {% else %}{% set metrics=metrics+['estimated_revenue_per_host'] %}{% endif %}
    {% for metric in metrics %}
       or e.{{ metric }} is distinct from a.{{ metric }}
    {% endfor %}
)
{% if name!='dm_host_neighbourhood' %}
, growth_{{ loop.index }} as (
    select 'growth_{{ name }}'::text as issue
    from {{ ref(name) }} a left join expected_{{ loop.index }} p
      on p.month_year=(a.month_year-interval '1 month')::date
    {% for f in fields %}and p.{{ f }}=a.{{ f }} {% endfor %}
    where a.percentage_change_active_listings is distinct from
          100.0*(a.active_listings-p.active_listings)/nullif(p.active_listings,0)
       or a.percentage_change_inactive_listings is distinct from
          100.0*(a.inactive_listings-p.inactive_listings)/nullif(p.inactive_listings,0)
)
{% endif %}
{% endfor %}
{% for name,fields in specs %}
{% if not loop.first %}union all{% endif %}
select * from compared_{{ loop.index }}
{% if name!='dm_host_neighbourhood' %}union all select * from growth_{{ loop.index }}{% endif %}
{% endfor %}
