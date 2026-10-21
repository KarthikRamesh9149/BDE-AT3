-- depends_on: {{ ref('snapshot_ready') }}
{{ config(materialized='view') }}
-- Unknown and Overseas hosts remain explicit groups; no property-location imputation.
with observed as ({{ historical_fact_rows() }})
select host_neighbourhood_lga,host_lga_code,host_location_status,month_id as month_year,
       count(distinct host_id) as number_of_distinct_hosts,
       avg(estimated_revenue) filter(where has_availability) as average_estimated_revenue_per_active_listing,
       sum(estimated_revenue)/nullif(count(distinct host_id),0) as estimated_revenue_per_host,
       sum(estimated_revenue) as total_estimated_revenue
from observed
group by host_neighbourhood_lga,host_lga_code,host_location_status,month_id
order by host_neighbourhood_lga,month_id
