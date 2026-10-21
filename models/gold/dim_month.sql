-- depends_on: {{ ref('snapshot_ready') }}
{{ config(indexes=[{'columns':['month_id'],'unique':true}]) }}
-- Only completed source months are exposed, not future observations.
select month_start::date as month_id,
       extract(year from month_start)::integer as calendar_year,
       extract(month from month_start)::integer as month_number
from generate_series(date '2020-05-01', {{ observation_month() }}, interval '1 month') as m(month_start)
