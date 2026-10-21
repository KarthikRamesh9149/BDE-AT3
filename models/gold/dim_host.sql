-- depends_on: {{ ref('snapshot_ready') }}
{{ config(indexes=[{'columns':['host_version_id'],'unique':true},{'columns':['host_id']}]) }}
-- One row per historical Host version. NULL means no known end yet.
select dbt_scd_id as host_version_id, host_id, host_name, host_since,
       host_is_superhost, host_neighbourhood, host_lga_code, host_lga_name,
       host_location_status, dbt_valid_from as valid_from, dbt_valid_to as valid_to
from {{ ref('snap_host') }}
where observation_month <= {{ observation_month() }}
