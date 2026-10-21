-- depends_on: {{ ref('snapshot_ready') }}
{{ config(indexes=[{'columns':['property_version_id'],'unique':true},{'columns':['listing_id']}]) }}
-- Retain the owner and property attributes that belonged to each version.
select dbt_scd_id as property_version_id, listing_id, host_id,
       listing_neighbourhood, listing_lga_code, listing_lga_name,
       property_type, room_type, accommodates,
       dbt_valid_from as valid_from, dbt_valid_to as valid_to
from {{ ref('snap_property') }}
where observation_month <= {{ observation_month() }}
