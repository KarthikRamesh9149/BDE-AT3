-- DISTINCT collapses repeated host attributes across listings, never conflicting states.
-- A unique host_id test fails if one host has conflicting attributes in this month.
select distinct
    host_id, host_name, host_since, host_is_superhost,
    host_neighbourhood, host_lga_code, host_lga_name, host_location_status,
    observation_month, observed_at as updated_at
from {{ ref('slv_listings') }}
where observation_month = {{ observation_month() }}
