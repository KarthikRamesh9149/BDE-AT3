-- One actual listing observation per selected month; no synthetic missing listings.
select
    listing_id, host_id, listing_neighbourhood, listing_lga_code, listing_lga_name,
    property_type, room_type, accommodates,
    observation_month, observed_at as updated_at
from {{ ref('slv_listings') }}
where observation_month = {{ observation_month() }}
