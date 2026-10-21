-- Only observation periods up to the explicit run month enter this history.
with typed as (
    select
        nullif(btrim(listing_id), '')::bigint as listing_id,
        scrape_id,
        nullif(btrim(scraped_date), '')::date as scraped_date,
        nullif(btrim(host_id), '')::bigint as host_id,
        nullif(btrim(host_name), '') as host_name,
        {{ host_registration_date('host_since') }} as host_since,
        {{ source_flag('host_is_superhost') }} as host_is_superhost,
        nullif(btrim(host_neighbourhood), '') as host_neighbourhood,
        {{ geography_key('host_neighbourhood') }} as host_suburb_key,
        nullif(btrim(listing_neighbourhood), '') as listing_neighbourhood,
        {{ geography_key('listing_neighbourhood') }} as listing_lga_name_key,
        nullif(btrim(property_type), '') as property_type,
        nullif(btrim(room_type), '') as room_type,
        nullif(btrim(accommodates), '')::integer as accommodates,
        nullif(btrim(price), '')::numeric as price,
        {{ source_flag('has_availability') }} as has_availability,
        nullif(btrim(availability_30), '')::integer as availability_30,
        nullif(btrim(number_of_reviews), '')::integer as number_of_reviews,
        nullif(btrim(review_scores_rating), '')::numeric as review_scores_rating,
        nullif(btrim(review_scores_accuracy), '')::numeric as review_scores_accuracy,
        nullif(btrim(review_scores_cleanliness), '')::numeric as review_scores_cleanliness,
        nullif(btrim(review_scores_checkin), '')::numeric as review_scores_checkin,
        nullif(btrim(review_scores_communication), '')::numeric as review_scores_communication,
        nullif(btrim(review_scores_value), '')::numeric as review_scores_value,
        _source_month as observation_month,
        _source_month::timestamp as observed_at,
        _source_file,
        _loaded_at
    from {{ source('bronze', 'raw_airbnb') }}
    where _source_month <= {{ observation_month() }}
)
select
    t.*,
    p.lga_code as listing_lga_code,
    p.lga_name as listing_lga_name,
    h.lga_code as host_lga_code,
    h.lga_name as host_lga_name,
    case when t.host_neighbourhood is null then 'Unknown'
         when t.host_suburb_key = 'OVERSEAS' then 'Overseas'
         when h.lga_code is not null then 'Matched'
         else 'Unmatched' end as host_location_status
from typed t
left join {{ ref('slv_lga') }} p on t.listing_lga_name_key = p.lga_name_key
left join {{ ref('slv_suburb') }} h on t.host_suburb_key = h.suburb_key
