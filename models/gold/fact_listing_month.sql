-- depends_on: {{ ref('snapshot_ready') }}
{{ config(indexes=[{'columns':['listing_id','month_id'],'unique':true},
                   {'columns':['host_version_id']}, {'columns':['property_version_id']}]) }}
-- IDs and measures only. Keep every observed listing-month; never infer absent rows.
select s.listing_id, s.observation_month as month_id, s.host_id,
       h.host_version_id, p.property_version_id,
       s.listing_lga_code, s.host_lga_code,
       s.price, s.has_availability, s.availability_30, s.number_of_reviews,
       s.review_scores_rating, s.review_scores_accuracy, s.review_scores_cleanliness,
       s.review_scores_checkin, s.review_scores_communication, s.review_scores_value,
       case when s.has_availability then 30-s.availability_30 else 0 end as number_of_stays,
       case when s.has_availability then (30-s.availability_30)*s.price else 0::numeric end as estimated_revenue
from {{ ref('slv_listings') }} s
left join {{ ref('dim_host') }} h
  on h.host_id=s.host_id and s.observed_at >= h.valid_from
 and (s.observed_at < h.valid_to or h.valid_to is null)
left join {{ ref('dim_property') }} p
  on p.listing_id=s.listing_id and p.host_id=s.host_id and s.observed_at >= p.valid_from
 and (s.observed_at < p.valid_to or p.valid_to is null)
where s.observation_month <= {{ observation_month() }}
