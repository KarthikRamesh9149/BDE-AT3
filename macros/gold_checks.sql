{% macro gold_checks() %}
{{ snapshot_checks('both') }}
do $gold_guard$
begin
  if exists (
    select s.listing_id,s.observation_month
    from {{ ref('slv_listings') }} s left join {{ snapshot_relation('host') }} h
      on h.host_id=s.host_id and s.observed_at>=h.dbt_valid_from
      and (s.observed_at<h.dbt_valid_to or h.dbt_valid_to is null)
    group by s.listing_id,s.observation_month having count(h.dbt_scd_id)<>1
    union all
    select s.listing_id,s.observation_month
    from {{ ref('slv_listings') }} s left join {{ snapshot_relation('property') }} p
      on p.listing_id=s.listing_id and p.host_id=s.host_id
      and s.observed_at>=p.dbt_valid_from
      and (s.observed_at<p.dbt_valid_to or p.dbt_valid_to is null)
    group by s.listing_id,s.observation_month having count(p.dbt_scd_id)<>1
  ) then
    raise exception 'Every listing observation must match exactly one historical Host and Property';
  end if;
end $gold_guard$;
{% endmacro %}
