{{ config(tags=['gold']) }}
-- Synthetic fixtures exercise boundaries; they are not claims about observed months.
{% set fixture_rows %}
select *,case when has_availability then 30-availability_30 else 0 end as number_of_stays,
       case when has_availability then (30-availability_30)*price else 0::numeric end as estimated_revenue
from (values
 ('A',date '2020-05-01',1,10,true,true,30,10.25::numeric,90::numeric),
 ('A',date '2020-05-01',2,10,true,true,28,20.75::numeric,null::numeric),
 ('A',date '2020-05-01',3,20,false,false,0,999::numeric,50::numeric),
 ('A',date '2020-05-01',4,30,null::boolean,true,29,40::numeric,80::numeric),
 ('A',date '2020-05-01',5,40,false,true,29,61::numeric,100::numeric),
 ('A',date '2020-07-01',6,20,false,true,28,50::numeric,80::numeric),
 ('A',date '2020-08-01',7,20,false,false,0,999::numeric,50::numeric),
 ('B',date '2020-07-01',8,50,false,false,0,999::numeric,null::numeric),
 ('B',date '2020-08-01',9,50,false,true,29,10::numeric,null::numeric)
) v(listing_neighbourhood,month_id,listing_id,host_id,host_is_superhost,
    has_availability,availability_30,price,review_scores_rating)
{% endset %}
{% set fixture_calendar %}
select d::date as month_id from generate_series(date '2020-05-01',date '2020-08-01',interval '1 month') d
{% endset %}
with actual_mart as ({{ listing_metrics(['listing_neighbourhood'],fixture_rows,fixture_calendar) }}),
expected_mart(neighbourhood,month_year,total,active,inactive,active_rate,median_price,superhost_rate,stays,revenue,change_active,change_inactive) as (
 values ('A',date '2020-05-01',5,4,1,80::numeric,30.375::numeric,25::numeric,4,142.50::numeric,null::numeric,null::numeric),
        ('A',date '2020-06-01',0,0,0,null,null,null,0,0,-100,-100),
        ('A',date '2020-07-01',1,1,0,100,50,0,2,100,null,null),
        ('A',date '2020-08-01',1,0,1,0,null,0,0,0,-100,null),
        ('B',date '2020-07-01',1,0,1,0,null,0,0,0,null,null),
        ('B',date '2020-08-01',1,1,0,100,10,0,1,10,null,-100)
), sample(listing_id,host_id,superhost,active,availability_30,price) as (
    values (1,10,true,true,30,10.25::numeric),
           (2,10,true,true,28,20.75::numeric),
           (3,20,false,false,0,999::numeric),
           (4,30,null,true,29,40.00::numeric),
           (5,40,false,true,29,61.00::numeric)
), calculated as (
    select 100.0*count(distinct host_id) filter(where superhost)/count(distinct host_id) as superhost_rate,
           count(*) filter(where active) as active_count,
           sum(case when active then 30-availability_30 else 0 end) as stays,
           sum(case when active then (30-availability_30)*price else 0 end) as revenue,
           percentile_cont(0.5) within group(order by price) filter(where active) as median_price
    from sample
), versions(listing_id,owner,version,valid_from,valid_to) as (
    values (501,10,'may',timestamp '2020-05-01',timestamp '2020-06-01'),
           (501,20,'june',timestamp '2020-06-01',null::timestamp),
           (502,99,'other_listing',timestamp '2020-05-01',null::timestamp)
), observations(listing_id,month_id,owner) as (
    values (501,date '2020-05-01',10),(501,date '2020-06-01',20)
), interval_join as (
    select o.month_id,o.owner,v.version,v.owner as version_owner
    from observations o left join versions v on v.listing_id=o.listing_id
      and o.month_id::timestamp>=v.valid_from
      and (o.month_id::timestamp<v.valid_to or v.valid_to is null)
), failures as (
    select 'production_metric_macro_calendar_or_nulls' as issue
    from actual_mart a full join expected_mart e
      on a.listing_neighbourhood=e.neighbourhood and a.month_year=e.month_year
    where a.month_year is null or e.month_year is null
       or a.total_listings is distinct from e.total
       or a.active_listings is distinct from e.active
       or a.inactive_listings is distinct from e.inactive
       or a.active_listings_rate is distinct from e.active_rate
       or a.median_price is distinct from e.median_price
       or a.superhost_rate is distinct from e.superhost_rate
       or a.total_number_of_stays is distinct from e.stays
       or a.total_estimated_revenue is distinct from e.revenue
       or a.percentage_change_active_listings is distinct from e.change_active
       or a.percentage_change_inactive_listings is distinct from e.change_inactive
    union all
    select 'fixture_duplicate_groups' where (select count(*) from actual_mart)<>6
    union all
    select 'distinct_hosts_active_stays_or_median' as issue from calculated
    where superhost_rate<>25 or active_count<>4 or stays<>4 or revenue<>142.50 or median_price<>30.375
    union all
    select 'half_open_owner_boundary' where (select count(*) from interval_join)<>2
      or exists(select 1 from interval_join where owner<>version_owner or version is null
            or (month_id=date '2020-05-01' and version<>'may')
            or (month_id=date '2020-06-01' and version<>'june'))
    union all
    select 'zero_and_missing_previous_count' where
       (100.0*(5-0)/nullif(0,0)) is not null
       or (100.0*(5-null::numeric)/nullif(null::numeric,0)) is not null
       or (100.0*(0-4)/nullif(4,0))<>-100
)
select * from failures
