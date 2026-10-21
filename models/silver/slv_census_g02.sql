-- Keep every supplied measure; no recomputation of Census totals.
select
    lga_code_2016 as source_lga_code,
    substring(btrim(lga_code_2016) from '^LGA([0-9]{5})$') as lga_code,
    nullif(btrim(median_age_persons), '')::numeric as median_age_persons,
    nullif(btrim(median_mortgage_repay_monthly), '')::numeric as median_mortgage_repay_monthly,
    nullif(btrim(median_tot_prsnl_inc_weekly), '')::numeric as median_tot_prsnl_inc_weekly,
    nullif(btrim(median_rent_weekly), '')::numeric as median_rent_weekly,
    nullif(btrim(median_tot_fam_inc_weekly), '')::numeric as median_tot_fam_inc_weekly,
    nullif(btrim(average_num_psns_per_bedroom), '')::numeric as average_num_psns_per_bedroom,
    nullif(btrim(median_tot_hhd_inc_weekly), '')::numeric as median_tot_hhd_inc_weekly,
    nullif(btrim(average_household_size), '')::numeric as average_household_size,
    2016::integer as census_year,
    _source_file,
    _loaded_at
from {{ source('bronze', 'raw_census_g02') }}
