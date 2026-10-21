-- depends_on: {{ ref('snapshot_ready') }}
-- Preserve all Census measures, including codes outside the LGA lookup.
select
    source_lga_code,
    lga_code,
    median_age_persons,
    median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly,
    median_rent_weekly,
    median_tot_fam_inc_weekly,
    average_num_psns_per_bedroom,
    median_tot_hhd_inc_weekly,
    average_household_size,
    census_year
from {{ ref('slv_census_g02') }}
