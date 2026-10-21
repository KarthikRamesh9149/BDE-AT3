-- Part 1: execute this whole script in DBeaver, connected to BDEAT3.
-- One-time setup only. Refuse an existing workload rather than erasing its history.
-- Raw source columns remain TEXT; cleaning belongs in Silver.
-- _source_month is the filename month, while _loaded_at records the load time.
BEGIN;
DO $guard$
BEGIN
    IF current_database() <> 'BDEAT3' THEN
        RAISE EXCEPTION 'Wrong database: setup requires BDEAT3';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname IN ('bronze','silver','gold','pipeline')) THEN
        RAISE EXCEPTION 'Restart schemas already exist; this is not a reset script';
    END IF;
END
$guard$;

CREATE SCHEMA bronze;

-- Source: 05_2020.csv
CREATE TABLE bronze.raw_airbnb (
    listing_id TEXT,
    scrape_id TEXT,
    scraped_date TEXT,
    host_id TEXT,
    host_name TEXT,
    host_since TEXT,
    host_is_superhost TEXT,
    host_neighbourhood TEXT,
    listing_neighbourhood TEXT,
    property_type TEXT,
    room_type TEXT,
    accommodates TEXT,
    price TEXT,
    has_availability TEXT,
    availability_30 TEXT,
    number_of_reviews TEXT,
    review_scores_rating TEXT,
    review_scores_accuracy TEXT,
    review_scores_cleanliness TEXT,
    review_scores_checkin TEXT,
    review_scores_communication TEXT,
    review_scores_value TEXT,
    _source_file TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_month DATE NOT NULL,
    CONSTRAINT raw_airbnb_month_start CHECK (EXTRACT(DAY FROM _source_month) = 1)
);

-- Source: 2016Census_G01_NSW_LGA.csv
CREATE TABLE bronze.raw_census_g01 (
    lga_code_2016 TEXT,
    tot_p_m TEXT,
    tot_p_f TEXT,
    tot_p_p TEXT,
    age_0_4_yr_m TEXT,
    age_0_4_yr_f TEXT,
    age_0_4_yr_p TEXT,
    age_5_14_yr_m TEXT,
    age_5_14_yr_f TEXT,
    age_5_14_yr_p TEXT,
    age_15_19_yr_m TEXT,
    age_15_19_yr_f TEXT,
    age_15_19_yr_p TEXT,
    age_20_24_yr_m TEXT,
    age_20_24_yr_f TEXT,
    age_20_24_yr_p TEXT,
    age_25_34_yr_m TEXT,
    age_25_34_yr_f TEXT,
    age_25_34_yr_p TEXT,
    age_35_44_yr_m TEXT,
    age_35_44_yr_f TEXT,
    age_35_44_yr_p TEXT,
    age_45_54_yr_m TEXT,
    age_45_54_yr_f TEXT,
    age_45_54_yr_p TEXT,
    age_55_64_yr_m TEXT,
    age_55_64_yr_f TEXT,
    age_55_64_yr_p TEXT,
    age_65_74_yr_m TEXT,
    age_65_74_yr_f TEXT,
    age_65_74_yr_p TEXT,
    age_75_84_yr_m TEXT,
    age_75_84_yr_f TEXT,
    age_75_84_yr_p TEXT,
    age_85ov_m TEXT,
    age_85ov_f TEXT,
    age_85ov_p TEXT,
    counted_census_night_home_m TEXT,
    counted_census_night_home_f TEXT,
    counted_census_night_home_p TEXT,
    count_census_nt_ewhere_aust_m TEXT,
    count_census_nt_ewhere_aust_f TEXT,
    count_census_nt_ewhere_aust_p TEXT,
    indigenous_psns_aboriginal_m TEXT,
    indigenous_psns_aboriginal_f TEXT,
    indigenous_psns_aboriginal_p TEXT,
    indig_psns_torres_strait_is_m TEXT,
    indig_psns_torres_strait_is_f TEXT,
    indig_psns_torres_strait_is_p TEXT,
    indig_bth_abor_torres_st_is_m TEXT,
    indig_bth_abor_torres_st_is_f TEXT,
    indig_bth_abor_torres_st_is_p TEXT,
    indigenous_p_tot_m TEXT,
    indigenous_p_tot_f TEXT,
    indigenous_p_tot_p TEXT,
    birthplace_australia_m TEXT,
    birthplace_australia_f TEXT,
    birthplace_australia_p TEXT,
    birthplace_elsewhere_m TEXT,
    birthplace_elsewhere_f TEXT,
    birthplace_elsewhere_p TEXT,
    lang_spoken_home_eng_only_m TEXT,
    lang_spoken_home_eng_only_f TEXT,
    lang_spoken_home_eng_only_p TEXT,
    lang_spoken_home_oth_lang_m TEXT,
    lang_spoken_home_oth_lang_f TEXT,
    lang_spoken_home_oth_lang_p TEXT,
    australian_citizen_m TEXT,
    australian_citizen_f TEXT,
    australian_citizen_p TEXT,
    age_psns_att_educ_inst_0_4_m TEXT,
    age_psns_att_educ_inst_0_4_f TEXT,
    age_psns_att_educ_inst_0_4_p TEXT,
    age_psns_att_educ_inst_5_14_m TEXT,
    age_psns_att_educ_inst_5_14_f TEXT,
    age_psns_att_educ_inst_5_14_p TEXT,
    age_psns_att_edu_inst_15_19_m TEXT,
    age_psns_att_edu_inst_15_19_f TEXT,
    age_psns_att_edu_inst_15_19_p TEXT,
    age_psns_att_edu_inst_20_24_m TEXT,
    age_psns_att_edu_inst_20_24_f TEXT,
    age_psns_att_edu_inst_20_24_p TEXT,
    age_psns_att_edu_inst_25_ov_m TEXT,
    age_psns_att_edu_inst_25_ov_f TEXT,
    age_psns_att_edu_inst_25_ov_p TEXT,
    high_yr_schl_comp_yr_12_eq_m TEXT,
    high_yr_schl_comp_yr_12_eq_f TEXT,
    high_yr_schl_comp_yr_12_eq_p TEXT,
    high_yr_schl_comp_yr_11_eq_m TEXT,
    high_yr_schl_comp_yr_11_eq_f TEXT,
    high_yr_schl_comp_yr_11_eq_p TEXT,
    high_yr_schl_comp_yr_10_eq_m TEXT,
    high_yr_schl_comp_yr_10_eq_f TEXT,
    high_yr_schl_comp_yr_10_eq_p TEXT,
    high_yr_schl_comp_yr_9_eq_m TEXT,
    high_yr_schl_comp_yr_9_eq_f TEXT,
    high_yr_schl_comp_yr_9_eq_p TEXT,
    high_yr_schl_comp_yr_8_belw_m TEXT,
    high_yr_schl_comp_yr_8_belw_f TEXT,
    high_yr_schl_comp_yr_8_belw_p TEXT,
    high_yr_schl_comp_d_n_g_sch_m TEXT,
    high_yr_schl_comp_d_n_g_sch_f TEXT,
    high_yr_schl_comp_d_n_g_sch_p TEXT,
    count_psns_occ_priv_dwgs_m TEXT,
    count_psns_occ_priv_dwgs_f TEXT,
    count_psns_occ_priv_dwgs_p TEXT,
    count_persons_other_dwgs_m TEXT,
    count_persons_other_dwgs_f TEXT,
    count_persons_other_dwgs_p TEXT,
    _source_file TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Source: 2016Census_G02_NSW_LGA.csv
CREATE TABLE bronze.raw_census_g02 (
    lga_code_2016 TEXT,
    median_age_persons TEXT,
    median_mortgage_repay_monthly TEXT,
    median_tot_prsnl_inc_weekly TEXT,
    median_rent_weekly TEXT,
    median_tot_fam_inc_weekly TEXT,
    average_num_psns_per_bedroom TEXT,
    median_tot_hhd_inc_weekly TEXT,
    average_household_size TEXT,
    _source_file TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Source: NSW_LGA_CODE.csv
CREATE TABLE bronze.raw_lga_code (
    lga_code TEXT,
    lga_name TEXT,
    _source_file TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Source: NSW_LGA_SUBURB.csv
CREATE TABLE bronze.raw_lga_suburb (
    lga_name TEXT,
    suburb_name TEXT,
    source_column_03 TEXT,
    source_column_04 TEXT,
    source_column_05 TEXT,
    source_column_06 TEXT,
    source_column_07 TEXT,
    source_column_08 TEXT,
    source_column_09 TEXT,
    source_column_10 TEXT,
    source_column_11 TEXT,
    source_column_12 TEXT,
    source_column_13 TEXT,
    source_column_14 TEXT,
    source_column_15 TEXT,
    source_column_16 TEXT,
    source_column_17 TEXT,
    source_column_18 TEXT,
    source_column_19 TEXT,
    source_column_20 TEXT,
    source_column_21 TEXT,
    source_column_22 TEXT,
    source_column_23 TEXT,
    source_column_24 TEXT,
    source_column_25 TEXT,
    source_column_26 TEXT,
    source_column_27 TEXT,
    _source_file TEXT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- Operational records are separate from the five raw business tables.
CREATE SCHEMA pipeline;
CREATE TABLE pipeline.configuration (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    contract_version TEXT NOT NULL,
    bucket_name TEXT NOT NULL,
    source_prefix TEXT NOT NULL
);
INSERT INTO pipeline.configuration VALUES
    (TRUE, 'at3-rebuild-v1', 'australia-southeast1-bigdat-bc43ec38-bucket', 'data/at3_rebuild/');

CREATE TABLE pipeline.load_runs (
    run_id TEXT PRIMARY KEY,
    observation_month DATE NOT NULL CHECK (
        observation_month BETWEEN DATE '2020-05-01' AND DATE '2021-04-01'
        AND EXTRACT(DAY FROM observation_month) = 1),
    source_manifest JSONB,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    verified_at TIMESTAMPTZ
);
CREATE TABLE pipeline.source_files (
    source_file TEXT PRIMARY KEY,
    target_table TEXT NOT NULL CHECK (target_table IN
        ('raw_airbnb','raw_census_g01','raw_census_g02','raw_lga_code','raw_lga_suburb')),
    observation_month DATE,
    generation BIGINT NOT NULL CHECK (generation > 0),
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    content_sha256 TEXT NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    row_count BIGINT NOT NULL CHECK (row_count > 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK ((target_table = 'raw_airbnb') = (observation_month IS NOT NULL))
);
CREATE TABLE pipeline.monthly_loads (
    observation_month DATE PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES pipeline.load_runs(run_id),
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    verified_at TIMESTAMPTZ
);
-- Part 2 will write a row only after all required nodes and tests in the saved job pass.
-- This table is intentionally empty throughout Part 1. Do not insert manual success markers.
CREATE TABLE pipeline.dbt_completions (
    observation_month DATE PRIMARY KEY REFERENCES pipeline.monthly_loads(observation_month),
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    dbt_invocation_id TEXT NOT NULL UNIQUE,
    dbt_cloud_job_id BIGINT NOT NULL CHECK (dbt_cloud_job_id > 0),
    dbt_cloud_run_id BIGINT NOT NULL UNIQUE CHECK (dbt_cloud_run_id > 0),
    git_sha TEXT NOT NULL CHECK (git_sha ~ '^[0-9a-f]{40}$'),
    dbt_version TEXT NOT NULL,
    passed_tests INTEGER NOT NULL CHECK (passed_tests > 0),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- The brief advises empty tables before the first run. These are newly created tables.
-- This statement is not part of the monthly DAG and cannot run past the guard on a rerun.
TRUNCATE TABLE bronze.raw_airbnb, bronze.raw_census_g01, bronze.raw_census_g02,
               bronze.raw_lga_code, bronze.raw_lga_suburb;
CREATE INDEX raw_airbnb_source_month_idx ON bronze.raw_airbnb (_source_month);
COMMIT;

-- Initial state: correct database, five raw tables, five zero row counts, no completion marker.
SELECT current_database() AS database_name,
       (SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()) AS ssl_active;
SELECT 'raw_airbnb' AS table_name, COUNT(*) AS row_count FROM bronze.raw_airbnb
UNION ALL SELECT 'raw_census_g01', COUNT(*) FROM bronze.raw_census_g01
UNION ALL SELECT 'raw_census_g02', COUNT(*) FROM bronze.raw_census_g02
UNION ALL SELECT 'raw_lga_code', COUNT(*) FROM bronze.raw_lga_code
UNION ALL SELECT 'raw_lga_suburb', COUNT(*) FROM bronze.raw_lga_suburb;
SELECT COUNT(*) AS dbt_completion_rows FROM pipeline.dbt_completions;
