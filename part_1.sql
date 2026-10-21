-- Part 1: use DBeaver, connected to BDEAT3.
-- Section A: run once before the first Airflow load.
-- Section B at the end: run separately after May succeeds.
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


-- Section B: post-May verification (read only).
-- Select and run this section after May succeeds; do not rerun Section A.
-- Counts and source-value fingerprints are calculated independently from the CSVs.
-- Each row hash serialises the original source fields in CSV column order.
-- Sorting the row hashes retains duplicate multiplicities without depending on row order.
-- Run after the May Airflow load, connected to BDEAT3.
-- Fingerprints cover every source value and retain duplicate multiplicity.
WITH actual AS (
SELECT 'raw_airbnb' AS table_name, count(*) AS actual_rows, md5(string_agg(md5(array_to_json(ARRAY[listing_id,
    scrape_id, scraped_date, host_id, host_name, host_since, host_is_superhost, host_neighbourhood,
    listing_neighbourhood, property_type, room_type, accommodates, price, has_availability, availability_30,
    number_of_reviews, review_scores_rating, review_scores_accuracy, review_scores_cleanliness,
    review_scores_checkin, review_scores_communication, review_scores_value])::text), '' ORDER BY
    md5(array_to_json(ARRAY[listing_id, scrape_id, scraped_date, host_id, host_name, host_since,
    host_is_superhost, host_neighbourhood, listing_neighbourhood, property_type, room_type, accommodates,
    price, has_availability, availability_30, number_of_reviews, review_scores_rating, review_scores_accuracy,
    review_scores_cleanliness, review_scores_checkin, review_scores_communication,
    review_scores_value])::text) COLLATE "C")) AS content_fingerprint FROM bronze.raw_airbnb
UNION ALL
SELECT 'raw_census_g01' AS table_name, count(*) AS actual_rows,
    md5(string_agg(md5(array_to_json(ARRAY[lga_code_2016, tot_p_m, tot_p_f, tot_p_p, age_0_4_yr_m,
    age_0_4_yr_f, age_0_4_yr_p, age_5_14_yr_m, age_5_14_yr_f, age_5_14_yr_p, age_15_19_yr_m, age_15_19_yr_f,
    age_15_19_yr_p, age_20_24_yr_m, age_20_24_yr_f, age_20_24_yr_p, age_25_34_yr_m, age_25_34_yr_f,
    age_25_34_yr_p, age_35_44_yr_m, age_35_44_yr_f, age_35_44_yr_p, age_45_54_yr_m, age_45_54_yr_f,
    age_45_54_yr_p, age_55_64_yr_m, age_55_64_yr_f, age_55_64_yr_p, age_65_74_yr_m, age_65_74_yr_f,
    age_65_74_yr_p, age_75_84_yr_m, age_75_84_yr_f, age_75_84_yr_p, age_85ov_m, age_85ov_f, age_85ov_p,
    counted_census_night_home_m, counted_census_night_home_f, counted_census_night_home_p,
    count_census_nt_ewhere_aust_m, count_census_nt_ewhere_aust_f, count_census_nt_ewhere_aust_p,
    indigenous_psns_aboriginal_m, indigenous_psns_aboriginal_f, indigenous_psns_aboriginal_p,
    indig_psns_torres_strait_is_m, indig_psns_torres_strait_is_f, indig_psns_torres_strait_is_p,
    indig_bth_abor_torres_st_is_m, indig_bth_abor_torres_st_is_f, indig_bth_abor_torres_st_is_p,
    indigenous_p_tot_m, indigenous_p_tot_f, indigenous_p_tot_p, birthplace_australia_m,
    birthplace_australia_f, birthplace_australia_p, birthplace_elsewhere_m, birthplace_elsewhere_f,
    birthplace_elsewhere_p, lang_spoken_home_eng_only_m, lang_spoken_home_eng_only_f,
    lang_spoken_home_eng_only_p, lang_spoken_home_oth_lang_m, lang_spoken_home_oth_lang_f,
    lang_spoken_home_oth_lang_p, australian_citizen_m, australian_citizen_f, australian_citizen_p,
    age_psns_att_educ_inst_0_4_m, age_psns_att_educ_inst_0_4_f, age_psns_att_educ_inst_0_4_p,
    age_psns_att_educ_inst_5_14_m, age_psns_att_educ_inst_5_14_f, age_psns_att_educ_inst_5_14_p,
    age_psns_att_edu_inst_15_19_m, age_psns_att_edu_inst_15_19_f, age_psns_att_edu_inst_15_19_p,
    age_psns_att_edu_inst_20_24_m, age_psns_att_edu_inst_20_24_f, age_psns_att_edu_inst_20_24_p,
    age_psns_att_edu_inst_25_ov_m, age_psns_att_edu_inst_25_ov_f, age_psns_att_edu_inst_25_ov_p,
    high_yr_schl_comp_yr_12_eq_m, high_yr_schl_comp_yr_12_eq_f, high_yr_schl_comp_yr_12_eq_p,
    high_yr_schl_comp_yr_11_eq_m, high_yr_schl_comp_yr_11_eq_f, high_yr_schl_comp_yr_11_eq_p,
    high_yr_schl_comp_yr_10_eq_m, high_yr_schl_comp_yr_10_eq_f, high_yr_schl_comp_yr_10_eq_p,
    high_yr_schl_comp_yr_9_eq_m, high_yr_schl_comp_yr_9_eq_f, high_yr_schl_comp_yr_9_eq_p,
    high_yr_schl_comp_yr_8_belw_m, high_yr_schl_comp_yr_8_belw_f, high_yr_schl_comp_yr_8_belw_p,
    high_yr_schl_comp_d_n_g_sch_m, high_yr_schl_comp_d_n_g_sch_f, high_yr_schl_comp_d_n_g_sch_p,
    count_psns_occ_priv_dwgs_m, count_psns_occ_priv_dwgs_f, count_psns_occ_priv_dwgs_p,
    count_persons_other_dwgs_m, count_persons_other_dwgs_f, count_persons_other_dwgs_p])::text), '' ORDER BY
    md5(array_to_json(ARRAY[lga_code_2016, tot_p_m, tot_p_f, tot_p_p, age_0_4_yr_m, age_0_4_yr_f,
    age_0_4_yr_p, age_5_14_yr_m, age_5_14_yr_f, age_5_14_yr_p, age_15_19_yr_m, age_15_19_yr_f, age_15_19_yr_p,
    age_20_24_yr_m, age_20_24_yr_f, age_20_24_yr_p, age_25_34_yr_m, age_25_34_yr_f, age_25_34_yr_p,
    age_35_44_yr_m, age_35_44_yr_f, age_35_44_yr_p, age_45_54_yr_m, age_45_54_yr_f, age_45_54_yr_p,
    age_55_64_yr_m, age_55_64_yr_f, age_55_64_yr_p, age_65_74_yr_m, age_65_74_yr_f, age_65_74_yr_p,
    age_75_84_yr_m, age_75_84_yr_f, age_75_84_yr_p, age_85ov_m, age_85ov_f, age_85ov_p,
    counted_census_night_home_m, counted_census_night_home_f, counted_census_night_home_p,
    count_census_nt_ewhere_aust_m, count_census_nt_ewhere_aust_f, count_census_nt_ewhere_aust_p,
    indigenous_psns_aboriginal_m, indigenous_psns_aboriginal_f, indigenous_psns_aboriginal_p,
    indig_psns_torres_strait_is_m, indig_psns_torres_strait_is_f, indig_psns_torres_strait_is_p,
    indig_bth_abor_torres_st_is_m, indig_bth_abor_torres_st_is_f, indig_bth_abor_torres_st_is_p,
    indigenous_p_tot_m, indigenous_p_tot_f, indigenous_p_tot_p, birthplace_australia_m,
    birthplace_australia_f, birthplace_australia_p, birthplace_elsewhere_m, birthplace_elsewhere_f,
    birthplace_elsewhere_p, lang_spoken_home_eng_only_m, lang_spoken_home_eng_only_f,
    lang_spoken_home_eng_only_p, lang_spoken_home_oth_lang_m, lang_spoken_home_oth_lang_f,
    lang_spoken_home_oth_lang_p, australian_citizen_m, australian_citizen_f, australian_citizen_p,
    age_psns_att_educ_inst_0_4_m, age_psns_att_educ_inst_0_4_f, age_psns_att_educ_inst_0_4_p,
    age_psns_att_educ_inst_5_14_m, age_psns_att_educ_inst_5_14_f, age_psns_att_educ_inst_5_14_p,
    age_psns_att_edu_inst_15_19_m, age_psns_att_edu_inst_15_19_f, age_psns_att_edu_inst_15_19_p,
    age_psns_att_edu_inst_20_24_m, age_psns_att_edu_inst_20_24_f, age_psns_att_edu_inst_20_24_p,
    age_psns_att_edu_inst_25_ov_m, age_psns_att_edu_inst_25_ov_f, age_psns_att_edu_inst_25_ov_p,
    high_yr_schl_comp_yr_12_eq_m, high_yr_schl_comp_yr_12_eq_f, high_yr_schl_comp_yr_12_eq_p,
    high_yr_schl_comp_yr_11_eq_m, high_yr_schl_comp_yr_11_eq_f, high_yr_schl_comp_yr_11_eq_p,
    high_yr_schl_comp_yr_10_eq_m, high_yr_schl_comp_yr_10_eq_f, high_yr_schl_comp_yr_10_eq_p,
    high_yr_schl_comp_yr_9_eq_m, high_yr_schl_comp_yr_9_eq_f, high_yr_schl_comp_yr_9_eq_p,
    high_yr_schl_comp_yr_8_belw_m, high_yr_schl_comp_yr_8_belw_f, high_yr_schl_comp_yr_8_belw_p,
    high_yr_schl_comp_d_n_g_sch_m, high_yr_schl_comp_d_n_g_sch_f, high_yr_schl_comp_d_n_g_sch_p,
    count_psns_occ_priv_dwgs_m, count_psns_occ_priv_dwgs_f, count_psns_occ_priv_dwgs_p,
    count_persons_other_dwgs_m, count_persons_other_dwgs_f, count_persons_other_dwgs_p])::text) COLLATE "C"))
    AS content_fingerprint FROM bronze.raw_census_g01
UNION ALL
SELECT 'raw_census_g02' AS table_name, count(*) AS actual_rows,
    md5(string_agg(md5(array_to_json(ARRAY[lga_code_2016, median_age_persons, median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly, median_rent_weekly, median_tot_fam_inc_weekly, average_num_psns_per_bedroom,
    median_tot_hhd_inc_weekly, average_household_size])::text), '' ORDER BY
    md5(array_to_json(ARRAY[lga_code_2016, median_age_persons, median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly, median_rent_weekly, median_tot_fam_inc_weekly, average_num_psns_per_bedroom,
    median_tot_hhd_inc_weekly, average_household_size])::text) COLLATE "C")) AS content_fingerprint FROM
    bronze.raw_census_g02
UNION ALL
SELECT 'raw_lga_code' AS table_name, count(*) AS actual_rows, md5(string_agg(md5(array_to_json(ARRAY[lga_code,
    lga_name])::text), '' ORDER BY md5(array_to_json(ARRAY[lga_code, lga_name])::text) COLLATE "C")) AS
    content_fingerprint FROM bronze.raw_lga_code
UNION ALL
SELECT 'raw_lga_suburb' AS table_name, count(*) AS actual_rows,
    md5(string_agg(md5(array_to_json(ARRAY[lga_name, suburb_name, source_column_03, source_column_04,
    source_column_05, source_column_06, source_column_07, source_column_08, source_column_09,
    source_column_10, source_column_11, source_column_12, source_column_13, source_column_14,
    source_column_15, source_column_16, source_column_17, source_column_18, source_column_19,
    source_column_20, source_column_21, source_column_22, source_column_23, source_column_24,
    source_column_25, source_column_26, source_column_27])::text), '' ORDER BY
    md5(array_to_json(ARRAY[lga_name, suburb_name, source_column_03, source_column_04, source_column_05,
    source_column_06, source_column_07, source_column_08, source_column_09, source_column_10,
    source_column_11, source_column_12, source_column_13, source_column_14, source_column_15,
    source_column_16, source_column_17, source_column_18, source_column_19, source_column_20,
    source_column_21, source_column_22, source_column_23, source_column_24, source_column_25,
    source_column_26, source_column_27])::text) COLLATE "C")) AS content_fingerprint FROM
    bronze.raw_lga_suburb
), expected(table_name, expected_rows, content_fingerprint) AS (VALUES
('raw_airbnb', 37562, '38dfedc91b209f4931af9f6a240f4ca8'),
('raw_census_g01', 132, '0e00bf4a4f9781fcf89f1b315b6a296e'),
('raw_census_g02', 132, 'd61e9ac58640c94c0f05c65d25c655b1'),
('raw_lga_code', 129, '0a3e565ddd7a152387b41f2881882ea5'),
('raw_lga_suburb', 4470, '0d1e2ca92689ae45a8a1d1ec2544bb8f'))
SELECT actual.table_name, actual.actual_rows, actual.actual_rows=expected.expected_rows AS count_matches,
       actual.content_fingerprint=expected.content_fingerprint AS source_values_match
FROM actual JOIN expected USING (table_name)
ORDER BY table_name;

-- May-only checkpoint: expect nine true results before starting Part 2.
SELECT 'correct_database' AS check_name, current_database()='BDEAT3' AS passed
UNION ALL SELECT 'ssl_active', (SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid())
UNION ALL SELECT 'only_may_rows', count(*)=37562 AND count(*) FILTER (WHERE _source_month=DATE '2020-05-01'
    AND _source_file='05_2020.csv')=37562 FROM bronze.raw_airbnb
UNION ALL SELECT 'one_verified_month', count(*)=1 AND count(*) FILTER (WHERE observation_month=DATE
    '2020-05-01' AND verified_at IS NOT NULL)=1 FROM pipeline.monthly_loads
UNION ALL SELECT 'one_verified_run', count(*)=1 AND count(*) FILTER (WHERE observation_month=DATE '2020-05-01'
    AND source_manifest IS NOT NULL AND verified_at IS NOT NULL)=1 FROM pipeline.load_runs
UNION ALL SELECT 'five_source_records', count(*)=5 AND bool_and(generation>0 AND loaded_at IS NOT NULL) FROM
    pipeline.source_files
UNION ALL SELECT 'dbt_still_pending', count(*)=0 FROM pipeline.dbt_completions
UNION ALL SELECT 'no_old_silver_or_gold', count(*)=0 FROM pg_namespace WHERE nspname IN ('silver','gold')
UNION ALL SELECT 'source_month_metadata', bool_and((table_schema='bronze' AND column_name='_source_month' AND
    data_type='date')) FROM information_schema.columns WHERE table_schema='bronze' AND table_name='raw_airbnb'
    AND column_name='_source_month'
ORDER BY check_name;
