-- -- Databricks notebook source
-- CREATE OR REPLACE TABLE healthcare.default.patient_clean_data AS

-- -- STEP 1: STANDARDIZE RAW DATA
-- WITH cleaned_raw AS (
--     SELECT
--         patient_id,

--         CAST(age AS INT) AS age,

--         CASE
--             WHEN LOWER(gender) IN ('m', 'male') THEN 'Male'
--             WHEN LOWER(gender) IN ('f', 'female') THEN 'Female'
--             ELSE 'Unknown'
--         END AS gender,

--         -- Normalize BP: convert '-', space → '/'
--         REGEXP_REPLACE(
--             REGEXP_REPLACE(blood_pressure, '[-\\s]+', '/'),
--             '[^0-9/]', ''
--         ) AS bp_clean_str,

--         -- Remove units from cholesterol
--         REGEXP_REPLACE(cholesterol, '[^0-9]', '') AS chol_clean_str,

--         heart_rate,
--         diagnosis

--     FROM healthcare.default.patients
--     WHERE patient_id IS NOT NULL
-- ),

-- -- STEP 2: SAFE EXTRACTION
-- extracted_values AS (
--     SELECT
--         patient_id,
--         age,
--         gender,

--         TRY_CAST(NULLIF(REGEXP_EXTRACT(bp_clean_str, '(\\d+)', 1), '') AS DOUBLE) AS systolic_raw,

--         TRY_CAST(NULLIF(REGEXP_EXTRACT(bp_clean_str, '/(\\d+)', 1), '') AS DOUBLE) AS diastolic_raw,

--         TRY_CAST(NULLIF(chol_clean_str, '') AS DOUBLE) AS cholesterol_raw,

--         TRY_CAST(heart_rate AS DOUBLE) AS heart_rate_raw,

--         diagnosis

--     FROM cleaned_raw
-- ),

-- -- STEP 3: CLINICAL VALIDATION
-- validated_values AS (
--     SELECT
--         patient_id,
--         age,
--         gender,

--         CASE 
--             WHEN systolic_raw BETWEEN 90 AND 200 THEN systolic_raw
--             ELSE NULL
--         END AS systolic_bp,

--         CASE 
--             WHEN diastolic_raw BETWEEN 60 AND 120 THEN diastolic_raw
--             ELSE NULL
--         END AS diastolic_bp,

--         CASE
--             WHEN cholesterol_raw BETWEEN 100 AND 400 THEN cholesterol_raw
--             ELSE NULL
--         END AS cholesterol_clean,

--         CASE
--             WHEN heart_rate_raw BETWEEN 40 AND 150 THEN heart_rate_raw
--             ELSE NULL
--         END AS heart_rate_clean,

--         diagnosis

--     FROM extracted_values
-- ),

-- -- STEP 4: STATS FOR IMPUTATION
-- stats AS (
--     SELECT
--         percentile_approx(age, 0.5) AS median_age,
--         AVG(systolic_bp) AS avg_sys_bp,
--         AVG(diastolic_bp) AS avg_dia_bp,
--         AVG(cholesterol_clean) AS avg_chol,
--         AVG(heart_rate_clean) AS avg_hr
--     FROM validated_values
-- )

-- -- STEP 5: FINAL OUTPUT
-- SELECT
--     v.patient_id,

--     COALESCE(v.age, s.median_age) AS age,
--     v.gender,

--     -- BP as INTEGER
--     CAST(ROUND(ABS(COALESCE(v.systolic_bp, s.avg_sys_bp))) AS INT) AS systolic_bp,

--     CAST(ROUND(ABS(COALESCE(v.diastolic_bp, s.avg_dia_bp))) AS INT) AS diastolic_bp,

--     -- Cholesterol with unit in column name
--     ROUND(COALESCE(v.cholesterol_clean, s.avg_chol), 2) AS cholesterol_mg_dl,

--     -- Heart rate as INTEGER
--     CAST(ROUND(COALESCE(v.heart_rate_clean, s.avg_hr)) AS INT) AS heart_rate,

--     v.diagnosis,

--     -- Job tracking
--     CURRENT_TIMESTAMP() AS job_run_timestamp

-- FROM validated_values v
-- CROSS JOIN stats s;

-- -- COMMAND ----------

-- select * from healthcare.default.patient_clean_data;



CREATE OR REPLACE TABLE healthcare.default.patient_clean_data AS

WITH cleaned_raw AS (
    SELECT
        patient_id,

        CAST(age AS INT) AS age,

        CASE
            WHEN LOWER(gender) IN ('m', 'male') THEN 'Male'
            WHEN LOWER(gender) IN ('f', 'female') THEN 'Female'
            ELSE 'Unknown'
        END AS gender,

        REGEXP_REPLACE(
            REGEXP_REPLACE(blood_pressure, '[-\\s]+', '/'),
            '[^0-9/]', ''
        ) AS bp_clean_str,

        REGEXP_REPLACE(cholesterol, '[^0-9]', '') AS chol_clean_str,

        heart_rate,
        diagnosis

    FROM healthcare.default.patients
    WHERE patient_id IS NOT NULL
),

extracted_values AS (
    SELECT
        patient_id,
        age,
        gender,

        TRY_CAST(NULLIF(REGEXP_EXTRACT(bp_clean_str, '(\\d+)', 1), '') AS DOUBLE) AS systolic_raw,

        TRY_CAST(NULLIF(REGEXP_EXTRACT(bp_clean_str, '/(\\d+)', 1), '') AS DOUBLE) AS diastolic_raw,

        TRY_CAST(NULLIF(chol_clean_str, '') AS DOUBLE) AS cholesterol_raw,

        TRY_CAST(heart_rate AS DOUBLE) AS heart_rate_raw,

        diagnosis

    FROM cleaned_raw
),

validated_values AS (
    SELECT
        patient_id,
        age,
        gender,

        CASE 
            WHEN systolic_raw BETWEEN 90 AND 200 THEN systolic_raw
            ELSE NULL
        END AS systolic_bp,

        CASE 
            WHEN diastolic_raw BETWEEN 60 AND 120 THEN diastolic_raw
            ELSE NULL
        END AS diastolic_bp,

        CASE
            WHEN cholesterol_raw BETWEEN 100 AND 400 THEN cholesterol_raw
            ELSE NULL
        END AS cholesterol_clean,

        CASE
            WHEN heart_rate_raw BETWEEN 40 AND 150 THEN heart_rate_raw
            ELSE NULL
        END AS heart_rate_clean,

        diagnosis

    FROM extracted_values
),

stats AS (
    SELECT
        percentile_approx(age, 0.5) AS median_age,
        AVG(systolic_bp) AS avg_sys_bp,
        AVG(diastolic_bp) AS avg_dia_bp,
        AVG(cholesterol_clean) AS avg_chol,
        AVG(heart_rate_clean) AS avg_hr
    FROM validated_values
)

SELECT
    v.patient_id,

    COALESCE(v.age, s.median_age) AS age,
    v.gender,

    CAST(ROUND(ABS(COALESCE(v.systolic_bp, s.avg_sys_bp))) AS INT) AS systolic_bp,

    CAST(ROUND(ABS(COALESCE(v.diastolic_bp, s.avg_dia_bp))) AS INT) AS diastolic_bp,

    ROUND(COALESCE(v.cholesterol_clean, s.avg_chol), 2) AS cholesterol_mg_dl,

    CAST(ROUND(COALESCE(v.heart_rate_clean, s.avg_hr)) AS INT) AS heart_rate,

    v.diagnosis,

    CURRENT_TIMESTAMP() AS job_run_timestamp

FROM validated_values v
CROSS JOIN stats s;

select * from healthcare.default.patient_clean_data;