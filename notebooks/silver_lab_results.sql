-- -- Databricks notebook source
-- CREATE OR REPLACE TABLE healthcare.default.lab_results_clean_data AS

-- -- STEP 1: CLEAN RAW DATA
-- WITH cleaned_raw AS (
--     SELECT
--         lab_id,
--         patient_id,

--         LOWER(TRIM(test_name)) AS test_name_clean,

--         REGEXP_REPLACE(test_value, '[^0-9.]', '') AS test_value_clean,

--         test_date

--     FROM healthcare.default.lab_results
--     WHERE patient_id IS NOT NULL
-- ),

-- -- STEP 2: STANDARDIZE TEST NAMES (NO 'Other')
-- standardized AS (
--     SELECT
--         lab_id,
--         patient_id,

--         CASE
--             WHEN test_name_clean IN ('glucose', 'glu', 'blood sugar') THEN 'Glucose'
--             WHEN test_name_clean IN ('wbc', 'white blood cells') THEN 'WBC'
--             WHEN test_name_clean IN ('hemoglobin', 'hb') THEN 'Hemoglobin'
--             WHEN test_name_clean IN ('platelets', 'plt') THEN 'Platelets'
--             ELSE NULL
--         END AS test_name,

--         TRY_CAST(NULLIF(test_value_clean, '') AS DOUBLE) AS test_value,

--         COALESCE(
--             TRY_TO_DATE(test_date, 'yyyy-MM-dd'),
--             TRY_TO_DATE(test_date, 'dd-MM-yyyy'),
--             TRY_TO_DATE(test_date, 'MM/dd/yyyy')
--         ) AS test_date

--     FROM cleaned_raw
-- ),

-- -- STEP 3: FILTER INVALID ROWS
-- filtered AS (
--     SELECT *
--     FROM standardized
--     WHERE test_name IS NOT NULL   -- ✅ Drop useless rows
-- )

-- -- FINAL OUTPUT
-- SELECT
--     lab_id,
--     patient_id,
--     test_name,
--     ROUND(test_value, 2) AS test_value,
--     test_date,
--     CURRENT_TIMESTAMP() AS job_run_timestamp

-- FROM filtered;

-- -- COMMAND ----------

-- select * from healthcare.default.lab_results_clean_data;


CREATE OR REPLACE TABLE healthcare.default.lab_results_clean_data AS

WITH cleaned_raw AS (
    SELECT
        lab_id,
        patient_id,

        LOWER(TRIM(test_name)) AS test_name_clean,

        REGEXP_REPLACE(test_value, '[^0-9.]', '') AS test_value_clean,

        test_date

    FROM healthcare.default.lab_results
    WHERE patient_id IS NOT NULL
),

standardized AS (
    SELECT
        lab_id,
        patient_id,

        CASE
            WHEN test_name_clean IN ('glucose', 'glu', 'blood sugar') THEN 'Glucose'
            WHEN test_name_clean IN ('wbc', 'white blood cells') THEN 'WBC'
            WHEN test_name_clean IN ('hemoglobin', 'hb') THEN 'Hemoglobin'
            WHEN test_name_clean IN ('platelets', 'plt') THEN 'Platelets'
            ELSE NULL
        END AS test_name,

        TRY_CAST(NULLIF(test_value_clean, '') AS DOUBLE) AS test_value,

        COALESCE(
            TRY_TO_DATE(test_date, 'yyyy-MM-dd'),
            TRY_TO_DATE(test_date, 'dd-MM-yyyy'),
            TRY_TO_DATE(test_date, 'MM/dd/yyyy')
        ) AS test_date

    FROM cleaned_raw
),

filtered AS (
    SELECT *
    FROM standardized
    WHERE test_name IS NOT NULL  
)

SELECT
    lab_id,
    patient_id,
    test_name,
    ROUND(test_value, 2) AS test_value,
    test_date,
    CURRENT_TIMESTAMP() AS job_run_timestamp

FROM filtered;


select * from healthcare.default.lab_results_clean_data;