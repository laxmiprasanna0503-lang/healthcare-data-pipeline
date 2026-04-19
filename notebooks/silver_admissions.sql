-- -- Databricks notebook source
-- CREATE OR REPLACE TABLE healthcare.default.admissions_clean_data AS
-- WITH formatted AS (
--     SELECT
--         admission_id,
--         patient_id,

--         COALESCE(
--             TRY_TO_DATE(hospital_admission_date, 'yyyy-MM-dd'),
--             TRY_TO_DATE(hospital_admission_date, 'dd-MM-yyyy'),
--             TRY_TO_DATE(hospital_admission_date, 'MM/dd/yyyy')
--         ) AS admit_date,

--         COALESCE(
--             TRY_TO_DATE(discharge_date, 'yyyy-MM-dd'),
--             TRY_TO_DATE(discharge_date, 'dd-MM-yyyy'),
--             TRY_TO_DATE(discharge_date, 'MM/dd/yyyy')
--         ) AS discharge_date

--     FROM healthcare.default.admissions
-- ),

-- filtered AS (
--     SELECT *
--     FROM formatted
--     WHERE admission_id IS NOT NULL
--       AND patient_id IS NOT NULL
--       AND admit_date IS NOT NULL   -- ❗ critical field
-- )

-- SELECT
--     admission_id,
--     patient_id,
--     admit_date,
--     discharge_date,

--     -- Safe LOS calculation
--     CASE 
--         WHEN discharge_date IS NOT NULL 
--         THEN DATEDIFF(discharge_date, admit_date)
--         ELSE NULL
--     END AS length_of_stay

-- FROM filtered;

-- -- COMMAND ----------

-- select * from healthcare.default.admissions_clean_data;



CREATE OR REPLACE TABLE healthcare.default.admissions_clean_data AS
WITH formatted AS (
    SELECT
        admission_id,
        patient_id,

        COALESCE(
            TRY_TO_DATE(hospital_admission_date, 'yyyy-MM-dd'),
            TRY_TO_DATE(hospital_admission_date, 'dd-MM-yyyy'),
            TRY_TO_DATE(hospital_admission_date, 'MM/dd/yyyy')
        ) AS admit_date,

        COALESCE(
            TRY_TO_DATE(discharge_date, 'yyyy-MM-dd'),
            TRY_TO_DATE(discharge_date, 'dd-MM-yyyy'),
            TRY_TO_DATE(discharge_date, 'MM/dd/yyyy')
        ) AS discharge_date

    FROM healthcare.default.admissions
),

filtered AS (
    SELECT *
    FROM formatted
    WHERE admission_id IS NOT NULL
      AND patient_id IS NOT NULL
      AND admit_date IS NOT NULL 
)

SELECT
    admission_id,
    patient_id,
    admit_date,
    discharge_date,

    CASE 
        WHEN discharge_date IS NOT NULL 
        THEN DATEDIFF(discharge_date, admit_date)
        ELSE NULL
    END AS length_of_stay

FROM filtered;

select * from healthcare.default.admissions_clean_data;