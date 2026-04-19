-- -- Databricks notebook source
-- CREATE OR REPLACE TABLE healthcare.default.hospital_analytics AS

-- WITH base AS (
--     SELECT *
--     FROM healthcare.default.patient_risk_scores
-- ),

-- age_grouped AS (
--     SELECT
--         *,

--         CASE
--             WHEN age BETWEEN 0 AND 30 THEN '0-30'
--             WHEN age BETWEEN 31 AND 50 THEN '31-50'
--             WHEN age BETWEEN 51 AND 70 THEN '51-70'
--             ELSE '70+'
--         END AS age_group

--     FROM base
-- )

-- SELECT
--     age_group,

--     COUNT(*) AS total_patients,

--     ROUND(AVG(systolic_bp), 2) AS avg_systolic_bp,
--     ROUND(AVG(diastolic_bp), 2) AS avg_diastolic_bp,
--     ROUND(AVG(cholesterol_mg_dl), 2) AS avg_cholesterol,
--     ROUND(AVG(heart_rate), 2) AS avg_heart_rate,

--     ROUND(AVG(risk_score), 3) AS avg_risk_score,

--     SUM(readmission_flag) AS high_risk_patients

-- FROM age_grouped
-- GROUP BY age_group;

-- -- COMMAND ----------

-- select * from healthcare.default.hospital_analytics;


CREATE OR REPLACE TABLE healthcare.default.hospital_analytics AS

WITH base AS (
    SELECT *
    FROM healthcare.default.patient_risk_scores
),

age_grouped AS (
    SELECT *,
        CASE
            WHEN age BETWEEN 0 AND 30 THEN '0-30'
            WHEN age BETWEEN 31 AND 50 THEN '31-50'
            WHEN age BETWEEN 51 AND 70 THEN '51-70'
            ELSE '70+'
        END AS age_group
    FROM base
)

SELECT
    age_group,
    COUNT(*) AS total_patients,
    ROUND(AVG(systolic_bp), 2) AS avg_systolic_bp,
    ROUND(AVG(diastolic_bp), 2) AS avg_diastolic_bp,
    ROUND(AVG(cholesterol_mg_dl), 2) AS avg_cholesterol,
    ROUND(AVG(heart_rate), 2) AS avg_heart_rate,
    ROUND(AVG(risk_score), 3) AS avg_risk_score,
    SUM(readmission_flag) AS high_risk_patients
FROM age_grouped
GROUP BY age_group;

select * from healthcare.default.hospital_analytics;