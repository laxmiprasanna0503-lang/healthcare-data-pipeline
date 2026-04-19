-- -- Databricks notebook source
-- CREATE OR REPLACE TABLE healthcare.default.patient_risk_scores AS

-- WITH base AS (
--     SELECT *
--     FROM healthcare.default.patient_clean_data
-- ),

-- -- Step 1: Get min & max for normalization
-- stats AS (
--     SELECT
--         MIN(systolic_bp) AS min_sys,
--         MAX(systolic_bp) AS max_sys,

--         MIN(diastolic_bp) AS min_dia,
--         MAX(diastolic_bp) AS max_dia,

--         MIN(cholesterol_mg_dl) AS min_chol,
--         MAX(cholesterol_mg_dl) AS max_chol,

--         MIN(heart_rate) AS min_hr,
--         MAX(heart_rate) AS max_hr
--     FROM base
-- ),

-- -- Step 2: Normalize values
-- normalized AS (
--     SELECT
--         b.*,

--         -- Avoid division by zero
--         (b.systolic_bp - s.min_sys) / NULLIF(s.max_sys - s.min_sys, 0) AS norm_sys_bp,
--         (b.diastolic_bp - s.min_dia) / NULLIF(s.max_dia - s.min_dia, 0) AS norm_dia_bp,
--         (b.cholesterol_mg_dl - s.min_chol) / NULLIF(s.max_chol - s.min_chol, 0) AS norm_chol,
--         (b.heart_rate - s.min_hr) / NULLIF(s.max_hr - s.min_hr, 0) AS norm_hr

--     FROM base b
--     CROSS JOIN stats s
-- ),

-- -- Step 3: Risk score using normalized values
-- risk_calc AS (
--     SELECT
--         *,

--         ROUND(
--             0.25 * norm_sys_bp +
--             0.15 * norm_dia_bp +
--             0.30 * norm_chol +
--             0.30 * norm_hr
--         , 2) AS risk_score

--     FROM normalized
-- ),

-- -- Step 4: Categorization
-- final AS (
--     SELECT
--         *,

--         CASE
--             WHEN risk_score >= 0.7 THEN 'High Risk'
--             WHEN risk_score >= 0.4 THEN 'Medium Risk'
--             ELSE 'Low Risk'
--         END AS risk_category,

--         CASE
--             WHEN risk_score >= 0.7 THEN 1
--             ELSE 0
--         END AS readmission_flag

--     FROM risk_calc
-- )

-- SELECT
--     patient_id,
--     age,
--     gender,
--     systolic_bp,
--     diastolic_bp,
--     cholesterol_mg_dl,
--     heart_rate,
--     diagnosis,

--     -- normalized values (important for explainability)
--     ROUND(norm_sys_bp, 2) AS norm_systolic_bp,
--     ROUND(norm_dia_bp, 2) AS norm_diastolic_bp,
--     ROUND(norm_chol, 2) AS norm_cholesterol,
--     ROUND(norm_hr, 2) AS norm_heart_rate,

--     risk_score,
--     risk_category,
--     readmission_flag,

--     CURRENT_TIMESTAMP() AS job_run_timestamp

-- FROM final;

-- -- COMMAND ----------

-- select * from healthcare.default.patient_risk_scores;

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

SELECT *
FROM healthcare.default.hospital_analytics;