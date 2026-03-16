-- 4-dwh.sql — DWH Layer: Dimensional Model
-- Star schema built from staging tables
-- Creates:
--   dwh.dim_candidates
--   dwh.dim_roles
--   dwh.fact_applications
--   dwh.fact_interviews

-- dim_candidates
CREATE OR REPLACE TABLE dwh.dim_candidates AS
SELECT
    candidate_id,
    full_name,
    source,
    profile_created_date
FROM dwh.stg_candidates;

-- dim_roles — static lookup table
CREATE OR REPLACE TABLE dwh.dim_roles (
    role_level   STRING,
    level_order  INT64
);

INSERT INTO dwh.dim_roles VALUES
    ('Junior',    1),
    ('Senior',    2),
    ('Executive', 3);

-- fact_applications
CREATE OR REPLACE TABLE dwh.fact_applications AS
SELECT
    app_id,
    candidate_id,
    role_level,
    applied_date,
    decision_date,
    expected_salary,
    DATE_DIFF(decision_date, applied_date, DAY) AS days_to_decision,
    CASE
        WHEN decision_date IS NULL THEN 'Active'
        ELSE 'Closed'
    END AS pipeline_status
FROM dwh.stg_applications;

-- fact_interviews
CREATE OR REPLACE TABLE dwh.fact_interviews AS
SELECT
    i.interview_id,
    i.app_id,
    i.interview_date,
    i.outcome,
    a.candidate_id,
    a.applied_date
FROM dwh.stg_interviews i
JOIN dwh.stg_applications a ON i.app_id = a.app_id;