-- 3-staging.sql — Staging Layer
-- Cleans and deduplicates raw data.
-- Creates:
--   dwh.stg_candidates   — removes NULL id/name rows
--   dwh.stg_applications — removes invalid role levels and NULL ids
--   dwh.stg_interviews   — removes invalid outcomes + deduplicates
--   dwh.dq_alerts        — logs logical errors found in raw data

-- stg_candidates
CREATE OR REPLACE TABLE dwh.stg_candidates AS
SELECT
    candidate_id,
    TRIM(full_name)   AS full_name,
    TRIM(source)      AS source,
    profile_created_date
FROM dwh.raw_candidates
WHERE candidate_id IS NOT NULL
  AND full_name IS NOT NULL;

-- stg_applications
CREATE OR REPLACE TABLE dwh.stg_applications AS
SELECT
    app_id,
    candidate_id,
    role_level,
    applied_date,
    decision_date,
    expected_salary
FROM dwh.raw_applications
WHERE app_id IS NOT NULL
  AND candidate_id IS NOT NULL
  AND role_level IN ('Junior', 'Senior', 'Executive');

-- stg_interviews
-- Filters: outcome must be valid (removes 'Cancelled', 'Pending' etc.)
-- Dedup: ROW_NUMBER() keeps earliest row per (app_id, interview_date, outcome)
CREATE OR REPLACE TABLE dwh.stg_interviews AS
SELECT
    interview_id,
    app_id,
    interview_date,
    outcome
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY app_id, interview_date, outcome
            ORDER BY interview_id
        ) AS rn
    FROM dwh.raw_interviews
    WHERE outcome IN ('Passed', 'Rejected', 'No Show')
)
WHERE rn = 1;

-- dq_alerts — data quality issue log
-- captures logical errors found in raw data
CREATE OR REPLACE TABLE dwh.dq_alerts (
    alert_id      INT64,
    check_name    STRING,
    source_table  STRING,
    record_id     INT64,
    description   STRING
);

-- DQ Check 1: interview_date before applied_date
INSERT INTO dwh.dq_alerts
SELECT
    ROW_NUMBER() OVER ()              AS alert_id,
    'interview_before_application'    AS check_name,
    'raw_interviews'                  AS source_table,
    i.interview_id                    AS record_id,
    CONCAT(
        'interview_date (', CAST(i.interview_date AS STRING),
        ') is before applied_date (', CAST(a.applied_date AS STRING), ')'
    )                                 AS description
FROM dwh.raw_interviews i
JOIN dwh.raw_applications a ON i.app_id = a.app_id
WHERE i.interview_date < a.applied_date;

-- DQ Check 2: decision_date before applied_date
INSERT INTO dwh.dq_alerts
SELECT
    (SELECT IFNULL(MAX(alert_id), 0) FROM dwh.dq_alerts) + ROW_NUMBER() OVER () AS alert_id,
    'decision_before_application'     AS check_name,
    'raw_applications'                AS source_table,
    app_id                            AS record_id,
    CONCAT(
        'decision_date (', CAST(decision_date AS STRING),
        ') is before applied_date (', CAST(applied_date AS STRING), ')'
    )                                 AS description
FROM dwh.raw_applications
WHERE decision_date IS NOT NULL
  AND decision_date < applied_date;

-- DQ Check 3: duplicate interviews detected
INSERT INTO dwh.dq_alerts
SELECT
    (SELECT IFNULL(MAX(alert_id), 0) FROM dwh.dq_alerts) + ROW_NUMBER() OVER () AS alert_id,
    'duplicate_interview'             AS check_name,
    'raw_interviews'                  AS source_table,
    interview_id                      AS record_id,
    CONCAT(
        'Duplicate slot (app_id=', CAST(app_id AS STRING),
        ', date=', CAST(interview_date AS STRING),
        ', outcome=', outcome, ')'
    )                                 AS description
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY app_id, interview_date, outcome
            ORDER BY interview_id
        ) AS rn
    FROM dwh.raw_interviews
    WHERE outcome IN ('Passed', 'Rejected', 'No Show')
)
WHERE rn > 1;