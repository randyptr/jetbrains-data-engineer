-- 5-mart.sql — Data Mart Layer
-- Business tables for reporting
-- Creates: dwh.dm_hiring_process

-- dm_hiring_process
-- One row per application
-- Includes: candidate name, source, time-to-decision, passed interview count
CREATE OR REPLACE TABLE dwh.dm_hiring_process AS
SELECT
    fa.app_id,
    fa.candidate_id,
    dc.full_name                                        AS candidate_name,
    dc.source                                           AS candidate_source,
    fa.role_level,
    fa.applied_date,
    fa.decision_date,
    fa.pipeline_status,
    fa.days_to_decision,
    fa.expected_salary,
    COUNTIF(fi.outcome = 'Passed')                      AS passed_interview_count
FROM dwh.fact_applications fa
JOIN dwh.dim_candidates dc
    ON fa.candidate_id = dc.candidate_id
LEFT JOIN dwh.fact_interviews fi
    ON fa.app_id = fi.app_id
GROUP BY
    fa.app_id,
    fa.candidate_id,
    dc.full_name,
    dc.source,
    fa.role_level,
    fa.applied_date,
    fa.decision_date,
    fa.pipeline_status,
    fa.days_to_decision,
    fa.expected_salary;