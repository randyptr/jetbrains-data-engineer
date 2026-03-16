-- 1-schema.sql — Source Layer: Raw ATS Tables
-- Run this first, creates the raw immutable tables
-- No constraints, raw data can be dirty

CREATE SCHEMA IF NOT EXISTS dwh;

-- Raw candidates table
CREATE TABLE IF NOT EXISTS dwh.raw_candidates (
    candidate_id          INT64,
    full_name             STRING,
    source                STRING,   -- 'LinkedIn', 'Referral', 'Career Page', 'Job Board'
    profile_created_date  DATE
);

-- Raw applications table
CREATE TABLE IF NOT EXISTS dwh.raw_applications (
    app_id           INT64,
    candidate_id     INT64,
    role_level       STRING,        -- 'Junior', 'Senior', 'Executive'
    applied_date     DATE,
    decision_date    DATE,          -- NULL if process is still active
    expected_salary  NUMERIC
);

-- Raw interviews table
CREATE TABLE IF NOT EXISTS dwh.raw_interviews (
    interview_id    INT64,
    app_id          INT64,
    interview_date  DATE,
    outcome         STRING         -- 'Passed', 'Rejected', 'No Show'
);