-- 6-analytics.sql — Analytical Queries
-- Run each query independently in BigQuery console

-- METRIC 1: Monthly Active Pipeline
-- An application is "active" in a month if that month falls between applied_date and
-- decision_date (or today if still active).
-- Generates one row per month per application
WITH expanded_months AS (
    SELECT
        app_id,
        active_month
    FROM dwh.dm_hiring_process,
    UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_TRUNC(applied_date, MONTH),
            DATE_TRUNC(COALESCE(decision_date, CURRENT_DATE()), MONTH),
            INTERVAL 1 MONTH
        )
    ) AS active_month
)

SELECT
    active_month                    AS report_month,
    COUNT(DISTINCT app_id)          AS active_applications
FROM expanded_months
GROUP BY report_month
ORDER BY report_month;


-- METRIC 2: Cumulative Hires by Source
-- A hire = closed application (decision_date NOT NULL) with at least one Passed interview.
-- Shows cumulative total per source, month by month.
WITH monthly_hires AS (
    SELECT
        candidate_source                            AS source,
        DATE_TRUNC(decision_date, MONTH)            AS hire_month,
        COUNT(*)                                    AS hires_this_month
    FROM dwh.dm_hiring_process
    WHERE decision_date IS NOT NULL
      AND passed_interview_count > 0
    GROUP BY
        candidate_source,
        hire_month
)

SELECT
    source,
    hire_month,
    hires_this_month,
    SUM(hires_this_month) OVER (
        PARTITION BY source
        ORDER BY hire_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                               AS cumulative_hires
FROM monthly_hires
ORDER BY source, hire_month;