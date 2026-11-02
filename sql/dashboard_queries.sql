-- Dashboard queries for Databricks SQL / AI-BI dashboards
-- Run against workspace.je_testing after the five notebooks have run.
-- Each query below becomes one dataset (and one visual) in the dashboard.

USE CATALOG workspace;
USE SCHEMA je_testing;

-- 1. Headline numbers (counter tiles)
SELECT
  COUNT(*)                                              AS total_entries,
  SUM(CASE WHEN risk_score > 0 THEN 1 ELSE 0 END)       AS flagged_entries,
  SUM(CASE WHEN risk_level = 'High' THEN 1 ELSE 0 END)  AS high_risk_entries,
  ROUND(SUM(CASE WHEN risk_score > 0 THEN total_amount ELSE 0 END), 2) AS flagged_amount_inr
FROM gold_je_test_results;

-- 2. Entries flagged by each test (bar chart)
SELECT test, flagged_entries, flagged_amount, pct_of_entries
FROM gold_test_summary
ORDER BY flagged_entries DESC;

-- 3. Flagged entries by month (bar chart, colour by risk_level)
SELECT date_trunc('month', posting_date) AS month, risk_level, COUNT(*) AS entries
FROM gold_je_test_results
WHERE risk_score > 0
GROUP BY ALL
ORDER BY month;

-- 4. Who posted the flagged entries (table)
SELECT created_by,
       COUNT(*)                          AS flagged_entries,
       ROUND(SUM(total_amount), 2)       AS flagged_amount,
       MAX(risk_score)                   AS highest_risk_score
FROM gold_je_test_results
WHERE risk_score > 0
GROUP BY created_by
ORDER BY flagged_entries DESC;

-- 5. Review list: highest-risk entries first (table)
SELECT je_id, posting_date, created_by, approved_by, description, total_amount,
       risk_score, risk_level, array_join(flags, ', ') AS reasons
FROM gold_je_exceptions
ORDER BY risk_score DESC, total_amount DESC
LIMIT 50;

-- 6. Benford's law: observed vs expected first digit (grouped bar chart)
SELECT digit, observed_pct, expected_pct
FROM gold_benford
ORDER BY digit;

-- 7. Risk by business process (table)
SELECT process, entries, flagged_entries, high_risk_entries,
       ROUND(flagged_entries / entries * 100, 2) AS pct_flagged
FROM gold_process_summary
ORDER BY pct_flagged DESC;

-- 8. Data quality report from the silver layer (table)
SELECT * FROM silver_dq_report;
