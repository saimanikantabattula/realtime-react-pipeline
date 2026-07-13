-- Growth Accounting Project
-- 04_monthly_summary.sql
--
-- Pivots growth_classification into one row per month with counts for
-- each status, plus the Quick Ratio: (New + Resurrected) / Churned.
-- Quick Ratio > 1 means you're adding people faster than you're losing
-- them; < 1 means you're shrinking even if raw signups look fine.

DROP VIEW IF EXISTS monthly_growth_summary;

CREATE VIEW monthly_growth_summary AS
SELECT
    activity_month,
    SUM(CASE WHEN status = 'new'         THEN 1 ELSE 0 END) AS new_users,
    SUM(CASE WHEN status = 'retained'    THEN 1 ELSE 0 END) AS retained_users,
    SUM(CASE WHEN status = 'resurrected' THEN 1 ELSE 0 END) AS resurrected_users,
    SUM(CASE WHEN status = 'churned'     THEN 1 ELSE 0 END) AS churned_users,
    SUM(CASE WHEN status IN ('new','retained','resurrected') THEN 1 ELSE 0 END) AS active_users,
    (
        SUM(CASE WHEN status IN ('new','resurrected') THEN 1 ELSE 0 END)::numeric
        / NULLIF(SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END), 0)
    ) AS quick_ratio
FROM growth_classification
WHERE status IS NOT NULL
GROUP BY activity_month
ORDER BY activity_month;