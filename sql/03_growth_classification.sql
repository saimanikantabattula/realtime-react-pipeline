-- Growth Accounting Project
-- 03_growth_classification.sql
--
-- Classifies every (user, month) combination into exactly one status:
--   new         - first month this user was ever active
--   retained    - active this month AND active last calendar month
--   resurrected - active this month, inactive last month, but active at
--                 some earlier point (i.e. a comeback)
--   churned     - inactive this month, but was active last calendar month
--                 (this is what "loses" a user in that month)
--
-- The tricky part: users can have gaps (active Jan, silent Feb-Apr, active
-- May again). To detect that correctly we can't just LAG() over each
-- user's own activity rows -- we need a full calendar spine per user so
-- "last calendar month" means the actual previous month, not just their
-- previous active row.

DROP VIEW IF EXISTS growth_classification;

CREATE VIEW growth_classification AS
WITH bounds AS (
    SELECT MIN(activity_month) AS min_month,
           MAX(activity_month) AS max_month
    FROM user_activity
),
month_spine AS (
    SELECT generate_series(
        (SELECT min_month FROM bounds),
        (SELECT max_month FROM bounds),
        interval '1 month'
    )::date AS activity_month
),
users AS (
    SELECT DISTINCT user_id FROM user_activity
),
user_month_grid AS (
    SELECT u.user_id, m.activity_month
    FROM users u
    CROSS JOIN month_spine m
),
grid_flagged AS (
    SELECT
        g.user_id,
        g.activity_month,
        CASE WHEN ua.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_active
    FROM user_month_grid g
    LEFT JOIN user_activity ua
      ON ua.user_id = g.user_id
     AND ua.activity_month = g.activity_month
),
with_history AS (
    SELECT
        *,
        LAG(is_active) OVER (
            PARTITION BY user_id ORDER BY activity_month
        ) AS active_last_month,
        MAX(is_active) OVER (
            PARTITION BY user_id ORDER BY activity_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS active_any_month_before
    FROM grid_flagged
)
SELECT
    user_id,
    activity_month,
    is_active,
    CASE
        WHEN is_active = 1
             AND (active_any_month_before IS NULL OR active_any_month_before = 0)
            THEN 'new'
        WHEN is_active = 1 AND active_last_month = 1
            THEN 'retained'
        WHEN is_active = 1
             AND (active_last_month = 0 OR active_last_month IS NULL)
             AND active_any_month_before = 1
            THEN 'resurrected'
        WHEN is_active = 0 AND active_last_month = 1
            THEN 'churned'
        ELSE NULL
    END AS status
FROM with_history;