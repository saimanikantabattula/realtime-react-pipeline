-- This VIEW is the bridge to Project 1. Instead of a one-time BigQuery
-- snapshot, "user_activity" now derives live from the continuously
-- growing raw_github_events table -- so Project 1's exact growth
-- accounting logic can run against real-time data.

CREATE OR REPLACE VIEW user_activity AS
SELECT DISTINCT
    actor_login AS user_id,
    DATE_TRUNC('month', event_created_at)::date AS activity_month
FROM raw_github_events
WHERE actor_login NOT ILIKE '%[bot]%'
  AND event_type IN ('PushEvent','PullRequestEvent','IssuesEvent',
                      'IssueCommentEvent','PullRequestReviewEvent');
                      