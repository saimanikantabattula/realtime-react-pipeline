-- Raw GitHub events, written continuously by the Spark streaming job.
-- One row per real event pulled from GitHub's public Events API.

CREATE TABLE IF NOT EXISTS raw_github_events (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    actor_login     TEXT NOT NULL,
    repo_name       TEXT NOT NULL,
    event_created_at TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_created ON raw_github_events (event_created_at);
CREATE INDEX IF NOT EXISTS idx_raw_events_actor ON raw_github_events (actor_login);

-- Real-time rolling stats, written by Spark's windowed aggregation.
CREATE TABLE IF NOT EXISTS realtime_aggregates (
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    total_events        INTEGER NOT NULL,
    unique_active_users  INTEGER NOT NULL,
    computed_at         TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end)
);