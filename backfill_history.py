import os
import csv
import hashlib
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5433"
POSTGRES_DB = os.getenv("POSTGRES_DB", "react_pipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")

CSV_PATH = "data/gh_archive_export.csv"
HISTORICAL_REPO_NAME = "facebook/react"


def make_event_id(user_id, activity_month):
    raw = "backfill-" + user_id + "-" + activity_month
    return "bf_" + hashlib.sha1(raw.encode()).hexdigest()[:16]


def main():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor()

    rows = []
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row["user_id"]
            activity_month = row["activity_month"]
            event_created_at = datetime.strptime(activity_month, "%Y-%m-%d")
            rows.append((
                make_event_id(user_id, activity_month),
                "PushEvent",
                user_id,
                HISTORICAL_REPO_NAME,
                event_created_at,
            ))

    print("Prepared " + str(len(rows)) + " historical placeholder events")

    insert_sql = """
        INSERT INTO raw_github_events
            (event_id, event_type, actor_login, repo_name, event_created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """
    cur.executemany(insert_sql, rows)
    conn.commit()

    cur.execute("SELECT count(*) FROM raw_github_events")
    total = cur.fetchone()[0]
    print("Backfill complete. raw_github_events now has " + str(total) + " total rows.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
