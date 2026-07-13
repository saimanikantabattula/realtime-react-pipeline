import os
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5433"
POSTGRES_DB = os.getenv("POSTGRES_DB", "react_pipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")

EXPORTS_DIR = "exports"


def export_query(cur, query, filename):
    cur.execute(query)
    colnames = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)
    print("Wrote " + str(len(rows)) + " rows to " + path)


def main():
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cur = conn.cursor()
    export_query(cur, "SELECT * FROM monthly_growth_summary ORDER BY activity_month", "monthly_growth_summary.csv")
    export_query(cur, "SELECT * FROM realtime_aggregates ORDER BY window_start", "realtime_aggregates.csv")
    cur.close()
    conn.close()
    print("Export complete.")


if __name__ == "__main__":
    main()
