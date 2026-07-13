import os
import time
import json
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
TARGET_REPO = os.getenv("TARGET_REPO", "facebook/react")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = "react-events"

API_URL = f"https://api.github.com/repos/{TARGET_REPO}/events"

HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

seen_event_ids = set()
etag = None


def delivery_report(err, msg):
    if err is not None:
        print(f"  delivery failed: {err}")


def poll_once():
    global etag
    headers = dict(HEADERS)
    if etag:
        headers["If-None-Match"] = etag

    resp = requests.get(API_URL, headers=headers, timeout=10)

    if resp.status_code == 304:
        print("No new events (304 Not Modified)")
        return

    if resp.status_code != 200:
        print(f"Unexpected status {resp.status_code}: {resp.text[:200]}")
        return

    etag = resp.headers.get("ETag")
    events = resp.json()

    new_count = 0
    for event in events:
        event_id = event["id"]
        if event_id in seen_event_ids:
            continue
        seen_event_ids.add(event_id)
        new_count += 1

        message = {
            "event_id": event_id,
            "event_type": event["type"],
            "actor_login": event["actor"]["login"],
            "repo_name": event["repo"]["name"],
            "event_created_at": event["created_at"],
        }
        producer.produce(TOPIC, value=json.dumps(message), callback=delivery_report)

    producer.flush()

    remaining = resp.headers.get("X-RateLimit-Remaining")
    print(f"Poll complete: {new_count} new events published. Rate limit remaining: {remaining}")

    if len(seen_event_ids) > 5000:
        seen_event_ids.clear()


def main():
    print(f"Starting producer for {TARGET_REPO}, polling every {POLL_INTERVAL}s...")
    print(f"Publishing to Kafka topic '{TOPIC}' at {KAFKA_BROKER}")
    while True:
        try:
            poll_once()
        except Exception as e:
            print(f"Error during poll: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
