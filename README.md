# Real-Time Contributor Growth Pipeline — facebook/react

## Why I built this

This is the follow-up to my first project (growth accounting for React
contributors). That one used a one-time data pull from BigQuery — a
snapshot, frozen the day I ran the query. The obvious next question was:
what if the pipeline just kept running, and the growth numbers stayed
current on their own instead of going stale the day after I built them?

So this project rebuilds the same growth accounting logic, but instead
of a single BigQuery pull, it's fed by a real, live, always-on pipeline:
GitHub events flow in continuously through Kafka, get processed by
Spark, land in Postgres, and the exact same SQL views from Project 1
just keep recalculating on top of it.

It's also aimed at a different job title than Project 1. Project 1 was
built for data analyst roles. This one leans into **Analytics
Engineer** — a role that's part data engineer (build the pipeline) and
part analyst (make sure the numbers mean something), which is honestly
a better fit for someone with a data engineering background who wants
to move into analytics.

## What's actually running

```
[Producer] --> [Kafka] --> [Spark Structured Streaming] --> [Postgres]
                                                                 |
                                                    (same SQL views from
                                                     Project 1, now fed
                                                     by live data)
                                                                 |
                                                          [Tableau Dashboard]
```

- **Producer** (`producer/github_event_producer.py`): polls GitHub's
  real public Events API for `facebook/react` every 30 seconds, pushes
  new events into a Kafka topic.
- **Kafka + Zookeeper**: the message queue sitting between the producer
  and Spark, so they don't need to talk to each other directly.
- **Spark Structured Streaming** (`spark_streaming/stream_processor.py`):
  reads from Kafka, writes every event into Postgres, and separately
  computes a rolling 10-minute window of activity (event count, unique
  contributors) for a "right now" view.
- **Postgres**: stores the raw events, the rolling windows, and (reused
  directly from Project 1) the `user_activity` view, the
  `growth_classification` view, and the `monthly_growth_summary` view.
- **Docker Compose**: runs Kafka, Zookeeper, and Postgres as one stack
  with `docker compose up`.

## An interesting real-world wrinkle: the repo got renamed mid-project

While building this, the data started showing `repo_name: react/react`
instead of `facebook/react` — which looked like a bug at first. It
wasn't. In June 2026, Meta transferred the React repository from the
`facebook` GitHub org to its own dedicated `react` org. Old
`facebook/react` links still redirect there, which is why the pipeline
(still pointed at the old API path) correctly receives events, but
GitHub reports the current canonical name in the data. I kept
historical rows labeled `facebook/react` (accurate for Jan 2024-Dec
2025, before the rename) and live rows labeled `react/react` (accurate
for anything pulled after June 2026) rather than picking one name and
pretending it was always true.

## Connecting to Project 1's history

The live pipeline only started collecting data the day I built it — so
on its own, the growth numbers would look empty for months. Instead, I
backfilled `raw_github_events` with Project 1's ~2 years of real
historical data (`backfill_history.py`), turning each historical
`(user, month)` row into one placeholder event dated to that month.
That's a deliberate simplification — the growth accounting logic only
ever needed `(user, month)` pairs, never individual event details — and
I'd rather say so plainly than pretend the backfilled rows are
identical to the live ones.

The result: one continuous timeline, January 2024 through today, that
keeps extending itself live from here on.

## Bots, and one honest data-quality note

Like Project 1, bot accounts (`github-actions[bot]`, `dependabot[bot]`,
etc.) are filtered out of the growth accounting view, since they don't
behave like real contributors.

One thing I noticed and chose not to hide: two events that showed up in
a single day's live poll had timestamps from months earlier (one from
April 2026, one from early July) — both `github-actions[bot]` actions.
GitHub's Events API timestamps reflect when the underlying action
actually happened, not when it surfaces in the feed, so an old
automated action can genuinely show up "late." It briefly stretched my
real-time chart's timeline out to three months before I noticed and
filtered the dashboard down to the last day. The raw data in Postgres
is untouched — the filter only applies to that one dashboard view.

## The dashboard

Tableau Public can't connect live to a database (it only accepts file
uploads), so I export the two key views to CSV
(`export_for_tableau.py`) and rebuild the dashboard from those. Not
truly live, but real, and refreshable any time by rerunning the export
and clicking Refresh in Tableau.

Three views, one dashboard:
- **Growth Composition** — the same New/Retained/Resurrected vs.
  Churned diverging bar chart from Project 1, now covering Jan
  2024-today in one continuous line instead of a frozen snapshot.
- **Quick Ratio Trend** — Quick Ratio per month with a break-even
  reference line at 1.0.
- **Live Activity** — a rolling view of the last day's real event
  volume and unique active contributors, straight from the Spark
  aggregation.

## What I actually found

The 2024-2025 numbers exactly match Project 1 (as they should — same
underlying data): contributor activity roughly halved over the two
years, driven mainly by fewer new contributors joining, not a spike in
people leaving. The Quick Ratio bounces between roughly 0.6 and 1.7
across the whole period, crossing above and below the 1.0 break-even
line repeatedly rather than trending cleanly in one direction — growth
here isn't a smooth story, it's a genuinely volatile one, and I think
that's worth saying rather than oversimplifying it into a single
trend line.

The live layer, even after just one day running, already shows real
activity clustering — two clear spikes in the day's event volume,
each involving 1-2 unique contributors doing several things in a short
window (a burst of comments or reviews on one issue, most likely). Not
a big enough sample yet to draw a real conclusion from, but it confirms
the pipeline is capturing the kind of bursty, human activity pattern
you'd actually expect from a live open-source project.

## How to run it

```bash
# 1. start the infrastructure
docker compose up -d

# 2. set up Python (once)
python3.10 -m venv venv
source venv/bin/activate
pip install -r producer/requirements.txt
pip install -r spark_streaming/requirements.txt
pip install psycopg2-binary

# 3. run the producer (separate terminal, keep running)
python producer/github_event_producer.py

# 4. run the Spark job (separate terminal, keep running)
python spark_streaming/stream_processor.py

# 5. one-time: backfill Project 1's historical data
python backfill_history.py

# 6. whenever you want fresh CSVs for the dashboard
python export_for_tableau.py
```

## How this connects to my other two projects

- **Project 1** (growth accounting, one-time pull) diagnosed the
  problem: React's contributor growth is fragile and driven more by
  acquisition than retention.
- **Project 2** (uplift model on real marketing data) demonstrates the
  method for deciding who's actually worth targeting, if you wanted to
  act on a finding like that.
- **Project 3** (this one) shows I can take that same diagnosis and
  turn it into an always-on system instead of a one-time report — the
  part of the job that's closer to data/analytics engineering than
  pure analysis.
