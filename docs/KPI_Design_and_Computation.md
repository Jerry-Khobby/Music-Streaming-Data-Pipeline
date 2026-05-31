# KPI Design and Computation — From Raw Streams to Business Metrics

## What This Document Covers

This document explains every KPI (Key Performance Indicator) this pipeline produces: what each one
**means from a business perspective**, and exactly **how it is computed** from raw stream events
using PySpark. It covers the five metrics — listen count, unique listeners, total/average listen
time, top 3 songs per genre, and top 5 genres per day. It is written so a data engineer new to
Spark can follow both the *why* and the *how*. Everything maps to
[glue_jobs/kpi_aggregation_job.py](../glue_jobs/kpi_aggregation_job.py).

---

## 1. What a KPI Is, and Where These Come From

A **KPI** is a single number that answers a business question about how the product is performing —
"how much are people listening?", "which genres are winning?", "what are the hit songs?". A raw
stream event ("user U played track T at time X") answers none of these on its own. KPIs are what you
get when you **aggregate** millions of those raw events into meaningful summaries.

In this pipeline, KPIs are computed at the **Gold stage** (see
[Medallion_Architecture.md](Medallion_Architecture.md)). The input is the **Silver** dataset —
`silver/enriched_streams` — where each row is one deduplicated stream event already joined to its
song, so it carries: `user_id`, `track_id`, `listen_time`, `track_name`, `track_genre`,
`duration_ms`, and `stream_date`. The KPI job reads that and writes three Gold datasets that become
the three DynamoDB tables.

The grain of almost every metric here is **per genre, per day** — i.e. the data is grouped by
`(stream_date, track_genre)`. That grain was chosen because the downstream application asks all its
questions at the genre-and-day level (see [DynamoDB_Data_Modeling.md](DynamoDB_Data_Modeling.md)).

---

## 2. A Two-Minute PySpark Primer (for the formulas below)

The computations use a handful of Spark operations. Knowing these four is enough to read every
formula in this document:

- **`groupBy(cols).agg(...)`** — collapse all rows sharing the same values of `cols` into one row,
  computing an aggregate (count, sum, etc.) over each group. This is SQL's `GROUP BY`.
- **`F.count("*")`** — count how many rows are in each group.
- **`F.countDistinct("user_id")`** — count how many *distinct* values of `user_id` are in each group
  (duplicates collapsed).
- **`F.sum("duration_ms")`** — add up a numeric column across the group.
- **Window + `row_number()`** — instead of collapsing groups, *rank* rows within a group. You define
  a "window" partitioned by some columns and ordered by others, and `row_number()` numbers the rows
  1, 2, 3 … in that order. This is how "top N" is computed.

---

## 3. The Genre KPIs — `genre_kpis`

These four metrics are computed per genre per day and assembled into one row each by
`assembleGenreKpis` ([kpi_aggregation_job.py:50](../glue_jobs/kpi_aggregation_job.py#L50)), which
computes three sub-results and joins them. They land in the `genre_kpis` DynamoDB table.

### 3a. Listen Count — engagement volume

**Business meaning:** *How many times was anything in this genre played today?* It is the raw volume
of listening activity — the headline "how busy were we" number for a genre.

**Computation** ([kpi_aggregation_job.py:23](../glue_jobs/kpi_aggregation_job.py#L23)):

```python
enrichedDF
  .groupBy("stream_date", "track_genre")
  .agg(F.count("*").alias("listen_count"))
```

Every row in `enrichedDF` is one play event, so counting rows per `(date, genre)` is exactly the
number of plays in that genre that day.

### 3b. Unique Listeners — reach

**Business meaning:** *How many different people listened to this genre today?* This is **reach**, and
it is very different from listen count. One superfan playing 500 songs is `listen_count = 500` but
`unique_listeners = 1`. Together the two tell you whether engagement is broad (many people) or deep
(few people, lots of plays).

**Computation** ([kpi_aggregation_job.py:31](../glue_jobs/kpi_aggregation_job.py#L31)):

```python
enrichedDF
  .groupBy("stream_date", "track_genre")
  .agg(F.countDistinct("user_id").alias("unique_listeners"))
```

`countDistinct("user_id")` collapses repeated plays by the same user, so each listener is counted
once per genre per day.

### 3c. Total Listen Time — total consumption

**Business meaning:** *How much listening time did this genre generate today?* Where listen count
counts *plays*, total listen time measures *duration* — a genre of long tracks can have fewer plays
but more total time.

**Computation** ([kpi_aggregation_job.py:39](../glue_jobs/kpi_aggregation_job.py#L39)):

```python
.agg(F.sum("duration_ms").alias("total_listen_time_ms"), ...)
```

It sums `duration_ms` across all play events in the group.

> **An honest modeling note:** `duration_ms` is the **song's full length** (it comes from the songs
> catalogue), not a measured play-through time. So `total_listen_time_ms` is the sum of full track
> lengths over all plays — i.e. it assumes each play equals one full listen. This is a reasonable,
> common simplification given the raw data available; it is worth knowing the metric is "total track
> length played," not "verified seconds listened."

### 3d. Average Listen Time per User — engagement depth

**Business meaning:** *On average, how much listening time did each unique listener of this genre
rack up today?* It normalizes total time by reach, telling you how *intensely* the typical fan of a
genre engaged with it.

**Computation** ([kpi_aggregation_job.py:45](../glue_jobs/kpi_aggregation_job.py#L45)):

```python
(F.sum("duration_ms") / F.countDistinct("user_id")).alias("avg_listen_time_ms_per_user")
```

It is `total_listen_time_ms ÷ unique_listeners` — total consumption divided by the number of distinct
people, computed in the same aggregation pass.

### 3e. Assembling the row

`assembleGenreKpis` joins the three sub-results on `(stream_date, track_genre)` and adds the
composite key used by DynamoDB ([kpi_aggregation_job.py:59](../glue_jobs/kpi_aggregation_job.py#L59)):

```python
.withColumn("genre_date", F.concat_ws("#", F.col("track_genre"), F.col("stream_date").cast("string")))
# → "Afrobeats#2026-05-17"
```

Each final row therefore has: `genre_date`, `stream_date`, `track_genre`, `listen_count`,
`unique_listeners`, `total_listen_time_ms`, `avg_listen_time_ms_per_user`.

---

## 4. Top 3 Songs per Genre per Day — `top_songs`

**Business meaning:** *What were the hit songs in each genre today?* This is the per-genre
leaderboard — the songs to feature, promote, or surface in a "trending in Afrobeats" carousel.

**Computation** ([kpi_aggregation_job.py:66](../glue_jobs/kpi_aggregation_job.py#L66)):

```python
rankWindow = Window.partitionBy("stream_date", "track_genre").orderBy(F.desc("play_count"), "track_id")

enrichedDF
  .groupBy("stream_date", "track_genre", "track_id", "track_name")
  .agg(F.count("*").alias("play_count"))          # 1. count plays per song
  .withColumn("rank", F.row_number().over(rankWindow))  # 2. rank songs within each genre-day
  .filter(F.col("rank") <= TOP_SONGS_RANK)         # 3. keep only the top 3
  .withColumn("genre_date", F.concat_ws("#", ...)) # 4. build the DynamoDB key
```

Step by step:

1. **Count plays per song.** Group by genre-day *and the song* so each song gets its own
   `play_count`.
2. **Rank within the genre-day.** The window is partitioned by `(stream_date, track_genre)` and
   ordered by `desc(play_count)` — so the most-played song is rank 1. `track_id` is the tiebreaker.
3. **Keep the top 3.** `TOP_SONGS_RANK = 3` ([kpi_aggregation_job.py:13](../glue_jobs/kpi_aggregation_job.py#L13))
   is a named constant, not a magic number.

**Why `row_number()` and not `rank()`:** if two songs tie on play count, `rank()` would assign both
the same rank (1, 1, 3 …), which could yield *four* "top 3" rows and break the assumption that there
is exactly one song per rank. `row_number()` always produces strictly increasing, unique ranks
(1, 2, 3), with `track_id` as a deterministic tiebreaker — so the leaderboard is always exactly three
distinct entries. The code comment states this intent explicitly.

---

## 5. Top 5 Genres per Day — `top_genres`

**Business meaning:** *Which genres dominated the platform today?* This is the global daily
leaderboard — the "what's hot right now" view across all genres, not scoped to one.

**Computation** ([kpi_aggregation_job.py:82](../glue_jobs/kpi_aggregation_job.py#L82)):

```python
rankWindow = Window.partitionBy("stream_date").orderBy(F.desc("listen_count"), "track_genre")

genreKpisDF
  .withColumn("rank", F.row_number().over(rankWindow))  # rank genres within each day
  .filter(F.col("rank") <= TOP_GENRES_RANK)             # keep top 5
  .select("stream_date", "track_genre", "listen_count", "rank")
  .withColumnRenamed("stream_date", "date")             # rename for the leaderboard table
```

Key points:

- **It is computed *from* `genreKpisDF`, not the raw events.** The genre KPIs already hold
  `listen_count` per genre per day, so ranking genres is just ordering that existing result — a nice
  example of reusing a derived dataset rather than recomputing from scratch.
- **The window is partitioned by `stream_date` only** (not by genre), because we are ranking *genres
  against each other within a day*. Ordered by `desc(listen_count)`, the busiest genre is rank 1,
  with `track_genre` as the tiebreaker.
- **`TOP_GENRES_RANK = 5`** keeps the top five.
- **`stream_date` is renamed to `date`** because this dataset becomes the `top_genres` table whose
  partition key is `date` — the rename makes the key self-describing (see
  [DynamoDB_Key_Design.md](DynamoDB_Key_Design.md)).

---

## 6. How It All Fits Together

```
 silver/enriched_streams  (one row per deduplicated, song-enriched play event)
        │
        ├── groupBy(date, genre) ─────────────▶ listen_count, unique_listeners,
        │                                        total_listen_time_ms, avg_listen_time_ms_per_user
        │                                              │
        │                                              ▼  gold/genre_kpis  → DynamoDB genre_kpis
        │
        ├── groupBy(date, genre, song) + rank ─▶ top 3 songs per genre
        │                                              ▼  gold/top_songs   → DynamoDB top_songs
        │
        └── (from genre_kpis) rank genres ─────▶ top 5 genres per day
                                                       ▼  gold/top_genres  → DynamoDB top_genres
```

The Silver dataset is read once (and cached), the three Gold datasets are computed from it, written
as Parquet partitioned by date, and later loaded into DynamoDB by `dynamodb_loader.py` (see
[Glue_Transformation_Code.md](Glue_Transformation_Code.md)).

---

## 7. Summary

| KPI | Business question | PySpark computation | Grain |
|---|---|---|---|
| **listen_count** | How many plays in this genre today? | `count(*)` per `(date, genre)` | genre-day |
| **unique_listeners** | How many different people? (reach) | `countDistinct(user_id)` | genre-day |
| **total_listen_time_ms** | How much total consumption? | `sum(duration_ms)` | genre-day |
| **avg_listen_time_ms_per_user** | How intensely did each fan engage? | `sum(duration_ms) / countDistinct(user_id)` | genre-day |
| **top 3 songs** | What were the hit songs per genre? | `count(*)` per song → `row_number()` window → keep ≤ 3 | genre-day-song |
| **top 5 genres** | Which genres dominated the platform? | rank `genre_kpis` by `listen_count` → keep ≤ 5 | day-genre |

Each KPI turns raw "who played what when" events into a number a product or marketing team can act
on — and each is computed with a small, readable PySpark aggregation, using named constants for the
"top N" thresholds and `row_number()` to guarantee clean, tie-safe leaderboards.
