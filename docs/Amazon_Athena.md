# Amazon Athena for Ad-Hoc Querying

## What This Document Covers

This document explains how **Amazon Athena** lets analysts query the Gold data directly in S3 with no
database server, why it depends on the **Glue Data Catalog** to know the schema, how **partitioning**
reduces query cost, and gives **sample queries** against this pipeline's KPI datasets. It is written
for a data engineer new to Athena. It connects to
[Schema_Management_and_Glue_Catalog.md](Schema_Management_and_Glue_Catalog.md) and
[Partitioning_Strategy_S3.md](Partitioning_Strategy_S3.md).

> **Scope note:** Athena is not provisioned as a Terraform resource in this project — it needs none.
> It is the *intended ad-hoc query layer*, enabled implicitly: the **curated crawler** registers the
> `gold/` datasets in the Glue Catalog specifically "for Athena" ([main.tf:326](../terraform/main.tf#L326)),
> and the Step Functions pipeline refreshes those partitions after each run
> ([step_functions.tf:384](../terraform/step_functions.tf#L384)). This doc explains how to use it.

---

## 1. What Athena Is and Why It's Useful Here

The DynamoDB tables serve the *known, fixed* questions the dashboard asks (top 3 songs for a genre
today, etc. — see [DynamoDB_Data_Modeling.md](DynamoDB_Data_Modeling.md)). But analysts also have
*ad-hoc* questions nobody designed a table for: "which genre grew fastest over the last week?", "what's
the average listen time across all genres this month?", "show me every day Afrobeats was #1."

**Amazon Athena** is a **serverless, interactive query service** that runs standard SQL **directly
over files in S3**. You point it at your S3 data, write SQL, and get results — with:

- **No database server to provision or manage.** Athena is serverless; there's nothing running between
  queries.
- **No data loading.** Athena queries the Parquet files *in place* in the curated bucket. You don't
  copy data into a warehouse first.
- **Pay-per-query.** You're billed only for the data each query scans (see Section 4) — nothing when
  idle.

This makes Athena the perfect complement to DynamoDB: DynamoDB serves the fast, fixed lookups;
Athena answers the open-ended analytical questions over the same Gold data.

---

## 2. Why Athena Needs the Glue Data Catalog

Here is the crucial dependency. Athena reads raw Parquet files in S3 — but a pile of `.parquet` files
is meaningless without knowing **what columns they contain, what types those columns are, and where
the files live**. Athena does not figure this out itself; it asks the **Glue Data Catalog**.

The Catalog is Athena's **schema source** (see
[Schema_Management_and_Glue_Catalog.md](Schema_Management_and_Glue_Catalog.md)). The relationship:

```
 Analyst writes SQL  →  Athena  →  asks Glue Data Catalog "what is the genre_kpis table?"
                                      ↳ Catalog returns: columns, types, S3 location, partitions
                          Athena  →  reads the matching Parquet files in S3  →  returns results
```

In this pipeline that wiring is automatic:

1. The **KPI job** writes Gold Parquet to `gold/genre_kpis`, `gold/top_songs`, `gold/top_genres`.
2. The **curated crawler** scans `gold/`, infers each dataset's schema, and registers it as a table in
   the `music_streaming_db` catalog database — including the date partitions.
3. **Athena** then sees `genre_kpis`, `top_songs`, `top_genres` as ordinary SQL tables it can query,
   because the Catalog told it the schema and location.

Without the crawler keeping the Catalog current, Athena would not know the Gold tables exist or what
shape they have. That's why the pipeline runs the curated crawler after every successful load (and why
it's non-fatal — if it's skipped, Athena just misses the newest partition until the next run).

---

## 3. The Tables Athena Sees

After the curated crawler runs, these are the queryable tables (matching the Gold datasets and the
DynamoDB tables — see [KPI_Design_and_Computation.md](KPI_Design_and_Computation.md)):

| Athena table | Columns | Partition column |
|---|---|---|
| `genre_kpis` | `genre_date`, `track_genre`, `listen_count`, `unique_listeners`, `total_listen_time_ms`, `avg_listen_time_ms_per_user` | `stream_date` |
| `top_songs` | `genre_date`, `track_genre`, `track_id`, `track_name`, `play_count`, `rank` | `stream_date` |
| `top_genres` | `track_genre`, `listen_count`, `rank` | `date` |

The partition column appears as a normal column you can filter on in SQL — that's what unlocks cheap,
fast queries (next section).

---

## 4. How Partitioning Reduces Query Cost

Athena charges **per amount of data scanned**. The single most important lever for cost (and speed) is
**filtering on the partition column**, because Athena then reads only the matching partition folders
and skips the rest — this is **partition pruning** (see
[Partitioning_Strategy_S3.md](Partitioning_Strategy_S3.md)).

Compare two queries:

```sql
-- ❌ Scans EVERY day's files — cost grows with all history
SELECT * FROM genre_kpis WHERE track_genre = 'Afrobeats';

-- ✅ Scans only the 2026-05-17 partition folder — tiny, constant cost
SELECT * FROM genre_kpis
WHERE stream_date = DATE '2026-05-17' AND track_genre = 'Afrobeats';
```

The second query reads only `gold/genre_kpis/stream_date=2026-05-17/`, so it scans a fraction of the
bytes and costs a fraction as much — and stays cheap even as years of data accumulate. **The habit to
teach every analyst: always include a `stream_date` / `date` filter.** Two more cost reducers come for
free here:

- **Parquet is columnar**, so `SELECT track_genre, listen_count` reads only those two columns' bytes,
  not whole rows.
- **Parquet is compressed**, so there are fewer bytes to scan than the equivalent CSV.

---

## 5. Sample Queries Over the KPI Data

These illustrate the ad-hoc questions Athena answers that DynamoDB's fixed key-lookups cannot. All
include a partition filter where possible.

**a) All KPIs for one genre on one day** (the DynamoDB lookup, expressed in SQL):
```sql
SELECT track_genre, listen_count, unique_listeners, avg_listen_time_ms_per_user
FROM genre_kpis
WHERE stream_date = DATE '2026-05-17' AND track_genre = 'Afrobeats';
```

**b) The day's overall most-listened genres** (read the leaderboard):
```sql
SELECT rank, track_genre, listen_count
FROM top_genres
WHERE date = DATE '2026-05-17'
ORDER BY rank;
```

**c) Average listen time across all genres for a day** (an aggregate no DynamoDB table stores):
```sql
SELECT AVG(avg_listen_time_ms_per_user) AS avg_ms_per_user
FROM genre_kpis
WHERE stream_date = DATE '2026-05-17';
```

**d) Which genre had the most unique listeners over a week** (cross-day analysis — Athena's sweet spot):
```sql
SELECT track_genre, SUM(unique_listeners) AS weekly_unique_listeners
FROM genre_kpis
WHERE stream_date BETWEEN DATE '2026-05-11' AND DATE '2026-05-17'
GROUP BY track_genre
ORDER BY weekly_unique_listeners DESC
LIMIT 10;
```

**e) How often each genre reached the daily top 5 in a month** (trend analysis):
```sql
SELECT track_genre, COUNT(*) AS days_in_top5
FROM top_genres
WHERE date BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY track_genre
ORDER BY days_in_top5 DESC;
```

**f) The most-played song in a genre over time** (spanning partitions deliberately):
```sql
SELECT track_name, SUM(play_count) AS total_plays
FROM top_songs
WHERE stream_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
  AND track_genre = 'Afrobeats'
GROUP BY track_name
ORDER BY total_plays DESC
LIMIT 10;
```

---

## 6. Athena vs DynamoDB — Two Complementary Read Paths

| | **DynamoDB** | **Athena** |
|---|---|---|
| Question type | Fixed, known key-lookups | Ad-hoc, exploratory, analytical |
| Speed | Single-digit ms | Seconds (scans files) |
| Query language | Key-based `GetItem`/`Query` | Full SQL (joins, aggregates, ranges) |
| Cross-day / cross-genre analysis | Awkward (designed per-pattern) | Natural (`GROUP BY`, `BETWEEN`) |
| Cost model | Per request | Per byte scanned (partition pruning helps) |
| Powers | The live dashboard | Analyst exploration / reporting |

They read the *same* Gold data from different angles: DynamoDB is the fast serving layer for the app;
Athena is the flexible analytical layer for humans asking new questions.

---

## 7. Summary

| Aspect | This pipeline |
|---|---|
| **What Athena is** | Serverless SQL directly over S3 Parquet — no server, no data loading, pay-per-scan |
| **Why no Terraform resource** | Athena needs none; it's enabled via the Glue Catalog the curated crawler populates |
| **Why it needs the Catalog** | The Catalog supplies the schema, types, location, and partitions Athena requires to read the files |
| **How partitioning helps** | Filtering on `stream_date`/`date` prunes to one folder → scans far fewer bytes → cheaper & faster |
| **What it queries** | `genre_kpis`, `top_songs`, `top_genres` in `music_streaming_db` |
| **Role** | The ad-hoc/analytical read path complementing DynamoDB's fixed-lookup serving path |

Athena turns the Gold S3 layer into a queryable analytics database without any server: the Glue
Catalog (kept current by the curated crawler) tells it the schema, partitioning keeps queries cheap,
and standard SQL lets analysts ask any question of the KPI data the fixed DynamoDB tables were never
designed to answer.
