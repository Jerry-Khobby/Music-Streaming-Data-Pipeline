# DynamoDB Partition Keys and Sort Keys — Key Design in the Music Streaming Pipeline

## What This Document Covers

This document explains the NoSQL key design decisions made for the three DynamoDB tables in this
project: `genre_kpis`, `top_songs`, and `top_genres`. It covers why the key design in a NoSQL
database like DynamoDB is fundamentally different from SQL, how each table's keys were chosen
by working backwards from the access patterns the downstream application needs to support, and
what the concrete consequences would have been if the keys had been designed differently.

---

## 1. Why Key Design in DynamoDB Is Not Like SQL

In a relational database like PostgreSQL, you design your schema to represent your data correctly,
and you add indexes and write queries later to serve whatever access pattern emerges. SQL is
flexible: a `JOIN`, a `WHERE` clause over any column, or an ad-hoc aggregate query can satisfy
a new question without changing the table structure.

DynamoDB works on the opposite principle. **You design the table's keys around the exact queries
your application will make, before you store any data.** The reason is that DynamoDB has no
query planner, no joins, and no full-table scans that perform well. Every efficient read in
DynamoDB is a direct key lookup — either a `GetItem` (by partition key alone) or a `Query`
(by partition key plus a sort key condition). Anything that does not use the key is a `Scan`,
which reads every item in the table regardless of what you need, scales linearly with table
size, and costs proportionally more as data grows.

This means the partition key and sort key are not just identifiers — they are the query interface.
The question you must answer before creating any DynamoDB table is: **what exact question will
the application ask this table, and at what granularity?**

### The Partition Key (Hash Key)

The partition key determines which physical partition DynamoDB stores the item in. When you call
`GetItem` or `Query`, DynamoDB hashes the partition key value and routes the request directly to
the correct partition. This makes lookups O(1) regardless of table size — the same speed whether
the table has 100 rows or 100 million.

The constraint is that all items with the same partition key value live on the same partition.
DynamoDB imposes a limit of 10 GB per partition and a per-partition throughput limit of 3,000
read capacity units and 1,000 write capacity units per second. If too many items share the same
partition key (a "hot partition"), you hit these limits and requests are throttled.

### The Sort Key (Range Key)

The sort key is optional. When present, it is stored alongside the partition key to form a
composite primary key. Items within the same partition are stored in sort key order, which
enables range queries: `rank BETWEEN 1 AND 3`, `date > "2026-01-01"`, `begins_with(sk, "Pop")`.
The combination of partition key + sort key must be unique across the table — that combination
is the item's identity.

---

## 2. The Access Patterns That Drove the Key Choices in This Project

Before writing any Terraform, the three access patterns the downstream application needs to
support were identified:

| Access Pattern | Question Being Asked |
|---|---|
| A | "Give me all KPI metrics for a specific genre on a specific day" |
| B | "Give me the top 3 songs for a specific genre on a specific day" |
| C | "Give me the top 5 genres for a specific day" |

Every key decision below was made to satisfy exactly one of these patterns with a single
`GetItem` or `Query` call — no scans, no filters, no joins.

---

## 3. Table-by-Table Key Design

### Table 1: `genre_kpis`

**What it stores:** `listen_count`, `unique_listeners`, `total_listen_time_ms`, and
`avg_listen_time_ms_per_user` — one row per genre per day.

**Access pattern it must serve (Pattern A):**
> "Give me all KPIs for genre = Afrobeats on date = 2026-05-17"

**Key design chosen:**
```
Partition Key (hash_key): genre_date   — type: String
Sort Key:                 none
```

**The composite value:**
```python
# kpi_aggregation_job.py — assembleGenreKpis()
.withColumn(
    "genre_date",
    F.concat_ws("#", F.col("track_genre"), F.col("stream_date").cast("string"))
)
# Result: "Afrobeats#2026-05-17"
```

**Why this works for Pattern A:**

The downstream application knows both the genre and the date at query time. It constructs the
key `"Afrobeats#2026-05-17"` and calls `GetItem`. DynamoDB hashes that string, routes to the
correct partition, and returns the item. One API call, O(1) time, no scan.

**Why no sort key is needed here:**

There is exactly one KPI record per genre per day. A composite key of genre + date already
uniquely identifies the item. Adding a sort key would add structural complexity with no benefit.

**Terraform definition (`main.tf:143–160`):**
```hcl
resource "aws_dynamodb_table" "genre_kpis" {
  name         = "genre_kpis"
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "genre_date"   # format: "Afrobeats#2026-05-17"

  attribute {
    name = "genre_date"
    type = "S"
  }
  ...
}
```

**What would have happened with a bad key choice:**

If the partition key had been set to `track_genre` alone:
- All records for "Pop" would land on the same partition regardless of date.
- A query for "Pop on 2026-05-17" would need to scan all Pop items and filter by date in
  application code, reading and paying for every day's data every time.
- As the pipeline runs daily, the "Pop" partition would accumulate unboundedly, eventually
  hitting the 10 GB partition limit and being throttled.

If the partition key had been set to `stream_date` alone:
- All genres for a given day would land on the same partition.
- A query for "Afrobeats on 2026-05-17" would return all genres for that date and force
  the application to filter for Afrobeats, reading and paying for dozens of records to get one.
- High-volume days would hot-partition on the date.

---

### Table 2: `top_songs`

**What it stores:** The top 3 ranked songs per genre per day — up to 3 items per genre-date
combination, each with `track_id`, `track_name`, `play_count`, and `rank`.

**Access pattern it must serve (Pattern B):**
> "Give me the top 3 songs for genre = Afrobeats on date = 2026-05-17"

**Key design chosen:**
```
Partition Key (hash_key): genre_date   — type: String
Sort Key     (range_key): rank         — type: Number
```

**Why the same partition key as `genre_kpis`:**

The application already knows the genre and date (same as Pattern A). The partition key
`"Afrobeats#2026-05-17"` groups all three ranked songs for that genre-date on the same
DynamoDB partition. A single `Query` call on that partition key returns all three items
in rank order in one round trip.

**Why `rank` is the sort key and not `track_id` or `track_name`:**

Sort keys within a partition are stored in sorted order. Since the application wants to
retrieve items in rank order (1st, 2nd, 3rd), using `rank` as the sort key means DynamoDB
stores them in that order physically. The query `genre_date = "Afrobeats#2026-05-17" AND
rank BETWEEN 1 AND 3` is a direct range scan on pre-sorted data — no in-application sorting
is needed.

Using `track_id` as the sort key would mean items are stored in track ID order (arbitrary
alphabetical), and the application would need to sort by play count after fetching — adding
application-side complexity and making the sort key semantically meaningless.

**The composite primary key uniqueness guarantee:**

`genre_date` + `rank` together are unique: there can only be one item with rank 1 for
"Afrobeats#2026-05-17". This maps directly onto the deduplication in `dynamodb_loader.py`:

```python
# dynamodb_loader.py:93
topSongsDF = loadParquet(spark, f"{goldBase}/top_songs").dropDuplicates(["genre_date", "rank"])
```

The `dropDuplicates` mirrors the primary key — it ensures that if the pipeline runs twice for
the same date, `put_item` is idempotent (same key = same item overwrites itself with the same
data).

**Terraform definition (`main.tf:166–189`):**
```hcl
resource "aws_dynamodb_table" "top_songs" {
  name         = "top_songs"
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "genre_date"   # format: "Afrobeats#2026-05-17"
  range_key    = "rank"         # 1, 2, or 3

  attribute {
    name = "genre_date"
    type = "S"
  }
  attribute {
    name = "rank"
    type = "N"
  }
  ...
}
```

**What would have happened with a bad key choice:**

If only `genre_date` had been used as the partition key with no sort key:
- DynamoDB tables without a sort key allow only one item per partition key value.
- Attempting to put rank 1, rank 2, and rank 3 for "Afrobeats#2026-05-17" would result
  in each `put_item` overwriting the previous one — only rank 3 (the last write) would
  survive. The table would store one song per genre per day instead of three.

If `track_id` had been the sort key instead of `rank`:
- The primary key uniqueness would be `genre_date + track_id`, which is correct for storage.
- But queries for "top 3 songs for Afrobeats today" would return all songs for that genre-date
  unsorted, and rank ordering would have to happen in application code after fetching.
- Ties in play count would also be ambiguous since the rank would not be stored.

---

### Table 3: `top_genres`

**What it stores:** The top 5 genres globally per day ranked by listen count — up to 5 items
per date, each with `track_genre`, `listen_count`, and `rank`.

**Access pattern it must serve (Pattern C):**
> "Give me the top 5 genres for date = 2026-05-17"

**Key design chosen:**
```
Partition Key (hash_key): date   — type: String
Sort Key     (range_key): rank   — type: Number
```

**Why `date` as the partition key and not `genre_date`:**

In `genre_kpis` and `top_songs`, the application queries by genre + date together, so the
composite string `"Afrobeats#2026-05-17"` collocates items the application retrieves together.
Here, the application only knows the date — it does not know which genres will be in the top 5.
Using `date` as the partition key groups all 5 genre records for a day together. A single
`Query` on `date = "2026-05-17"` returns all 5 in rank order.

If `genre_date` had been reused here, the application would need 5 separate `GetItem` calls
(one per genre it guesses might be in the top 5), which it cannot do because it does not know
which genres to query.

**Why `rank` as the sort key:**

Same reasoning as `top_songs` — items stored in rank order (1 through 5) means the `Query`
returns them pre-sorted, and the application can display the leaderboard directly without
any additional sorting logic.

**The rename in the KPI job:**

Note that `kpi_aggregation_job.py` renames `stream_date` to `date` before writing this dataset:

```python
# kpi_aggregation_job.py:90
.withColumnRenamed("stream_date", "date")
```

This is intentional. In `genre_kpis` and `top_songs`, the date column is named `stream_date`
because those records are genre-specific and carry additional context. In `top_genres`, the
partition key is named `date` to reflect that this table is a simple daily leaderboard — the
rename makes the key name self-describing at the DynamoDB level.

**The deduplication mirrors the primary key:**

```python
# dynamodb_loader.py:94
topGenresDF = loadParquet(spark, f"{goldBase}/top_genres").dropDuplicates(["date", "rank"])
```

`date + rank` is unique — there is one genre at rank 1 for any given day. The deduplication
enforces this before writing, making repeated pipeline runs for the same date idempotent.

**Terraform definition (`main.tf:195–218`):**
```hcl
resource "aws_dynamodb_table" "top_genres" {
  name         = "top_genres"
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "date"    # format: "2026-05-17"
  range_key    = "rank"    # 1 through 5

  attribute {
    name = "date"
    type = "S"
  }
  attribute {
    name = "rank"
    type = "N"
  }
  ...
}
```

**What would have happened with a bad key choice:**

If `track_genre` had been the partition key:
- To get the top 5 for a date, the application would need to query all known genres
  individually, fetch their rank for that day from each partition, and assemble the result
  in application code — replacing one `Query` with potentially dozens of `GetItem` calls.

If there had been no sort key and `date` was the only key:
- Only one genre could be stored per date (last write wins), destroying the top-5 structure
  entirely.

---

## 4. The Composite String Key Pattern — Why `"Afrobeats#2026-05-17"`

Both `genre_kpis` and `top_songs` use a single string attribute `genre_date` as the partition
key rather than two separate attributes `track_genre` and `stream_date`.

DynamoDB does not support composite partition keys — a partition key is always a single
attribute. When the natural identity of an item is the combination of two values (genre + date),
the standard pattern is to concatenate them into one string with a delimiter that cannot appear
in either value. The `#` separator was chosen because genre names and ISO dates (`YYYY-MM-DD`)
never contain `#`.

This pattern is constructed in `kpi_aggregation_job.py` at computation time:

```python
F.concat_ws("#", F.col("track_genre"), F.col("stream_date").cast("string"))
```

And used directly as the DynamoDB key in `dynamodb_loader.py`:

```python
def buildGenreKpisItem(row):
    return {
        "genre_date": row["genre_date"],   # already "Afrobeats#2026-05-17"
        ...
    }
```

The downstream application reconstructs the key the same way before calling `GetItem`:

```python
key = f"{genre}#{date}"   # "Afrobeats#2026-05-17"
```

This is a deliberate design contract between the pipeline and the application layer.

---

## 5. Summary — All Three Tables Side by Side

| Table | Partition Key | Sort Key | Why That Key | Query Made |
|---|---|---|---|---|
| `genre_kpis` | `genre_date` (`"Pop#2026-05-17"`) | none | One KPI row per genre-date; application always knows both values | `GetItem(genre_date="Pop#2026-05-17")` |
| `top_songs` | `genre_date` (`"Pop#2026-05-17"`) | `rank` (Number) | Multiple songs per genre-date; rank sorts them for the application | `Query(genre_date="Pop#2026-05-17", rank BETWEEN 1 AND 3)` |
| `top_genres` | `date` (`"2026-05-17"`) | `rank` (Number) | Application only knows the date, not which genres will be top-5 | `Query(date="2026-05-17", rank BETWEEN 1 AND 5)` |

---

## 6. What Happens If You Get the Keys Wrong — Consolidated

Getting DynamoDB key design wrong does not cause an immediate crash. It causes silent
degradation that becomes visible only under load or at scale. The failure modes fall into
four categories:

**Correctness failure:** A table without a sort key where multiple items per partition key
are expected (as in `top_songs`) causes `put_item` to overwrite previous items. The table
silently stores one item where three are expected. No error is raised; the data is simply lost.

**Performance failure:** A partition key that is too coarse (e.g., `stream_date` alone in
`genre_kpis`) means a query for one genre's KPIs returns all genres for that day. The
application must filter in code after paying for all the reads. As the number of genres grows,
read cost grows linearly with no benefit.

**Scalability failure:** A hot partition key (e.g., using `stream_date` in a table receiving
thousands of writes per second all for the current date) concentrates all write traffic on one
partition. DynamoDB throttles that partition at 1,000 write capacity units per second and
returns `ProvisionedThroughputExceededException`. The pipeline backs off, retries pile up, and
the Glue job either slows dramatically or times out.

**Operational failure:** A key design that requires application-side filtering, sorting, or
multi-request assembly to reconstruct what should be a single query means the application
cannot be tested or reasoned about simply. Every access pattern change requires a data migration
because DynamoDB primary keys cannot be altered after table creation — the table must be
deleted and recreated.

The key design in this project avoids all four failure modes by matching each key exactly to
the single query each table must serve.
