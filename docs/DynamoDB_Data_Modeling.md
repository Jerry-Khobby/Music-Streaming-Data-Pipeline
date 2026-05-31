# Data Modeling for DynamoDB — NoSQL Thinking, Single- vs Multi-Table

## What This Document Covers

This document explains how to *think* about data modeling in DynamoDB (NoSQL) compared to a
relational database, what the **single-table vs multi-table** design debate actually means, and the
reasoning behind modeling this project's data as **three separate tables**. It is the *modeling
philosophy* companion to [DynamoDB_Key_Design.md](DynamoDB_Key_Design.md), which covers the concrete
partition/sort key mechanics. Everything maps to the tables in
[terraform/main.tf](../terraform/main.tf) and the loader in
[glue_jobs/dynamodb_loader.py](../glue_jobs/dynamodb_loader.py).

---

## 1. The Mental Flip — Relational vs NoSQL Modeling

The hardest part of DynamoDB for someone coming from SQL is that **the entire modeling process runs
in reverse**. It is worth stating the contrast bluntly because it governs every decision that
follows.

### How you model in a relational database

In PostgreSQL or MySQL you model the **data** first:

1. Identify your entities (users, songs, streams) and give each a normalized table.
2. Define relationships with foreign keys.
3. *Then* — later, independently — write whatever `SELECT`, `JOIN`, `GROUP BY`, or `WHERE` you need.

The schema describes the data *correctly*, and the query engine figures out how to answer any
question you pose, even questions you never anticipated. Flexibility is the whole point: a new
report is a new query, not a schema change.

### How you model in DynamoDB

DynamoDB inverts this. You model the **queries** first:

1. List the *exact* questions the application will ask, and how often.
2. Design the table's keys so each question is answered by a single, direct key lookup.
3. *Then* store the data in the shape those keys require.

The reason for the inversion is mechanical: **DynamoDB has no query planner, no joins, and no
efficient full-table scan.** Every fast read is either a `GetItem` (fetch one item by its full key)
or a `Query` (fetch a range of items sharing a partition key). Anything that does not use the key is
a `Scan`, which reads the entire table and gets slower and costlier as data grows. So the keys are
not just identifiers — **the keys are the query interface.**

The governing question before you create *any* DynamoDB table is therefore:
> *"What exact questions will the application ask, and at what granularity?"*

This project answered that question first, and the table design fell out of it (see Section 4).

| | Relational | DynamoDB (NoSQL) |
|---|---|---|
| Model first | The data (entities, relations) | The access patterns (queries) |
| Answer new questions by | Writing a new query | Often needing a new key/table/index |
| Joins | Native, at query time | None — you pre-join or denormalize at write time |
| Normalization | Prized (avoid duplication) | Often *denormalized* on purpose for read speed |
| Read cost model | Query optimizer does the work | You design the key so reads are O(1) |

---

## 2. Denormalization — Embracing Duplication

A relational instinct is to *normalize*: store each fact once, join to assemble it. DynamoDB
deliberately does the opposite when it helps reads. Because there are no joins, you **pre-compute and
denormalize** the data into the exact shape the application will read.

This pipeline is a perfect example: the heavy lifting (joining streams to songs, aggregating,
ranking) all happens *upstream in Spark* during the Gold stage. By the time data reaches DynamoDB,
each item is already a self-contained, ready-to-serve answer. The `top_songs` item, for instance,
already carries `track_name` alongside `track_id` and `play_count` — the application never has to
join back to a songs table to display a name. The duplication of `track_name` across items is
accepted on purpose, in exchange for single-lookup reads.

This is the NoSQL trade in one sentence: **spend work and storage at write time so reads are trivial
and fast.**

---

## 3. Single-Table vs Multi-Table Design

This is the central design debate in DynamoDB, and it is worth explaining honestly because the
"advanced" advice and this project's choice differ — for good reasons.

### What single-table design is

In **single-table design**, *all* entity types — users, orders, products, whatever — live in **one**
DynamoDB table with generic key attributes (commonly named `PK` and `SK`). Different entity types are
distinguished by how their keys are formatted, e.g. `PK = "USER#123"`, `PK = "ORDER#456"`. Carefully
chosen keys let a single `Query` fetch *related items of different types together* — for example, a
user and all their orders in one round trip.

**Why people advocate it:** when your access patterns require fetching related entities together,
single-table design lets one query return them all, avoiding multiple round trips. It can also be
cheaper at very high scale.

**Its costs:** it is genuinely hard to design and reason about. The table holds heterogeneous items
with overloaded, cryptic keys; adding a new access pattern can force a redesign; and the data is far
less readable to anyone inspecting the table directly.

### What multi-table design is

In **multi-table design**, each entity or dataset gets its **own** table with keys tailored to it.
This is closer to the relational instinct and is simpler to understand, but it means a query cannot
span tables (there are no joins) — so it is the right choice when you *never need to fetch different
entity types together in one query*.

### Which this project uses — and why

This project uses **multi-table design**: three separate tables — `genre_kpis`, `top_songs`,
`top_genres`. That is the correct choice here, and the reasoning is specific:

1. **The three datasets are independent.** A KPI row, a ranked-song row, and a ranked-genre row are
   not related records that the application fetches together. No screen says "give me the genre KPIs
   *and* the top songs *and* the top genres in one atomic read." Each is queried on its own. The main
   advantage of single-table design — co-locating related items for one-query retrieval — simply does
   not apply.
2. **The access patterns are few, fixed, and known.** There are exactly three (see Section 4). With
   no need to evolve toward unanticipated cross-entity queries, the flexibility cost of multi-table
   design never bites.
3. **The key schemas genuinely differ.** `genre_kpis` needs no sort key; `top_songs` and `top_genres`
   need a numeric `rank` sort key; and `top_genres` is partitioned by `date` while the others use a
   composite `genre_date`. Forcing these into one overloaded table would add complexity for no
   benefit.
4. **It is a serving layer for an analytics pipeline.** The tables are populated in bulk by a Glue
   job from Gold Parquet and read by a dashboard. Clarity, debuggability, and a clean one-to-one
   mapping from Gold dataset → table matter more than squeezing out the last drop of single-table
   efficiency.

In short: single-table design is powerful when you must fetch related, heterogeneous items together
under one key. This pipeline has three unrelated, independently-queried datasets with simple fixed
access patterns — so **multi-table is simpler, clearer, and exactly sufficient.**

---

## 4. How the Three Tables Were Modeled

The modeling started from the three questions the downstream application asks:

| Pattern | The question | Table | Partition key | Sort key |
|---|---|---|---|---|
| A | "All KPIs for genre X on day Y" | `genre_kpis` | `genre_date` = `"Afrobeats#2026-05-17"` | none |
| B | "Top 3 songs for genre X on day Y" | `top_songs` | `genre_date` | `rank` (1–3) |
| C | "Top 5 genres on day Y" | `top_genres` | `date` = `"2026-05-17"` | `rank` (1–5) |

A few modeling decisions worth highlighting (full detail in
[DynamoDB_Key_Design.md](DynamoDB_Key_Design.md)):

- **Composite string keys.** DynamoDB partition keys are a single attribute, but the natural identity
  of a KPI is *genre + date*. The model concatenates them into one string with a `#` delimiter
  (`"Afrobeats#2026-05-17"`), a standard NoSQL technique for a composite identity. This is built in
  Spark via `concat_ws("#", track_genre, stream_date)`.
- **`rank` as the sort key.** For the "top N" tables, items are stored physically in rank order, so a
  single `Query` returns them already sorted 1, 2, 3 — the application does no sorting. `rank` was
  chosen over `track_id` precisely so the read is ordered the way the UI displays it.
- **Different partition key for `top_genres`.** It uses `date` alone (not `genre_date`) because the
  application knows only the date and does not know *which* genres will be in the top 5 — so all five
  must live under the same `date` partition to be fetched in one query.
- **The model mirrors the dedup keys.** The loader deduplicates each dataset on exactly its table's
  primary key (`["genre_date"]`, `["genre_date","rank"]`, `["date","rank"]`,
  [dynamodb_loader.py:92](../glue_jobs/dynamodb_loader.py#L92)). This makes writes **idempotent** —
  re-running the pipeline overwrites each item with identical data rather than creating duplicates,
  because in DynamoDB a write to an existing primary key is an overwrite.

---

## 5. Modeling Decisions That Fall Out of NoSQL Thinking

Several choices in this project are direct consequences of modeling for DynamoDB rather than SQL:

- **Aggregation happens before the database, not in it.** A relational design might load raw events
  and `GROUP BY` at query time. Here, all aggregation is done in Spark (Gold layer) so DynamoDB only
  ever stores and serves pre-computed answers — because DynamoDB cannot `GROUP BY`.
- **Type coercion for the data model.** DynamoDB has no float type, so the loader converts decimals to
  `Decimal` and counts to `int` (`toDecimal`, [dynamodb_loader.py:18](../glue_jobs/dynamodb_loader.py#L18)).
  Modeling for DynamoDB includes modeling the *types* it supports.
- **No "just add a query later."** Because primary keys cannot be changed after a table is created,
  the three access patterns were nailed down *before* the tables existed. A new access pattern would
  mean a new table or a secondary index, not just a new query — which is exactly why query-first
  modeling is mandatory.

---

## 6. Summary

| Question | This project's answer |
|---|---|
| Model data or queries first? | **Queries first** — the three access patterns drove the keys |
| Normalize or denormalize? | **Denormalize** — pre-join and pre-aggregate in Spark; items are self-contained |
| Single-table or multi-table? | **Multi-table** — three independent, separately-queried datasets; no need to co-fetch related entities |
| How are composite identities handled? | Concatenated string keys (`"genre#date"`) since a partition key is one attribute |
| How is ordering served? | `rank` sort key stores items pre-sorted for the UI |
| How are writes kept safe? | Dedup on the exact primary key → idempotent overwrites |
| Where does aggregation happen? | Upstream in Spark (Gold layer), never in DynamoDB |

The throughline: in DynamoDB you design for the read you will perform, denormalize so that read is a
single key lookup, and choose the simplest table layout that serves your actual access patterns. For
three independent KPI datasets with fixed queries, that is three purpose-built tables — and the
upstream Spark pipeline does all the joining and aggregating so DynamoDB only ever serves finished
answers.
