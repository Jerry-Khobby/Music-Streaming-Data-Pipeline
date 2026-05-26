# DynamoDB Sample Queries

> Quick reference for downstream applications, dashboards, and analysts to retrieve KPIs from the three tables this pipeline writes: `genre_kpis`, `top_songs`, `top_genres`.

Every query below is a direct key lookup — no scans, no filters, single-digit-millisecond response. For the reasoning behind each key design, see [DynamoDB_Key_Design.md](DynamoDB_Key_Design.md).

---

## Table 1 — `genre_kpis`

> Daily KPIs for a single genre.

**Key shape:** partition key `genre_date` (string, e.g. `"Afrobeats#2026-05-17"`).

### Q1.1 — Get all KPIs for one genre on one day

**AWS CLI**

```bash
aws dynamodb get-item \
  --table-name genre_kpis \
  --key '{"genre_date": {"S": "Afrobeats#2026-05-17"}}'
```

**boto3 (Python)**

```python
import boto3

table = boto3.resource("dynamodb").Table("genre_kpis")
response = table.get_item(Key={"genre_date": "Afrobeats#2026-05-17"})
item = response.get("Item")
```

**PartiQL (in console or AWS CLI)**

```sql
SELECT * FROM genre_kpis WHERE genre_date = 'Afrobeats#2026-05-17';
```

### Q1.2 — Get KPIs for multiple genres on the same day (batched)

```python
import boto3

dynamodb = boto3.resource("dynamodb")
genres = ["Afrobeats", "Pop", "Rock"]
date = "2026-05-17"

response = dynamodb.batch_get_item(
    RequestItems={
        "genre_kpis": {
            "Keys": [{"genre_date": f"{g}#{date}"} for g in genres],
        }
    }
)
items = response["Responses"]["genre_kpis"]
```

> **One round trip** — DynamoDB returns up to 100 items per `BatchGetItem` call.

---

## Table 2 — `top_songs`

> Top 3 songs for one genre on one day, returned in rank order.

**Key shape:** partition key `genre_date` (string), sort key `rank` (number, 1–3).

### Q2.1 — Get the top 3 songs for one genre on one day

**AWS CLI**

```bash
aws dynamodb query \
  --table-name top_songs \
  --key-condition-expression "genre_date = :gd" \
  --expression-attribute-values '{":gd": {"S": "Afrobeats#2026-05-17"}}'
```

**boto3**

```python
from boto3.dynamodb.conditions import Key

table = boto3.resource("dynamodb").Table("top_songs")
response = table.query(
    KeyConditionExpression=Key("genre_date").eq("Afrobeats#2026-05-17"),
)
top_songs = response["Items"]
```

**PartiQL**

```sql
SELECT * FROM top_songs WHERE genre_date = 'Afrobeats#2026-05-17';
```

### Q2.2 — Get just the #1 song for a genre on a day

```python
response = table.get_item(
    Key={"genre_date": "Afrobeats#2026-05-17", "rank": 1}
)
top_song = response.get("Item")
```

### Q2.3 — Get only ranks 1 and 2 (skip rank 3)

```python
response = table.query(
    KeyConditionExpression=(
        Key("genre_date").eq("Afrobeats#2026-05-17")
        & Key("rank").between(1, 2)
    ),
)
```

---

## Table 3 — `top_genres`

> Top 5 genres globally on one day, ranked by listen count.

**Key shape:** partition key `date` (string, e.g. `"2026-05-17"`), sort key `rank` (number, 1–5).

### Q3.1 — Get the top 5 genres for a single day

**AWS CLI**

```bash
aws dynamodb query \
  --table-name top_genres \
  --key-condition-expression "#d = :date" \
  --expression-attribute-names '{"#d": "date"}' \
  --expression-attribute-values '{":date": {"S": "2026-05-17"}}'
```

> `date` is a reserved word in DynamoDB expressions — alias it with `#d`.

**boto3**

```python
from boto3.dynamodb.conditions import Key

table = boto3.resource("dynamodb").Table("top_genres")
response = table.query(
    KeyConditionExpression=Key("date").eq("2026-05-17"),
)
top_genres = response["Items"]  # already sorted by rank ascending
```

**PartiQL**

```sql
SELECT * FROM top_genres WHERE "date" = '2026-05-17';
```

### Q3.2 — Get the #1 genre for a single day

```python
response = table.get_item(Key={"date": "2026-05-17", "rank": 1})
top_genre = response.get("Item")
```

### Q3.3 — Get the top genre across a date range

```python
from boto3.dynamodb.conditions import Key

results = []
for date in ["2026-05-15", "2026-05-16", "2026-05-17"]:
    response = table.get_item(Key={"date": date, "rank": 1})
    if "Item" in response:
        results.append(response["Item"])
```

> No range query on the partition key — make one `GetItem` per date and parallelise client-side if needed.

---

## Cross-table — Build a Dashboard for One Day

A common application access pattern: render a full dashboard for `2026-05-17`.

```python
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
top_genres_tbl = dynamodb.Table("top_genres")
genre_kpis_tbl = dynamodb.Table("genre_kpis")
top_songs_tbl  = dynamodb.Table("top_songs")

date = "2026-05-17"

# Step 1 — top 5 genres for the day (one Query)
top_genres = top_genres_tbl.query(
    KeyConditionExpression=Key("date").eq(date)
)["Items"]

# Step 2 — KPIs + top songs for each of those 5 genres (batch + 5 queries)
genre_dates = [f'{g["track_genre"]}#{date}' for g in top_genres]

genre_kpis = dynamodb.batch_get_item(
    RequestItems={
        "genre_kpis": {
            "Keys": [{"genre_date": gd} for gd in genre_dates],
        }
    }
)["Responses"]["genre_kpis"]

top_songs_per_genre = {
    gd: top_songs_tbl.query(
        KeyConditionExpression=Key("genre_date").eq(gd)
    )["Items"]
    for gd in genre_dates
}
```

> Total: **1 Query + 1 BatchGetItem + 5 Queries = 7 round trips** for the whole dashboard.

---

## Quick Sanity Checks After a Pipeline Run

Use these right after a pipeline execution succeeds to verify data landed correctly.

### Count items in each table

```bash
aws dynamodb scan --table-name genre_kpis --select COUNT
aws dynamodb scan --table-name top_songs  --select COUNT
aws dynamodb scan --table-name top_genres --select COUNT
```

> `Scan` is fine here because it is operator-driven and infrequent. Never put scans in application code.

### Verify a single recent record exists

```bash
aws dynamodb query \
  --table-name top_genres \
  --key-condition-expression "#d = :date" \
  --expression-attribute-names '{"#d": "date"}' \
  --expression-attribute-values "{\":date\": {\"S\": \"$(date -u +%F)\"}}" \
  --max-items 5
```

### Check item exists for a known genre

```bash
aws dynamodb get-item \
  --table-name genre_kpis \
  --key '{"genre_date": {"S": "Afrobeats#2026-05-17"}}' \
  --query 'Item'
```

If `null` is returned, the genre had no streams on that date (expected) or the pipeline failed to write (investigate via Step Functions execution history).

---

## Reserved Words to Watch

DynamoDB has a list of reserved words that cannot be used directly in expressions. The ones that appear in our schema:

| Word in our schema | Reserved? | How to alias |
|---|---|---|
| `date` | **Yes** | `--expression-attribute-names '{"#d": "date"}'`, then use `#d` |
| `rank` | **Yes** | `--expression-attribute-names '{"#r": "rank"}'`, then use `#r` |
| `genre_date`, `track_genre`, `listen_count`, etc. | No | Use directly |

Full list: <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html>

---

## Pagination Note

`Query` returns up to 1 MB of data per call. Our partitions are small (3 songs per genre-date, 5 genres per date) so pagination is unnecessary in this project. If you ever Query a denormalised side table that returns more than 1 MB, paginate with `LastEvaluatedKey`:

```python
items = []
last_evaluated = None
while True:
    kwargs = {"KeyConditionExpression": Key("date").eq("2026-05-17")}
    if last_evaluated:
        kwargs["ExclusiveStartKey"] = last_evaluated
    response = table.query(**kwargs)
    items.extend(response["Items"])
    last_evaluated = response.get("LastEvaluatedKey")
    if not last_evaluated:
        break
```
