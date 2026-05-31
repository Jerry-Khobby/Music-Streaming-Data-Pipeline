# Data Lineage and Auditability

## What This Document Covers

This document explains what **data lineage** means, how the **Bronze layer** preserves the original
source of truth, how the **Glue Data Catalog** tracks schema over time, and **why regulators and
businesses care** about lineage and auditability. It is written for a data engineer new to the
concept. It maps to the layer design in [Medallion_Architecture.md](Medallion_Architecture.md), the
archival design in [Archival_Strategy.md](Archival_Strategy.md), and the catalog behavior in
[Schema_Management_and_Glue_Catalog.md](Schema_Management_and_Glue_Catalog.md).

---

## 1. What Data Lineage Is

**Data lineage** is the documented answer to one question about any number in your system:

> *"Where did this come from, and what happened to it along the way?"*

For a KPI like "Afrobeats had 12,400 listens on 2026-05-17," lineage is the full trail backward:

```
DynamoDB item (genre_kpis: Afrobeats#2026-05-17, listen_count=12400)
   ⬑ derived from gold/genre_kpis  (aggregated by kpi_aggregation_job)
       ⬑ derived from silver/enriched_streams  (joined + deduped by etl_transform_job)
           ⬑ derived from raw streams/*.csv + songs/songs.csv  (the original uploaded files)
```

Lineage is "the family tree of a data point." **Auditability** is the property that this trail is
*complete and trustworthy* — you can actually follow it end to end and prove each step, rather than
just hoping the number is right.

A pipeline has good lineage when, for any output, you can name (a) the exact inputs it came from, (b)
every transformation applied, and (c) recover the original inputs to re-derive it. This pipeline is
designed so all three are possible.

---

## 2. How the Medallion Layers Create Lineage

The medallion architecture (Bronze → Silver → Gold) is, by its very structure, a lineage record. Each
layer is **preserved** and each is **derived** from the one below, so the layers themselves form the
trail:

| Layer | Role in lineage |
|---|---|
| **Bronze** (`raw` + `archive`) | The immutable origin — "what actually arrived" |
| **Silver** (`silver/enriched_streams`) | "What it became after cleaning/joining" — one hop from Bronze |
| **Gold** (`gold/*`) | "What was computed from the clean data" — one hop from Silver |
| **DynamoDB** | "What was served" — one hop from Gold |

Because data flows in exactly one direction and **no layer is destroyed when the next is produced**,
you can always walk backward from a served KPI to the raw bytes that produced it. The directional,
preserved-layer design *is* the lineage. (See [Medallion_Architecture.md](Medallion_Architecture.md).)

---

## 3. How Bronze Preserves the Source of Truth

The Bronze layer is the anchor of the whole lineage story — it is the **single source of truth** for
"what the upstream system actually sent." Several deliberate choices protect it:

### It is never modified in place

Raw CSV files are **read, never edited or overwritten** by the pipeline. The transform job reads them;
it does not change them. So the bytes in Bronze are exactly the bytes that arrived — the unaltered
record.

### It is versioned

The raw bucket has **S3 versioning enabled** ([main.tf:20](../terraform/main.tf#L20)). Even if a file
were accidentally overwritten, the previous version is retained and recoverable. The source of truth
cannot be silently clobbered.

### Processed files are archived, not deleted

This is the crucial part for auditability. When a stream file has been processed, the archive job does
**not delete it into oblivion** — it *moves* it to the dedicated archive bucket
([archive_job.py:53](../glue_jobs/archive_job.py#L53)), preserving the original key. The archive
bucket keeps every raw file ever processed (transitioning to cheap Glacier storage after 90 days for
cost — see [Cost_Optimisation.md](Cost_Optimisation.md)).

The effect: **every raw input the pipeline ever consumed still exists** — either currently in
`streams/` (not yet processed) or in the archive bucket (already processed). So for any historical
KPI, the exact source files are recoverable, and the entire result could be re-derived from scratch.
That is the foundation of an auditable pipeline. (See [Archival_Strategy.md](Archival_Strategy.md).)

### Encryption protects integrity

All buckets are encrypted at rest (AES256), so the preserved source of truth is also protected from
unauthorized access at the storage layer (see [Encryption_in_This_Pipeline.md](Encryption_in_This_Pipeline.md)).

---

## 4. How the Glue Data Catalog Tracks Schema

Lineage is not only about *values* — it's also about *structure*. The **Glue Data Catalog** records
the **schema** (columns and types) of every dataset, and it is the authoritative record of "what shape
the data had" at each layer (see
[Schema_Management_and_Glue_Catalog.md](Schema_Management_and_Glue_Catalog.md)).

Two behaviors make it part of the audit story:

- **It is the documented contract of structure.** The catalog tables (`streams`, `songs`, `users`,
  and the `gold/*` datasets) record exactly what columns existed and where the data lives. Anyone
  auditing the pipeline can read the catalog to understand the structure of every layer without
  opening files.
- **Schema changes are tracked, not silently dropped.** The crawler's policy is
  `update_behavior = "UPDATE_IN_DATABASE"` (evolve the table in place when source columns change) and
  `delete_behavior = "LOG"` (only *log* when something disappears — never auto-delete the catalog
  entry) ([main.tf:295](../terraform/main.tf#L295)). The `LOG` behavior is an auditability choice: a
  vanished column or table is **recorded** rather than quietly erased, so the history of structural
  changes is observable rather than lost.

Combined with the immutable Bronze files, the catalog means you can audit both **what the data was**
(the files) and **what shape it had** (the schema) at any point.

---

## 5. The Execution Trail — Auditing *What Happened*

Beyond data and schema, an auditor often needs to know *which run produced which output, and did it
succeed?* This pipeline captures that too:

- **Step Functions execution history** logs every run at `level = ALL` with full input/output data
  (`/aws/states/<project>`), so each execution is a recorded, replayable trail of which steps ran on
  which data.
- **Glue job logs** (`/aws/glue/<project>`) record what each transformation did, including row counts
  ("Merged N partitions; M rows after deduplication").
- **Idempotency** means re-deriving an output from the preserved Bronze files yields the *same* result
  — so an audit can reproduce and verify any number (see
  [Idempotency_in_Data_Pipelines.md](Idempotency_in_Data_Pipelines.md)).

Together: the data trail (Bronze→Silver→Gold→DynamoDB), the schema trail (Catalog), and the execution
trail (CloudWatch logs) give a complete, three-dimensional audit record.

---

## 6. Why Regulators and Businesses Care

Lineage and auditability are not academic — they are demanded by real stakeholders for concrete
reasons:

### Why businesses care

- **Trust in the numbers.** When an executive questions a KPI, lineage lets you *prove* it — trace it
  to source and show every transformation — instead of saying "the system says so."
- **Debugging and root-cause analysis.** When a metric looks wrong, lineage points to *where* it went
  wrong (bad source file? join bug? aggregation error?) by letting you inspect each layer.
- **Safe change and reprocessing.** If a transformation bug is found, preserved Bronze data lets you
  fix the code and **re-derive corrected history** rather than living with permanently wrong numbers.
- **Impact analysis.** Lineage shows what downstream outputs depend on a given source, so you know what
  breaks if a source changes.

### Why regulators care

- **Compliance and reporting.** Regulated industries (finance, healthcare, etc.) must *demonstrate*
  that reported figures derive correctly from authentic source data. Lineage is the evidence.
- **Reproducibility / provenance.** Auditors require that any reported number can be reproduced from
  retained source data — exactly what preserved, immutable Bronze enables.
- **Data retention mandates.** Many regulations require keeping original records for years. The archive
  bucket (with Glacier lifecycle) is a cost-efficient way to satisfy long-term retention while keeping
  the data recoverable.
- **Change accountability.** Being able to show *what changed and when* — including schema changes the
  catalog records — supports audit requirements around data governance.

In short, lineage turns "we believe this number is right" into "we can prove this number is right and
reproduce it" — which is the difference between a hobby pipeline and a production, audit-ready one.

---

## 7. Summary

| Element | How this pipeline provides lineage / auditability |
|---|---|
| **Layered derivation** | Bronze → Silver → Gold → DynamoDB, one-directional, every layer preserved |
| **Immutable source of truth** | Raw files never edited; raw bucket versioned |
| **Nothing thrown away** | Processed files archived (not deleted), retained in Glacier — every input recoverable |
| **Schema history** | Glue Catalog records structure; `UPDATE_IN_DATABASE` + `LOG` evolve, never silently drop |
| **Execution trail** | Step Functions execution history + Glue logs record which run did what |
| **Reproducibility** | Idempotent design re-derives identical outputs from preserved Bronze |
| **Why it matters** | Business trust/debugging/reprocessing; regulatory compliance/provenance/retention |

The pipeline's medallion structure, immutable-and-archived Bronze layer, schema-tracking catalog, and
full execution logs together mean that **any served KPI can be traced back to the exact raw files and
transformations that produced it, and re-derived to prove it** — the essence of data lineage and
auditability.
