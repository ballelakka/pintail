# Pintail analytical benchmark results

Measured 2026-09-30T13:20:20.788Z with 20,000,000 orders.

All engines run on the docker host under identical limits (8 CPUs, 8 GB);
pintail's per-query memory ceiling is 4 GiB inside its container.
Canonical queries: 15 measured runs after 2 warmups; ad-hoc queries: 5 distinct cold variants. MySQL baseline measured 2026-09-30T13:16:45.785Z.
CH RMT+FINAL = ReplacingMergeTree read with `final = 1` — ClickHouse doing
pintail's always-correct merge-on-read duty. It is charged WITHOUT a live
update tail (the snapshot is fully merged before the timed queries), so it
is a lower bound on ClickHouse's merge-on-read cost; issue #31 tracks the
phase that keeps writes flowing while the queries run.

NOT like for like: the canonical table is served from pintail's settled
aggregate memo, while ClickHouse's query cache is off and it executes every
run. It measures what a repeated dashboard query costs, not engine speed.
The novel-query table below is the engine-speed comparison - both engines
execute there, and ClickHouse is currently faster.

> Historical evidence warning: the 2026-08-11 run banked by `de974db` is
> withdrawn. Pintail minima regressed on Q1/Q3 while unchanged MySQL and
> ClickHouse controls did not, so the repository's host-noise rule did not apply.
> The current artifact supersedes it; the harness now rejects that signature.

## Repeated queries (memo-served — dashboard refresh cost, not engine speed)

| Query | MySQL | Pintail (memo) | vs MySQL | CH MergeTree | CH RMT+FINAL | vs CH | Exact |
|---|---:|---:|---:|---:|---:|---:|:--|
| Q1: Full table count | 740 ms | 1 ms | 740.0× | 1 ms | 3 ms | 3.00× | yes |
| Q2: Filtered count | 310 ms | 2 ms | 155.0× | 8 ms | 9 ms | 4.50× | yes |
| Q3: Group by status | 15,045 ms | 2 ms | 7522.5× | 33 ms | 34 ms | 17.00× | yes |
| Q4: Region × status breakdown | 6,563 ms | 2 ms | 3281.5× | 56 ms | 57 ms | 28.50× | yes |
| Q5: Monthly revenue (2023) | 2,982 ms | 2 ms | 1491.0× | 20 ms | 19 ms | 9.50× | yes |
| Q6: Top 10 spenders | 154,340 ms | 38 ms | 4061.6× | 54 ms | 54 ms | 1.42× | yes |
| Q7: Regional analytics | 22,651 ms | 2 ms | 11325.5× | 49 ms | 58 ms | 29.00× | yes |
| Q8: Join users + orders | 127,169 ms | 2 ms | 63584.5× | 79 ms | 77 ms | 38.50× | yes |
| **Total** | **329,800 ms** | **51 ms** | **6466.7×** | **300 ms** | **311 ms** | **6.10×** | |

Memo-dashboard release gate: PASS (required ≥50× and exact results; not an engine-speed gate).

## Concurrency (memo disabled — both engines executing)

One client measures an engine at rest. This is the shape a server
actually meets, and where admission, memory accounting and lock
contention appear. Throughput and p95 together: throughput alone can
rise while the slowest decile becomes unusable, and a flat p95 can
hide an engine that has stopped accepting work. The mixed workload
round-robins Q2 through Q8 per call across all clients, so no client
is pinned to one shape; the single-query row is the full-table count
alone, the cheapest shape, kept as a ceiling on request rate.

### mixed Q2–Q8

| Clients | Pintail /s | Pintail p95 | Pintail errors | CH /s | CH p95 | CH errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 7.3 | 293 ms | 0 | 22.6 | 84 ms | 0 |
| 4 | 8.5 | 792 ms | 0 | 22.5 | 314 ms | 0 |
| 8 | 14.4 | 1160 ms | 0 | 20.1 | 723 ms | 0 |
| 16 | 35.2 | 1026 ms | 0 | 18.8 | 1906 ms | 0 |

Per query at 16 clients (every level is in results.json):

| Query | Pintail median | Pintail p95 | Pintail done | CH median | CH p95 | CH done |
|---|---:|---:|---:|---:|---:|---:|
| Q2: Filtered count | 207 ms | 333 ms | 52 | 158 ms | 239 ms | 29 |
| Q3: Group by status | 398 ms | 625 ms | 52 | 494 ms | 604 ms | 29 |
| Q4: Region × status breakdown | 400 ms | 703 ms | 52 | 840 ms | 1012 ms | 29 |
| Q5: Monthly revenue (2023) | 425 ms | 714 ms | 52 | 368 ms | 438 ms | 29 |
| Q6: Top 10 spenders | 476 ms | 876 ms | 52 | 1200 ms | 1382 ms | 29 |
| Q7: Regional analytics | 724 ms | 1243 ms | 52 | 1054 ms | 1229 ms | 29 |
| Q8: Join users + orders | 626 ms | 1067 ms | 51 | 1832 ms | 2133 ms | 29 |

### Q1: Full table count

| Clients | Pintail /s | Pintail p95 | Pintail errors | CH /s | CH p95 | CH errors |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 678.5 | 2 ms | 0 | 356.5 | 3 ms | 0 |
| 4 | 974.4 | 10 ms | 0 | 524.7 | 12 ms | 0 |
| 8 | 1060.3 | 35 ms | 0 | 556.1 | 21 ms | 0 |
| 16 | 1127.9 | 32 ms | 0 | 585.9 | 42 ms | 0 |

## Engine speed (memo DISABLED — both engines execute)

The canonical queries against a pintail restarted with its settled
aggregate memo off, on the same replica. This is the like-for-like
comparison: the table at the top measures a cache hit against
ClickHouse's execution, which is a different question.

Pintail (wire) reaches the same query over Pintail's MySQL wire
protocol - the path a BI tool actually uses - timed beside the
HTTP call, not instead of it; the gap between the two is HTTP's
own fixed cost (auth, JSON, connection setup).

| Query | MySQL | Pintail (no memo) | Pintail (wire) | CH MergeTree | CH RMT+FINAL | vs CH |
|---|---:|---:|---:|---:|---:|---:|
| Q1: Full table count | 740 ms | 2 ms | 1 ms | 1 ms | 4 ms | 2.00× |
| Q2: Filtered count | 310 ms | 26 ms | 24 ms | 9 ms | 9 ms | 0.35× |
| Q3: Group by status | 15,045 ms | 87 ms | 87 ms | 31 ms | 30 ms | 0.34× |
| Q4: Region × status breakdown | 6,563 ms | 106 ms | 101 ms | 58 ms | 58 ms | 0.55× |
| Q5: Monthly revenue (2023) | 2,982 ms | 54 ms | 52 ms | 20 ms | 21 ms | 0.39× |
| Q6: Top 10 spenders | 154,340 ms | 275 ms | 276 ms | 49 ms | 50 ms | 0.18× |
| Q7: Regional analytics | 22,651 ms | 227 ms | 226 ms | 43 ms | 50 ms | 0.22× |
| Q8: Join users + orders | 127,169 ms | 165 ms | 167 ms | 84 ms | 86 ms | 0.52× |

## Novel queries (median of 5 memo-cold variants — RAW ENGINE SPEED)

Both engines execute every run here. This is the comparison that speaks
to execution performance.

Each row is the median of five distinct predicate variants, each run once
per engine with no warmup. Pintail therefore cannot replay an exact-result
memo entry. Excluded from the release-gate totals.

| Query | MySQL | Pintail | vs MySQL | CH MergeTree | CH RMT+FINAL | vs CH | Exact |
|---|---:|---:|---:|---:|---:|---:|:--|
| N1: Filtered count, novel constant | 524 ms | 2 ms | 262.0× | 25 ms | 23 ms | 11.50× | yes |
| N2: Group by region (novel group column) | 6,038 ms | 112 ms | 53.9× | 46 ms | 46 ms | 0.41× | yes |
| N3: Monthly revenue, novel year | 3,628 ms | 55 ms | 66.0× | 21 ms | 21 ms | 0.38× | yes |
| N4: Regional analytics, novel range | 21,913 ms | 249 ms | 88.0× | 63 ms | 65 ms | 0.26× | yes |

## Resources during measured runs

Peak container CPU (cumulative across 8 cores, so up to 800%) and peak
memory, sampled from one long-lived `docker stats` stream per container
at the daemon's own update cadence while each engine ran. MySQL shows
n/a when its cold baseline came from the cache.

| Query | Pintail CPU | Pintail mem | CH CPU | CH mem | MySQL CPU | MySQL mem |
|---|---:|---:|---:|---:|---:|---:|
| Q1: Full table count | n/a | n/a | n/a | n/a | 1% | 1,525 MB |
| Q2: Filtered count | 5% | 32 MB | n/a | n/a | 1% | 1,525 MB |
| Q3: Group by status | n/a | n/a | 3% | 319 MB | 94% | 1,525 MB |
| Q4: Region × status breakdown | n/a | n/a | 555% | 385 MB | 107% | 1,537 MB |
| Q5: Monthly revenue (2023) | n/a | n/a | n/a | n/a | 110% | 1,537 MB |
| Q6: Top 10 spenders | 207% | 267 MB | 488% | 486 MB | 63% | 1,538 MB |
| Q7: Regional analytics | n/a | n/a | 60% | 385 MB | 88% | 1,537 MB |
| Q8: Join users + orders | 0% | 71 MB | 569% | 539 MB | 57% | 1,692 MB |

