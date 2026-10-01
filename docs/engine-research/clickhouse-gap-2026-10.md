# Engine-speed gap to ClickHouse on the 20M-row benchmark (2026-10)

Measurement and research only: no engine code changed. Target: the engine
track of `benchmark/run.ts` (Pintail with the settled memo disabled, against
ClickHouse ReplacingMergeTree read with `final = 1`), queries Q2-Q8.

## Setup

- One 8 vCPU / 16 GB VM (AMD Ryzen 9 9950X, KVM), all three engines in
  containers under the benchmark's own limits (`--cpus 8 --memory 8g`).
  Pintail at dev `1bf7dba6`, ClickHouse `26.8`, MySQL `8.4`.
- `benchmark/run.ts` ran once, with its containers kept afterwards. Every
  number below comes from that dataset: 20,000,000 orders, 100,000 users.
- Driver: 2 warmups and 10 timed runs per query and engine, sequential, over
  HTTP. Per batch it records client wall, container CPU (cgroup
  `cpu.stat`), per-thread CPU (`/proc/<pid>/task/*/stat`), `perf record`
  (1999 Hz, 5 s per query and engine) and `perf stat` syscall and fault
  counters. ClickHouse also reports through `system.query_log`
  (`read_rows`, `read_bytes`, `SelectedMarks`, `peak_threads_usage`, CPU
  ProfileEvents), `EXPLAIN PIPELINE` and `EXPLAIN indexes = 1`. Pintail
  reports through `EXPLAIN ANALYZE` (the `PINTAIL_PROFILE` operator tree).

Engine track from the `run.ts` pass on this box (median ms, 15 runs):

| Query | Pintail (no memo) | CH MergeTree | CH RMT+FINAL | Pintail / CH FINAL |
|---|---:|---:|---:|---:|
| Q2 filtered count | 29 | 10 | 9 | 3.2x |
| Q3 group by status | 66 | 33 | 34 | 1.9x |
| Q4 region x status | 86 | 63 | 66 | 1.3x |
| Q5 monthly revenue 2023 | 67 | 22 | 21 | 3.2x |
| Q6 top 10 spenders | 173 | 58 | 56 | 3.1x |
| Q7 regional analytics | 126 | 46 | 53 | 2.4x |
| Q8 join users + orders | 193 | 86 | 87 | 2.2x |

## Headline: same data, same parallelism, 2-3x the CPU per row

| Query | Pintail wall | CH FINAL wall | Pintail CPU ms/query | CH CPU ms/query | CPU ratio | Pintail avg cores | CH avg cores |
|---|---:|---:|---:|---:|---:|---:|---:|
| Q2 | 26.7 | 8.8 | 104 | 46 | 2.3x | 3.9 | 5.3 |
| Q3 | 60.0 | 32.4 | 381 | 180 | 2.1x | 6.4 | 5.5 |
| Q4 | 86.9 | 68.0 | 562 | 380 | 1.5x | 6.5 | 5.6 |
| Q5 | 57.5 | 20.8 | 311 | 111 | 2.8x | 5.4 | 5.3 |
| Q6 | 167.2 | 54.4 | 882 | 310 | 2.8x | 5.2 | 5.6 |
| Q7 | 123.7 | 52.1 | 712 | 290 | 2.5x | 5.7 | 5.5 |
| Q8 | 184.5 | 87.3 | 946 | 518 | 1.8x | 5.2 | 5.8 |

(Driver medians; mins track within 5%. "Avg cores" is container CPU divided
by wall over the timed batch.)

1. **The cause is not that Pintail reads more data.** Neither engine skips
   anything: ClickHouse selects 2,443/2,443 granules of 3 parts on every
   query (its sort key is `id`, and no predicate touches it), and Pintail
   decodes 100% of its blocks (`actual_blocks=N/N decoded_blocks=N`). The
   seed makes every date and every status value recur every few thousand
   ids, so no block-level min/max could skip anything on this dataset.
   Pintail's orders table is the smaller of the two on disk (321 MB in
   200 segments of 100k rows, against 416 MB compressed in ClickHouse), and
   it reads strings as dictionary codes where ClickHouse reads them raw
   (Q2: 228 MB decompressed `status` bytes; Q4: 593 MB).
2. **The cause is not parallelism.** Both engines keep 8 worker threads
   evenly busy, and Pintail's average core count matches ClickHouse's or
   beats it on Q3/Q4/Q7. ClickHouse runs 6 read streams on this table, 8
   for the join. Shrinking Pintail's scan pool to 4 threads
   (`PINTAIL_SCAN_THREADS=4`) left CPU per query unchanged and made wall
   15-30% worse (Q3 68→82 ms, Q5 57→75, Q7 129→156), so the two-pool
   oversubscription costs nothing measurable. The one exception is Q2: at
   26 ms it averages only 3.9-4.3 cores, because the first batch reaches
   the aggregate after about 4.4 ms and the scan works in rounds.
3. **The gap is CPU per row,** split three ways that the profiles separate
   cleanly:
   - kernel time from allocation churn in decode;
   - row-at-a-time aggregation, and in Q8 row-at-a-time join aggregation;
   - avoidable per-row work in scan decode: validity expansion, and the
     row-by-row slow path for partially selected blocks.

   ClickHouse's profile is the reverse: 25-55% of its CPU is LZ4
   decompression, and the rest is a tight aggregation or string-compare
   loop.

## Where Pintail's CPU goes, per query

Shares are from `perf` over each query's own loop (pintail binary vs kernel vs
libc; top symbols demangled). Converted to ms with the CPU-per-query column.

| Query | Kernel | Aggregation row loop | Scan decode | Validity `Vec<bool>` expansion | Other |
|---|---:|---:|---:|---:|---|
| Q2 (104 ms) | **37%** (~39 ms) | ~0 | lz4 8%, codes 4%, mask 10% | **26%** (~27 ms) | libc 8% |
| Q3 (381) | 15% (~57) | **54%** (~205) | int decode 9%, lz4 3% | 7% | |
| Q4 (562) | 19% (~104) | **48%** (~270) | int decode 6%, lz4 2% | 10% (~57) | libc 8% |
| Q5 (311) | 14% (~44) | decimal sum 21%, group 8% | **int decode slow path 25%** (~78) | | rayon glue 7% |
| Q6 (882) | 6% | **54%** (~480): scatter 27%, map flush 22%, rehash | int decode 9% | | libc 12%, serial tail ~33 ms wall |
| Q7 (712) | **34%** (~243) | 28% (~200) incl. distinct 5% | int decode 16% (~114) | 2% | |
| Q8 (946) | 5% | **77%** (~730): per-row typed update in fused join aggregate | int decode 8% | | |

What each bucket is:

- **Kernel time = decode allocation churn.** Pintail makes about 870
  `madvise(MADV_DONTNEED)` calls and about 15,000 page faults per Q2 query,
  and about 2,250 and 37,000 per Q7 query. ClickHouse makes 2 and 66 per Q2.
  Call stacks (`perf record --call-graph dwarf` on
  `syscalls:sys_enter_madvise`) put every one of them under one path:
  `decode_column_chunk` → `read_file_block_*_into` → `decompress_block` →
  `calloc`. That path ends in jemalloc's large-extent zeroing
  (`extent_commit_zero` → `pages_purge_forced`), because each block's
  output buffer is a fresh zeroed allocation of about 84 KiB. Each such
  buffer costs a purge, a refault of its 21 pages, a zero-fill
  (`clear_page_erms`) and TLB-shootdown IPIs to the other workers
  (`asm_sysvec_call_function` is the single largest symbol in Q2 and Q7,
  at about 18%).
- **Validity expansion.** `StrArray::from_dictionary`
  (`crates/pintail-exec/src/array.rs`) builds `validity: Vec<bool>` from
  `ColumnValidity::iter()`. That iterator is a `Box<dyn Iterator>` over
  `repeat_n(true, n)` even when the column is NOT NULL, so every row pays a
  virtual call. The function also copies the code vector. These are the
  `Vec<bool>::from_iter` and `RepeatN::next` symbols, 26% of Q2.
- **Aggregation row loop.** The streaming two-pass aggregate
  (`execution/two_pass.rs`) handles one row at a time:
  - it computes the key bits, and for each lane it matches on a
    `LaneReader` enum, checks validity, and returns `Option<u64>`
    (`LaneReader::bits`, 4-10%);
  - it scatters the row into one of `cores x 4` partition buckets
    (`scatter_two_pass_row`), then replays the buckets into
    `HashMap<(u64,bool), Vec<AggregateState>>`.
  - With 5 groups (Q3) or 40 (Q4), the scatter and replay are pure
    overhead next to a per-thread dense array. With 100k groups (Q6), the
    heavy per-group state and the second pass over the scattered rows cost
    about 480 ms of CPU, where ClickHouse's whole Q6 uses 310 ms.
  - `drop_glue::<ExecError>` shows at 1-5%: per-row `Result`s whose error
    type is not trivially droppable.
- **Q8's fused join aggregate** handles one row and one aggregate at a time
  through `update_state_from_typed_column`
  (`execution/aggregate.rs`). For every row and every aggregate it
  re-resolves the typed column, checks validity, matches on the function,
  and for COUNT builds a `Value::Boolean`. That function alone is 57% of
  Q8. The join itself is cheap (a dense direct-address build on
  `users.id`).
- **Partially selected int blocks decode one row at a time.**
  `decode_int_payload_into` (`crates/pintail-store/src/segment/mod.rs`)
  bulk-unpacks only when a block has no nulls and every row is selected.
  Under a prewhere filter (Q5's 2023 range keeps one 365-row stretch in
  every 1,825 rows), the non-predicate columns go down the slow path: a
  temporary `Vec<u64>` from `unpack`, then per row an `i128` checked add
  and a `builder.push_integer`. That is 25% of Q5 and most of Q7's 16%
  decode.
- **Decimal SUM.** `update_decimal_sum_exact` is 21% of Q5 and 5-7% of Q6
  and Q8. It is a per-row exact accumulation path where a typed i64-into-i128
  loop per group would do.
- **Serial tail in Q6.** After the aggregate, `Project` materializes all
  100,000 groups as `Value` rows (20.6 ms self) before `Sort top_k=10`
  (12.5 ms self). Together that is about 20% of Q6's wall, and it runs on
  one thread.

What ClickHouse spends its time on, for contrast (it is the comparison
target, not a design source):

- Q2: string compare on raw bytes 24%, LZ4 33%, string deserialize 18%.
- Q3-Q7: LZ4 17-60%, then one tight hash-aggregation loop of 22-49% (Q6:
  49% in a two-level hash table keyed on the 4-byte int).
- Q8: about 50% in the join's column gather and probe, 14% in aggregation.
- Kernel 3.5-8%, on every query.

## Build target

Rebuilding the same commit with `RUSTFLAGS="-C target-cpu=x86-64-v3"`
(AVX2/BMI2/FMA) was measured in two alternating rounds against the generic
x86-64 image on the same replica. The minimums moved:

- Q5: 55 → 49-51 ms
- Q7: 118-121 → 111-114 ms
- Q2: 27.5 → 25-26 ms
- Q3, Q4, Q6, Q8: within noise.

That is 0-8%. The hot loops are branchy row-at-a-time code that the
compiler cannot vectorize, so the target only pays once the kernels are
restructured. It is not a lever on its own today.

## Top opportunities, ranked

Estimates are CPU removed per query, read from the profile shares above.
They assume wall time scales with CPU at about 5.5 cores, which held for
every query except Q2.

| # | Opportunity | Owner | Queries | Estimated gain |
|---|---|---|---|---|
| 1 | **Columnar aggregation kernels.** Compute group ids for a whole batch first (dictionary code, or a dense date part, as the slot index), then run one typed update loop per aggregate over the batch: COUNT, i64-unit SUM into i128, MIN/MAX, distinct insert. Low-cardinality keys (up to about 1k slots) get per-thread dense arrays with no scatter or replay. High-cardinality int keys get a flat open-addressing table with states inline, not `Vec<AggregateState>`. Fold the per-row `Option`/`Result` returns out of the inner loop. | **S4** (S1 for the SIMD inner loops once they exist) | Q3, Q4, Q5, Q6, Q7 | Aggregation is 48-54% of Q3/Q4/Q6 CPU and about 28-30% of Q5/Q7. Halving it: Q3 66→~45 ms, Q4 86→~65, Q6 167→~110, Q7 124→~105, Q5 about -10%. |
| 2 | **Stop zero-allocating a decode buffer per block.** Reuse a per-worker scratch buffer for LZ4 output (or decompress straight into the column builder's storage) instead of a fresh `vec![0; n]`/`calloc` per block. That removes the `madvise` purge, the refaults, the zero-fill and the shootdown IPIs. | **S2** | Q2, Q7, Q4, Q3, Q5 | Kernel share is 37% of Q2, 34% of Q7, and 14-19% of Q3/Q4/Q5. Most of it should go. Q2 29→~19 ms, Q7 124→~85, Q4 86→~72, Q3/Q5 about -12%. |
| 3 | **Columnar fused join aggregate.** In the join-into-aggregate path, map each probe batch's keys to the build side's group slot through the dense direct-address table (`users.region` → 8 slots), then reuse #1's batch update. Drop the per-row `update_state_from_typed_column` dispatch. | **S5** (with S4's kernels) | Q8 | 57-77% of Q8 CPU is that loop. Q8 184→~90-100 ms, which is about parity. |
| 4 | **Cheap validity and dictionary hand-off.** Carry `ColumnValidity::AllValid(n)` through `StrDictionary` rather than expanding it to `Vec<bool>` through a boxed iterator. Share or move the code vector instead of copying it. More generally, give `ColumnValidity::iter` a non-boxed, slice-or-count form so NOT NULL columns cost nothing per row. | **S3** (selection/validity), touching S2's hand-off | Q2, Q4, Q3 | 26% of Q2, 10% of Q4, 7% of Q3. Q2 another ~-25% on top of #2. With #2 and #3 together Q2 should reach CPU parity (~45 ms CPU, ~10-12 ms wall). |
| 5 | **Bulk decode under a selection, and typed decimal sums.** When prewhere ranges do not cover a block, bulk-unpack the block (the same `unpack_*_into` the full path uses) and gather the selected ranges with slice copies, instead of the per-row `i128` checked add and `push_integer`. Pair it with an i64-units SUM loop (the accumulation half belongs to #1). | **S2** (decode), **S4** (sum) | Q5, Q7 | Q5: slow-path decode 25% plus exact sum 21%. Q5 57→~35 ms, Q7 about -10%. |

Further, smaller items:

- **Top-k over the aggregate without materializing every group** (S4). Q6
  spends about 33 ms of single-threaded wall projecting 100k groups to
  `Value` rows and then sorting them. A per-partition top-k over the
  finished states, or pushing the threshold into the flush, takes Q6 from
  ~110 (after #1) to ~80 ms.
- **Scan-to-consumer pipelining** (S2). The scan works in rounds and the
  consumer waits for each one. That is visible only on Q2, where the first
  batch arrives after 4.4 ms of a 26 ms query and the query averages 4
  cores. Worth ~3-5 ms on Q2 once #2 and #4 make the rest of Q2 cheap.
- **Build target `x86-64-v3`** (S1). Measured at 0-8% today, as above.
  Revisit after #1 and #5 turn the hot paths into straight-line loops over
  slices. Until then it does not justify losing portability.

No lane owns these, and none of them is a lever on this benchmark:

- **Block skipping, sort keys, granule size.** Neither engine skips a byte,
  because the predicates are on non-key columns whose values cycle every
  1,825 rows. Block min/max pruning of non-key columns, which Pintail
  writes but does not consult, would matter on real data, not here.
- **Storage layout and compression.** Pintail is already smaller on disk
  (321 MB vs 416 MB) and reads fewer decompressed bytes. LZ4 is 1-8% of
  Pintail's profile against 17-60% of ClickHouse's. Dictionary codes stay
  a plain `u32` per row in memory. Bit-packing them would save bandwidth,
  not CPU, at this size.
- **Segment size.** The replica holds 200 segments of 100k rows. Per-segment
  fixed costs did not show in any profile, so the segment size is not
  worth chasing for this benchmark.
- **Thread count.** As measured above, the two rayon pools did not cost
  anything, and fewer scan threads only hurt.

If #1-#5 land at the estimates above, the projected engine track is:

| Query | Now | Projected | CH FINAL |
|---|---:|---:|---:|
| Q2 | ~29 | ~11 | 9 |
| Q3 | ~66 | ~38 | 34 |
| Q4 | ~86 | ~60 | 66 |
| Q5 | ~67 | ~30 | 21 |
| Q6 | ~173 | ~80 | 56 |
| Q7 | ~126 | ~65 | 53 |
| Q8 | ~193 | ~95 | 87 |

That leaves Q5 and Q6 as the two that still need ClickHouse-class inner
loops (#1 done well, plus SIMD), rather than overhead removal.

## Method notes for the lanes

- Use the per-query profile shares as the before/after check:
  - #2 is done when `perf stat -e syscalls:sys_enter_madvise` during a Q2
    loop reads near zero, and the kernel share drops under 8%.
  - #4 is done when `Vec<bool>::from_iter` and `RepeatN::next` leave Q2's
    top symbols.
- Measure CPU per query, not just wall. Wall on this box carried about
  ±8% between alternating restarts of the same image (Q3 63-68 ms, Q8
  178-199 ms), while CPU per query held within about ±5%.
- The ClickHouse FINAL reference costs the same as plain MergeTree here
  (within 5% on every query but Q7, where it is 16% slower). Its parts do not overlap in key range, so
  merge-on-read adds nothing, and the comparison is effectively against
  ClickHouse's plain scan.
