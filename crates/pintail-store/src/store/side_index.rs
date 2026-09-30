//! Experimental secondary side index for one integer column of a segment.
//!
//! Off unless `PINTAIL_SECONDARY_INDEX=1`. A filter on a column that is not
//! the table's key, whose values scatter across the whole segment, touches
//! nearly every block, so block extremes skip nothing and the filter-first
//! scan decodes and tests every row of its predicate columns. The side index
//! holds the column's `(value, row)` pairs sorted by value, built lazily the
//! first time a scan asks for it and cached for the life of the immutable
//! segment file. A scan that knows the only values its rows can hold (an
//! equality, an IN list, a join's key set) asks it for their rows and hands
//! the filter-first decode those rows alone; the scan's own predicates still
//! decide every row, so the index only chooses which rows are looked at.
//!
//! Segment rows only: rows still in the memtable are read as before, and an
//! overlay part masks superseded segment rows among the candidates exactly
//! as it masks them among all rows.

use std::{
    collections::HashMap,
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, atomic::AtomicUsize},
    time::Instant,
};

use pintail_types::TableSchema;

use super::scan::DecodedColumn;
use crate::{StoreError, segment};

/// Candidates past this share of a slice's rows are not worth the detour:
/// the plain filter-first decode reads the same blocks in one pass.
const MAX_CANDIDATE_SHARE: usize = 4;

thread_local! {
    static THREAD_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Whether the experimental side index is on: the process environment, or
/// the calling thread's override. Only the thread that plans a scan asks;
/// the decode follows whatever lookup the scan was given.
#[must_use]
pub fn side_index_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    THREAD_OVERRIDE
        .with(std::cell::Cell::get)
        .unwrap_or_else(|| {
            *ENABLED.get_or_init(|| {
                std::env::var("PINTAIL_SECONDARY_INDEX").is_ok_and(|value| value == "1")
            })
        })
}

/// Switches the side index on or off for scans planned on this thread, or
/// back to the environment's setting with `None`: what lets a test compare
/// both paths in one process.
pub fn override_side_index(enabled: Option<bool>) {
    THREAD_OVERRIDE.with(|cell| cell.set(enabled));
}

/// The values a scan's rows can hold in one integer column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexProbe {
    /// Exactly these values (sorted, deduplicated); NULL is never one.
    Values(Vec<i128>),
    /// Any value in `[lower, upper]`.
    Span(i128, i128),
}

/// A side-index request a scan carries: the column and its probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexLookup {
    pub column_id: u32,
    pub probe: IndexProbe,
}

/// One segment column's non-NULL values sorted with their rows.
pub(crate) struct Postings {
    values: Vec<i64>,
    rows: Vec<u32>,
}

impl Postings {
    fn from_pairs(mut pairs: Vec<(i64, u32)>) -> Self {
        pairs.sort_unstable();
        let (values, rows) = pairs.into_iter().unzip();
        Self { values, rows }
    }

    /// Heap bytes the postings hold.
    pub(crate) fn heap_bytes(&self) -> usize {
        self.values.capacity() * size_of::<i64>() + self.rows.capacity() * size_of::<u32>()
    }

    fn rows_between(&self, lower: i128, upper: i128, out: &mut Vec<u32>) {
        if lower > upper {
            return;
        }
        let start = self
            .values
            .partition_point(|value| i128::from(*value) < lower);
        let end = self
            .values
            .partition_point(|value| i128::from(*value) <= upper);
        if start < end {
            out.extend_from_slice(&self.rows[start..end]);
        }
    }

    /// The rows of `[start, end)` whose value the probe admits, coalesced
    /// into ascending ranges; `None` when they are too many to be worth
    /// reading apart from the rest.
    pub(crate) fn candidate_ranges(
        &self,
        probe: &IndexProbe,
        start: usize,
        end: usize,
    ) -> Option<Vec<Range<usize>>> {
        let mut rows = Vec::new();
        match probe {
            IndexProbe::Values(values) => {
                for value in values {
                    self.rows_between(*value, *value, &mut rows);
                }
            }
            IndexProbe::Span(lower, upper) => self.rows_between(*lower, *upper, &mut rows),
        }
        let slice_rows = end.saturating_sub(start);
        rows.retain(|row| {
            let row = *row as usize;
            row >= start && row < end
        });
        if rows.len().saturating_mul(MAX_CANDIDATE_SHARE) > slice_rows {
            return None;
        }
        rows.sort_unstable();
        let mut ranges: Vec<Range<usize>> = Vec::new();
        for row in rows {
            let row = row as usize;
            match ranges.last_mut() {
                Some(last) if last.end == row => last.end = row + 1,
                _ => ranges.push(row..row + 1),
            }
        }
        Some(ranges)
    }
}

/// Names one segment column's postings. The file name alone is not enough:
/// a table resynchronized into a fresh directory can reuse it, and a type
/// change keeps the file while changing what its values mean, so the
/// segment's identity, versions and schema fingerprint and the column's
/// declared type are part of the key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    path: PathBuf,
    segment_id: u64,
    row_count: u64,
    versions: (u64, u64),
    schema_fingerprint: u64,
    column_id: u32,
    data_type: Option<pintail_types::DataType>,
}

impl CacheKey {
    fn new(
        directory: &Path,
        meta: &segment::SegmentMeta,
        schema: &TableSchema,
        column_id: u32,
    ) -> Self {
        Self {
            path: directory.join(&meta.file_name),
            segment_id: meta.id,
            row_count: meta.row_count,
            versions: (meta.min_version, meta.max_version),
            schema_fingerprint: meta.schema_fingerprint,
            column_id,
            data_type: schema
                .columns()
                .iter()
                .find(|column| column.id() == column_id)
                .map(pintail_types::Column::data_type),
        }
    }
}

/// One slot per segment column: the first scan to reach it builds, and
/// scans reaching it meanwhile wait for that build rather than repeat it.
type Slot = Arc<OnceLock<Option<Arc<Postings>>>>;

/// Default ceiling on the heap the cached postings hold together.
const DEFAULT_CACHE_BYTES: usize = 256 << 20;

/// The bytes the cached postings may hold, from
/// `PINTAIL_SECONDARY_INDEX_CACHE_MB` when set.
fn cache_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("PINTAIL_SECONDARY_INDEX_CACHE_MB")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .map_or(DEFAULT_CACHE_BYTES, |megabytes| {
                megabytes.saturating_mul(1 << 20)
            })
    })
}

struct CacheEntry {
    slot: Slot,
    /// Heap bytes of the built postings; zero until the build finishes.
    bytes: usize,
    last_used: u64,
}

/// The postings cache, bounded by bytes and evicted least recently used
/// first. An evicted entry stays alive for the scans already holding it and
/// is rebuilt by the next scan that asks.
#[derive(Default)]
struct Cache {
    entries: HashMap<CacheKey, CacheEntry>,
    resident: usize,
    clock: u64,
    evictions: u64,
}

impl Cache {
    fn slot(&mut self, key: CacheKey) -> Slot {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.entry(key).or_insert_with(|| CacheEntry {
            slot: Slot::default(),
            bytes: 0,
            last_used: clock,
        });
        entry.last_used = clock;
        Arc::clone(&entry.slot)
    }

    /// Charges a finished build to its entry, then evicts the least recently
    /// used other entries until the total fits the limit again.
    fn charge(&mut self, key: &CacheKey, bytes: usize, limit: usize) {
        // One segment's postings alone past the limit are not kept, and
        // evict nothing else to make room.
        if bytes > limit {
            if self.entries.remove(key).is_some() {
                self.evictions += 1;
            }
            return;
        }
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        self.resident = self.resident - entry.bytes + bytes;
        entry.bytes = bytes;
        while self.resident > limit {
            let victim = self
                .entries
                .iter()
                .filter(|(candidate, entry)| *candidate != key && entry.bytes > 0)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(candidate, _)| candidate.clone());
            let Some(victim) = victim else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&victim) {
                self.resident -= evicted.bytes;
                self.evictions += 1;
            }
        }
    }

    fn forget(&mut self, key: &CacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.resident -= entry.bytes;
        }
    }
}

fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Cache::default()))
}

fn lock_cache() -> std::sync::MutexGuard<'static, Cache> {
    cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The side-index cache as it stands: entries resident, heap bytes they
/// hold, and entries evicted since the process started.
#[must_use]
pub fn side_index_cache_usage() -> (usize, usize, u64) {
    let cache = lock_cache();
    (cache.entries.len(), cache.resident, cache.evictions)
}

/// Totals over every side index built in this process: entries, heap
/// bytes, and build time in microseconds.
#[must_use]
pub fn side_index_totals() -> (usize, usize, u128) {
    let totals = TOTALS.get_or_init(|| Mutex::new((0, 0, 0)));
    *totals
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

static TOTALS: OnceLock<Mutex<(usize, usize, u128)>> = OnceLock::new();

/// The postings of `column_id` in one segment, built on first use. `None`
/// when the column does not decode as a plain integer column (or holds an
/// unsigned value past the signed range), which declines the index.
pub(crate) fn postings(
    directory: &Path,
    meta: &segment::SegmentMeta,
    schema: &TableSchema,
    column_id: u32,
) -> Result<Option<Arc<Postings>>, StoreError> {
    let key = CacheKey::new(directory, meta, schema, column_id);
    let slot = lock_cache().slot(key.clone());
    if let Some(found) = slot.get() {
        return Ok(found.clone());
    }
    let mut failure = None;
    let mut built_here = false;
    let built = slot.get_or_init(|| {
        let started = Instant::now();
        built_here = true;
        match build(directory, meta, schema, column_id) {
            Ok(built) => {
                let built = built.map(Arc::new);
                if let Some(postings) = &built {
                    record_build(meta, column_id, postings, started.elapsed());
                }
                built
            }
            Err(error) => {
                failure = Some(error);
                None
            }
        }
    });
    if let Some(error) = failure {
        // A failed build is not remembered as a decline.
        lock_cache().forget(&key);
        return Err(error);
    }
    if built_here {
        // A decline is remembered at the cost of its entry alone.
        let bytes = built
            .as_ref()
            .map_or(1, |postings| postings.heap_bytes().max(1));
        lock_cache().charge(&key, bytes, cache_limit());
    }
    Ok(built.clone())
}

fn record_build(
    meta: &segment::SegmentMeta,
    column_id: u32,
    postings: &Postings,
    elapsed: std::time::Duration,
) {
    let mut totals = TOTALS
        .get_or_init(|| Mutex::new((0, 0, 0)))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    totals.0 += postings.rows.len();
    totals.1 += postings.heap_bytes();
    totals.2 += elapsed.as_micros();
    pintail_log::log_info!(
        "side index built file={} column={column_id} entries={} bytes={} build_us={} total_entries={} total_bytes={} total_build_us={}",
        meta.file_name,
        postings.rows.len(),
        postings.heap_bytes(),
        elapsed.as_micros(),
        totals.0,
        totals.1,
        totals.2
    );
}

fn build(
    directory: &Path,
    meta: &segment::SegmentMeta,
    schema: &TableSchema,
    column_id: u32,
) -> Result<Option<Postings>, StoreError> {
    let Some(position) = schema
        .columns()
        .iter()
        .position(|column| column.id() == column_id)
    else {
        return Ok(None);
    };
    let Ok(row_count) = usize::try_from(meta.row_count) else {
        return Ok(None);
    };
    if u32::try_from(row_count).is_err() {
        return Ok(None);
    }
    let used = AtomicUsize::new(0);
    let budget = segment::ScanMemoryBudget::new(&used, usize::MAX);
    let fetch = segment::read_projected_columns(
        directory,
        meta,
        schema,
        &[position],
        0,
        row_count,
        &budget,
    )?;
    let mut pairs = Vec::with_capacity(row_count);
    match fetch.columns.first() {
        Some(DecodedColumn::Int64 { values, validity }) => {
            for (row, value) in values.iter().enumerate() {
                if validity.is_valid(row) {
                    pairs.push((*value, u32::try_from(row).unwrap_or(u32::MAX)));
                }
            }
        }
        Some(DecodedColumn::UInt64 { values, validity }) => {
            for (row, value) in values.iter().enumerate() {
                if validity.is_valid(row) {
                    let Ok(value) = i64::try_from(*value) else {
                        return Ok(None);
                    };
                    pairs.push((value, u32::try_from(row).unwrap_or(u32::MAX)));
                }
            }
        }
        _ => return Ok(None),
    }
    Ok(Some(Postings::from_pairs(pairs)))
}

/// Maps ranges over the concatenated candidate rows back to segment rows.
pub(crate) fn absolute_ranges(
    candidates: &[Range<usize>],
    relative: &[Range<usize>],
) -> Vec<Range<usize>> {
    let mut out: Vec<Range<usize>> = Vec::new();
    let mut push = |range: Range<usize>| match out.last_mut() {
        Some(last) if last.end == range.start => last.end = range.end,
        _ => out.push(range),
    };
    let mut candidate = 0_usize;
    let mut offset = 0_usize;
    for range in relative {
        let mut position = range.start;
        while position < range.end && candidate < candidates.len() {
            let span = &candidates[candidate];
            let span_end = offset + span.len();
            if position >= span_end {
                offset = span_end;
                candidate += 1;
                continue;
            }
            let take_end = range.end.min(span_end);
            push(span.start + (position - offset)..span.start + (take_end - offset));
            position = take_end;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_ranges_find_scattered_values_and_decline_common_ones() {
        let pairs = (0..1_000_u32)
            .map(|row| (i64::from(row % 97), row))
            .collect::<Vec<_>>();
        let postings = Postings::from_pairs(pairs);
        let ranges = postings
            .candidate_ranges(&IndexProbe::Values(vec![5, 6]), 0, 1_000)
            .expect("selective");
        let rows = ranges.iter().flat_map(Clone::clone).collect::<Vec<_>>();
        let expected = (0..1_000_usize)
            .filter(|row| matches!(row % 97, 5 | 6))
            .collect::<Vec<_>>();
        assert_eq!(rows, expected);
        assert_eq!(ranges.len(), expected.len() / 2);
        let bounded = postings
            .candidate_ranges(&IndexProbe::Span(5, 5), 100, 300)
            .expect("selective");
        assert_eq!(bounded, vec![102..103, 199..200, 296..297]);
        assert!(
            postings
                .candidate_ranges(&IndexProbe::Span(0, 90), 0, 1_000)
                .is_none()
        );
    }

    #[test]
    fn cache_evicts_least_recently_used_postings_past_its_limit() {
        let key = |column_id| CacheKey {
            path: PathBuf::from("segment"),
            segment_id: 1,
            row_count: 10,
            versions: (1, 2),
            schema_fingerprint: 3,
            column_id,
            data_type: None,
        };
        let mut cache = Cache::default();
        for column in 0..3 {
            let _ = cache.slot(key(column));
            cache.charge(&key(column), 40, 100);
        }
        // The third build pushed the total to 120: the oldest went.
        assert!(!cache.entries.contains_key(&key(0)));
        assert_eq!(
            (cache.entries.len(), cache.resident, cache.evictions),
            (2, 80, 1)
        );
        // Touching column 1 makes column 2 the older one.
        let _ = cache.slot(key(1));
        let _ = cache.slot(key(3));
        cache.charge(&key(3), 40, 100);
        assert!(cache.entries.contains_key(&key(1)));
        assert!(!cache.entries.contains_key(&key(2)));
        // Postings larger than the whole limit are not kept at all.
        let _ = cache.slot(key(4));
        cache.charge(&key(4), 200, 100);
        assert!(!cache.entries.contains_key(&key(4)));
        assert_eq!((cache.entries.len(), cache.resident), (2, 80));
        // A different type for the same column is a different entry.
        let typed = CacheKey {
            data_type: Some(pintail_types::DataType::Int64),
            ..key(5)
        };
        assert_ne!(typed, key(5));
    }

    #[test]
    fn absolute_ranges_split_across_candidate_spans() {
        let candidates = vec![10..13, 20..21, 30..34];
        assert_eq!(
            absolute_ranges(&candidates, &[1..5, 6..8]),
            vec![11..13, 20..21, 30..31, 32..34]
        );
        let whole = [0..8, 8..8];
        assert_eq!(
            absolute_ranges(&candidates, &whole),
            vec![10..13, 20..21, 30..34]
        );
        assert!(absolute_ranges(&candidates, &[]).is_empty());
    }
}
