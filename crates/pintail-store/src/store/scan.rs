//! Projected scans: the row and chunk shapes readers consume, column
//! decoding, and the merged multi-source scan stream.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, atomic::AtomicUsize},
};

use pintail_types::{KeyPart, PrimaryKey, StoredRow};
use rayon::prelude::*;

use super::{TableSnapshot, projected_scan_pool};
use crate::{StoreError, segment, segment::ColumnDecode};

/// A scan row containing only the requested user columns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedRow {
    pub(super) key: PrimaryKey,
    pub(super) values: Vec<pintail_types::Value>,
    pub(super) version: u64,
}

impl ProjectedRow {
    /// Returns the physical primary, unique, or generated row key.
    #[must_use]
    pub fn key(&self) -> &PrimaryKey {
        &self.key
    }

    /// Returns values in the caller's requested column-ID order.
    #[must_use]
    pub fn values(&self) -> &[pintail_types::Value] {
        &self.values
    }

    /// Returns the winning source version.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Moves projected values out of this scan row.
    #[must_use]
    pub fn into_values(self) -> Vec<pintail_types::Value> {
        self.values
    }

    /// Keeps values at the supplied positions in caller order.
    ///
    /// Positions are expected to have been validated against this row's
    /// projected layout.
    #[must_use]
    pub fn project_values(mut self, positions: &[usize]) -> Self {
        self.values = positions
            .iter()
            .map(|position| self.values[*position].clone())
            .collect();
        self
    }

    /// Estimates bytes retained by this projected row.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        size_of::<Self>()
            + std::mem::size_of_val(self.key.parts())
            + self.key.heap_bytes()
            + self.values.capacity() * size_of::<pintail_types::Value>()
            + self
                .values
                .iter()
                .map(pintail_types::Value::heap_bytes)
                .sum::<usize>()
    }
}

/// Physical work performed by a projected range scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScanStats {
    pub(super) segments_pruned: usize,
    pub(super) segments_read: usize,
    pub(super) blocks_pruned: usize,
    pub(super) blocks_read: usize,
    pub(super) blocks_decoded: usize,
    pub(super) bytes_decompressed: u64,
    pub(super) values_decoded: u64,
    pub(super) blocks_value_skipped: usize,
}

impl ScanStats {
    /// Returns the bytes the scan's decoded blocks decompressed to.
    #[must_use]
    pub fn bytes_decompressed(self) -> u64 {
        self.bytes_decompressed
    }

    /// Returns the column values the scan's reads delivered, predicate
    /// columns included.
    #[must_use]
    pub fn values_decoded(self) -> u64 {
        self.values_decoded
    }

    /// Adds what the columns of one read cost to these counters.
    fn with_decode(mut self, decode: &[ColumnDecode]) -> Self {
        for column in decode {
            self.bytes_decompressed = self
                .bytes_decompressed
                .saturating_add(column.bytes_decompressed);
            self.values_decoded = self.values_decoded.saturating_add(column.values_decoded);
        }
        self
    }

    /// Returns row blocks of direct segments skipped because their stored
    /// minimum and maximum prove no row satisfies a range predicate. Each
    /// such block is skipped in every projected column, so it also shows in
    /// [`Self::blocks_pruned`] once per column.
    #[must_use]
    pub fn blocks_value_skipped(self) -> usize {
        self.blocks_value_skipped
    }

    /// Returns segments rejected from manifest key bounds.
    #[must_use]
    pub fn segments_pruned(self) -> usize {
        self.segments_pruned
    }

    /// Returns segments whose block metadata was inspected.
    #[must_use]
    pub fn segments_read(self) -> usize {
        self.segments_read
    }

    /// Returns key blocks rejected by typed zone maps.
    #[must_use]
    pub fn blocks_pruned(self) -> usize {
        self.blocks_pruned
    }

    /// Returns logical primary-key blocks selected by range zone maps.
    #[must_use]
    pub fn blocks_read(self) -> usize {
        self.blocks_read
    }

    /// Returns blocks whose encoded values were decompressed and decoded.
    #[must_use]
    pub fn blocks_decoded(self) -> usize {
        self.blocks_decoded
    }

    pub(super) fn add(&mut self, other: Self) {
        self.segments_pruned += other.segments_pruned;
        self.segments_read += other.segments_read;
        self.blocks_pruned += other.blocks_pruned;
        self.blocks_read += other.blocks_read;
        self.blocks_decoded += other.blocks_decoded;
        self.bytes_decompressed = self
            .bytes_decompressed
            .saturating_add(other.bytes_decompressed);
        self.values_decoded = self.values_decoded.saturating_add(other.values_decoded);
        self.blocks_value_skipped += other.blocks_value_skipped;
    }
}

/// Rows and physical counters from a projected range scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedScan {
    pub(super) rows: Vec<ProjectedRow>,
    pub(super) stats: ScanStats,
    pub(super) retained_bytes: usize,
}

impl ProjectedScan {
    /// Returns visible projected rows in key order.
    #[must_use]
    pub fn rows(&self) -> &[ProjectedRow] {
        &self.rows
    }

    /// Moves visible projected rows into a pull-based consumer.
    #[must_use]
    pub fn into_rows(self) -> Vec<ProjectedRow> {
        self.rows
    }

    /// Returns bytes retained by the projected row set.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns pruning and decoding counters.
    #[must_use]
    pub fn stats(&self) -> ScanStats {
        self.stats
    }
}

/// One contiguous key-range slice of a scan, classified by how its rows
/// become visible: directly (disjoint unique-key segments untouched by the
/// memtable), through a bounded last-write-wins merge over an overlapping
/// cluster, or from the memtable alone (a gap between clusters).
pub(super) enum ScanPart {
    Direct {
        segments: Vec<segment::SegmentMeta>,
    },
    /// A contiguous row range of one segment, provably untouched by newer
    /// segments or the memtable (granule-level sweep classification).
    DirectRange {
        segment: segment::SegmentMeta,
        start_row: u64,
        end_row: u64,
    },
    Merge {
        segments: Vec<segment::SegmentMeta>,
        lo: std::ops::Bound<PrimaryKey>,
        hi: std::ops::Bound<PrimaryKey>,
    },
    /// One unique-key segment, wholly inside the scanned range, whose key
    /// span holds memtable rows. Decoded directly, column by column, with
    /// the rows the memtable supersedes masked out by their key and the
    /// memtable's live rows added; the row-wise merge is not paid. Needs
    /// the table's key column named (see
    /// [`ProjectedScanStream::enable_memtable_overlay`]); without it the
    /// part falls back to a merge over the segment.
    Overlay {
        segment: segment::SegmentMeta,
        /// The rows that overlay the segment: the memtable's when `None`,
        /// else a layered cluster's resolved newer rows.
        rows: Option<LayerRows>,
    },
    MemtableOnly {
        lo: std::ops::Bound<PrimaryKey>,
        hi: std::ops::Bound<PrimaryKey>,
        /// As for [`ScanPart::Overlay`]: the memtable's rows when `None`.
        rows: Option<LayerRows>,
    },
    /// A merge cluster of large base segments, pairwise disjoint and each
    /// with unique keys, under newer rows few enough to hold resolved: the
    /// small segments written after the bases and the memtable, merged into
    /// one row per key (a tombstone kept, since it masks). Served as an
    /// overlay of each base plus those rows in the gaps between bases, so the
    /// bases decode column by column instead of through the row-wise merge.
    /// Without an overlay key to mask by, it is the merge it replaces.
    Layered {
        segments: Vec<segment::SegmentMeta>,
        lo: std::ops::Bound<PrimaryKey>,
        hi: std::ops::Bound<PrimaryKey>,
        /// The base segments, in key order.
        bases: Vec<segment::SegmentMeta>,
        rows: LayerRows,
    },
}

/// Rows resolved to one per key, newest winning, in key order.
pub(super) type LayerRows = Arc<BTreeMap<PrimaryKey, StoredRow>>;

/// The overlay part in progress: the segment's block boundaries, so a slice
/// knows the key span it covers and which memtable rows belong to it.
pub(super) struct OverlayState {
    sparse: Vec<(u64, PrimaryKey)>,
}

/// Whether `key` lies within the inclusive/exclusive bound pair.
/// Row-major values as one column chunk, charged to `memory_limit`.
fn values_chunk(
    columns: Vec<Vec<pintail_types::Value>>,
    row_count: usize,
    memory_limit: usize,
) -> Result<ProjectedColumnChunk, StoreError> {
    let retained_bytes = size_of::<ProjectedColumnChunk>()
        .saturating_add(
            columns
                .capacity()
                .saturating_mul(size_of::<Vec<pintail_types::Value>>()),
        )
        .saturating_add(
            columns
                .iter()
                .map(|values| {
                    values
                        .capacity()
                        .saturating_mul(size_of::<pintail_types::Value>())
                        .saturating_add(values.iter().map(pintail_types::Value::heap_bytes).sum())
                })
                .sum(),
        );
    if retained_bytes > memory_limit {
        return Err(StoreError::MemoryLimitExceeded {
            used: 0,
            requested: retained_bytes,
            limit: memory_limit,
        });
    }
    Ok(ProjectedColumnChunk {
        prefiltered: false,
        columns: columns.into_iter().map(DecodedColumn::Values).collect(),
        row_count,
        stats: ScanStats::default(),
        retained_bytes,
        column_decode: Vec::new(),
    })
}

/// The integer at `row` of a decoded key column, whatever shape the decode
/// produced it in.
fn integer_at(column: &DecodedColumn, row: usize) -> Option<i128> {
    match column {
        DecodedColumn::Int64 { values, .. } | DecodedColumn::NativeUnits { values, .. } => {
            values.get(row).map(|value| i128::from(*value))
        }
        DecodedColumn::UInt64 { values, .. } => values.get(row).map(|value| i128::from(*value)),
        DecodedColumn::Values(values) => match values.get(row)? {
            pintail_types::Value::Int64(value) => Some(i128::from(*value)),
            pintail_types::Value::UInt64(value) => Some(i128::from(*value)),
            _ => None,
        },
        _ => None,
    }
}

impl DecodedColumn {
    /// This column with `inserts` placed at their final positions
    /// (ascending, indexing the output). Integer columns stay packed when
    /// every inserted value is of their type or null; any other column, or
    /// a mismatched value, is rebuilt as plain values.
    pub(super) fn interleave(self, inserts: &[(usize, &pintail_types::Value)]) -> Self {
        if inserts.is_empty() {
            return self;
        }
        match self {
            Self::Int64 { values, validity } => {
                match typed_inserts(inserts, |value| match value {
                    pintail_types::Value::Int64(value) => Some(Some(*value)),
                    pintail_types::Value::Null => Some(None),
                    _ => None,
                }) {
                    Some(typed) => {
                        let (values, validity) = interleave_typed(values, &validity, &typed);
                        Self::Int64 { values, validity }
                    }
                    None => Self::Values(interleave_values(
                        Self::Int64 { values, validity }.into_values(),
                        inserts,
                    )),
                }
            }
            Self::UInt64 { values, validity } => {
                match typed_inserts(inserts, |value| match value {
                    pintail_types::Value::UInt64(value) => Some(Some(*value)),
                    pintail_types::Value::Null => Some(None),
                    _ => None,
                }) {
                    Some(typed) => {
                        let (values, validity) = interleave_typed(values, &validity, &typed);
                        Self::UInt64 { values, validity }
                    }
                    None => Self::Values(interleave_values(
                        Self::UInt64 { values, validity }.into_values(),
                        inserts,
                    )),
                }
            }
            other => Self::Values(interleave_values(other.into_values(), inserts)),
        }
    }
}

/// The inserts as typed values (`None` for null), or `None` when one does
/// not fit the column's type.
fn typed_inserts<T>(
    inserts: &[(usize, &pintail_types::Value)],
    convert: impl Fn(&pintail_types::Value) -> Option<Option<T>>,
) -> Option<Vec<(usize, Option<T>)>> {
    inserts
        .iter()
        .map(|(at, value)| convert(value).map(|value| (*at, value)))
        .collect()
}

/// `values` with typed `inserts` placed at their final positions; nulls
/// take a default placeholder and clear their validity bit.
fn interleave_typed<T: Copy + Default>(
    values: Vec<T>,
    validity: &ColumnValidity,
    inserts: &[(usize, Option<T>)],
) -> (Vec<T>, ColumnValidity) {
    let total = values.len() + inserts.len();
    let mut out = Vec::with_capacity(total);
    let mut valid = Vec::with_capacity(total);
    let mut existing = values.into_iter().enumerate();
    let mut pending = existing.next();
    let mut next_insert = 0;
    while out.len() < total {
        let at = out.len();
        if next_insert < inserts.len() && inserts[next_insert].0 == at {
            let (_, value) = inserts[next_insert];
            out.push(value.unwrap_or_default());
            valid.push(value.is_some());
            next_insert += 1;
        } else if let Some((index, value)) = pending {
            out.push(value);
            valid.push(validity.is_valid(index));
            pending = existing.next();
        } else {
            // An insert position past the end: append the rest in order.
            let (_, value) = inserts[next_insert];
            out.push(value.unwrap_or_default());
            valid.push(value.is_some());
            next_insert += 1;
        }
    }
    let validity = if valid.iter().all(|flag| *flag) {
        ColumnValidity::AllValid(total)
    } else {
        ColumnValidity::Bytes(valid)
    };
    (out, validity)
}

/// Plain values with `inserts` placed at their final positions.
fn interleave_values(
    values: Vec<pintail_types::Value>,
    inserts: &[(usize, &pintail_types::Value)],
) -> Vec<pintail_types::Value> {
    let total = values.len() + inserts.len();
    let mut out = Vec::with_capacity(total);
    let mut existing = values.into_iter();
    let mut next_insert = 0;
    while out.len() < total {
        let at = out.len();
        if next_insert < inserts.len() && inserts[next_insert].0 == at {
            out.push(inserts[next_insert].1.clone());
            next_insert += 1;
        } else if let Some(value) = existing.next() {
            out.push(value);
        } else {
            out.push(inserts[next_insert].1.clone());
            next_insert += 1;
        }
    }
    out
}

/// The integer key of `row` compared with a memtable key, part by part.
fn compare_row_key(key_columns: &[&DecodedColumn], row: usize, key: &[i128]) -> std::cmp::Ordering {
    for (column, part) in key_columns.iter().zip(key) {
        match integer_at(column, row) {
            Some(value) => match value.cmp(part) {
                std::cmp::Ordering::Equal => {}
                other => return other,
            },
            // A key column that does not decode as an integer cannot match
            // any memtable key; order it first so the walk moves on.
            None => return std::cmp::Ordering::Less,
        }
    }
    std::cmp::Ordering::Equal
}

/// One walk over the slice's rows (sorted by key, one row per key) and the
/// memtable's rows for the same span (sorted the same way): the positions
/// of the segment rows a memtable key supersedes, and for each live memtable
/// row its position in the output that interleaves it with the surviving
/// rows, which are the kept rows not superseded before it plus the memtable
/// rows placed before it.
/// Rows the memtable supersedes, found by looking up each of its keys
/// rather than by walking the segment.
///
/// The walk below costs the segment whatever changed, so a table that took
/// two updates pays what one that took two million pays. Both sides are
/// sorted and the segment's keys are searchable, so the same answer can be
/// had for the cost of the change instead: measured over ten million rows,
/// twenty thousand changes cost 1.9 ms this way against 6.7 ms walking, and
/// two changes cost microseconds. Past roughly a twentieth of the segment
/// the walk is cheaper again, which is what `overlay_positions` decides.
fn searched_overlay_positions(
    key_columns: &[&DecodedColumn],
    row_count: usize,
    kept: Option<&[std::ops::Range<usize>]>,
    memtable: &[(Vec<i128>, Option<&StoredRow>)],
) -> (Vec<usize>, Vec<usize>) {
    // Kept rows before a position, from the ranges rather than by counting:
    // a prefix sum over the ranges answers it in a binary search.
    let prefix: Vec<usize> = kept
        .map(|ranges| {
            let mut total = 0;
            ranges
                .iter()
                .map(|range| {
                    let before = total;
                    total += range.len();
                    before
                })
                .collect()
        })
        .unwrap_or_default();
    let kept_before = |position: usize| -> usize {
        let Some(ranges) = kept else { return position };
        // The last range starting at or before `position`.
        let index = ranges.partition_point(|range| range.start < position);
        let mut count = if index == 0 { 0 } else { prefix[index - 1] };
        if index > 0 {
            let range = &ranges[index - 1];
            count += position.min(range.end).saturating_sub(range.start);
        }
        count
    };
    let is_kept = |row: usize| -> bool {
        let Some(ranges) = kept else { return true };
        let index = ranges.partition_point(|range| range.end <= row);
        ranges
            .get(index)
            .is_some_and(|range| range.start <= row && row < range.end)
    };
    // Where a key sits in the segment, or where it would be inserted.
    let search = |key: &[i128]| -> Result<usize, usize> {
        let mut low = 0_usize;
        let mut high = row_count;
        while low < high {
            let middle = low + (high - low) / 2;
            match compare_row_key(key_columns, middle, key) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(middle),
            }
        }
        Err(low)
    };

    let mut excluded = Vec::new();
    let mut inserts = Vec::new();
    // Excluded rows already passed that were kept: the walk never counts an
    // excluded row as a survivor, so neither does this.
    let mut excluded_kept = 0_usize;
    for (key, row) in memtable {
        // The walk counts survivors strictly before the row it is looking
        // at, and never counts an excluded row, so this row's own exclusion
        // is added only after its insert position is decided.
        let (position, excludes_this_row) = match search(key) {
            Ok(found) => {
                excluded.push(found);
                (found, usize::from(is_kept(found)))
            }
            Err(insertion) => (insertion, 0),
        };
        if row.is_some() {
            let survivors_before = kept_before(position).saturating_sub(excluded_kept);
            inserts.push(survivors_before + inserts.len());
        }
        excluded_kept += excludes_this_row;
    }
    (excluded, inserts)
}

/// One row in two hundred: past this share of the segment, looking each
/// change up costs more than walking both sides once.
///
/// The first value here was one in twenty, taken from a measurement that
/// timed a mask built block by block rather than the whole-column lookup
/// this actually does. Timed against the real thing, a walk of ten million
/// keys costs about four milliseconds whatever changed, while the lookups
/// grow with the changes and pass it at one percent. Half of that is the
/// threshold, so the search is chosen only where it clearly wins rather
/// than where the two are level.
const SEARCHED_OVERLAY_SHARE: usize = 200;

fn overlay_positions(
    key_columns: &[&DecodedColumn],
    row_count: usize,
    kept: Option<&[std::ops::Range<usize>]>,
    memtable: &[(Vec<i128>, Option<&StoredRow>)],
) -> (Vec<usize>, Vec<usize>) {
    if memtable.len().saturating_mul(SEARCHED_OVERLAY_SHARE) <= row_count {
        return searched_overlay_positions(key_columns, row_count, kept, memtable);
    }
    let mut excluded = Vec::new();
    let mut inserts = Vec::new();
    let mut survivors_before = 0_usize;
    let mut range_index = 0_usize;
    let mut is_kept = |row: usize| -> bool {
        let Some(ranges) = kept else { return true };
        while range_index < ranges.len() && ranges[range_index].end <= row {
            range_index += 1;
        }
        ranges
            .get(range_index)
            .is_some_and(|range| range.start <= row && row < range.end)
    };
    let mut row = 0_usize;
    let mut next = 0_usize;
    while row < row_count && next < memtable.len() {
        match compare_row_key(key_columns, row, &memtable[next].0) {
            std::cmp::Ordering::Less => {
                if is_kept(row) {
                    survivors_before += 1;
                }
                row += 1;
            }
            std::cmp::Ordering::Equal => {
                // The memtable's version replaces the segment's, at the
                // segment row's place (or is gone, for a tombstone).
                excluded.push(row);
                if memtable[next].1.is_some() {
                    inserts.push(survivors_before + inserts.len());
                }
                is_kept(row);
                row += 1;
                next += 1;
            }
            std::cmp::Ordering::Greater => {
                // A key the segment does not hold: an insert.
                if memtable[next].1.is_some() {
                    inserts.push(survivors_before + inserts.len());
                }
                next += 1;
            }
        }
    }
    while row < row_count {
        if is_kept(row) {
            survivors_before += 1;
        }
        row += 1;
    }
    while next < memtable.len() {
        if memtable[next].1.is_some() {
            inserts.push(survivors_before + inserts.len());
        }
        next += 1;
    }
    (excluded, inserts)
}

/// `ranges` with the ascending `excluded` positions cut out.
fn subtract_positions(
    ranges: Vec<std::ops::Range<usize>>,
    excluded: &[usize],
) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::with_capacity(ranges.len() + excluded.len());
    let mut next = 0;
    for range in ranges {
        let mut cursor = range.start;
        while next < excluded.len() && excluded[next] < range.start {
            next += 1;
        }
        while next < excluded.len() && excluded[next] < range.end {
            if excluded[next] > cursor {
                out.push(cursor..excluded[next]);
            }
            cursor = excluded[next] + 1;
            next += 1;
        }
        if cursor < range.end {
            out.push(cursor..range.end);
        }
    }
    out
}

/// Whether `key` lies beyond the upper bound `hi`.
fn bound_below(hi: &std::ops::Bound<PrimaryKey>, key: &PrimaryKey) -> bool {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    match hi {
        Included(bound) => key > bound,
        Excluded(bound) => key >= bound,
        Unbounded => false,
    }
}

fn bounds_contain(
    lo: &std::ops::Bound<PrimaryKey>,
    hi: &std::ops::Bound<PrimaryKey>,
    key: &PrimaryKey,
) -> bool {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    (match lo {
        Included(bound) => key >= bound,
        Excluded(bound) => key > bound,
        Unbounded => true,
    }) && (match hi {
        Included(bound) => key <= bound,
        Excluded(bound) => key < bound,
        Unbounded => true,
    })
}

/// Rows a direct segment is handed to the decoders in. A slice is the
/// scan's work unit: parallel width comes from how many slices are in
/// flight, not from how many segments there are, and the rows in flight
/// are bounded by width times this whatever the segment size. It matches
/// the executor's largest pass-through batch, so a slice becomes one batch.
pub(super) const DIRECT_SLICE_ROWS: u64 = 131_072;

/// One unit of direct-segment decode work.
#[derive(Clone, Debug)]
pub(super) enum DirectSlice {
    /// A segment decoded as it was before slicing: small enough to be one
    /// slice, or only partly inside the scanned key range.
    Whole(segment::SegmentMeta),
    /// A block-aligned row range of a segment that lies wholly inside the
    /// scanned key range.
    Range {
        segment: segment::SegmentMeta,
        start_row: u64,
        end_row: u64,
    },
}

/// Pull-based projected scan over immutable segments and WAL-backed rows.
///
/// The scanned key range is partitioned into [`ScanPart`]s at open time;
/// merge cost is paid only inside clusters whose key ranges actually overlap
/// (docs/decisions.md, "Merge-on-read uses granule-level sweep-line
/// classification").
pub struct ProjectedScanStream {
    pub(super) snapshot: TableSnapshot,
    pub(super) segments: Vec<segment::SegmentMeta>,
    pub(super) start: PrimaryKey,
    pub(super) end: PrimaryKey,
    pub(super) column_ids: Vec<u32>,
    pub(super) next_segment: usize,
    pub(super) pruned_segments: usize,
    pub(super) candidate_segments: usize,
    pub(super) reported_pruned: bool,
    pub(super) parts: std::collections::VecDeque<ScanPart>,
    pub(super) memtable_cursor: Option<(std::ops::Bound<PrimaryKey>, std::ops::Bound<PrimaryKey>)>,
    /// The rows the current overlay or memtable-only part reads: the
    /// memtable's, or a layered cluster's resolved rows.
    pub(super) overlay_rows: LayerRows,
    pub(super) direct_range: Option<(segment::SegmentMeta, u64, u64)>,
    /// Rows per slice that last fit the budget for the pending direct range.
    pub(super) direct_slice_rows: Option<u64>,
    /// Direct-segment work units not yet decoded, cut from the segments of
    /// the current part as they are reached.
    pub(super) slices: VecDeque<DirectSlice>,
    pub(super) merge: Option<MergedProjectedStream>,
    /// The user column that carries the table's single integer key, when
    /// the caller named it; what lets an [`ScanPart::Overlay`] mask the rows
    /// the memtable supersedes from a packed column instead of merging.
    pub(super) overlay_key: Option<Vec<u32>>,
    pub(super) overlay: Option<OverlayState>,
    /// Chunks an overlay slice produced beyond the one the single-chunk
    /// API could hand out, waiting their turn.
    pub(super) pending: VecDeque<ProjectedColumnChunk>,
    /// The side-index request the scan's predicates or a join
    /// gave it; consulted only while the index is switched on.
    pub(super) index_lookup: Option<super::side_index::IndexLookup>,
    /// The scan predicates' value bounds, consulted against each direct
    /// block's stored extremes on the filter-first path.
    pub(super) value_bounds: Vec<segment::ColumnBounds>,
}

pub(super) struct MergedProjectedStream {
    streams: Vec<segment::SegmentRowStream>,
    heads: Vec<Option<segment::SegmentRowHeader>>,
    memtable_head: Option<StoredRow>,
    reported_segments: bool,
    lo: std::ops::Bound<PrimaryKey>,
    hi: std::ops::Bound<PrimaryKey>,
}

/// Whether `BTreeMap::range((lo, hi))` may be called without panicking and
/// can yield rows: rejects inverted ranges and the empty equal-bound forms.
pub(super) fn bound_range_is_searchable(
    lo: &std::ops::Bound<PrimaryKey>,
    hi: &std::ops::Bound<PrimaryKey>,
) -> bool {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let lo_key = match lo {
        Included(key) | Excluded(key) => key,
        Unbounded => return true,
    };
    let hi_key = match hi {
        Included(key) | Excluded(key) => key,
        Unbounded => return true,
    };
    match lo_key.cmp(hi_key) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => matches!((lo, hi), (Included(_), Included(_))),
    }
}

enum MergedWinnerSource {
    Segment {
        segment_index: usize,
        row_index: usize,
    },
    Memtable(Vec<pintail_types::Value>),
}

/// One bounded set of projected values from an independently visible segment.
pub struct ProjectedValueChunk {
    rows: Vec<Vec<pintail_types::Value>>,
    stats: ScanStats,
    retained_bytes: usize,
}

/// Chooses surviving row ranges from a chunk's decoded predicate columns:
/// `Ok(None)` keeps every row (no restriction); ranges must be ascending and
/// disjoint. Errors abort the scan.
pub type PrewhereSelect<'a> =
    &'a (dyn Fn(&[DecodedColumn], usize) -> Result<Option<PrewhereRanges>, String> + Sync);

/// The rows a prewhere selector keeps from one chunk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrewhereRanges {
    /// Surviving row ranges, ascending and disjoint.
    pub ranges: Vec<std::ops::Range<usize>>,
    /// Whether every row inside `ranges` satisfies the selector's whole
    /// predicate. A selector that widens its ranges to decode fewer, larger
    /// regions keeps rows that fail, and must say so here; a chunk decoded
    /// from exact ranges alone is [`ProjectedColumnChunk::prefiltered`].
    pub exact: bool,
}

impl From<Vec<std::ops::Range<usize>>> for PrewhereRanges {
    /// Ranges that restrict the decode but promise nothing about the rows.
    fn from(ranges: Vec<std::ops::Range<usize>>) -> Self {
        Self {
            ranges,
            exact: false,
        }
    }
}

/// One projected column decoded straight into packed columnar storage.
///
/// Typed variants pad null slots with defaults and carry per-row validity so
/// a columnar executor can adopt them without materializing per-row values;
/// `Values` is the row-value fallback for shapes without a packed layout
/// Per-row validity of a decoded column.
///
/// Every NOT NULL column - the common case - used to carry a byte per row
/// that was uniformly true: 20MB per 20M-row column, written by the decoder
/// and scanned again by the executor's mask builder, all to say "no nulls".
/// All-valid is now a count, produced and consumed without touching memory
/// per row. Columns that really hold nulls keep the byte-per-row form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnValidity {
    /// Every row is valid; this many rows.
    AllValid(usize),
    /// Per-row validity, `true` = non-null.
    Bytes(Vec<bool>),
}

impl<'validity> IntoIterator for &'validity ColumnValidity {
    type Item = bool;
    type IntoIter = ValidityIter<'validity>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl ColumnValidity {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::AllValid(count) => *count,
            Self::Bytes(bytes) => bytes.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn is_valid(&self, row: usize) -> bool {
        match self {
            Self::AllValid(count) => row < *count,
            Self::Bytes(bytes) => bytes.get(row).copied().unwrap_or(false),
        }
    }

    /// Rows the backing store could hold without growing.
    #[must_use]
    pub fn capacity(&self) -> usize {
        match self {
            Self::AllValid(count) => *count,
            Self::Bytes(bytes) => bytes.capacity(),
        }
    }

    /// Whether no row is null - the executor's fast paths key off this.
    #[must_use]
    pub fn all_valid(&self) -> bool {
        match self {
            Self::AllValid(_) => true,
            Self::Bytes(bytes) => bytes.iter().all(|valid| *valid),
        }
    }

    /// Per-row validity, without a call through a trait object per row.
    #[must_use]
    pub fn iter(&self) -> ValidityIter<'_> {
        match self {
            Self::AllValid(count) => ValidityIter::AllValid(std::iter::repeat_n(true, *count)),
            Self::Bytes(bytes) => ValidityIter::Bytes(bytes.iter().copied()),
        }
    }

    /// Splits off the tail at `at`, mirroring `Vec::split_off` so decoded
    /// columns slice into batches without expanding the all-valid form.
    #[must_use]
    pub fn split_off(&mut self, at: usize) -> Self {
        match self {
            Self::AllValid(count) => {
                let tail = count.saturating_sub(at);
                *count = at.min(*count);
                Self::AllValid(tail)
            }
            Self::Bytes(bytes) => Self::Bytes(bytes.split_off(at)),
        }
    }

    /// The byte-per-row form, for consumers not yet migrated.
    #[must_use]
    pub fn into_bytes(self) -> Vec<bool> {
        match self {
            Self::AllValid(count) => vec![true; count],
            Self::Bytes(bytes) => bytes,
        }
    }
}

/// Iterator over a [`ColumnValidity`]'s rows, `true` = non-null.
#[derive(Clone, Debug)]
pub enum ValidityIter<'validity> {
    /// Every row valid.
    AllValid(std::iter::RepeatN<bool>),
    /// One byte per row.
    Bytes(std::iter::Copied<std::slice::Iter<'validity, bool>>),
}

impl Iterator for ValidityIter<'_> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<bool> {
        match self {
            Self::AllValid(rows) => rows.next(),
            Self::Bytes(rows) => rows.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::AllValid(rows) => rows.size_hint(),
            Self::Bytes(rows) => rows.size_hint(),
        }
    }
}

impl ExactSizeIterator for ValidityIter<'_> {}

/// Moves the first `count` elements out of `values` into a vector sized to
/// exactly `count`, leaving the tail in place.
fn split_prefix<T>(values: &mut Vec<T>, count: usize) -> Vec<T> {
    let rest = values.split_off(count);
    let mut prefix = std::mem::replace(values, rest);
    prefix.shrink_to_fit();
    prefix
}

fn split_validity_prefix(validity: &mut ColumnValidity, count: usize) -> ColumnValidity {
    let rest = validity.split_off(count);
    let prefix = std::mem::replace(validity, rest);
    match prefix {
        ColumnValidity::Bytes(mut bytes) => {
            bytes.shrink_to_fit();
            ColumnValidity::Bytes(bytes)
        }
        all_valid @ ColumnValidity::AllValid(_) => all_valid,
    }
}

/// (Boolean, Binary, merged or memtable rows).
#[derive(Clone, Debug)]
pub enum DecodedColumn {
    /// Row values, one per row.
    Values(Vec<pintail_types::Value>),
    /// Packed signed integers; null slots hold zero.
    Int64 {
        /// One packed value per row.
        values: Vec<i64>,
        /// Per-row null mask (`true` = non-null).
        validity: ColumnValidity,
    },
    /// Packed unsigned integers; null slots hold zero.
    UInt64 {
        /// One packed value per row.
        values: Vec<u64>,
        /// Per-row null mask (`true` = non-null).
        validity: ColumnValidity,
    },
    /// Packed IEEE-754 bit patterns; null slots hold zero.
    Float64 {
        /// One packed bit pattern per row.
        bits: Vec<u64>,
        /// Per-row null mask (`true` = non-null).
        validity: ColumnValidity,
    },
    /// Fixed-width native units decoded from a PTSEG v2 column; canonical
    /// text regenerates through `units.format` only where a consumer needs
    /// it.
    NativeUnits {
        /// The unit interpretation (date days, datetime micros, or scaled
        /// decimal) tied to the column's schema type.
        units: crate::segment::NativeUnits,
        /// One packed unit value per row; null slots hold zero.
        values: Vec<i64>,
        /// Per-row null mask (`true` = non-null).
        validity: ColumnValidity,
    },
    /// Dictionary-coded UTF-8: `codes[i]` indexes the (small) distinct-entry
    /// arena; null rows hold code 0 with `validity` false. Produced when a
    /// column's blocks arrive dictionary-encoded, so 20M rows of a 5-value
    /// column ship as 20M u32s plus a few entry bytes.
    DictionaryUtf8 {
        /// Distinct entry bytes.
        dict_heap: Vec<u8>,
        /// `entries + 1` boundaries into `dict_heap`.
        dict_offsets: Vec<usize>,
        /// One entry index per row.
        codes: Vec<u32>,
        /// Per-row null mask (`true` = non-null).
        validity: ColumnValidity,
    },
    /// UTF-8 bytes in one arena; row `i` spans `heap[offsets[i]..offsets[i+1]]`
    /// and null rows span zero bytes.
    Utf8 {
        /// Concatenated UTF-8 payloads.
        heap: Vec<u8>,
        /// `len + 1` row boundaries into `heap`.
        offsets: Vec<usize>,
        /// Per-row null mask (`true` = non-null).
        validity: ColumnValidity,
    },
}

impl DecodedColumn {
    /// Returns the number of rows in the column.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Values(values) => values.len(),
            Self::Int64 { validity, .. }
            | Self::UInt64 { validity, .. }
            | Self::Float64 { validity, .. }
            | Self::NativeUnits { validity, .. }
            | Self::DictionaryUtf8 { validity, .. }
            | Self::Utf8 { validity, .. } => validity.len(),
        }
    }

    /// Returns whether the column has no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimates bytes retained by the column's owned buffers.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        match self {
            Self::Values(values) => values
                .capacity()
                .saturating_mul(size_of::<pintail_types::Value>())
                .saturating_add(values.iter().map(pintail_types::Value::heap_bytes).sum()),
            Self::Int64 { values, validity }
            | Self::NativeUnits {
                values, validity, ..
            } => values
                .capacity()
                .saturating_mul(size_of::<i64>())
                .saturating_add(validity.capacity()),
            Self::UInt64 { values, validity } => values
                .capacity()
                .saturating_mul(size_of::<u64>())
                .saturating_add(validity.capacity()),
            Self::Float64 { bits, validity } => bits
                .capacity()
                .saturating_mul(size_of::<u64>())
                .saturating_add(validity.capacity()),
            Self::Utf8 {
                heap,
                offsets,
                validity,
            } => heap
                .capacity()
                .saturating_add(offsets.capacity().saturating_mul(size_of::<usize>()))
                .saturating_add(validity.capacity()),
            Self::DictionaryUtf8 {
                dict_heap,
                dict_offsets,
                codes,
                validity,
            } => dict_heap
                .capacity()
                .saturating_add(dict_offsets.capacity().saturating_mul(size_of::<usize>()))
                .saturating_add(codes.capacity().saturating_mul(size_of::<u32>()))
                .saturating_add(validity.capacity()),
        }
    }

    /// Splits off the first `count` rows (clamped to the column length),
    /// leaving the remainder in place. Used by executors slicing one decoded
    /// chunk into fixed-size batches.
    ///
    /// The prefix is right-sized. `Vec::split_off` hands the tail a fresh
    /// exact allocation and leaves the head holding the WHOLE original
    /// capacity, so a 1M-row segment sliced into sixteen 64K-row batches
    /// used to retain sixteen prefixes of 1M, 940K, 875K ... rows each -
    /// about eight times the segment - for as long as those batches lived.
    /// Measured on a two-column 1M-row segment: 118 MB retained for 16 MB
    /// of data, which is what made a plain GROUP BY under the shipped
    /// ceiling fail on its first pull.
    #[must_use]
    pub fn take_prefix(&mut self, count: usize) -> Self {
        let count = count.min(self.len());
        match self {
            Self::Values(values) => Self::Values(split_prefix(values, count)),
            Self::Int64 { values, validity } => Self::Int64 {
                values: split_prefix(values, count),
                validity: split_validity_prefix(validity, count),
            },
            Self::NativeUnits {
                units,
                values,
                validity,
            } => Self::NativeUnits {
                units: *units,
                values: split_prefix(values, count),
                validity: split_validity_prefix(validity, count),
            },
            Self::UInt64 { values, validity } => Self::UInt64 {
                values: split_prefix(values, count),
                validity: split_validity_prefix(validity, count),
            },
            Self::Float64 { bits, validity } => Self::Float64 {
                bits: split_prefix(bits, count),
                validity: split_validity_prefix(validity, count),
            },
            Self::DictionaryUtf8 {
                dict_heap,
                dict_offsets,
                codes,
                validity,
            } => Self::DictionaryUtf8 {
                dict_heap: dict_heap.clone(),
                dict_offsets: dict_offsets.clone(),
                codes: split_prefix(codes, count),
                validity: split_validity_prefix(validity, count),
            },
            Self::Utf8 {
                heap,
                offsets,
                validity,
            } => {
                let cut = offsets[count];
                let rest_offsets = offsets[count..]
                    .iter()
                    .map(|offset| offset - cut)
                    .collect::<Vec<_>>();
                offsets.truncate(count + 1);
                let mut prefix_offsets = std::mem::replace(offsets, rest_offsets);
                prefix_offsets.shrink_to_fit();
                Self::Utf8 {
                    heap: split_prefix(heap, cut),
                    offsets: prefix_offsets,
                    validity: split_validity_prefix(validity, count),
                }
            }
        }
    }

    /// Materializes one row's value, or `None` past the end.
    ///
    /// # Panics
    ///
    /// Panics if stored native units cannot regenerate their text, which the
    /// writer's round-trip probe makes impossible.
    #[must_use]
    pub fn value_at(&self, row: usize) -> Option<pintail_types::Value> {
        if row >= self.len() {
            return None;
        }
        Some(match self {
            Self::Values(values) => values[row].clone(),
            Self::Int64 { values, validity } => {
                if validity.is_valid(row) {
                    pintail_types::Value::Int64(values[row])
                } else {
                    pintail_types::Value::Null
                }
            }
            Self::UInt64 { values, validity } => {
                if validity.is_valid(row) {
                    pintail_types::Value::UInt64(values[row])
                } else {
                    pintail_types::Value::Null
                }
            }
            Self::Float64 { bits, validity } => {
                if validity.is_valid(row) {
                    pintail_types::Value::Float64(pintail_types::Float64::new(f64::from_bits(
                        bits[row],
                    )))
                } else {
                    pintail_types::Value::Null
                }
            }
            Self::NativeUnits {
                units,
                values,
                validity,
            } => {
                if validity.is_valid(row) {
                    let text = units
                        .format(values[row])
                        .expect("stored native units round-trip");
                    pintail_types::Value::Utf8(text)
                } else {
                    pintail_types::Value::Null
                }
            }
            Self::DictionaryUtf8 {
                dict_heap,
                dict_offsets,
                codes,
                validity,
            } => {
                if validity.is_valid(row) {
                    let code = codes[row] as usize;
                    let bytes = dict_heap[dict_offsets[code]..dict_offsets[code + 1]].to_vec();
                    let text = String::from_utf8(bytes).unwrap_or_else(|error| {
                        String::from_utf8_lossy(error.as_bytes()).into_owned()
                    });
                    pintail_types::Value::Utf8(text)
                } else {
                    pintail_types::Value::Null
                }
            }
            Self::Utf8 {
                heap,
                offsets,
                validity,
            } => {
                if validity.is_valid(row) {
                    let bytes = heap[offsets[row]..offsets[row + 1]].to_vec();
                    let text = String::from_utf8(bytes).unwrap_or_else(|error| {
                        String::from_utf8_lossy(error.as_bytes()).into_owned()
                    });
                    pintail_types::Value::Utf8(text)
                } else {
                    pintail_types::Value::Null
                }
            }
        })
    }

    /// Materializes the column into per-row values.
    ///
    /// # Panics
    ///
    /// Panics if stored native units cannot regenerate their text, which the
    /// writer's round-trip probe makes impossible.
    #[must_use]
    pub fn into_values(self) -> Vec<pintail_types::Value> {
        match self {
            Self::Values(values) => values,
            Self::Int64 { values, validity } => values
                .into_iter()
                .zip(validity.iter())
                .map(|(value, valid)| {
                    if valid {
                        pintail_types::Value::Int64(value)
                    } else {
                        pintail_types::Value::Null
                    }
                })
                .collect(),
            Self::UInt64 { values, validity } => values
                .into_iter()
                .zip(validity.iter())
                .map(|(value, valid)| {
                    if valid {
                        pintail_types::Value::UInt64(value)
                    } else {
                        pintail_types::Value::Null
                    }
                })
                .collect(),
            Self::Float64 { bits, validity } => bits
                .into_iter()
                .zip(validity.iter())
                .map(|(bits, valid)| {
                    if valid {
                        pintail_types::Value::Float64(pintail_types::Float64::new(f64::from_bits(
                            bits,
                        )))
                    } else {
                        pintail_types::Value::Null
                    }
                })
                .collect(),
            Self::NativeUnits {
                units,
                values,
                validity,
            } => values
                .into_iter()
                .zip(validity.iter())
                .map(|(value, valid)| {
                    if valid {
                        pintail_types::Value::Utf8(
                            units.format(value).expect("stored native units round-trip"),
                        )
                    } else {
                        pintail_types::Value::Null
                    }
                })
                .collect(),
            Self::DictionaryUtf8 {
                dict_heap,
                dict_offsets,
                codes,
                validity,
            } => codes
                .iter()
                .zip(validity.iter())
                .map(|(code, valid)| {
                    if valid {
                        let code = *code as usize;
                        let bytes = dict_heap[dict_offsets[code]..dict_offsets[code + 1]].to_vec();
                        let text = String::from_utf8(bytes).unwrap_or_else(|error| {
                            String::from_utf8_lossy(error.as_bytes()).into_owned()
                        });
                        pintail_types::Value::Utf8(text)
                    } else {
                        pintail_types::Value::Null
                    }
                })
                .collect(),
            Self::Utf8 {
                heap,
                offsets,
                validity,
            } => validity
                .iter()
                .enumerate()
                .map(|(row, valid)| {
                    if !valid {
                        return pintail_types::Value::Null;
                    }
                    let bytes = heap[offsets[row]..offsets[row + 1]].to_vec();
                    // Arena bytes were UTF-8-validated at block decode; the
                    // lossy fallback never fires but avoids a panic path.
                    let text = String::from_utf8(bytes).unwrap_or_else(|error| {
                        String::from_utf8_lossy(error.as_bytes()).into_owned()
                    });
                    pintail_types::Value::Utf8(text)
                })
                .collect(),
        }
    }
}

/// The run of a segment's rows a key range covers, and the key blocks it
/// selected and skipped to find them.
struct KeySpan {
    rows: std::ops::Range<usize>,
    /// Key blocks decoded to find the run.
    key_blocks_decoded: usize,
    blocks_read: usize,
    blocks_pruned: usize,
}

/// One bounded column-major projection from an independently visible segment.
pub struct ProjectedColumnChunk {
    columns: Vec<DecodedColumn>,
    row_count: usize,
    stats: ScanStats,
    retained_bytes: usize,
    /// Every row satisfies the scan's prewhere predicate: the chunk holds
    /// only rows an exact selector kept.
    prefiltered: bool,
    /// What each column read for this chunk cost, predicate columns
    /// included; a column read twice appears twice.
    column_decode: Vec<ColumnDecode>,
}

impl ProjectedValueChunk {
    /// Returns projected values in physical key order.
    #[must_use]
    pub fn rows(&self) -> &[Vec<pintail_types::Value>] {
        &self.rows
    }

    /// Moves the projected values into the pull-based executor.
    #[must_use]
    pub fn into_rows(self) -> Vec<Vec<pintail_types::Value>> {
        self.rows
    }

    /// Returns bytes retained by the projected values.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns pruning and decoding counters for this segment.
    #[must_use]
    pub const fn stats(&self) -> ScanStats {
        self.stats
    }
}

impl ProjectedColumnChunk {
    /// Returns projected columns in query projection order.
    #[must_use]
    pub fn columns(&self) -> &[DecodedColumn] {
        &self.columns
    }

    /// What each column read for this chunk cost: decompressed bytes and
    /// delivered values per column id. Empty for rows that came from the
    /// memtable or the row-merge path.
    #[must_use]
    pub fn column_decode(&self) -> &[ColumnDecode] {
        &self.column_decode
    }

    /// Moves the packed projected columns into a columnar executor.
    #[must_use]
    pub fn into_decoded_columns(self) -> Vec<DecodedColumn> {
        self.columns
    }

    /// Materializes projected columns into per-row values.
    #[must_use]
    /// The decoded columns and the row count, without turning packed
    /// values into one `Value` per cell. A consumer with its own typed
    /// representation wants these, not `into_columns`.
    pub fn take_columns(self) -> (Vec<DecodedColumn>, usize) {
        (self.columns, self.row_count)
    }

    pub fn into_columns(self) -> Vec<Vec<pintail_types::Value>> {
        self.columns
            .into_iter()
            .map(DecodedColumn::into_values)
            .collect()
    }

    /// Returns the number of physical rows represented by the columns.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns bytes retained by the projected columns.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns pruning and decoding counters for this segment.
    #[must_use]
    pub const fn stats(&self) -> ScanStats {
        self.stats
    }

    /// Whether every row of this chunk is known to satisfy the prewhere
    /// predicate the scan was given, so a consumer need not test it again.
    #[must_use]
    pub const fn prefiltered(&self) -> bool {
        self.prefiltered
    }
}

impl ProjectedScanStream {
    /// Decodes the next independently visible segment within `memory_limit`.
    ///
    /// # Errors
    ///
    /// Returns a precise storage, corruption, schema, or memory-limit error.
    pub fn next_chunk(
        &mut self,
        memory_limit: usize,
    ) -> Result<Option<ProjectedValueChunk>, StoreError> {
        let Some(chunk) = self.next_column_chunk(memory_limit)? else {
            return Ok(None);
        };
        let stats = chunk.stats;
        let row_count = chunk.row_count;
        let rows = columns_to_rows(
            chunk
                .columns
                .into_iter()
                .map(DecodedColumn::into_values)
                .collect(),
            row_count,
        )?;
        let retained_bytes = size_of::<ProjectedValueChunk>()
            .saturating_add(
                rows.capacity()
                    .saturating_mul(size_of::<Vec<pintail_types::Value>>()),
            )
            .saturating_add(
                rows.iter()
                    .map(|values| {
                        values
                            .capacity()
                            .saturating_mul(size_of::<pintail_types::Value>())
                            .saturating_add(
                                values.iter().map(pintail_types::Value::heap_bytes).sum(),
                            )
                    })
                    .sum(),
            );
        Ok(Some(ProjectedValueChunk {
            rows,
            stats,
            retained_bytes,
        }))
    }

    /// Decodes the next independently visible segment in column-major form.
    ///
    /// # Errors
    ///
    /// Returns a precise storage, corruption, schema, or memory-limit error.
    #[allow(clippy::too_many_lines)]
    pub fn next_column_chunk(
        &mut self,
        memory_limit: usize,
    ) -> Result<Option<ProjectedColumnChunk>, StoreError> {
        loop {
            if let Some(chunk) = self.pending.pop_front() {
                return Ok(Some(chunk));
            }
            if self.merge.is_some() {
                if let Some(chunk) = self.next_merged_column_chunk(memory_limit)? {
                    return Ok(Some(chunk));
                }
                self.merge = None;
            } else if self.memtable_cursor.is_some() {
                if let Some(chunk) = self.next_memtable_chunk(memory_limit)? {
                    return Ok(Some(chunk));
                }
                self.memtable_cursor = None;
            } else if let Some((segment, start_row, end_row)) = self.direct_range.take() {
                return self
                    .decode_direct_range_within(segment, start_row, end_row, memory_limit)
                    .map(Some);
            } else if self.overlay.is_some() {
                // An overlay segment is decoded slice by slice with the
                // mask, whichever API pulls; the slice's chunks queue up.
                self.fill_direct_slices(1)?;
                let Some(slice) = self.slices.pop_front() else {
                    if !self.advance_part()? {
                        return Ok(None);
                    }
                    continue;
                };
                let mut chunks = self.decode_overlay_slice_bounded(slice, memory_limit, None)?;
                if chunks.is_empty() {
                    continue;
                }
                let first = chunks.remove(0);
                self.pending.extend(chunks);
                return Ok(Some(first));
            } else if let Some(segment) = self.segments.get(self.next_segment).cloned() {
                self.next_segment += 1;
                return match self.decode_column_chunk(segment.clone(), memory_limit) {
                    // A segment the budget cannot hold whole is read in row
                    // slices instead of refused: a compacted table can hold
                    // tens of millions of rows in one segment.
                    // The slices cover only the rows the scan's key range
                    // selects.
                    Err(error @ StoreError::MemoryLimitExceeded { .. })
                        if segment.row_count > 1 =>
                    {
                        let (start_row, end_row) =
                            self.bounded_rows(&segment, memory_limit)?.ok_or(error)?;
                        if start_row == end_row {
                            continue;
                        }
                        self.decode_direct_range_within(segment, start_row, end_row, memory_limit)
                            .map(Some)
                    }
                    other => other.map(Some),
                };
            }
            if !self.advance_part()? {
                return Ok(None);
            }
        }
    }

    /// Activates the next classified scan part, returning `false` at the end.
    #[allow(clippy::too_many_lines)]
    fn advance_part(&mut self) -> Result<bool, StoreError> {
        let Some(part) = self.parts.pop_front() else {
            return Ok(false);
        };
        self.merge = None;
        self.memtable_cursor = None;
        self.direct_range = None;
        self.direct_slice_rows = None;
        self.slices.clear();
        self.overlay = None;
        match part {
            ScanPart::Direct { segments } => {
                self.segments = segments;
                self.next_segment = 0;
            }
            ScanPart::Layered {
                segments,
                lo,
                hi,
                bases,
                rows,
            } => {
                let expanded = self.expand_layered(segments, lo, hi, bases, rows);
                for part in expanded.into_iter().rev() {
                    self.parts.push_front(part);
                }
                return self.advance_part();
            }
            ScanPart::Overlay { segment, rows } => {
                let sparse = if self.overlay_key.is_some() {
                    segment::read_sparse_index(&self.snapshot.directory, &segment).ok()
                } else {
                    None
                };
                let Some(sparse) = sparse else {
                    if rows.is_some() {
                        // A layered cluster checked every base's index
                        // before it expanded; a merge over this base alone
                        // would miss the rows layered over it.
                        return Err(StoreError::FormatLimit(
                            "a layered base segment lost its sparse index".into(),
                        ));
                    }
                    // No key column named, or no index to place slices by:
                    // the merge answers for the whole segment as before.
                    self.parts.push_front(ScanPart::Merge {
                        lo: std::ops::Bound::Included(segment.min_key.clone()),
                        hi: std::ops::Bound::Included(segment.max_key.clone()),
                        segments: vec![segment],
                    });
                    return self.advance_part();
                };
                self.overlay_rows = rows.unwrap_or_else(|| self.snapshot.memtable.clone());
                self.segments = vec![segment];
                self.next_segment = 0;
                self.overlay = Some(OverlayState { sparse });
            }
            ScanPart::DirectRange {
                segment,
                start_row,
                end_row,
            } => {
                self.segments = Vec::new();
                self.next_segment = 0;
                self.direct_range = Some((segment, start_row, end_row));
            }
            ScanPart::Merge { segments, lo, hi } => {
                let mut streams = segments
                    .iter()
                    .map(|meta| {
                        let mut stream = segment::SegmentRowStream::open_headers(
                            &self.snapshot.directory,
                            meta,
                            &self.snapshot.schema,
                        )?;
                        // The merge starts at the part's lower bound; the
                        // blocks before it are passed over, not walked.
                        if let std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) = &lo
                        {
                            stream.skip_to_key(meta, key)?;
                        }
                        Ok(stream)
                    })
                    .collect::<Result<Vec<_>, StoreError>>()?;
                let heads = streams
                    .iter_mut()
                    .map(segment::SegmentRowStream::next_header)
                    .collect::<Result<Vec<_>, _>>()?;
                let memtable_head = if bound_range_is_searchable(&lo, &hi) {
                    self.snapshot
                        .memtable
                        .range((lo.clone(), hi.clone()))
                        .next()
                        .map(|(_, row)| row.clone())
                } else {
                    None
                };
                self.segments = segments;
                self.next_segment = self.segments.len();
                self.merge = Some(MergedProjectedStream {
                    streams,
                    heads,
                    memtable_head,
                    reported_segments: false,
                    lo,
                    hi,
                });
            }
            ScanPart::MemtableOnly { lo, hi, rows } => {
                self.overlay_rows = rows.unwrap_or_else(|| self.snapshot.memtable.clone());
                self.segments = Vec::new();
                self.next_segment = 0;
                self.memtable_cursor = Some((lo, hi));
            }
        }
        Ok(true)
    }

    /// The parts a layered cluster is served as: an overlay of each base and
    /// the resolved rows of the gaps around them, in key order. Without an
    /// overlay key, or a base without a sparse index to place slices by, it
    /// is the merge it stands for.
    fn expand_layered(
        &self,
        segments: Vec<segment::SegmentMeta>,
        lo: std::ops::Bound<PrimaryKey>,
        hi: std::ops::Bound<PrimaryKey>,
        bases: Vec<segment::SegmentMeta>,
        rows: LayerRows,
    ) -> Vec<ScanPart> {
        let layerable = self.overlay_key.is_some()
            && bases.iter().all(|base| {
                segment::read_sparse_index(&self.snapshot.directory, base)
                    .is_ok_and(|sparse| !sparse.is_empty())
            });
        if !layerable {
            pintail_log::log_debug!(
                "store scan merges a layered cluster row by row: {}",
                if self.overlay_key.is_none() {
                    "the scan names no integer key to mask by"
                } else {
                    "a base has no sparse index"
                }
            );
            return self
                .snapshot
                .refine_merge_parts(
                    &self.start,
                    &self.end,
                    VecDeque::from([ScanPart::Merge { segments, lo, hi }]),
                )
                .into();
        }
        let has_rows = |lo: &std::ops::Bound<PrimaryKey>, hi: &std::ops::Bound<PrimaryKey>| {
            bound_range_is_searchable(lo, hi)
                && rows.range((lo.clone(), hi.clone())).next().is_some()
        };
        let mut expanded = Vec::with_capacity(bases.len() * 2 + 1);
        let mut cursor = lo;
        for base in bases {
            let gap_hi = std::ops::Bound::Excluded(base.min_key.clone());
            if has_rows(&cursor, &gap_hi) {
                expanded.push(ScanPart::MemtableOnly {
                    lo: cursor,
                    hi: gap_hi,
                    rows: Some(rows.clone()),
                });
            }
            cursor = std::ops::Bound::Excluded(base.max_key.clone());
            expanded.push(ScanPart::Overlay {
                segment: base,
                rows: Some(rows.clone()),
            });
        }
        if has_rows(&cursor, &hi) {
            expanded.push(ScanPart::MemtableOnly {
                lo: cursor,
                hi,
                rows: Some(rows),
            });
        }
        expanded
    }

    /// Produces the next chunk of memtable-resident rows for a gap part.
    fn next_memtable_chunk(
        &mut self,
        memory_limit: usize,
    ) -> Result<Option<ProjectedColumnChunk>, StoreError> {
        const MAX_MEMTABLE_CHUNK_ROWS: usize = 8 * 1024;
        let Some((lo, hi)) = self.memtable_cursor.clone() else {
            return Ok(None);
        };
        if !bound_range_is_searchable(&lo, &hi) {
            self.memtable_cursor = None;
            return Ok(None);
        }
        let projection = self
            .column_ids
            .iter()
            .map(|id| {
                self.snapshot
                    .schema
                    .columns()
                    .iter()
                    .position(|column| column.id() == *id)
                    .ok_or_else(|| {
                        StoreError::FormatLimit(format!("unknown projected column id {id}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let chunk_rows = if projection.is_empty() {
            MAX_MEMTABLE_CHUNK_ROWS
        } else {
            memory_limit
                .checked_div(
                    projection
                        .len()
                        .saturating_mul(size_of::<pintail_types::Value>())
                        .saturating_mul(2),
                )
                .unwrap_or(0)
                .clamp(1, MAX_MEMTABLE_CHUNK_ROWS)
        };
        let mut columns = projection
            .iter()
            .map(|_| Vec::new())
            .collect::<Vec<Vec<pintail_types::Value>>>();
        let mut row_count = 0usize;
        let mut last_key = None;
        let mut admission = self.row_admission();
        for (key, row) in self.overlay_rows.range((lo, hi.clone())) {
            last_key = Some(key.clone());
            if row.is_deleted()
                || admission
                    .as_mut()
                    .is_some_and(|admission| !admission.admits(row))
            {
                continue;
            }
            for (column, position) in columns.iter_mut().zip(&projection) {
                column.push(row.values()[*position].clone());
            }
            row_count += 1;
            if row_count >= chunk_rows {
                break;
            }
        }
        match last_key {
            Some(key) => {
                self.memtable_cursor = Some((std::ops::Bound::Excluded(key), hi));
            }
            None => self.memtable_cursor = None,
        }
        if row_count == 0 {
            // A window that held only tombstones: continue into the next
            // window, or finish the part when the range is drained.
            if self.memtable_cursor.is_some() {
                return self.next_memtable_chunk(memory_limit);
            }
            return Ok(None);
        }
        values_chunk(columns, row_count, memory_limit).map(Some)
    }

    /// Decodes several independently visible segments concurrently.
    ///
    /// The supplied memory budget is divided across the selected segments, so
    /// their aggregate temporary and retained memory cannot exceed it.
    ///
    /// # Errors
    ///
    /// Returns a precise storage, corruption, schema, or memory-limit error.
    pub fn next_column_chunks(
        &mut self,
        max_chunks: usize,
        memory_limit: usize,
    ) -> Result<Vec<ProjectedColumnChunk>, StoreError> {
        self.next_column_chunks_inner(max_chunks, memory_limit, None)
    }

    /// Like [`Self::next_column_chunks`], but full direct segments decode
    /// filter-first: the predicate columns decode alone, `select` chooses the
    /// surviving row ranges (or `None` to keep everything), and only those
    /// ranges of the full projection decode afterwards.
    ///
    /// # Errors
    ///
    /// Returns a precise storage, corruption, schema, or memory-limit error.
    pub fn next_column_chunks_filtered(
        &mut self,
        max_chunks: usize,
        memory_limit: usize,
        predicate_ids: &[u32],
        select: PrewhereSelect<'_>,
    ) -> Result<Vec<ProjectedColumnChunk>, StoreError> {
        self.next_column_chunks_inner(max_chunks, memory_limit, Some((predicate_ids, select)))
    }

    fn next_column_chunks_inner(
        &mut self,
        max_chunks: usize,
        memory_limit: usize,
        prewhere: Option<(&[u32], PrewhereSelect<'_>)>,
    ) -> Result<Vec<ProjectedColumnChunk>, StoreError> {
        if !self.pending.is_empty() {
            return Ok(self.pending.drain(..).collect());
        }
        if self.merge.is_some() || self.memtable_cursor.is_some() || self.direct_range.is_some() {
            return Ok(self.next_column_chunk(memory_limit)?.into_iter().collect());
        }
        self.fill_direct_slices(max_chunks.max(1))?;
        if self.slices.is_empty() {
            if !self.advance_part()? {
                return Ok(Vec::new());
            }
            return self.next_column_chunks_inner(max_chunks, memory_limit, prewhere);
        }
        let chunk_count = max_chunks.max(1).min(self.slices.len());
        let taken: Vec<DirectSlice> = self.slices.drain(..chunk_count).collect();
        if chunk_count == 1 {
            let slice = taken.into_iter().next().expect("one slice");
            let (segment, start_row, end_row) = match &slice {
                DirectSlice::Whole(segment) => (segment.clone(), 0, segment.row_count),
                DirectSlice::Range {
                    segment,
                    start_row,
                    end_row,
                } => (segment.clone(), *start_row, *end_row),
            };
            if self.overlay.is_some() {
                return self.decode_overlay_slice_bounded(slice, memory_limit, prewhere);
            }
            return match self.decode_slice(&slice, memory_limit, prewhere) {
                Err(error @ StoreError::MemoryLimitExceeded { .. }) if end_row - start_row > 1 => {
                    // A whole segment the range covers only in part keeps
                    // its key bounds when read in slices.
                    let (start_row, end_row) = if matches!(slice, DirectSlice::Whole(_)) {
                        self.bounded_rows(&segment, memory_limit)?.ok_or(error)?
                    } else {
                        (start_row, end_row)
                    };
                    if start_row == end_row {
                        return Ok(Vec::new());
                    }
                    self.decode_direct_range_within(segment, start_row, end_row, memory_limit)
                        .map(|chunk| vec![chunk])
                }
                other => other,
            };
        }
        let per_chunk_limit = memory_limit / chunk_count;
        let decoded: Result<Vec<Vec<ProjectedColumnChunk>>, StoreError> = projected_scan_pool()?
            .install(|| {
                taken
                    .par_iter()
                    .map(|slice| self.decode_slice(slice, per_chunk_limit, prewhere))
                    .collect()
            });
        if matches!(decoded, Err(StoreError::MemoryLimitExceeded { .. })) {
            for slice in taken.into_iter().rev() {
                self.slices.push_front(slice);
            }
            return self.next_column_chunks_inner(chunk_count.div_ceil(2), memory_limit, prewhere);
        }
        decoded.map(|chunks| chunks.into_iter().flatten().collect())
    }

    /// Decodes one overlay slice within `memory_limit`, halving it at block
    /// boundaries while it does not fit. The overlay must mask every slice
    /// it decodes, so a slice is never decoded unmasked in pieces; a single
    /// block that does not fit is a memory error.
    fn decode_overlay_slice_bounded(
        &self,
        slice: DirectSlice,
        memory_limit: usize,
        prewhere: Option<(&[u32], PrewhereSelect<'_>)>,
    ) -> Result<Vec<ProjectedColumnChunk>, StoreError> {
        let mut work = VecDeque::from([slice]);
        let mut chunks: Vec<ProjectedColumnChunk> = Vec::new();
        while let Some(slice) = work.pop_front() {
            // The pieces of one slice share its allowance: what the pieces
            // already decoded retain comes off what the next may take.
            let retained = chunks
                .iter()
                .map(|chunk| chunk.retained_bytes)
                .sum::<usize>();
            let remaining = memory_limit.saturating_sub(retained);
            match self.decode_slice(&slice, remaining, prewhere) {
                Ok(decoded) => chunks.extend(decoded),
                Err(error @ StoreError::MemoryLimitExceeded { .. }) => {
                    // A single block that does not fit is the real answer,
                    // with the request that failed.
                    let (head, tail) = self.split_overlay_slice(&slice).ok_or(error)?;
                    work.push_front(tail);
                    work.push_front(head);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(chunks)
    }

    /// Halves an overlay slice at the block boundary nearest its middle;
    /// `None` when it is a single block.
    fn split_overlay_slice(&self, slice: &DirectSlice) -> Option<(DirectSlice, DirectSlice)> {
        let sparse = &self.overlay.as_ref()?.sparse;
        let (segment, start_row, end_row) = match slice {
            DirectSlice::Whole(segment) => (segment, 0, segment.row_count),
            DirectSlice::Range {
                segment,
                start_row,
                end_row,
            } => (segment, *start_row, *end_row),
        };
        let middle = start_row + (end_row - start_row) / 2;
        let boundary = sparse
            .iter()
            .map(|(row, _)| *row)
            .filter(|row| *row > start_row && *row < end_row)
            .min_by_key(|row| row.abs_diff(middle))?;
        Some((
            DirectSlice::Range {
                segment: segment.clone(),
                start_row,
                end_row: boundary,
            },
            DirectSlice::Range {
                segment: segment.clone(),
                start_row: boundary,
                end_row,
            },
        ))
    }

    fn fill_direct_slices(&mut self, wanted: usize) -> Result<(), StoreError> {
        while self.slices.len() < wanted {
            let Some(segment) = self.segments.get(self.next_segment).cloned() else {
                return Ok(());
            };
            self.next_segment += 1;
            let full_direct = self.start <= segment.min_key && self.end >= segment.max_key;
            if !full_direct || segment.row_count <= DIRECT_SLICE_ROWS {
                self.slices.push_back(DirectSlice::Whole(segment));
                continue;
            }
            let block = u64::try_from(segment::block_rows(
                &self.snapshot.directory,
                &segment,
                &self.snapshot.schema,
            )?)
            .unwrap_or(u64::MAX)
            .max(1);
            let rows = (DIRECT_SLICE_ROWS / block).max(1).saturating_mul(block);
            let mut start_row = 0;
            while start_row < segment.row_count {
                let end_row = start_row.saturating_add(rows).min(segment.row_count);
                self.slices.push_back(DirectSlice::Range {
                    segment: segment.clone(),
                    start_row,
                    end_row,
                });
                start_row = end_row;
            }
        }
        Ok(())
    }

    fn decode_slice(
        &self,
        slice: &DirectSlice,
        memory_limit: usize,
        prewhere: Option<(&[u32], PrewhereSelect<'_>)>,
    ) -> Result<Vec<ProjectedColumnChunk>, StoreError> {
        if self.overlay.is_some() {
            return self.decode_overlay_slice(slice, memory_limit, prewhere);
        }
        self.decode_slice_plain(slice, memory_limit, prewhere)
            .map(|chunk| vec![chunk])
    }

    fn decode_slice_plain(
        &self,
        slice: &DirectSlice,
        memory_limit: usize,
        prewhere: Option<(&[u32], PrewhereSelect<'_>)>,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        match slice {
            DirectSlice::Whole(segment) => {
                self.decode_column_chunk_maybe_filtered(segment.clone(), memory_limit, prewhere)
            }
            DirectSlice::Range {
                segment,
                start_row,
                end_row,
            } => self.decode_range_maybe_filtered(
                segment,
                *start_row,
                *end_row,
                memory_limit,
                prewhere,
            ),
        }
    }

    /// The key span a direct slice covers, from the segment's block
    /// boundaries: the first key of its first block up to (excluding) the
    /// first key of the block after it, or the segment's last key.
    fn overlay_slice_span(
        &self,
        slice: &DirectSlice,
    ) -> (std::ops::Bound<PrimaryKey>, std::ops::Bound<PrimaryKey>) {
        use std::ops::Bound::{Excluded, Included};
        let sparse = self
            .overlay
            .as_ref()
            .map(|state| state.sparse.as_slice())
            .unwrap_or_default();
        match slice {
            DirectSlice::Whole(segment) => (
                Included(segment.min_key.clone()),
                Included(segment.max_key.clone()),
            ),
            DirectSlice::Range {
                segment,
                start_row,
                end_row,
            } => {
                let lo = sparse
                    .iter()
                    .find(|(row, _)| *row == *start_row)
                    .map_or_else(
                        || Included(segment.min_key.clone()),
                        |(_, key)| Included(key.clone()),
                    );
                let hi = sparse.iter().find(|(row, _)| *row == *end_row).map_or_else(
                    || Included(segment.max_key.clone()),
                    |(_, key)| Excluded(key.clone()),
                );
                (lo, hi)
            }
        }
    }

    /// Decodes a slice of an overlay part: the segment's rows minus those
    /// whose key the memtable holds (updated or deleted since the flush),
    /// followed by the memtable's live rows for the slice's key span. The
    /// mask rides the filter-first path as one more predicate column, so a
    /// slice with no memtable rows in its span costs what a direct slice
    /// costs, and one with a few costs one extra packed column.
    fn decode_overlay_slice(
        &self,
        slice: &DirectSlice,
        memory_limit: usize,
        prewhere: Option<(&[u32], PrewhereSelect<'_>)>,
    ) -> Result<Vec<ProjectedColumnChunk>, StoreError> {
        let Some(key_ids) = self.overlay_key.as_deref() else {
            return self
                .decode_slice_plain(slice, memory_limit, prewhere)
                .map(|chunk| vec![chunk]);
        };
        let mut memtable = self.overlay_span_rows(slice, key_ids.len())?;
        self.mask_unwanted_live_rows(&mut memtable);
        if memtable.is_empty() {
            return self
                .decode_slice_plain(slice, memory_limit, prewhere)
                .map(|chunk| vec![chunk]);
        }
        let live = memtable
            .iter()
            .filter_map(|(_, row)| *row)
            .collect::<Vec<_>>();
        // The overlay's own working set comes out of the slice's allowance
        // before the decode: the memtable keys, the live-row list, and the
        // positions and ranges the mask can produce at worst (one per row).
        let slice_rows = match slice {
            DirectSlice::Whole(segment) => segment.row_count,
            DirectSlice::Range {
                start_row, end_row, ..
            } => end_row - start_row,
        };
        let slice_rows = usize::try_from(slice_rows).unwrap_or(usize::MAX);
        let overhead = memtable
            .len()
            .saturating_mul(
                key_ids
                    .len()
                    .saturating_mul(size_of::<i128>())
                    .saturating_add(size_of::<(Vec<i128>, Option<&StoredRow>)>()),
            )
            .saturating_add(live.len().saturating_mul(size_of::<&StoredRow>()))
            .saturating_add(
                slice_rows.saturating_mul(size_of::<usize>() + size_of::<std::ops::Range<usize>>()),
            );
        let Some(decode_limit) = memory_limit
            .checked_sub(overhead)
            .filter(|limit| *limit > 0)
        else {
            return Err(StoreError::MemoryLimitExceeded {
                used: 0,
                requested: overhead,
                limit: memory_limit,
            });
        };
        let mut ids = prewhere.map_or_else(Vec::new, |(ids, _)| ids.to_vec());
        let key_indices = key_ids
            .iter()
            .map(|key_id| {
                ids.iter().position(|id| id == key_id).unwrap_or_else(|| {
                    ids.push(*key_id);
                    ids.len() - 1
                })
            })
            .collect::<Vec<_>>();
        let caller = prewhere;
        // Where each live memtable row belongs among the surviving segment
        // rows, so the chunk comes out in key order: a consumer that takes
        // the first value it meets for a group (as the source does, in key
        // order) must meet the same one.
        let insert_positions = std::sync::Mutex::new(Vec::new());
        let select = |columns: &[DecodedColumn], row_count: usize| {
            let kept = match caller {
                Some((caller_ids, select)) => select(&columns[..caller_ids.len()], row_count)?,
                None => None,
            };
            let key_columns = key_indices
                .iter()
                .map(|index| &columns[*index])
                .collect::<Vec<_>>();
            let (excluded, positions) = overlay_positions(
                &key_columns,
                row_count,
                kept.as_ref().map(|kept| kept.ranges.as_slice()),
                &memtable,
            );
            *insert_positions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = positions;
            if excluded.is_empty() {
                return Ok(kept);
            }
            // Removing superseded rows keeps an exact selection exact; the
            // live memtable rows interleaved afterwards clear the mark.
            let (ranges, exact) = kept.map_or_else(
                || (std::iter::once(0..row_count).collect(), false),
                |kept| (kept.ranges, kept.exact),
            );
            Ok(Some(PrewhereRanges {
                ranges: subtract_positions(ranges, &excluded),
                exact,
            }))
        };
        let segment_chunk = self.decode_slice_plain(slice, decode_limit, Some((&ids, &select)))?;
        if live.is_empty() {
            return Ok(vec![segment_chunk]);
        }
        let positions = insert_positions
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.interleave_live_rows(segment_chunk, &positions, &live, decode_limit)
            .map(|chunk| vec![chunk])
    }

    /// The memtable rows of a slice's key span in key order, each as its
    /// key's integer parts and, for a live row, the row itself (a tombstone
    /// carries `None`: it only masks).
    #[allow(clippy::type_complexity)]
    fn overlay_span_rows(
        &self,
        slice: &DirectSlice,
        key_parts: usize,
    ) -> Result<Vec<(Vec<i128>, Option<&StoredRow>)>, StoreError> {
        let span = self.overlay_slice_span(slice);
        let mut rows = Vec::new();
        if bound_range_is_searchable(&span.0, &span.1) {
            for (key, row) in self.overlay_rows.range(span) {
                if key.parts().len() != key_parts {
                    return Err(StoreError::FormatLimit(
                        "the memtable overlay's key has a different number of parts".into(),
                    ));
                }
                let mut parts = Vec::with_capacity(key_parts);
                for part in key.parts() {
                    parts.push(match part {
                        KeyPart::Int64(value) => i128::from(*value),
                        KeyPart::UInt64(value) => i128::from(*value),
                        _ => {
                            return Err(StoreError::FormatLimit(
                                "the memtable overlay needs integer key parts".into(),
                            ));
                        }
                    });
                }
                rows.push((parts, (!row.is_deleted()).then_some(row)));
            }
        }
        Ok(rows)
    }

    /// The segment chunk with the live memtable rows placed at `positions`
    /// (one per row, ascending, indexing the output) in every projected
    /// column.
    fn interleave_live_rows(
        &self,
        segment_chunk: ProjectedColumnChunk,
        positions: &[usize],
        live: &[&StoredRow],
        decode_limit: usize,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        if positions.len() != live.len() {
            return Err(StoreError::FormatLimit(
                "the memtable overlay could not place its rows".into(),
            ));
        }
        let projection = self
            .column_ids
            .iter()
            .map(|id| {
                self.snapshot
                    .schema
                    .columns()
                    .iter()
                    .position(|column| column.id() == *id)
                    .ok_or_else(|| {
                        StoreError::FormatLimit(format!("unknown projected column id {id}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ProjectedColumnChunk {
            columns,
            row_count,
            stats,
            retained_bytes: _,
            prefiltered: _,
            column_decode,
        } = segment_chunk;
        let columns = columns
            .into_iter()
            .zip(&projection)
            .map(|(column, position)| {
                let inserts = positions
                    .iter()
                    .zip(live)
                    .map(|(at, row)| (*at, &row.values()[*position]))
                    .collect::<Vec<_>>();
                column.interleave(&inserts)
            })
            .collect::<Vec<_>>();
        let retained_bytes = size_of::<ProjectedColumnChunk>()
            .saturating_add(
                columns
                    .capacity()
                    .saturating_mul(size_of::<DecodedColumn>()),
            )
            .saturating_add(columns.iter().map(DecodedColumn::retained_bytes).sum());
        if retained_bytes > decode_limit {
            return Err(StoreError::MemoryLimitExceeded {
                used: 0,
                requested: retained_bytes,
                limit: decode_limit,
            });
        }
        Ok(ProjectedColumnChunk {
            prefiltered: false,
            columns,
            row_count: row_count + live.len(),
            stats,
            retained_bytes,
            column_decode,
        })
    }

    /// The filter-first path for one row range of a direct segment: the
    /// predicate columns decode for the range alone, the selector picks the
    /// surviving sub-ranges relative to it, and those decode in full at
    /// their absolute positions. Without a selector, or when it keeps
    /// everything, the range decodes whole.
    #[allow(clippy::too_many_lines)]
    fn decode_range_maybe_filtered(
        &self,
        segment: &segment::SegmentMeta,
        start_row: u64,
        end_row: u64,
        memory_limit: usize,
        prewhere: Option<(&[u32], PrewhereSelect<'_>)>,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        let Some((predicate_ids, select)) = prewhere.filter(|(ids, _)| !ids.is_empty()) else {
            return self.decode_column_chunk_rows(segment, start_row, end_row, memory_limit);
        };
        if let Some(chunk) = self.decode_by_side_index(
            segment,
            start_row,
            end_row,
            memory_limit,
            predicate_ids,
            select,
        )? {
            return Ok(chunk);
        }
        let map_projection = |ids: &[u32]| -> Result<Vec<usize>, StoreError> {
            ids.iter()
                .map(|id| {
                    self.snapshot
                        .schema
                        .columns()
                        .iter()
                        .position(|column| column.id() == *id)
                        .ok_or_else(|| {
                            StoreError::FormatLimit(format!("unknown projected column id {id}"))
                        })
                })
                .collect()
        };
        let start = usize::try_from(start_row)
            .map_err(|_| StoreError::FormatLimit("range start exceeds usize".into()))?;
        let end = usize::try_from(end_row)
            .map_err(|_| StoreError::FormatLimit("range end exceeds usize".into()))?;
        let row_count = end.saturating_sub(start);
        let scan_memory = AtomicUsize::new(0);
        let scan_budget = segment::ScanMemoryBudget::new(&scan_memory, memory_limit);
        if let Some((candidates, skipped)) = self.value_candidate_ranges(segment, start, end)? {
            return self.decode_value_candidates(
                segment,
                &candidates,
                skipped,
                predicate_ids,
                &map_projection(predicate_ids)?,
                select,
                usize::from(start_row == 0),
                &scan_budget,
            );
        }
        let fetch = segment::read_projected_columns(
            &self.snapshot.directory,
            segment,
            &self.snapshot.schema,
            &map_projection(predicate_ids)?,
            start,
            end,
            &scan_budget,
        )?;
        let ranges = select(&fetch.columns, row_count).map_err(StoreError::FormatLimit)?;
        if predicate_ids == self.column_ids {
            return retain_predicate_fetch(
                fetch,
                ranges.as_ref(),
                row_count,
                usize::from(start_row == 0),
                &scan_budget,
            );
        }
        let Some(PrewhereRanges { ranges, exact }) = ranges else {
            return self.project_after_predicates(
                segment,
                predicate_ids,
                fetch,
                None,
                KeptRows::Span(start, end),
                false,
                usize::from(start_row == 0),
                &scan_budget,
            );
        };
        // The selector saw the range's rows from zero; the segment reader
        // wants their positions in the segment.
        let absolute = ranges
            .iter()
            .map(|range| range.start + start..range.end + start)
            .collect::<Vec<_>>();
        self.project_after_predicates(
            segment,
            predicate_ids,
            fetch,
            Some(&ranges),
            KeptRows::Ranges(&absolute),
            exact,
            usize::from(start_row == 0),
            &scan_budget,
        )
    }

    /// The rows of `start..end` of a direct segment that lie in blocks the
    /// scan's value bounds cannot rule out, and how many blocks they rule
    /// out; `None` when no bound applies or none is ruled out.
    ///
    /// A block is ruled out when, for some bound, its stored extremes lie
    /// wholly outside the bound or every row of it is NULL (a NULL satisfies
    /// no range or equality comparison). Every bound comes from a top-level
    /// conjunct of the scan's predicates, so a row in such a block fails the
    /// filter the executor would apply to it anyway.
    ///
    /// Only the direct filter-first path asks: its rows are the segment's
    /// own, unshadowed by any other segment. An overlay's memtable rows are
    /// interleaved after the segment rows are chosen, whatever this skips.
    fn value_candidate_ranges(
        &self,
        segment: &segment::SegmentMeta,
        start: usize,
        end: usize,
    ) -> Result<Option<ValueCandidates>, StoreError> {
        if self.value_bounds.is_empty() || start >= end {
            return Ok(None);
        }
        let mut blocks: Option<std::sync::Arc<Vec<segment::BlockExtremes>>> = None;
        let mut keep: Vec<bool> = Vec::new();
        for bound in &self.value_bounds {
            if bound.lower.is_none() && bound.upper.is_none() {
                continue;
            }
            let Some(extremes) = segment::block_extremes(
                &self.snapshot.directory,
                segment,
                &self.snapshot.schema,
                bound.column_id,
                bound.domain,
            )?
            else {
                continue;
            };
            if let Some(first) = &blocks {
                // Every column of a segment is cut at the same rows; a
                // directory that disagrees proves nothing.
                if first.len() != extremes.len()
                    || first
                        .iter()
                        .zip(extremes.iter())
                        .any(|(left, right)| (left.start, left.end) != (right.start, right.end))
                {
                    return Ok(None);
                }
            } else {
                keep = vec![true; extremes.len()];
            }
            for (flag, block) in keep.iter_mut().zip(extremes.iter()) {
                let ruled_out = match block.range {
                    None => true,
                    Some((least, greatest)) => {
                        bound.lower.is_some_and(|lower| greatest < lower)
                            || bound.upper.is_some_and(|upper| least > upper)
                    }
                };
                if ruled_out {
                    *flag = false;
                }
            }
            if blocks.is_none() {
                blocks = Some(extremes);
            }
        }
        let Some(blocks) = blocks else {
            return Ok(None);
        };
        let mut candidates: Vec<std::ops::Range<usize>> = Vec::new();
        let mut skipped = 0_usize;
        for (block, kept) in blocks.iter().zip(&keep) {
            let lo = block.start.max(start);
            let hi = block.end.min(end);
            if lo >= hi {
                continue;
            }
            if !kept {
                skipped += 1;
                continue;
            }
            match candidates.last_mut() {
                Some(last) if last.end == lo => last.end = hi,
                _ => candidates.push(lo..hi),
            }
        }
        if skipped == 0 {
            return Ok(None);
        }
        Ok(Some((candidates, skipped)))
    }

    /// The filter-first read of a direct segment restricted to the
    /// `candidates` rows (absolute, ascending, disjoint) that block value
    /// skipping left: the predicate columns decode for those rows alone,
    /// the selector judges them as one run, and its choice maps back to
    /// segment positions for the rest of the projection.
    #[allow(clippy::too_many_arguments)]
    fn decode_value_candidates(
        &self,
        segment: &segment::SegmentMeta,
        candidates: &[std::ops::Range<usize>],
        skipped: usize,
        predicate_ids: &[u32],
        predicate_projection: &[usize],
        select: PrewhereSelect<'_>,
        segments_read: usize,
        scan_budget: &segment::ScanMemoryBudget<'_>,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        let fetch = segment::read_projected_column_ranges(
            &self.snapshot.directory,
            segment,
            &self.snapshot.schema,
            predicate_projection,
            candidates,
            scan_budget,
        )?;
        let row_count = candidates
            .iter()
            .map(std::iter::ExactSizeIterator::len)
            .sum::<usize>();
        let ranges = select(&fetch.columns, row_count).map_err(StoreError::FormatLimit)?;
        let mut chunk = if predicate_ids == self.column_ids {
            retain_predicate_fetch(
                fetch,
                ranges.as_ref(),
                row_count,
                segments_read,
                scan_budget,
            )?
        } else if let Some(PrewhereRanges { ranges, exact }) = ranges {
            check_selected_ranges(&ranges, row_count)?;
            let absolute = candidate_positions(&ranges, candidates);
            self.project_after_predicates(
                segment,
                predicate_ids,
                fetch,
                Some(&ranges),
                KeptRows::Ranges(&absolute),
                exact,
                segments_read,
                scan_budget,
            )?
        } else {
            self.project_after_predicates(
                segment,
                predicate_ids,
                fetch,
                None,
                KeptRows::Ranges(candidates),
                false,
                segments_read,
                scan_budget,
            )?
        };
        chunk.stats.blocks_value_skipped += skipped;
        Ok(chunk)
    }

    /// Finishes a filter-first read once the selector has judged the
    /// predicate columns: the projection for the kept rows, with every
    /// projected column the predicate fetch already holds compacted from it
    /// rather than decoded a second time. Only the other columns are read,
    /// at `rows`, the kept rows' positions in the segment.
    ///
    /// A join key handed to a scan as a runtime filter is both a predicate
    /// and an output column, so without this every block of it was decoded
    /// twice - on a scan the filter keeps whole, twice for nothing.
    ///
    /// `kept` is relative to the predicate fetch's rows (`None`: all of
    /// them) and must name the same rows as `rows`.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn project_after_predicates(
        &self,
        segment: &segment::SegmentMeta,
        predicate_ids: &[u32],
        predicate: segment::ProjectedColumnFetch,
        kept: Option<&[std::ops::Range<usize>]>,
        rows: KeptRows<'_>,
        prefiltered: bool,
        segments_read: usize,
        scan_budget: &segment::ScanMemoryBudget<'_>,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        let predicate_rows = predicate.columns.first().map(DecodedColumn::len);
        let row_count = match rows {
            KeptRows::Span(start, end) => end.saturating_sub(start),
            KeptRows::Ranges(ranges) => ranges.iter().map(std::iter::ExactSizeIterator::len).sum(),
        };
        let reuse = self
            .column_ids
            .iter()
            .map(|id| predicate_ids.iter().position(|predicate| predicate == id))
            .collect::<Vec<_>>();
        if let Some(kept) = kept {
            check_selected_ranges(kept, predicate_rows.unwrap_or(0))?;
            let selected = kept
                .iter()
                .map(std::iter::ExactSizeIterator::len)
                .sum::<usize>();
            if selected != row_count {
                return Err(StoreError::FormatLimit(
                    "kept predicate rows disagree with the rows to read".into(),
                ));
            }
        } else if predicate_rows.is_some_and(|rows| rows != row_count) {
            return Err(StoreError::FormatLimit(
                "predicate rows disagree with the rows to read".into(),
            ));
        }
        // Keep only the predicate columns the projection reuses, compacted
        // to the kept rows, and hold the budget for just those while the
        // rest decode: the peak stays what reading them apart cost.
        let mut predicate_columns = predicate
            .columns
            .into_iter()
            .enumerate()
            .map(|(index, mut column)| {
                reuse.contains(&Some(index)).then(|| {
                    if let Some(kept) = kept {
                        compact_decoded_column(&mut column, kept, row_count);
                    }
                    column
                })
            })
            .collect::<Vec<_>>();
        let held = predicate_columns
            .iter()
            .flatten()
            .map(DecodedColumn::retained_bytes)
            .sum::<usize>();
        scan_budget.release(predicate.reserved_bytes);
        scan_budget.reserve(held)?;
        let rest = self
            .column_ids
            .iter()
            .zip(&reuse)
            .filter(|(_, reused)| reused.is_none())
            .map(|(id, _)| {
                self.snapshot
                    .schema
                    .columns()
                    .iter()
                    .position(|column| column.id() == *id)
                    .ok_or_else(|| {
                        StoreError::FormatLimit(format!("unknown projected column id {id}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rest = if rest.is_empty() {
            None
        } else {
            Some(match rows {
                KeptRows::Span(start, end) => segment::read_projected_columns(
                    &self.snapshot.directory,
                    segment,
                    &self.snapshot.schema,
                    &rest,
                    start,
                    end,
                    scan_budget,
                )?,
                KeptRows::Ranges(ranges) => segment::read_projected_column_ranges(
                    &self.snapshot.directory,
                    segment,
                    &self.snapshot.schema,
                    &rest,
                    ranges,
                    scan_budget,
                )?,
            })
        };
        let mut stats = ScanStats {
            segments_read,
            blocks_read: predicate.blocks_read,
            blocks_pruned: predicate.blocks_pruned,
            blocks_decoded: predicate.blocks_decoded,
            ..ScanStats::default()
        };
        let mut reserved = held;
        let mut rest_columns = Vec::new().into_iter();
        let mut column_decode = predicate.column_decode;
        if let Some(fetch) = rest {
            stats.blocks_read += fetch.blocks_read;
            stats.blocks_pruned += fetch.blocks_pruned;
            stats.blocks_decoded += fetch.blocks_decoded;
            reserved = reserved.saturating_add(fetch.reserved_bytes);
            rest_columns = fetch.columns.into_iter();
            column_decode.extend(fetch.column_decode);
        }
        let stats = stats.with_decode(&column_decode);
        let mut columns = Vec::with_capacity(self.column_ids.len());
        for (position, reused) in reuse.iter().enumerate() {
            let column = match reused {
                Some(index) => {
                    // A column projected twice takes a copy the second time.
                    let later = reuse[position + 1..].contains(&Some(*index));
                    let slot = &mut predicate_columns[*index];
                    if later { slot.clone() } else { slot.take() }
                }
                None => rest_columns.next(),
            };
            columns.push(column.ok_or_else(|| {
                StoreError::FormatLimit("a projected column is missing from its fetch".into())
            })?);
        }
        let retained_bytes = size_of::<ProjectedColumnChunk>()
            .saturating_add(
                columns
                    .capacity()
                    .saturating_mul(size_of::<DecodedColumn>()),
            )
            .saturating_add(columns.iter().map(DecodedColumn::retained_bytes).sum());
        scan_budget.release(reserved);
        scan_budget.reserve(retained_bytes)?;
        Ok(ProjectedColumnChunk {
            columns,
            row_count,
            stats,
            retained_bytes,
            prefiltered,
            column_decode,
        })
    }

    /// Sets the side-index request (see
    /// [`super::side_index`]). Callers set it only while the index is on.
    pub fn set_index_lookup(&mut self, lookup: super::side_index::IndexLookup) {
        self.index_lookup = Some(lookup);
    }

    /// The side-index lookup as a test of whole rows (the memtable's, or a
    /// layered cluster's resolved ones): the lookup and its column's schema
    /// position. A row it rejects is one the scan does not want, so it is
    /// left out rather than materialized for the filter to drop.
    /// Turns the overlay's live rows the side-index lookup rejects into
    /// masks: such a row still supersedes its segment row, it is only not
    /// interleaved.
    fn mask_unwanted_live_rows<'a>(&'a self, rows: &mut [(Vec<i128>, Option<&'a StoredRow>)]) {
        if let Some(mut admission) = self.row_admission() {
            for (_, row) in rows {
                if row.is_some_and(|row| !admission.admits(row)) {
                    *row = None;
                }
            }
        }
    }

    fn row_admission(&self) -> Option<super::side_index::RowAdmission<'_>> {
        let lookup = self.index_lookup.as_ref()?;
        let position = self
            .snapshot
            .schema
            .columns()
            .iter()
            .position(|column| column.id() == lookup.column_id)?;
        Some(super::side_index::RowAdmission::new(lookup, position))
    }

    /// Where the first `k` rows of this scan's segments in the order of the
    /// integer column `column_id` end, and whether a NULL may sort among
    /// them: the bound over the segments' postings (see the side index's
    /// order bound), with the memtable's rows counted as possible NULLs
    /// under a nullable column, since nothing indexes them. A scan
    /// restricted to the rows at or before the bound answers the first `k`
    /// exactly only when at least `k` rows come back: superseded and
    /// deleted segment rows count toward the bound but not toward the
    /// answer.
    ///
    /// # Errors
    ///
    /// Returns a storage error reading a segment's postings.
    pub fn side_index_order_bound(
        &self,
        column_id: u32,
        k: usize,
        descending: bool,
    ) -> Result<Option<(i128, bool)>, StoreError> {
        // Every segment the scan's key range overlaps: an open scan has
        // already dealt them out into its parts.
        let segments = self
            .snapshot
            .manifest
            .segments
            .iter()
            .filter(|segment| segment.max_key >= self.start && segment.min_key <= self.end)
            .cloned()
            .collect::<Vec<_>>();
        Ok(super::side_index::order_bound(
            &self.snapshot.directory,
            &segments,
            &self.snapshot.schema,
            column_id,
            k,
            descending,
        )?
        .map(|(bound, nulls)| {
            let nullable = self
                .snapshot
                .schema
                .columns()
                .iter()
                .find(|column| column.id() == column_id)
                .is_some_and(pintail_types::Column::is_nullable);
            // A segment's own NULLs count whatever the schema says now; the
            // memtable's rows can hold one only under a nullable column.
            (
                bound,
                nulls || (nullable && !self.snapshot.memtable.is_empty()),
            )
        }))
    }

    /// The side-index request set so far, if any.
    #[must_use]
    pub const fn index_lookup(&self) -> Option<&super::side_index::IndexLookup> {
        self.index_lookup.as_ref()
    }

    /// The filter-first decode over only the rows the side index names for
    /// the scan's lookup: the predicate columns decode for those rows, the
    /// selector judges them, and the projection decodes for the survivors.
    /// `None` when the index is off, declines the column, or finds too many
    /// rows to be worth reading apart.
    #[allow(clippy::too_many_lines)]
    fn decode_by_side_index(
        &self,
        segment: &segment::SegmentMeta,
        start_row: u64,
        end_row: u64,
        memory_limit: usize,
        predicate_ids: &[u32],
        select: PrewhereSelect<'_>,
    ) -> Result<Option<ProjectedColumnChunk>, StoreError> {
        let Some(lookup) = self.index_lookup.as_ref() else {
            return Ok(None);
        };
        let (Ok(start), Ok(end)) = (usize::try_from(start_row), usize::try_from(end_row)) else {
            return Ok(None);
        };
        let Some(postings) = super::side_index::postings(
            &self.snapshot.directory,
            segment,
            &self.snapshot.schema,
            lookup.column_id,
            &lookup.key,
        )?
        else {
            return Ok(None);
        };
        let Some(candidates) = postings.candidate_ranges(&lookup.probe, start, end) else {
            return Ok(None);
        };
        super::side_index::note_useful(&self.snapshot.directory, lookup.column_id);
        let map_projection = |ids: &[u32]| -> Result<Vec<usize>, StoreError> {
            ids.iter()
                .map(|id| {
                    self.snapshot
                        .schema
                        .columns()
                        .iter()
                        .position(|column| column.id() == *id)
                        .ok_or_else(|| {
                            StoreError::FormatLimit(format!("unknown projected column id {id}"))
                        })
                })
                .collect()
        };
        let scan_memory = AtomicUsize::new(0);
        let scan_budget = segment::ScanMemoryBudget::new(&scan_memory, memory_limit);
        let candidate_rows = candidates
            .iter()
            .map(std::iter::ExactSizeIterator::len)
            .sum::<usize>();
        // Even with no candidates the selector runs, over zero rows: an
        // overlay part places its memtable rows from what it sees there.
        let fetch = segment::read_projected_column_ranges(
            &self.snapshot.directory,
            segment,
            &self.snapshot.schema,
            &map_projection(predicate_ids)?,
            &candidates,
            &scan_budget,
        )?;
        let selected = select(&fetch.columns, candidate_rows).map_err(StoreError::FormatLimit)?;
        let segments_read = usize::from(start_row == 0);
        let chunk = match selected {
            Some(PrewhereRanges { ranges, exact }) => {
                let absolute = super::side_index::absolute_ranges(&candidates, &ranges);
                self.project_after_predicates(
                    segment,
                    predicate_ids,
                    fetch,
                    Some(&ranges),
                    KeptRows::Ranges(&absolute),
                    exact,
                    segments_read,
                    &scan_budget,
                )?
            }
            // The selector judged nothing: the candidates pass only the
            // lookup's own column test.
            None => self.project_after_predicates(
                segment,
                predicate_ids,
                fetch,
                None,
                KeptRows::Ranges(&candidates),
                false,
                segments_read,
                &scan_budget,
            )?,
        };
        Ok(Some(chunk))
    }

    /// Routes one segment through the filter-first path when a predicate
    /// selector applies and the segment decodes as a full direct chunk.
    fn decode_column_chunk_maybe_filtered(
        &self,
        segment: segment::SegmentMeta,
        memory_limit: usize,
        prewhere: Option<(&[u32], PrewhereSelect<'_>)>,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        let full_direct = self.start <= segment.min_key && self.end >= segment.max_key;
        if let Some((predicate_ids, select)) = prewhere
            && full_direct
            && !predicate_ids.is_empty()
        {
            if let Some(chunk) = self.decode_by_side_index(
                &segment,
                0,
                segment.row_count,
                memory_limit,
                predicate_ids,
                select,
            )? {
                return Ok(chunk);
            }
            let map_projection = |ids: &[u32]| -> Result<Vec<usize>, StoreError> {
                ids.iter()
                    .map(|id| {
                        self.snapshot
                            .schema
                            .columns()
                            .iter()
                            .position(|column| column.id() == *id)
                            .ok_or_else(|| {
                                StoreError::FormatLimit(format!("unknown projected column id {id}"))
                            })
                    })
                    .collect()
            };
            let predicate_projection = map_projection(predicate_ids)?;
            let row_count = usize::try_from(segment.row_count)
                .map_err(|_| StoreError::FormatLimit("segment row count exceeds usize".into()))?;
            let scan_memory = AtomicUsize::new(0);
            let scan_budget = segment::ScanMemoryBudget::new(&scan_memory, memory_limit);
            if let Some((candidates, skipped)) =
                self.value_candidate_ranges(&segment, 0, row_count)?
            {
                return self.decode_value_candidates(
                    &segment,
                    &candidates,
                    skipped,
                    predicate_ids,
                    &predicate_projection,
                    select,
                    1,
                    &scan_budget,
                );
            }
            let fetch = segment::read_projected_columns(
                &self.snapshot.directory,
                &segment,
                &self.snapshot.schema,
                &predicate_projection,
                0,
                row_count,
                &scan_budget,
            )?;
            let ranges = select(&fetch.columns, row_count).map_err(StoreError::FormatLimit)?;
            if predicate_ids == self.column_ids {
                return retain_predicate_fetch(fetch, ranges.as_ref(), row_count, 1, &scan_budget);
            }
            return match ranges {
                Some(PrewhereRanges { ranges, exact }) => self.project_after_predicates(
                    &segment,
                    predicate_ids,
                    fetch,
                    Some(&ranges),
                    KeptRows::Ranges(&ranges),
                    exact,
                    1,
                    &scan_budget,
                ),
                None => self.project_after_predicates(
                    &segment,
                    predicate_ids,
                    fetch,
                    None,
                    KeptRows::Span(0, row_count),
                    false,
                    1,
                    &scan_budget,
                ),
            };
        }
        self.decode_column_chunk(segment, memory_limit)
    }

    #[allow(clippy::too_many_lines)]
    fn next_merged_column_chunk(
        &mut self,
        memory_limit: usize,
    ) -> Result<Option<ProjectedColumnChunk>, StoreError> {
        const MAX_MERGED_CHUNK_ROWS: usize = 8 * 1024;
        let projection = self
            .column_ids
            .iter()
            .map(|id| {
                self.snapshot
                    .schema
                    .columns()
                    .iter()
                    .position(|column| column.id() == *id)
                    .ok_or_else(|| {
                        StoreError::FormatLimit(format!("unknown projected column id {id}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let merge = self.merge.as_mut().expect("checked merged scan");
        let part_lo = merge.lo.clone();
        let part_hi = merge.hi.clone();
        let chunk_rows = if projection.is_empty() {
            MAX_MERGED_CHUNK_ROWS
        } else {
            memory_limit
                .checked_div(
                    projection
                        .len()
                        .saturating_mul(size_of::<pintail_types::Value>())
                        .saturating_mul(2),
                )
                .unwrap_or(0)
                .clamp(1, MAX_MERGED_CHUNK_ROWS)
        };
        let mut winner_sources = Vec::with_capacity(chunk_rows);
        while winner_sources.len() < chunk_rows {
            let minimum = merge
                .heads
                .iter()
                .filter_map(|row| row.as_ref().map(|row| &row.key))
                .chain(merge.memtable_head.as_ref().map(StoredRow::key))
                .min()
                .cloned();
            let Some(minimum) = minimum else {
                break;
            };
            // Every remaining head is at or past the smallest one, so once
            // that is beyond the part's upper bound nothing else qualifies:
            // the streams are left where they are rather than drained.
            if bound_below(&part_hi, &minimum) {
                break;
            }
            let mut winner = None::<(u64, bool, MergedWinnerSource)>;
            for (segment_index, (stream, head)) in
                merge.streams.iter_mut().zip(&mut merge.heads).enumerate()
            {
                while head.as_ref().is_some_and(|row| row.key == minimum) {
                    let candidate = head.take().expect("matching stream head");
                    if winner
                        .as_ref()
                        .is_none_or(|current| candidate.version >= current.0)
                    {
                        winner = Some((
                            candidate.version,
                            candidate.deleted,
                            MergedWinnerSource::Segment {
                                segment_index,
                                row_index: candidate.physical_index,
                            },
                        ));
                    }
                    *head = stream.next_header()?;
                }
            }
            if merge
                .memtable_head
                .as_ref()
                .is_some_and(|row| row.key() == &minimum)
            {
                let candidate = merge.memtable_head.take().expect("matching memtable head");
                if winner
                    .as_ref()
                    .is_none_or(|current| candidate.version() >= current.0)
                {
                    winner = Some((
                        candidate.version(),
                        candidate.is_deleted(),
                        MergedWinnerSource::Memtable(
                            projection
                                .iter()
                                .map(|index| candidate.values()[*index].clone())
                                .collect(),
                        ),
                    ));
                }
                let reseek_lo = std::ops::Bound::Excluded(minimum.clone());
                merge.memtable_head = if bound_range_is_searchable(&reseek_lo, &part_hi) {
                    self.snapshot
                        .memtable
                        .range((reseek_lo, part_hi.clone()))
                        .next()
                        .map(|(_, row)| row.clone())
                } else {
                    None
                };
            }
            let winner = winner.expect("minimum key has a winning row");
            if !bounds_contain(&part_lo, &part_hi, &minimum) || winner.1 {
                continue;
            }
            winner_sources.push(winner.2);
        }
        let row_count = winner_sources.len();
        if row_count == 0 {
            return Ok(None);
        }
        let first_chunk = !std::mem::replace(&mut merge.reported_segments, true);
        let report_pruned = first_chunk && !std::mem::replace(&mut self.reported_pruned, true);
        // The winners are placed straight into the output columns. The
        // fetch below is already column-major and so is the chunk, so
        // turning it into rows and back cost two transposes and one vector
        // allocation per row, for a representation nothing downstream
        // wanted.
        let mut columns = projection
            .iter()
            .map(|_| vec![pintail_types::Value::Null; row_count])
            .collect::<Vec<_>>();
        let mut placed = 0_usize;
        let mut segment_rows = BTreeMap::<usize, Vec<(usize, usize)>>::new();
        for (winner_index, source) in winner_sources.into_iter().enumerate() {
            match source {
                MergedWinnerSource::Segment { .. } if projection.is_empty() => placed += 1,
                MergedWinnerSource::Segment {
                    segment_index,
                    row_index,
                } => segment_rows
                    .entry(segment_index)
                    .or_default()
                    .push((row_index, winner_index)),
                MergedWinnerSource::Memtable(values) => {
                    if values.len() != columns.len() {
                        return Err(StoreError::FormatLimit(
                            "a merged memtable winner has a different width from the projection"
                                .into(),
                        ));
                    }
                    for (column, value) in columns.iter_mut().zip(values) {
                        column[winner_index] = value;
                    }
                    placed += 1;
                }
            }
        }
        let mut blocks_decoded = 0;
        for (segment_index, selected) in segment_rows {
            let row_indices = selected
                .iter()
                .map(|(row_index, _)| *row_index)
                .collect::<Vec<_>>();
            let scan_memory = AtomicUsize::new(0);
            let scan_budget = segment::ScanMemoryBudget::new(&scan_memory, memory_limit);
            let fetch = segment::read_projected_rows(
                &self.snapshot.directory,
                &self.segments[segment_index],
                &self.snapshot.schema,
                &projection,
                &row_indices,
                &scan_budget,
            )?;
            blocks_decoded += fetch.blocks_decoded;
            if fetch.columns.len() != columns.len() {
                return Err(StoreError::FormatLimit(
                    "a merged segment fetch has a different width from the projection".into(),
                ));
            }
            for (column, fetched) in columns.iter_mut().zip(fetch.columns) {
                if fetched.len() != selected.len() {
                    return Err(StoreError::FormatLimit(
                        "projected column length differs from its selected row count".into(),
                    ));
                }
                for ((_, winner_index), value) in selected.iter().zip(fetched) {
                    column[*winner_index] = value;
                }
            }
            scan_budget.release(fetch.reserved_bytes);
            placed += selected.len();
        }
        if placed != row_count {
            return Err(StoreError::FormatLimit(
                "a merged winner was not materialized".into(),
            ));
        }
        let retained_bytes = size_of::<ProjectedColumnChunk>()
            .saturating_add(
                columns
                    .capacity()
                    .saturating_mul(size_of::<Vec<pintail_types::Value>>()),
            )
            .saturating_add(
                columns
                    .iter()
                    .map(|values| {
                        values
                            .capacity()
                            .saturating_mul(size_of::<pintail_types::Value>())
                            .saturating_add(
                                values.iter().map(pintail_types::Value::heap_bytes).sum(),
                            )
                    })
                    .sum(),
            );
        if retained_bytes > memory_limit {
            return Err(StoreError::MemoryLimitExceeded {
                used: 0,
                requested: retained_bytes,
                limit: memory_limit,
            });
        }
        Ok(Some(ProjectedColumnChunk {
            prefiltered: false,
            columns: columns.into_iter().map(DecodedColumn::Values).collect(),
            row_count,
            stats: ScanStats {
                segments_read: usize::from(first_chunk) * self.segments.len(),
                segments_pruned: usize::from(report_pruned) * self.pruned_segments,
                blocks_decoded,
                ..ScanStats::default()
            },
            retained_bytes,
            column_decode: Vec::new(),
        }))
    }

    /// The physical rows of `segment` this scan's key range selects, for a
    /// decode that reads them in row slices: every row when the range covers
    /// the segment, the located run otherwise, and `None` when the run
    /// cannot be located - a slice decode applies no key bounds, so it must
    /// not be given rows the range excludes.
    fn bounded_rows(
        &self,
        segment: &segment::SegmentMeta,
        memory_limit: usize,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        if self.start <= segment.min_key && self.end >= segment.max_key {
            return Ok(Some((0, segment.row_count)));
        }
        let scan_memory = AtomicUsize::new(0);
        let budget = segment::ScanMemoryBudget::new(&scan_memory, memory_limit);
        Ok(self.key_row_span(segment, &budget)?.map(|span| {
            let row = |value: usize| u64::try_from(value).unwrap_or(u64::MAX);
            (row(span.rows.start), row(span.rows.end))
        }))
    }

    /// Decodes `[start_row, end_row)` of `segment` within `memory_limit`, in
    /// as many slices as the budget needs: a slice that does not fit is
    /// halved and retried, the size that fits is kept for the rest of the
    /// segment, and the remainder stays queued as the next direct range.
    /// A slice is never finer than a block, which is what the reader
    /// decodes at once, so the budget must hold one block of the projection.
    fn decode_direct_range_within(
        &mut self,
        segment: segment::SegmentMeta,
        start_row: u64,
        end_row: u64,
        memory_limit: usize,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        // Slices are whole blocks: the reader decodes a block at once, so a
        // slice cut inside one pays for the whole block anyway, and a slice
        // straddling two would shrink toward single rows.
        let block = u64::try_from(segment::block_rows(
            &self.snapshot.directory,
            &segment,
            &self.snapshot.schema,
        )?)
        .unwrap_or(u64::MAX)
        .max(1);
        let span = end_row.saturating_sub(start_row).max(1);
        let align = |rows: u64| rows.div_ceil(block).max(1).saturating_mul(block).min(span);
        let mut rows = align(self.direct_slice_rows.unwrap_or(span));
        loop {
            let slice_end = start_row.saturating_add(rows).min(end_row);
            match self.decode_column_chunk_rows(&segment, start_row, slice_end, memory_limit) {
                Ok(chunk) => {
                    if slice_end < end_row {
                        self.direct_range = Some((segment, slice_end, end_row));
                        self.direct_slice_rows = Some(rows);
                    } else {
                        self.direct_slice_rows = None;
                    }
                    return Ok(chunk);
                }
                Err(StoreError::MemoryLimitExceeded { .. }) if rows > block => {
                    rows = align(rows / 2);
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Decodes one contiguous row range of a segment (a granule-classified
    /// direct part) into a column chunk, bypassing merge machinery.
    fn decode_column_chunk_rows(
        &self,
        segment: &segment::SegmentMeta,
        start_row: u64,
        end_row: u64,
        memory_limit: usize,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        let projection = self
            .column_ids
            .iter()
            .map(|id| {
                self.snapshot
                    .schema
                    .columns()
                    .iter()
                    .position(|column| column.id() == *id)
                    .ok_or_else(|| {
                        StoreError::FormatLimit(format!("unknown projected column id {id}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let start = usize::try_from(start_row)
            .map_err(|_| StoreError::FormatLimit("range start exceeds usize".into()))?;
        let end = usize::try_from(end_row)
            .map_err(|_| StoreError::FormatLimit("range end exceeds usize".into()))?;
        let scan_memory = AtomicUsize::new(0);
        let scan_budget = segment::ScanMemoryBudget::new(&scan_memory, memory_limit);
        // A segment read in ranges is still one segment read.
        self.decode_projected_rows(
            segment,
            start..end,
            &projection,
            usize::from(start_row == 0),
            &scan_budget,
        )
    }

    /// Decodes `rows` of a direct segment in every projected column, less
    /// the blocks the scan's value bounds rule out (see
    /// [`Self::value_candidate_ranges`]): their rows fail the scan's filter,
    /// which the executor applies to whatever is returned.
    fn decode_projected_rows(
        &self,
        segment: &segment::SegmentMeta,
        rows: std::ops::Range<usize>,
        projection: &[usize],
        segments_read: usize,
        scan_budget: &segment::ScanMemoryBudget<'_>,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        let (fetch, row_count, value_skipped) =
            match self.value_candidate_ranges(segment, rows.start, rows.end)? {
                Some((candidates, skipped)) => (
                    segment::read_projected_column_ranges(
                        &self.snapshot.directory,
                        segment,
                        &self.snapshot.schema,
                        projection,
                        &candidates,
                        scan_budget,
                    )?,
                    candidates
                        .iter()
                        .map(std::iter::ExactSizeIterator::len)
                        .sum::<usize>(),
                    skipped,
                ),
                None => (
                    segment::read_projected_columns(
                        &self.snapshot.directory,
                        segment,
                        &self.snapshot.schema,
                        projection,
                        rows.start,
                        rows.end,
                        scan_budget,
                    )?,
                    rows.len(),
                    0,
                ),
            };
        let retained_bytes = size_of::<ProjectedColumnChunk>()
            .saturating_add(
                fetch
                    .columns
                    .capacity()
                    .saturating_mul(size_of::<DecodedColumn>()),
            )
            .saturating_add(
                fetch
                    .columns
                    .iter()
                    .map(DecodedColumn::retained_bytes)
                    .sum(),
            );
        scan_budget.release(fetch.reserved_bytes);
        scan_budget.reserve(retained_bytes)?;
        Ok(ProjectedColumnChunk {
            prefiltered: false,
            columns: fetch.columns,
            row_count,
            stats: ScanStats {
                segments_read,
                blocks_decoded: fetch.blocks_decoded,
                blocks_read: fetch.blocks_read,
                blocks_pruned: fetch.blocks_pruned,
                blocks_value_skipped: value_skipped,
                ..ScanStats::default()
            }
            .with_decode(&fetch.column_decode),
            retained_bytes,
            column_decode: fetch.column_decode,
        })
    }

    #[allow(clippy::too_many_lines)]
    /// The rows of a segment the scan's key range covers in part, as one
    /// run: a directly served segment holds each key once, in key order, so
    /// the rows in range are those between the first key at or above the
    /// range's start and the first key past its end. The sparse index names
    /// the blocks that can hold them; only those blocks' integer key
    /// columns, which the executor named, are read to find the run. `None`
    /// without them.
    fn key_row_span(
        &self,
        segment: &segment::SegmentMeta,
        memory: &segment::ScanMemoryBudget<'_>,
    ) -> Result<Option<KeySpan>, StoreError> {
        let Some(key_ids) = self.overlay_key.as_deref() else {
            return Ok(None);
        };
        if !segment.unique_keys
            || key_ids.len() != self.start.parts().len()
            || key_ids.len() != self.end.parts().len()
        {
            return Ok(None);
        }
        let Some(projection) = key_ids
            .iter()
            .map(|id| {
                self.snapshot
                    .schema
                    .columns()
                    .iter()
                    .position(|column| column.id() == *id)
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let row_count = usize::try_from(segment.row_count)
            .map_err(|_| StoreError::FormatLimit("segment row count exceeds usize".into()))?;
        // Each entry is a block's first row and first key. The blocks that
        // can hold the range run from the last one starting at or before its
        // start to the last one starting at or before its end.
        let sparse = segment::read_sparse_index(&self.snapshot.directory, segment)?;
        let block_row = |block: usize| {
            sparse
                .get(block)
                .map_or(Ok(row_count), |(row, _)| usize::try_from(*row))
                .map_err(|_| StoreError::FormatLimit("block row exceeds usize".into()))
        };
        let (first_block, end_block) = if sparse.is_empty() {
            (0, 1)
        } else {
            (
                sparse
                    .partition_point(|(_, key)| *key <= self.start)
                    .saturating_sub(1),
                sparse.partition_point(|(_, key)| *key <= self.end),
            )
        };
        let blocks = sparse.len().max(1);
        if end_block <= first_block {
            return Ok(Some(KeySpan {
                rows: 0..0,
                key_blocks_decoded: 0,
                blocks_read: 0,
                blocks_pruned: blocks,
            }));
        }
        let window = if sparse.is_empty() {
            0..row_count
        } else {
            block_row(first_block)?..block_row(end_block)?
        };
        let fetch = segment::read_projected_columns(
            &self.snapshot.directory,
            segment,
            &self.snapshot.schema,
            &projection,
            window.start,
            window.end,
            memory,
        )?;
        let reserved = fetch.reserved_bytes;
        let key_blocks_decoded = fetch.blocks_decoded;
        let keys = fetch
            .columns
            .iter()
            .map(|column| match column {
                DecodedColumn::Int64 { values, .. } => {
                    Some(values.iter().map(|value| i128::from(*value)).collect())
                }
                DecodedColumn::UInt64 { values, .. } => {
                    Some(values.iter().map(|value| i128::from(*value)).collect())
                }
                _ => None,
            })
            .collect::<Option<Vec<Vec<i128>>>>();
        let bound = |key: &PrimaryKey| {
            key.parts()
                .iter()
                .map(|part| match part {
                    pintail_types::KeyPart::Int64(value) => Some(i128::from(*value)),
                    pintail_types::KeyPart::UInt64(value) => Some(i128::from(*value)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
        };
        let span = match (keys, bound(&self.start), bound(&self.end)) {
            (Some(keys), Some(start), Some(end)) => {
                let compare = |row: usize, bound: &[i128]| {
                    keys.iter()
                        .zip(bound)
                        .map(|(column, part)| column[row].cmp(part))
                        .find(|ordering| ordering.is_ne())
                        .unwrap_or(std::cmp::Ordering::Equal)
                };
                // The first window row for which `past` holds; rows are in
                // key order.
                let first_where = |past: &dyn Fn(usize) -> bool| {
                    let (mut low, mut high) = (0, window.len());
                    while low < high {
                        let middle = low + (high - low) / 2;
                        if past(middle) {
                            high = middle;
                        } else {
                            low = middle + 1;
                        }
                    }
                    low
                };
                let first = first_where(&|row| compare(row, &start).is_ge());
                let last = first_where(&|row| compare(row, &end).is_gt());
                Some(KeySpan {
                    rows: window.start + first..window.start + last.max(first),
                    key_blocks_decoded,
                    blocks_read: end_block - first_block,
                    blocks_pruned: blocks - (end_block - first_block),
                })
            }
            _ => None,
        };
        drop(fetch);
        memory.release(reserved);
        Ok(span)
    }

    /// A run of a segment's rows decoded as the projected columns.
    fn decode_segment_rows(
        &self,
        segment: &segment::SegmentMeta,
        rows: std::ops::Range<usize>,
        scan_budget: &segment::ScanMemoryBudget<'_>,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        let projection = self
            .column_ids
            .iter()
            .map(|id| {
                self.snapshot
                    .schema
                    .columns()
                    .iter()
                    .position(|column| column.id() == *id)
                    .ok_or_else(|| {
                        StoreError::FormatLimit(format!("unknown projected column id {id}"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.decode_projected_rows(segment, rows, &projection, 1, scan_budget)
    }

    fn decode_column_chunk(
        &self,
        segment: segment::SegmentMeta,
        memory_limit: usize,
    ) -> Result<ProjectedColumnChunk, StoreError> {
        let covered = self.start <= segment.min_key && self.end >= segment.max_key;
        let scan_memory = AtomicUsize::new(0);
        let span_budget = segment::ScanMemoryBudget::new(&scan_memory, memory_limit);
        let span = if covered {
            None
        } else {
            self.key_row_span(&segment, &span_budget)?
        };
        if covered || span.is_some() {
            let rows = match &span {
                Some(span) => span.rows.clone(),
                None => {
                    0..usize::try_from(segment.row_count).map_err(|_| {
                        StoreError::FormatLimit("segment row count exceeds usize".into())
                    })?
                }
            };
            let mut chunk = self.decode_segment_rows(&segment, rows, &span_budget)?;
            // A run reports the key blocks its range selected, as the merge
            // path does.
            if let Some(span) = span {
                chunk.stats.blocks_read = span.blocks_read;
                chunk.stats.blocks_pruned = span.blocks_pruned;
                chunk.stats.blocks_decoded += span.key_blocks_decoded;
            }
            return Ok(chunk);
        }
        let mut manifest = self.snapshot.manifest.as_ref().clone();
        manifest.segments = vec![segment];
        let chunk = TableSnapshot {
            instance: self.snapshot.instance,
            memtable: Arc::new(BTreeMap::new()),
            manifest: Arc::new(manifest),
            directory: self.snapshot.directory.clone(),
            schema: self.snapshot.schema.clone(),
            estimated_bytes: 0,
        };
        let projected = chunk.scan_projected_range_bounded(
            &self.start,
            &self.end,
            &self.column_ids,
            memory_limit,
        )?;
        let stats = projected.stats();
        let rows = projected
            .into_rows()
            .into_iter()
            .map(ProjectedRow::into_values)
            .collect::<Vec<_>>();
        let row_count = rows.len();
        let columns = rows_to_columns(rows, self.column_ids.len())?
            .into_iter()
            .map(DecodedColumn::Values)
            .collect::<Vec<_>>();
        let retained_bytes = size_of::<ProjectedColumnChunk>()
            .saturating_add(
                columns
                    .capacity()
                    .saturating_mul(size_of::<DecodedColumn>()),
            )
            .saturating_add(columns.iter().map(DecodedColumn::retained_bytes).sum());
        Ok(ProjectedColumnChunk {
            prefiltered: false,
            columns,
            row_count,
            stats,
            retained_bytes,
            column_decode: Vec::new(),
        })
    }

    /// Returns the scanned key range.
    /// Names the user columns holding the table's key, in key order; every
    /// part must be an integer column. A segment the memtable overlaps can
    /// then be decoded directly, with the superseded rows masked out by
    /// those columns, instead of merged row by row. Ignored for an absent or
    /// non-integer column. Call before the first chunk is pulled.
    ///
    /// The memtable's live rows are placed among the segment's rows by key,
    /// so the stream stays in key order; a consumer that takes the first
    /// value it meets for a group sees the same row the merge would show.
    pub fn enable_memtable_overlay(&mut self, key_column_ids: &[u32]) {
        let integer = |id: u32| {
            self.snapshot
                .schema
                .columns()
                .iter()
                .find(|column| column.id() == id)
                .is_some_and(|column| {
                    matches!(
                        column.data_type(),
                        pintail_types::DataType::Int8
                            | pintail_types::DataType::Int16
                            | pintail_types::DataType::Int32
                            | pintail_types::DataType::Int64
                            | pintail_types::DataType::UInt8
                            | pintail_types::DataType::UInt16
                            | pintail_types::DataType::UInt32
                            | pintail_types::DataType::UInt64
                    )
                })
        };
        if !key_column_ids.is_empty() && key_column_ids.iter().all(|id| integer(*id)) {
            self.overlay_key = Some(key_column_ids.to_vec());
        }
    }

    /// The key columns [`Self::enable_memtable_overlay`] accepted, so a
    /// stream rebuilt over a narrower range can be given the same.
    #[must_use]
    pub fn memtable_overlay_key(&self) -> Option<&[u32]> {
        self.overlay_key.as_deref()
    }

    #[must_use]
    pub fn key_range(&self) -> (&PrimaryKey, &PrimaryKey) {
        (&self.start, &self.end)
    }

    /// Returns the projected stable column IDs in output order.
    #[must_use]
    pub fn column_ids(&self) -> &[u32] {
        &self.column_ids
    }

    /// Returns the snapshot this stream decodes from.
    #[must_use]
    pub const fn snapshot(&self) -> &TableSnapshot {
        &self.snapshot
    }

    /// Returns immutable segments that will be decoded.
    #[must_use]
    pub const fn segment_count(&self) -> usize {
        self.candidate_segments
    }

    /// Returns immutable segments excluded by key-range or bloom pruning.
    #[must_use]
    pub const fn pruned_segment_count(&self) -> usize {
        self.pruned_segments
    }
}

pub(super) fn columns_to_rows(
    mut columns: Vec<Vec<pintail_types::Value>>,
    row_count: usize,
) -> Result<Vec<Vec<pintail_types::Value>>, StoreError> {
    if columns.iter().any(|column| column.len() != row_count) {
        return Err(StoreError::FormatLimit(
            "projected column length differs from its segment row count".into(),
        ));
    }
    for column in &mut columns {
        column.reverse();
    }
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        rows.push(
            columns
                .iter_mut()
                .map(|column| {
                    column.pop().ok_or_else(|| {
                        StoreError::FormatLimit("projected column ended before its rows".into())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    Ok(rows)
}

fn rows_to_columns(
    rows: Vec<Vec<pintail_types::Value>>,
    column_count: usize,
) -> Result<Vec<Vec<pintail_types::Value>>, StoreError> {
    let row_count = rows.len();
    let mut columns = (0..column_count)
        .map(|_| Vec::with_capacity(row_count))
        .collect::<Vec<_>>();
    for row in rows {
        if row.len() != column_count {
            return Err(StoreError::FormatLimit(
                "projected row length differs from its projection".into(),
            ));
        }
        for (column, value) in columns.iter_mut().zip(row) {
            column.push(value);
        }
    }
    Ok(columns)
}

/// Where a filter-first read finds its kept rows in the segment.
#[derive(Clone, Copy)]
enum KeptRows<'ranges> {
    /// Every row of `start..end`.
    Span(usize, usize),
    /// These ordered, disjoint row ranges.
    Ranges(&'ranges [std::ops::Range<usize>]),
}

/// Keeps only the rows `ranges` names in one decoded column, in place:
/// native values, text arenas and dictionary codes are moved down rather
/// than decoded again. `ranges` must be ordered, disjoint and in bounds, and
/// `selected` their total length.
fn compact_decoded_column(
    column: &mut DecodedColumn,
    ranges: &[std::ops::Range<usize>],
    selected: usize,
) {
    fn compact<T: Copy>(values: &mut Vec<T>, ranges: &[std::ops::Range<usize>]) {
        // A scattered filter keeps runs of a row or two; a memmove call per
        // run cost more than the copy, so short runs move value by value.
        const SHORT_RUN: usize = 16;
        let mut written = 0;
        for range in ranges {
            if range.len() <= SHORT_RUN {
                for row in range.clone() {
                    values[written] = values[row];
                    written += 1;
                }
            } else {
                values.copy_within(range.clone(), written);
                written += range.len();
            }
        }
        values.truncate(written);
        values.shrink_to_fit();
    }
    let validity = match column {
        DecodedColumn::Int64 { values, validity }
        | DecodedColumn::NativeUnits {
            values, validity, ..
        } => {
            compact(values, ranges);
            validity
        }
        DecodedColumn::UInt64 { values, validity }
        | DecodedColumn::Float64 {
            bits: values,
            validity,
        } => {
            compact(values, ranges);
            validity
        }
        DecodedColumn::DictionaryUtf8 {
            codes, validity, ..
        } => {
            compact(codes, ranges);
            validity
        }
        DecodedColumn::Utf8 {
            heap,
            offsets,
            validity,
        } => {
            let mut written_rows = 0;
            let mut written_bytes = 0;
            for range in ranges {
                for row in range.clone() {
                    let start = offsets[row];
                    let end = offsets[row + 1];
                    heap.copy_within(start..end, written_bytes);
                    offsets[written_rows] = written_bytes;
                    written_bytes += end - start;
                    written_rows += 1;
                }
            }
            offsets[written_rows] = written_bytes;
            offsets.truncate(written_rows + 1);
            heap.truncate(written_bytes);
            offsets.shrink_to_fit();
            heap.shrink_to_fit();
            validity
        }
        DecodedColumn::Values(values) => {
            let mut row = 0;
            let mut range_index = 0;
            values.retain(|_| {
                while range_index < ranges.len() && row >= ranges[range_index].end {
                    range_index += 1;
                }
                let keep = ranges
                    .get(range_index)
                    .is_some_and(|range| range.contains(&row));
                row += 1;
                keep
            });
            values.shrink_to_fit();
            return;
        }
    };
    match validity {
        ColumnValidity::AllValid(count) => *count = selected,
        ColumnValidity::Bytes(bits) => compact(bits, ranges),
    }
}

/// Checks a selector's ranges before anything indexes by them: a selector
/// is an API callback.
fn check_selected_ranges(ranges: &[std::ops::Range<usize>], rows: usize) -> Result<(), StoreError> {
    if ranges
        .iter()
        .any(|range| range.start > range.end || range.end > rows)
        || ranges.windows(2).any(|pair| pair[0].end > pair[1].start)
    {
        return Err(StoreError::FormatLimit(
            "invalid predicate row ranges".to_owned(),
        ));
    }
    Ok(())
}

/// Rows of a direct segment that block value skipping leaves to decode
/// (absolute, ascending, disjoint), and how many blocks it skipped.
type ValueCandidates = (Vec<std::ops::Range<usize>>, usize);

/// Maps `ranges` over the rows of `candidates` laid end to end (as a fetch
/// of those row ranges returns them) back to the segment positions they
/// name. `ranges` must be checked against the candidates' row total.
fn candidate_positions(
    ranges: &[std::ops::Range<usize>],
    candidates: &[std::ops::Range<usize>],
) -> Vec<std::ops::Range<usize>> {
    let mut positions: Vec<std::ops::Range<usize>> = Vec::with_capacity(ranges.len());
    let mut candidate = 0_usize;
    // Packed offset of `candidates[candidate].start`.
    let mut base = 0_usize;
    for range in ranges {
        let mut at = range.start;
        while at < range.end {
            while base + candidates[candidate].len() <= at {
                base += candidates[candidate].len();
                candidate += 1;
            }
            let span = &candidates[candidate];
            let until = range.end.min(base + span.len());
            let mapped = span.start + (at - base)..span.start + (until - base);
            match positions.last_mut() {
                Some(last) if last.end == mapped.start => last.end = mapped.end,
                _ => positions.push(mapped),
            }
            at = until;
        }
    }
    positions
}

/// The predicate projection already decoded every output column. Compact
/// those buffers before the prefetch round retains them, preserving native
/// values, text arenas and dictionary codes instead of decoding them again.
fn retain_predicate_fetch(
    mut fetch: segment::ProjectedColumnFetch,
    selection: Option<&PrewhereRanges>,
    rows: usize,
    segments_read: usize,
    memory: &segment::ScanMemoryBudget<'_>,
) -> Result<ProjectedColumnChunk, StoreError> {
    let ranges = selection.map(|selection| selection.ranges.as_slice());
    let selected = ranges.map_or(rows, |ranges| {
        ranges.iter().map(std::iter::ExactSizeIterator::len).sum()
    });
    if let Some(ranges) = ranges {
        check_selected_ranges(ranges, rows)?;
        for column in &mut fetch.columns {
            compact_decoded_column(column, ranges, selected);
        }
    }
    let retained_bytes = size_of::<ProjectedColumnChunk>()
        + fetch.columns.capacity() * size_of::<DecodedColumn>()
        + fetch
            .columns
            .iter()
            .map(DecodedColumn::retained_bytes)
            .sum::<usize>();
    memory.release(fetch.reserved_bytes);
    memory.reserve(retained_bytes)?;
    Ok(ProjectedColumnChunk {
        prefiltered: selection.is_some_and(|selection| selection.exact),
        columns: fetch.columns,
        row_count: selected,
        stats: ScanStats {
            segments_read,
            blocks_read: fetch.blocks_read,
            blocks_pruned: fetch.blocks_pruned,
            blocks_decoded: fetch.blocks_decoded,
            ..ScanStats::default()
        }
        .with_decode(&fetch.column_decode),
        retained_bytes,
        column_decode: fetch.column_decode,
    })
}

#[cfg(test)]
mod overlay_primitive_tests {
    use super::{ColumnValidity, DecodedColumn, overlay_positions, subtract_positions};
    use pintail_types::{KeyPart, PrimaryKey, StoredRow, Value};

    fn packed(values: &[u64]) -> DecodedColumn {
        DecodedColumn::UInt64 {
            values: values.to_vec(),
            validity: ColumnValidity::AllValid(values.len()),
        }
    }

    fn plain(values: &[u64]) -> DecodedColumn {
        DecodedColumn::Values(values.iter().map(|value| Value::UInt64(*value)).collect())
    }

    fn row(id: u64) -> StoredRow {
        StoredRow::new(
            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
            vec![Value::UInt64(id)],
            2,
            false,
        )
    }

    #[test]
    fn the_walk_masks_superseded_rows_and_places_inserts_in_key_order() {
        let live = [row(4), row(9), row(25), row(45)];
        // Segment keys 2..16 step 2; memtable: 4 updated, 8 deleted, 9
        // inserted between 8 and 10, 25 and 45 inserted past the end.
        let memtable: Vec<(Vec<i128>, Option<&StoredRow>)> = vec![
            (vec![4], Some(&live[0])),
            (vec![8], None),
            (vec![9], Some(&live[1])),
            (vec![25], Some(&live[2])),
            (vec![45], Some(&live[3])),
        ];
        let values = [2_u64, 4, 6, 8, 10, 12, 14, 16];
        for column in [packed(&values), plain(&values)] {
            let (excluded, inserts) = overlay_positions(&[&column], 8, None, &memtable);
            assert_eq!(excluded, vec![1, 3], "keys 4 and 8 are superseded");
            // Output: 2, 4*, 6, 9*, 10, 12, 14, 16, 25*, 45*.
            assert_eq!(inserts, vec![1, 3, 8, 9]);
        }
        // With only rows 2..6 (positions 2..=5) kept by a predicate. The
        // memtable's live rows are placed whatever the predicate said about
        // the segment rows they replace (the filter runs again above): the
        // output is 4*, 6, 9*, 10, 12, 25*, 45*.
        let kept: Vec<std::ops::Range<usize>> = std::iter::once(2..6_usize).collect();
        let (excluded, inserts) = overlay_positions(&[&packed(&values)], 8, Some(&kept), &memtable);
        assert_eq!(excluded, vec![1, 3]);
        assert_eq!(inserts, vec![0, 2, 5, 6]);
        // Every row superseded, nothing live: no inserts.
        let all: Vec<(Vec<i128>, Option<&StoredRow>)> = values
            .iter()
            .map(|value| (vec![i128::from(*value)], None))
            .collect();
        let (excluded, inserts) = overlay_positions(&[&packed(&values)], 8, None, &all);
        assert_eq!(excluded, (0..8).collect::<Vec<_>>());
        assert!(inserts.is_empty());
    }

    #[test]
    fn a_composite_key_compares_part_by_part() {
        let first = DecodedColumn::Int64 {
            values: vec![1, 1, 2, 2, 3],
            validity: ColumnValidity::AllValid(5),
        };
        let second = DecodedColumn::Int64 {
            values: vec![5, 9, 1, 7, 0],
            validity: ColumnValidity::AllValid(5),
        };
        let live = [row(1), row(2), row(3)];
        // (1,9) updated; (2,3) inserted between (2,1) and (2,7); (4,0)
        // inserted past the end; (3,0) deleted.
        let memtable: Vec<(Vec<i128>, Option<&StoredRow>)> = vec![
            (vec![1, 9], Some(&live[0])),
            (vec![2, 3], Some(&live[1])),
            (vec![3, 0], None),
            (vec![4, 0], Some(&live[2])),
        ];
        let (excluded, inserts) = overlay_positions(&[&first, &second], 5, None, &memtable);
        assert_eq!(excluded, vec![1, 4]);
        // Output: (1,5), (1,9)*, (2,1), (2,3)*, (2,7), (4,0)*.
        assert_eq!(inserts, vec![1, 3, 5]);
        // Signed keys past the unsigned range and large unsigned keys stay
        // distinct.
        let signed = DecodedColumn::Int64 {
            values: vec![-3, -1, 0],
            validity: ColumnValidity::AllValid(3),
        };
        let memtable: Vec<(Vec<i128>, Option<&StoredRow>)> =
            vec![(vec![-1], None), (vec![i128::from(u64::MAX)], None)];
        let (excluded, _) = overlay_positions(&[&signed], 3, None, &memtable);
        assert_eq!(excluded, vec![1]);
    }

    #[test]
    fn subtracting_positions_cuts_every_range_exactly() {
        let all: Vec<std::ops::Range<usize>> = std::iter::once(0..10_usize).collect();
        assert_eq!(subtract_positions(all.clone(), &[]), vec![0..10]);
        assert_eq!(subtract_positions(all.clone(), &[0]), vec![1..10]);
        assert_eq!(subtract_positions(all.clone(), &[9]), vec![0..9]);
        assert_eq!(
            subtract_positions(all.clone(), &[3, 4, 5]),
            vec![0..3, 6..10]
        );
        assert!(subtract_positions(all, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]).is_empty());
        let kept = vec![2..5_usize, 8..12];
        assert_eq!(
            subtract_positions(kept, &[0, 3, 6, 8, 11, 20]),
            vec![2..3, 4..5, 9..11]
        );
    }

    #[test]
    fn interleave_keeps_integer_columns_packed_and_falls_back_for_others() {
        let column = DecodedColumn::Int64 {
            values: vec![10, 30],
            validity: ColumnValidity::AllValid(2),
        };
        let (a, b, c) = (Value::Int64(5), Value::Int64(20), Value::Int64(40));
        let merged = column.interleave(&[(0, &a), (2, &b), (4, &c)]);
        match merged {
            DecodedColumn::Int64 { values, validity } => {
                assert_eq!(values, vec![5, 10, 20, 30, 40]);
                assert!(matches!(validity, ColumnValidity::AllValid(5)));
            }
            other => panic!("expected a packed column, got {other:?}"),
        }
        let column = DecodedColumn::Int64 {
            values: vec![1, 2],
            validity: ColumnValidity::Bytes(vec![true, false]),
        };
        let merged = column.interleave(&[(1, &Value::Null)]);
        match merged {
            DecodedColumn::Int64 { values, validity } => {
                assert_eq!(values, vec![1, 0, 2]);
                assert_eq!(
                    (0..3).map(|row| validity.is_valid(row)).collect::<Vec<_>>(),
                    vec![true, false, false]
                );
            }
            other => panic!("expected a packed column, got {other:?}"),
        }
        let column = DecodedColumn::Int64 {
            values: vec![1, 2],
            validity: ColumnValidity::AllValid(2),
        };
        let text = Value::Utf8("x".to_owned());
        let merged = column.interleave(&[(2, &text)]);
        assert_eq!(
            merged.into_values(),
            vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::Utf8("x".to_owned())
            ]
        );
        let column = DecodedColumn::Values(vec![Value::UInt64(2), Value::UInt64(4)]);
        let (a, b, c) = (Value::UInt64(1), Value::UInt64(3), Value::UInt64(5));
        assert_eq!(
            column
                .interleave(&[(0, &a), (2, &b), (4, &c)])
                .into_values(),
            [1_u64, 2, 3, 4, 5].map(Value::UInt64).to_vec()
        );
    }
}

#[cfg(test)]
mod candidate_position_tests {
    use super::candidate_positions;

    #[test]
    fn packed_ranges_map_back_across_candidate_gaps() {
        // Candidates 10..20 and 40..45 pack as rows 0..10 and 10..15.
        let candidates = [10..20, 40..45];
        assert_eq!(
            candidate_positions(&[0..3, 8..12, 14..15], &candidates),
            vec![10..13, 18..20, 40..42, 44..45]
        );
        // Candidates that touch map to one range.
        assert_eq!(
            candidate_positions(std::slice::from_ref(&(0..15)), &[0..5, 5..15]),
            std::iter::once(0..15).collect::<Vec<_>>()
        );
        assert_eq!(candidate_positions(&[], &candidates), Vec::new());
        assert_eq!(
            candidate_positions(std::slice::from_ref(&(4..4)), &candidates),
            Vec::new()
        );
    }
}
