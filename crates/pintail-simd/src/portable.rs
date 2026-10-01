//! The kernel bodies, written so the compiler vectorizes them.
//!
//! Every function here is plain safe Rust shaped for the auto-vectorizer:
//! fixed-width lane arrays as accumulators, `chunks_exact` so the inner loop
//! has a constant trip count, no early exits and no per-element bounds checks.
//! They are `#[inline(always)]` so that the dispatching wrappers in the crate
//! root can inline them into a function compiled for AVX2 or AVX-512; called
//! directly, they compile for whatever the build's baseline target is. The
//! benchmarks call both forms to keep the dispatch honest.

/// Independent accumulators per reduction. Eight `i64`/`f64` lanes fill one
/// AVX-512 register or two AVX2 registers, enough to hide add latency.
pub const LANES: usize = 8;

/// Rows per packed mask word, matching the executor's `u64` selection words.
pub const WORD_BITS: usize = 64;

/// Exact sum of `values` as `i128`.
///
/// Each value is split into its unsigned low 32 bits and its signed high 32
/// bits, summed in separate `u64`/`i64` lanes. Neither lane can overflow
/// within a block of 2^30 values, so the result is exact for any input
/// without a per-element overflow check, and the loop vectorizes.
#[inline(always)]
#[must_use]
pub fn sum_i64(values: &[i64]) -> i128 {
    const BLOCK: usize = 1 << 30;
    let mut total = 0_i128;
    for block in values.chunks(BLOCK) {
        let mut low = [0_u64; LANES];
        let mut high = [0_i64; LANES];
        let mut chunks = block.chunks_exact(LANES);
        for chunk in &mut chunks {
            for ((low, high), &value) in low.iter_mut().zip(high.iter_mut()).zip(chunk) {
                *low = low.wrapping_add(value.cast_unsigned() & 0xFFFF_FFFF);
                *high = high.wrapping_add(value >> 32);
            }
        }
        for (lane, &value) in chunks.remainder().iter().enumerate() {
            low[lane] = low[lane].wrapping_add(value.cast_unsigned() & 0xFFFF_FFFF);
            high[lane] = high[lane].wrapping_add(value >> 32);
        }
        let low: i128 = low.iter().map(|&lane| i128::from(lane)).sum();
        let high: i128 = high.iter().map(|&lane| i128::from(lane)).sum();
        total += low + (high << 32);
    }
    total
}

/// Sum of `values` with eight interleaved accumulators.
///
/// The additions are reassociated: lane `j` sums elements `j, j+8, ...`, the
/// lanes are then added in order, then the tail. The result can differ from
/// a strict left-to-right sum in the last bits, as any parallel float sum
/// does; callers that promise a specific order must not use it.
#[inline(always)]
#[must_use]
pub fn sum_f64(values: &[f64]) -> f64 {
    let mut lanes = [0.0_f64; LANES];
    let mut chunks = values.chunks_exact(LANES);
    for chunk in &mut chunks {
        for (lane, &value) in lanes.iter_mut().zip(chunk) {
            *lane += value;
        }
    }
    let mut total = lanes.iter().sum::<f64>();
    for &value in chunks.remainder() {
        total += value;
    }
    total
}

macro_rules! lane_extreme {
    ($name:ident, $ty:ty, $init:expr, $keep:tt, $doc:literal) => {
        #[doc = $doc]
        #[inline(always)]
        #[must_use]
        pub fn $name(values: &[$ty]) -> Option<$ty> {
            if values.is_empty() {
                return None;
            }
            let mut lanes = [$init; LANES];
            let mut chunks = values.chunks_exact(LANES);
            for chunk in &mut chunks {
                for (lane, &value) in lanes.iter_mut().zip(chunk) {
                    *lane = if value $keep *lane { value } else { *lane };
                }
            }
            let mut best = $init;
            for &value in lanes.iter().chain(chunks.remainder()) {
                best = if value $keep best { value } else { best };
            }
            Some(best)
        }
    };
}

lane_extreme!(
    min_i64,
    i64,
    i64::MAX,
    <,
    "Smallest value, or `None` for an empty slice. The lane form is for the \
     SSE2 baseline, which has no 64-bit compare; the AVX2 copy is a plain fold."
);
lane_extreme!(max_i64, i64, i64::MIN, >, "Largest value, or `None` for an empty slice.");
lane_extreme!(
    min_f64,
    f64,
    f64::INFINITY,
    <,
    "Smallest value, or `None` for an empty slice. Input must be NaN-free \
     (stored DOUBLE columns are); which zero wins between `-0.0` and `0.0` \
     is unspecified."
);
lane_extreme!(
    max_f64,
    f64,
    f64::NEG_INFINITY,
    >,
    "Largest value, or `None` for an empty slice. Input must be NaN-free \
     (stored DOUBLE columns are); which zero wins between `-0.0` and `0.0` \
     is unspecified."
);

/// Packs `predicate(value)` for each value into little-endian bit words:
/// bit `i % 64` of word `i / 64` is row `i`. Writes exactly
/// `values.len().div_ceil(64)` words; the unused high bits of the final word
/// are zero.
///
/// # Panics
///
/// When `out` is shorter than `values.len().div_ceil(64)`.
#[inline(always)]
pub fn pack_predicate<T: Copy>(values: &[T], out: &mut [u64], predicate: impl Fn(T) -> bool) {
    assert!(
        out.len() >= values.len().div_ceil(WORD_BITS),
        "mask output holds {} words, {} rows need {}",
        out.len(),
        values.len(),
        values.len().div_ceil(WORD_BITS)
    );
    let mut chunks = values.chunks_exact(WORD_BITS);
    let mut words = out.iter_mut();
    // Chunks drive the zip: when they run out, no word has been taken
    // for them, so the next word is the tail's.
    for (chunk, word) in (&mut chunks).zip(&mut words) {
        // Eight-row bytes first: the compare-and-pack of a byte is a pattern
        // the vectorizer lowers to a vector compare plus a mask move.
        let mut bits = 0_u64;
        for (byte_index, rows) in chunk.chunks_exact(8).enumerate() {
            let mut byte = 0_u8;
            for (bit, &value) in rows.iter().enumerate() {
                byte |= u8::from(predicate(value)) << bit;
            }
            bits |= u64::from(byte) << (byte_index * 8);
        }
        *word = bits;
    }
    let tail = chunks.remainder();
    if let Some(word) = words.next().filter(|_| !tail.is_empty()) {
        let mut bits = 0_u64;
        for (bit, &value) in tail.iter().enumerate() {
            bits |= u64::from(predicate(value)) << bit;
        }
        *word = bits;
    }
}

macro_rules! compare_kernels {
    ($compare:ident, $between:ident, $ty:ty, $unsigned:ty) => {
        /// Writes `value <op> constant` for each value as mask bits, as
        /// [`pack_predicate`] lays them out.
        ///
        /// # Panics
        ///
        /// When `out` is shorter than `values.len().div_ceil(64)`.
        #[inline(always)]
        pub fn $compare(values: &[$ty], op: crate::CmpOp, constant: $ty, out: &mut [u64]) {
            use crate::CmpOp;
            match op {
                CmpOp::Eq => pack_predicate(values, out, |value| value == constant),
                CmpOp::Ne => pack_predicate(values, out, |value| value != constant),
                CmpOp::Lt => pack_predicate(values, out, |value| value < constant),
                CmpOp::Le => pack_predicate(values, out, |value| value <= constant),
                CmpOp::Gt => pack_predicate(values, out, |value| value > constant),
                CmpOp::Ge => pack_predicate(values, out, |value| value >= constant),
            }
        }

        /// Writes `low <= value <= high` for each value as mask bits. An
        /// empty range (`low > high`) selects nothing.
        ///
        /// # Panics
        ///
        /// When `out` is shorter than `values.len().div_ceil(64)`.
        #[inline(always)]
        #[allow(
            clippy::cast_sign_loss,
            trivial_numeric_casts,
            clippy::unnecessary_cast
        )]
        pub fn $between(values: &[$ty], low: $ty, high: $ty, out: &mut [u64]) {
            if low > high {
                pack_predicate(values, out, |_| false);
                return;
            }
            // One unsigned compare: subtracting `low` maps [low, high] onto
            // [0, high - low] and every other value, wrapping, above it.
            let span = high.wrapping_sub(low) as $unsigned;
            pack_predicate(values, out, |value| {
                (value.wrapping_sub(low) as $unsigned) <= span
            });
        }
    };
}

compare_kernels!(compare_i64, between_i64, i64, u64);
compare_kernels!(compare_i32, between_i32, i32, u32);
compare_kernels!(compare_u32, between_u32, u32, u32);

/// Bit positions set in each byte value, packed to the front.
pub(crate) const BYTE_POSITIONS: [[u32; 8]; 256] = byte_positions();

const fn byte_positions() -> [[u32; 8]; 256] {
    let mut table = [[0_u32; 8]; 256];
    let mut byte = 0;
    while byte < 256 {
        let mut count = 0;
        let mut bit = 0;
        while bit < 8 {
            if byte >> bit & 1 == 1 {
                table[byte][count] = bit;
                count += 1;
            }
            bit += 1;
        }
        byte += 1;
    }
    table
}

/// Appends the index of every set bit below `len` to `out`, ascending.
///
/// Full words take a straight fill, sparse words (fewer than eight rows)
/// walk their set bits, and the rest expand a byte at a time: the byte's
/// eight positions come from a table, all eight are stored, and the write
/// position advances by the byte's population count - no branch per row.
/// Bits at or above `len` are ignored.
///
/// # Panics
///
/// When `words` holds fewer than `len.div_ceil(64)` words, or `len` exceeds
/// `u32::MAX + 1`.
#[inline(always)]
pub fn mask_to_indices(words: &[u64], len: usize, out: &mut Vec<u32>) {
    expand_mask(words, len, out, |window, positions, offset| {
        for (slot, &position) in window.iter_mut().zip(positions) {
            *slot = offset + position;
        }
    });
}

/// [`mask_to_indices`] with the eight-slot byte store supplied by the
/// caller, so an instruction-set copy can do it in one vector add.
#[inline(always)]
pub(crate) fn expand_mask(
    words: &[u64],
    len: usize,
    out: &mut Vec<u32>,
    store: impl Fn(&mut [u32; 8], &[u32; 8], u32),
) {
    let word_count = len.div_ceil(WORD_BITS);
    assert!(
        words.len() >= word_count,
        "mask holds fewer words than rows"
    );
    assert!(
        u64::try_from(len).is_ok_and(|len| len <= 1 << 32),
        "row index exceeds u32"
    );
    let tail_bits = len % WORD_BITS;
    let live = |index: usize, word: u64| {
        if index + 1 == word_count && tail_bits != 0 {
            word & ((1_u64 << tail_bits) - 1)
        } else {
            word
        }
    };
    let words = &words[..word_count];
    let total: usize = words
        .iter()
        .enumerate()
        .map(|(index, &word)| live(index, word).count_ones() as usize)
        .sum();
    let start = out.len();
    // Eight slots of slack: a byte expansion always stores eight.
    out.resize(start + total + 8, 0);
    let slots = &mut out[start..];
    let mut filled = 0_usize;
    for (index, &word) in words.iter().enumerate() {
        let word = live(index, word);
        // Proven above: every row index fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let base = (index * WORD_BITS) as u32;
        if word == u64::MAX {
            for (slot, offset) in slots[filled..filled + 64].iter_mut().zip(0_u32..) {
                *slot = base + offset;
            }
            filled += 64;
        } else if word.count_ones() < 8 {
            let mut rest = word;
            while rest != 0 {
                slots[filled] = base + rest.trailing_zeros();
                filled += 1;
                rest &= rest - 1;
            }
        } else {
            for (byte_index, byte) in word.to_le_bytes().into_iter().enumerate() {
                #[allow(clippy::cast_possible_truncation)]
                let offset = base + (byte_index * 8) as u32;
                let window: &mut [u32; 8] = (&mut slots[filled..filled + 8])
                    .try_into()
                    .expect("eight slots");
                store(window, &BYTE_POSITIONS[usize::from(byte)], offset);
                filled += byte.count_ones() as usize;
            }
        }
    }
    debug_assert_eq!(filled, total);
    out.truncate(start + total);
}

/// Appends `source[index]` for each index to `out`: dictionary decode when
/// `source` is a dictionary and `indices` its codes, compaction when
/// `indices` is a selection.
///
/// Plain checked indexing: the bounds check is a predictable branch, and
/// both alternatives measured slower - clamping each index and checking the
/// largest once (twice the time at baseline), and AVX2 hardware gathers
/// (no faster than scalar loads on the reference machine).
///
/// # Panics
///
/// When any index is out of bounds for `source`.
#[inline(always)]
pub fn gather<T: Copy>(source: &[T], indices: &[u32], out: &mut Vec<T>) {
    out.extend(indices.iter().map(|&index| source[index as usize]));
}

/// Applies `update(&mut accumulators[groups[i]], values[i])` for each row,
/// in row order.
///
/// A grouped update is a scatter: rows of one batch hit arbitrary slots, so
/// it does not vectorize, and splitting it across interleaved partial tables
/// (to break the store-to-load chain on repeated groups) measured within
/// noise. What it buys over a per-row engine loop is one typed pass per
/// batch with no per-row dispatch, `Option` or `Result`.
///
/// # Panics
///
/// When a group index is out of bounds or the slices differ in length.
#[inline(always)]
pub fn grouped<V: Copy, A>(
    values: &[V],
    groups: &[u32],
    accumulators: &mut [A],
    update: impl Fn(&mut A, V),
) {
    assert_eq!(values.len(), groups.len(), "one group index per value");
    for (&value, &group) in values.iter().zip(groups) {
        update(&mut accumulators[group as usize], value);
    }
}

/// Adds each value into `sums[groups[i]]`, exactly (`i128` accumulators,
/// for BIGINT and scaled-decimal sums).
///
/// # Panics
///
/// When a group index is out of bounds or the slices differ in length.
#[inline(always)]
pub fn sum_by_group_i64(values: &[i64], groups: &[u32], sums: &mut [i128]) {
    grouped(values, groups, sums, |sum, value| *sum += i128::from(value));
}

/// Adds each `i128` value (a Decimal128 payload) into `sums[groups[i]]`.
/// Overflow wraps; callers bound their inputs (65 decimal digits do not fit
/// `i128` either) or check the result's magnitude.
///
/// # Panics
///
/// When a group index is out of bounds or the slices differ in length.
#[inline(always)]
pub fn sum_by_group_i128(values: &[i128], groups: &[u32], sums: &mut [i128]) {
    grouped(values, groups, sums, |sum, value| {
        *sum = sum.wrapping_add(value);
    });
}

/// Adds each value into `sums[groups[i]]` in row order, so each group's float sum is the strict left-to-right sum.
///
/// # Panics
///
/// When a group index is out of bounds or the slices differ in length.
#[inline(always)]
pub fn sum_by_group_f64(values: &[f64], groups: &[u32], sums: &mut [f64]) {
    grouped(values, groups, sums, |sum, value| *sum += value);
}

/// Adds one to `counts[group]` for each group index.
///
/// # Panics
///
/// When a group index is out of bounds.
#[inline(always)]
pub fn count_by_group(groups: &[u32], counts: &mut [u64]) {
    grouped(groups, groups, counts, |count, _| *count += 1);
}

/// Lowers `minimums[groups[i]]` to each value. Start the slice at
/// `i64::MAX` (or the running minimum); a group with no rows keeps it.
///
/// # Panics
///
/// When a group index is out of bounds or the slices differ in length.
#[inline(always)]
pub fn min_by_group_i64(values: &[i64], groups: &[u32], minimums: &mut [i64]) {
    grouped(values, groups, minimums, |min, value| {
        *min = (*min).min(value);
    });
}

/// Raises `maximums[groups[i]]` to each value. Start the slice at
/// `i64::MIN` (or the running maximum); a group with no rows keeps it.
///
/// # Panics
///
/// When a group index is out of bounds or the slices differ in length.
#[inline(always)]
pub fn max_by_group_i64(values: &[i64], groups: &[u32], maximums: &mut [i64]) {
    grouped(values, groups, maximums, |max, value| {
        *max = (*max).max(value);
    });
}

/// Exact sum of `i128` values (Decimal128 payloads), `None` on overflow.
/// Four independent accumulators, checked per add.
#[inline(always)]
#[must_use]
pub fn sum_i128(values: &[i128]) -> Option<i128> {
    let mut lanes = [0_i128; 4];
    let mut overflow = false;
    let mut chunks = values.chunks_exact(4);
    for chunk in &mut chunks {
        for (lane, &value) in lanes.iter_mut().zip(chunk) {
            let (sum, carried) = lane.overflowing_add(value);
            *lane = sum;
            overflow |= carried;
        }
    }
    if overflow {
        return None;
    }
    lanes
        .iter()
        .chain(chunks.remainder())
        .try_fold(0_i128, |sum, &value| sum.checked_add(value))
}

/// Packs per-row flags (a decoded validity or a filter's `bool` output)
/// into mask words, as [`pack_predicate`] lays them out.
///
/// # Panics
///
/// When `out` is shorter than `bools.len().div_ceil(64)`.
#[inline(always)]
pub fn pack_bools(bools: &[bool], out: &mut [u64]) {
    pack_predicate(bools, out, |flag| flag);
}

/// AVG's state in one pass: `sums[g] += value` (exact) and `counts[g] += 1`.
///
/// # Panics
///
/// When a group index is out of bounds or the slices differ in length.
#[inline(always)]
pub fn sum_count_by_group_i64(
    values: &[i64],
    groups: &[u32],
    sums: &mut [i128],
    counts: &mut [u64],
) {
    assert_eq!(values.len(), groups.len(), "one group index per value");
    assert_eq!(sums.len(), counts.len(), "one count per sum");
    for (&value, &group) in values.iter().zip(groups) {
        sums[group as usize] += i128::from(value);
        counts[group as usize] += 1;
    }
}
