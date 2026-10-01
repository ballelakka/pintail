//! Hand-written AVX2 comparison kernels.
//!
//! The auto-vectorizer turns a compare-and-pack loop into a long chain of
//! shifts and ORs; AVX2 has a direct path - one vector compare and one mask
//! move per four (`i64`) or eight (`i32`/`u32`) rows. These kernels spell it
//! out with the dispatcher's safe intrinsic wrappers: every intrinsic here is
//! a method on the [`V3`] token, which can only exist once the CPU has been
//! checked for AVX2, and values move in and out of vector registers by value
//! (`pulp::cast` between `[i64; 4]` and `__m256i`), never through a pointer.

use core::arch::x86_64::__m256i;

use pulp::x86::V3;

use crate::CmpOp;
use crate::portable::{WORD_BITS, pack_predicate};

/// The comparison as AVX2 can do it natively - equal, greater, or greater
/// with swapped operands - plus whether the result is then inverted.
#[derive(Clone, Copy)]
enum Native {
    Eq,
    Gt,
    Lt,
}

const fn lower(op: CmpOp) -> (Native, bool) {
    match op {
        CmpOp::Eq => (Native::Eq, false),
        CmpOp::Ne => (Native::Eq, true),
        CmpOp::Gt => (Native::Gt, false),
        CmpOp::Le => (Native::Gt, true),
        CmpOp::Lt => (Native::Lt, false),
        CmpOp::Ge => (Native::Lt, true),
    }
}

#[inline(always)]
fn load_i64(rows: &[i64]) -> __m256i {
    let lanes: [i64; 4] = rows.try_into().expect("four rows");
    pulp::cast(lanes)
}

#[inline(always)]
fn load_i32(rows: &[i32]) -> __m256i {
    let lanes: [i32; 8] = rows.try_into().expect("eight rows");
    pulp::cast(lanes)
}

#[inline(always)]
fn load_u32(rows: &[u32]) -> __m256i {
    let lanes: [u32; 8] = rows.try_into().expect("eight rows");
    pulp::cast(lanes)
}

/// Packs full 64-row words with `word`, then the partial tail with the
/// portable kernel. `invert` flips every row's result (valid rows only).
#[inline(always)]
fn pack_words<T: Copy>(
    values: &[T],
    out: &mut [u64],
    invert: bool,
    mut word: impl FnMut(&[T]) -> u64,
    tail: impl Fn(T) -> bool,
) {
    assert!(
        out.len() >= values.len().div_ceil(WORD_BITS),
        "mask output holds {} words, {} rows need {}",
        out.len(),
        values.len(),
        values.len().div_ceil(WORD_BITS)
    );
    let flip = if invert { u64::MAX } else { 0 };
    let mut chunks = values.chunks_exact(WORD_BITS);
    let mut words = out.iter_mut();
    for (chunk, slot) in (&mut chunks).zip(&mut words) {
        *slot = word(chunk) ^ flip;
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        pack_predicate(rest, words.into_slice(), |value| tail(value) != invert);
    }
}

#[inline(always)]
fn word_64bit(simd: V3, rows: &[i64], compare: impl Fn(__m256i) -> __m256i) -> u64 {
    let mut bits = 0_u64;
    for (quad, lanes) in rows.chunks_exact(4).enumerate() {
        let hit = compare(load_i64(lanes));
        let nibble = simd
            .avx
            ._mm256_movemask_pd(simd.avx._mm256_castsi256_pd(hit));
        bits |= u64::from(nibble.cast_unsigned()) << (quad * 4);
    }
    bits
}

#[inline(always)]
fn word_32bit<T>(
    simd: V3,
    rows: &[T],
    load: impl Fn(&[T]) -> __m256i,
    compare: impl Fn(__m256i) -> __m256i,
) -> u64 {
    let mut bits = 0_u64;
    for (octet, lanes) in rows.chunks_exact(8).enumerate() {
        let hit = compare(load(lanes));
        let byte = simd
            .avx
            ._mm256_movemask_ps(simd.avx._mm256_castsi256_ps(hit));
        bits |= u64::from(byte.cast_unsigned()) << (octet * 8);
    }
    bits
}

/// [`crate::compare_i64`] on AVX2.
#[inline(always)]
pub fn compare_i64(simd: V3, values: &[i64], op: CmpOp, constant: i64, out: &mut [u64]) {
    let (native, invert) = lower(op);
    let splat = simd.avx._mm256_set1_epi64x(constant);
    let avx2 = simd.avx2;
    match native {
        Native::Eq => pack_words(
            values,
            out,
            invert,
            |rows| word_64bit(simd, rows, |v| avx2._mm256_cmpeq_epi64(v, splat)),
            |value| value == constant,
        ),
        Native::Gt => pack_words(
            values,
            out,
            invert,
            |rows| word_64bit(simd, rows, |v| avx2._mm256_cmpgt_epi64(v, splat)),
            |value| value > constant,
        ),
        Native::Lt => pack_words(
            values,
            out,
            invert,
            |rows| word_64bit(simd, rows, |v| avx2._mm256_cmpgt_epi64(splat, v)),
            |value| value < constant,
        ),
    }
}

/// [`crate::compare_i32`] on AVX2.
#[inline(always)]
pub fn compare_i32(simd: V3, values: &[i32], op: CmpOp, constant: i32, out: &mut [u64]) {
    let (native, invert) = lower(op);
    let splat = simd.avx._mm256_set1_epi32(constant);
    let avx2 = simd.avx2;
    match native {
        Native::Eq => pack_words(
            values,
            out,
            invert,
            |rows| word_32bit(simd, rows, load_i32, |v| avx2._mm256_cmpeq_epi32(v, splat)),
            |value| value == constant,
        ),
        Native::Gt => pack_words(
            values,
            out,
            invert,
            |rows| word_32bit(simd, rows, load_i32, |v| avx2._mm256_cmpgt_epi32(v, splat)),
            |value| value > constant,
        ),
        Native::Lt => pack_words(
            values,
            out,
            invert,
            |rows| word_32bit(simd, rows, load_i32, |v| avx2._mm256_cmpgt_epi32(splat, v)),
            |value| value < constant,
        ),
    }
}

/// [`crate::compare_u32`] on AVX2.
#[inline(always)]
pub fn compare_u32(simd: V3, values: &[u32], op: CmpOp, constant: u32, out: &mut [u64]) {
    let (native, invert) = lower(op);
    let avx2 = simd.avx2;
    let bias = simd.avx._mm256_set1_epi32(i32::MIN);
    let splat = avx2._mm256_xor_si256(simd.avx._mm256_set1_epi32(constant.cast_signed()), bias);
    let biased = |rows: &[u32]| avx2._mm256_xor_si256(load_u32(rows), bias);
    match native {
        Native::Eq => pack_words(
            values,
            out,
            invert,
            |rows| word_32bit(simd, rows, biased, |v| avx2._mm256_cmpeq_epi32(v, splat)),
            |value| value == constant,
        ),
        Native::Gt => pack_words(
            values,
            out,
            invert,
            |rows| word_32bit(simd, rows, biased, |v| avx2._mm256_cmpgt_epi32(v, splat)),
            |value| value > constant,
        ),
        Native::Lt => pack_words(
            values,
            out,
            invert,
            |rows| word_32bit(simd, rows, biased, |v| avx2._mm256_cmpgt_epi32(splat, v)),
            |value| value < constant,
        ),
    }
}

/// [`crate::between_i64`] on AVX2: `value - low`, sign-flipped, is at most
/// `high - low`, sign-flipped, exactly when `low <= value <= high`. The
/// vector computes the complement (greater than) and the word is inverted.
#[inline(always)]
pub fn between_i64(simd: V3, values: &[i64], low: i64, high: i64, out: &mut [u64]) {
    if low > high {
        pack_predicate(values, out, |_| false);
        return;
    }
    let avx2 = simd.avx2;
    let bias = simd.avx._mm256_set1_epi64x(i64::MIN);
    let low_splat = simd.avx._mm256_set1_epi64x(low);
    let span = high.wrapping_sub(low).cast_unsigned();
    let span_splat = simd
        .avx
        ._mm256_set1_epi64x((span ^ (1 << 63)).cast_signed());
    pack_words(
        values,
        out,
        true,
        |rows| {
            word_64bit(simd, rows, |v| {
                let shifted = avx2._mm256_xor_si256(avx2._mm256_sub_epi64(v, low_splat), bias);
                avx2._mm256_cmpgt_epi64(shifted, span_splat)
            })
        },
        |value| value.wrapping_sub(low).cast_unsigned() > span,
    );
}

/// [`crate::between_i32`] on AVX2 (see [`between_i64`]).
#[inline(always)]
pub fn between_i32(simd: V3, values: &[i32], low: i32, high: i32, out: &mut [u64]) {
    if low > high {
        pack_predicate(values, out, |_| false);
        return;
    }
    let span = high.wrapping_sub(low).cast_unsigned();
    between_32bit(
        simd,
        values,
        load_i32,
        low.cast_unsigned(),
        span,
        out,
        |value| value.wrapping_sub(low).cast_unsigned() > span,
    );
}

/// [`crate::between_u32`] on AVX2 (see [`between_i64`]).
#[inline(always)]
pub fn between_u32(simd: V3, values: &[u32], low: u32, high: u32, out: &mut [u64]) {
    if low > high {
        pack_predicate(values, out, |_| false);
        return;
    }
    let span = high.wrapping_sub(low);
    between_32bit(simd, values, load_u32, low, span, out, |value| {
        value.wrapping_sub(low) > span
    });
}

#[inline(always)]
fn between_32bit<T: Copy>(
    simd: V3,
    values: &[T],
    load: impl Fn(&[T]) -> __m256i + Copy,
    low: u32,
    span: u32,
    out: &mut [u64],
    outside: impl Fn(T) -> bool,
) {
    let avx2 = simd.avx2;
    let bias = simd.avx._mm256_set1_epi32(i32::MIN);
    let low_splat = simd.avx._mm256_set1_epi32(low.cast_signed());
    let span_splat = simd.avx._mm256_set1_epi32((span ^ (1 << 31)).cast_signed());
    pack_words(
        values,
        out,
        true,
        |rows| {
            word_32bit(simd, rows, load, |v| {
                let shifted = avx2._mm256_xor_si256(avx2._mm256_sub_epi32(v, low_splat), bias);
                avx2._mm256_cmpgt_epi32(shifted, span_splat)
            })
        },
        outside,
    );
}

/// [`crate::sum_i64`] on AVX2, exact as `i128`.
///
/// AVX2 has no 64-bit arithmetic shift, which is what left the portable
/// kernel's `value >> 32` scalar. Here the high half is built from two
/// 32-bit operations instead: a logical shift moves it down, and a blend
/// takes the upper 32 bits from the sign-filled copy. Low halves sum as
/// unsigned, high halves as signed; neither can overflow within a block.
#[inline(always)]
pub fn sum_i64(simd: V3, values: &[i64]) -> i128 {
    const BLOCK: usize = 1 << 30;
    let avx2 = simd.avx2;
    let low_mask = simd.avx._mm256_set1_epi64x(0xFFFF_FFFF);
    let mut total = 0_i128;
    for block in values.chunks(BLOCK) {
        let zero = simd.avx._mm256_setzero_si256();
        let (mut low, mut high) = ([zero; 2], [zero; 2]);
        let mut chunks = block.chunks_exact(8);
        for chunk in &mut chunks {
            for (half, lanes) in chunk.chunks_exact(4).enumerate() {
                let value = load_i64(lanes);
                let low_half = avx2._mm256_and_si256(value, low_mask);
                let high_half = avx2._mm256_blend_epi32::<0b1010_1010>(
                    avx2._mm256_srli_epi64::<32>(value),
                    avx2._mm256_srai_epi32::<31>(value),
                );
                low[half] = avx2._mm256_add_epi64(low[half], low_half);
                high[half] = avx2._mm256_add_epi64(high[half], high_half);
            }
        }
        let lows: [[u64; 4]; 2] = pulp::cast(low);
        let highs: [[i64; 4]; 2] = pulp::cast(high);
        let low_sum: i128 = lows.iter().flatten().map(|&lane| i128::from(lane)).sum();
        let high_sum: i128 = highs.iter().flatten().map(|&lane| i128::from(lane)).sum();
        total += low_sum + (high_sum << 32) + crate::portable::sum_i64(chunks.remainder());
    }
    total
}

/// [`crate::mask_to_indices`] on AVX2: each mask byte's eight positions are
/// one vector add of the row offset onto a table entry.
#[inline(always)]
pub fn mask_to_indices(simd: V3, words: &[u64], len: usize, out: &mut Vec<u32>) {
    crate::portable::expand_mask(words, len, out, |window, positions, offset| {
        let positions: __m256i = pulp::cast(*positions);
        let rows = simd
            .avx2
            ._mm256_add_epi32(positions, simd.avx._mm256_set1_epi32(offset.cast_signed()));
        *window = pulp::cast(rows);
    });
}

/// [`crate::pack_bools`] on AVX2: one byte compare and one byte mask move
/// per 32 rows.
#[inline(always)]
pub fn pack_bools(simd: V3, bools: &[bool], out: &mut [u64]) {
    let zero = simd.avx._mm256_setzero_si256();
    pack_words(
        bools,
        out,
        true,
        |rows| {
            let mut bits = 0_u64;
            for (half, lanes) in rows.chunks_exact(32).enumerate() {
                let bytes: [u8; 32] = core::array::from_fn(|lane| u8::from(lanes[lane]));
                let clear = simd.avx2._mm256_cmpeq_epi8(pulp::cast(bytes), zero);
                let mask = simd.avx2._mm256_movemask_epi8(clear).cast_unsigned();
                bits |= u64::from(mask) << (half * 32);
            }
            bits
        },
        |valid| !valid,
    );
}
