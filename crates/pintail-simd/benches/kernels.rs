//! Kernel microbenchmarks: `cargo bench -p pintail-simd`.
//!
//! Each kernel runs three ways - an idiomatic per-row loop (how the code
//! reads without this crate), the portable kernel called directly (compiled
//! for the build's baseline target), and the dispatched kernel - over a
//! batch that stays in L1/L2 and a column that streams from memory. Reports
//! the minimum and median nanoseconds per row over repeated passes.

use std::fmt::Write as _;
use std::hint::black_box;
use std::time::Instant;

use pintail_simd::{CmpOp, portable};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const SIZES: [(&str, usize); 3] = [
    ("batch-2k", 1 << 11),
    ("batch-64k", 1 << 16),
    ("column-16m", 1 << 24),
];

struct Scratch {
    words: Vec<u64>,
    indices: Vec<u32>,
    gathered: Vec<i64>,
}

fn measure(rows: usize, scratch: &mut Scratch, pass: &mut dyn FnMut(&mut Scratch)) -> (f64, f64) {
    let target_rows = 1 << 28;
    let passes = (target_rows / rows).clamp(5, 20_000);
    pass(scratch);
    let mut samples: Vec<f64> = (0..passes)
        .map(|_| {
            let start = Instant::now();
            pass(scratch);
            #[allow(clippy::cast_precision_loss)]
            let per_row = start.elapsed().as_nanos() as f64 / rows as f64;
            per_row
        })
        .collect();
    samples.sort_by(f64::total_cmp);
    (samples[0], samples[samples.len() / 2])
}

type Variant<'a> = (&'a str, &'a mut dyn FnMut(&mut Scratch));

fn report(
    kernel: &str,
    size: &str,
    rows: usize,
    scratch: &mut Scratch,
    variants: [Variant<'_>; 3],
) {
    let mut line = format!("{kernel:<18} {size:<11}");
    let mut naive_min = 0.0;
    for (index, (name, pass)) in variants.into_iter().enumerate() {
        let (min, median) = measure(rows, scratch, pass);
        if index == 0 {
            naive_min = min;
        }
        let _ = write!(
            line,
            "  {name} {min:>7.3}/{median:>7.3} ns ({:>5.1}x)",
            naive_min / min
        );
    }
    println!("{line}");
}

#[allow(clippy::too_many_lines)]
fn main() {
    println!("dispatch level: {}", pintail_simd::level().name());
    println!("columns: min/median ns per row, (speedup of min over naive)");
    let mut rng = StdRng::seed_from_u64(1);
    for (size, rows) in SIZES {
        let ints: Vec<i64> = (0..rows)
            .map(|_| rng.random_range(-1_000_000..1_000_000))
            .collect();
        let narrow: Vec<i32> = ints.iter().map(|&v| i32::try_from(v).unwrap()).collect();
        let floats: Vec<f64> = (0..rows).map(|_| rng.random::<f64>()).collect();
        let codes: Vec<u32> = (0..rows).map(|_| rng.random_range(0..4096)).collect();
        let dictionary: Vec<i64> = (0..4096).map(|_| rng.random()).collect();
        let scratch = &mut Scratch {
            words: vec![0_u64; rows.div_ceil(64)],
            indices: Vec::with_capacity(rows),
            gathered: Vec::with_capacity(rows),
        };

        report(
            "sum_i64",
            size,
            rows,
            scratch,
            [
                ("naive", &mut |_: &mut Scratch| {
                    black_box(
                        black_box(&ints)
                            .iter()
                            .map(|&v| i128::from(v))
                            .sum::<i128>(),
                    );
                }),
                ("portable", &mut |_: &mut Scratch| {
                    black_box(portable::sum_i64(black_box(&ints)));
                }),
                ("dispatch", &mut |_: &mut Scratch| {
                    black_box(pintail_simd::sum_i64(black_box(&ints)));
                }),
            ],
        );
        report(
            "sum_f64",
            size,
            rows,
            scratch,
            [
                ("naive", &mut |_: &mut Scratch| {
                    black_box(black_box(&floats).iter().sum::<f64>());
                }),
                ("portable", &mut |_: &mut Scratch| {
                    black_box(portable::sum_f64(black_box(&floats)));
                }),
                ("dispatch", &mut |_: &mut Scratch| {
                    black_box(pintail_simd::sum_f64(black_box(&floats)));
                }),
            ],
        );
        report(
            "min_i64",
            size,
            rows,
            scratch,
            [
                ("naive", &mut |_: &mut Scratch| {
                    black_box(black_box(&ints).iter().copied().min());
                }),
                ("portable", &mut |_: &mut Scratch| {
                    black_box(portable::min_i64(black_box(&ints)));
                }),
                ("dispatch", &mut |_: &mut Scratch| {
                    black_box(pintail_simd::min_i64(black_box(&ints)));
                }),
            ],
        );
        report(
            "max_f64",
            size,
            rows,
            scratch,
            [
                ("naive", &mut |_: &mut Scratch| {
                    black_box(black_box(&floats).iter().copied().reduce(f64::max));
                }),
                ("portable", &mut |_: &mut Scratch| {
                    black_box(portable::max_f64(black_box(&floats)));
                }),
                ("dispatch", &mut |_: &mut Scratch| {
                    black_box(pintail_simd::max_f64(black_box(&floats)));
                }),
            ],
        );
        let naive_mask = |values: &[i64], out: &mut [u64]| {
            out.fill(0);
            for (row, &value) in values.iter().enumerate() {
                if value > 0 {
                    out[row / 64] |= 1 << (row % 64);
                }
            }
        };
        report(
            "cmp_gt_i64",
            size,
            rows,
            scratch,
            [
                ("naive", &mut |s: &mut Scratch| {
                    naive_mask(black_box(&ints), &mut s.words);
                }),
                ("portable", &mut |s: &mut Scratch| {
                    portable::pack_predicate(black_box(&ints), &mut s.words, |v| v > 0);
                }),
                ("dispatch", &mut |s: &mut Scratch| {
                    pintail_simd::compare_i64(black_box(&ints), CmpOp::Gt, 0, &mut s.words);
                }),
            ],
        );
        report(
            "between_i32",
            size,
            rows,
            scratch,
            [
                ("naive", &mut |s: &mut Scratch| {
                    s.words.fill(0);
                    for (row, &value) in black_box(&narrow).iter().enumerate() {
                        if (-1000..=250_000).contains(&value) {
                            s.words[row / 64] |= 1 << (row % 64);
                        }
                    }
                }),
                ("portable", &mut |s: &mut Scratch| {
                    portable::pack_predicate(black_box(&narrow), &mut s.words, |v| {
                        v.wrapping_sub(-1000).cast_unsigned() <= 251_000
                    });
                }),
                ("dispatch", &mut |s: &mut Scratch| {
                    pintail_simd::between_i32(black_box(&narrow), -1000, 250_000, &mut s.words);
                }),
            ],
        );
        report(
            "eq_u32",
            size,
            rows,
            scratch,
            [
                ("naive", &mut |s: &mut Scratch| {
                    s.words.fill(0);
                    for (row, &value) in black_box(&codes).iter().enumerate() {
                        if value == 7 {
                            s.words[row / 64] |= 1 << (row % 64);
                        }
                    }
                }),
                ("portable", &mut |s: &mut Scratch| {
                    portable::pack_predicate(black_box(&codes), &mut s.words, |v| v == 7);
                }),
                ("dispatch", &mut |s: &mut Scratch| {
                    pintail_simd::compare_u32(black_box(&codes), CmpOp::Eq, 7, &mut s.words);
                }),
            ],
        );
        for (label, threshold) in [("indices_50pct", 0), ("indices_2pct", 960_000)] {
            pintail_simd::compare_i64(&ints, CmpOp::Gt, threshold, &mut scratch.words);
            report(
                label,
                size,
                rows,
                scratch,
                [
                    ("naive", &mut |s: &mut Scratch| {
                        s.indices.clear();
                        for (index, &word) in black_box(&s.words).iter().enumerate() {
                            let mut rest = word;
                            while rest != 0 {
                                s.indices.push(
                                    u32::try_from(index * 64).unwrap() + rest.trailing_zeros(),
                                );
                                rest &= rest - 1;
                            }
                        }
                    }),
                    ("portable", &mut |s: &mut Scratch| {
                        s.indices.clear();
                        portable::mask_to_indices(black_box(&s.words), rows, &mut s.indices);
                    }),
                    ("dispatch", &mut |s: &mut Scratch| {
                        s.indices.clear();
                        pintail_simd::mask_to_indices(black_box(&s.words), rows, &mut s.indices);
                    }),
                ],
            );
        }
        report(
            "gather_i64",
            size,
            rows,
            scratch,
            [
                ("naive", &mut |s: &mut Scratch| {
                    s.gathered.clear();
                    s.gathered.extend(
                        black_box(&codes)
                            .iter()
                            .map(|&code| dictionary[code as usize]),
                    );
                }),
                ("portable", &mut |s: &mut Scratch| {
                    s.gathered.clear();
                    portable::gather(&dictionary, black_box(&codes), &mut s.gathered);
                }),
                ("dispatch", &mut |s: &mut Scratch| {
                    s.gathered.clear();
                    pintail_simd::gather(&dictionary, black_box(&codes), &mut s.gathered);
                }),
            ],
        );
    }
}
