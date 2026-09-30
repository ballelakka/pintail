//! Filter-first buffer reuse preserves every physical column representation.
use pintail_store::{DecodedColumn, StoreOptions, TableStore, WalSync};
use pintail_types::{
    Column, DataType, Float64, KeyPart, PrimaryKey, StoredRow, TableSchema, Value,
};

#[allow(clippy::cast_precision_loss)]
fn values(id: u64) -> Vec<Value> {
    let nullable = |value| {
        if id.is_multiple_of(11) {
            Value::Null
        } else {
            value
        }
    };
    vec![
        Value::UInt64(id),
        nullable(Value::Int64(-i64::try_from(id).expect("small id"))),
        nullable(Value::Float64(Float64::new(id as f64 / 2.0))),
        nullable(Value::Utf8(format!("kind-{}", id % 3))),
        nullable(Value::Utf8(format!("unique-variable-width-{id}"))),
        nullable(Value::Utf8("2026-01-02".into())),
        nullable(Value::Boolean(id.is_multiple_of(2))),
        nullable(Value::Binary(id.to_le_bytes().to_vec())),
    ]
}

#[test]
#[allow(clippy::too_many_lines)]
fn filtered_columns_keep_values_nulls_and_decode_once_in_whole_and_sliced_scans() {
    for count in [127_u64, 131_089] {
        let directory = tempfile::tempdir().expect("directory");
        let schema = TableSchema::new(
            1,
            [
                DataType::UInt64,
                DataType::Int64,
                DataType::Float64,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Date32,
                DataType::Boolean,
                DataType::Binary,
            ]
            .into_iter()
            .enumerate()
            .map(|(i, ty)| {
                Column::new(
                    u32::try_from(i + 1).expect("id"),
                    format!("field_{i}"),
                    ty,
                    i != 0,
                )
            })
            .collect(),
        )
        .expect("schema");
        let mut table = TableStore::open(
            directory.path(),
            schema,
            StoreOptions {
                wal_sync: WalSync::Off,
                background_compaction: false,
                ..StoreOptions::default()
            },
        )
        .expect("open");
        table
            .bulk_ingest_snapshot(
                (0..count)
                    .map(|id| {
                        StoredRow::new(
                            PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                            values(id),
                            0,
                            false,
                        )
                    })
                    .collect(),
            )
            .expect("seed");
        let snapshot = table.snapshot();
        let (first, last) = snapshot.key_bounds().expect("bounds");
        let projection = [1, 2, 3, 4, 5, 6, 7, 8];
        for mode in 0..3 {
            let mut stream = snapshot
                .scan_projected_range_stream(&first, &last, &projection)
                .expect("scan")
                .expect("stream");
            let select = |columns: &[DecodedColumn],
                          rows: usize|
             -> Result<Option<pintail_store::PrewhereRanges>, String> {
                Ok(match mode {
                    0 => None,
                    1 => Some((0..rows).filter(|row| matches!(columns[0].value_at(*row), Some(Value::UInt64(id)) if id.is_multiple_of(8))).map(|row| row..row+1).collect()),
                    _ => Some(Vec::new()),
                }.map(Into::into))
            };
            let mut actual = Vec::new();
            let mut decoded = 0;
            loop {
                let chunks = stream
                    .next_column_chunks_filtered(2, 128 * 1024 * 1024, &projection, &select)
                    .expect("filtered chunks");
                if chunks.is_empty() {
                    break;
                }
                for chunk in chunks {
                    decoded += chunk.stats().blocks_decoded();
                    let rows = chunk.row_count();
                    let columns = chunk.into_columns();
                    for row in 0..rows {
                        actual.push(
                            columns
                                .iter()
                                .map(|column| column[row].clone())
                                .collect::<Vec<_>>(),
                        );
                    }
                }
            }
            let expected: Vec<_> = (0..count)
                .filter(|id| mode == 0 || (mode == 1 && id.is_multiple_of(8)))
                .map(values)
                .collect();
            assert_eq!(actual, expected, "row count {count}, mode {mode}");
            assert_eq!(
                decoded,
                usize::try_from(count).expect("count").div_ceil(16_384) * projection.len(),
                "each predicate block decoded once"
            );
        }
    }
}

/// A predicate column that is also an output column - a join key handed to
/// its scan as a runtime filter, say - decodes once: the projection reuses
/// the predicate's buffers for the kept rows instead of reading the column
/// a second time. Holds for a selector that keeps everything, one that
/// keeps some rows, one that keeps none, and the side-index path.
#[test]
#[allow(clippy::too_many_lines)]
fn a_predicate_column_that_is_also_projected_decodes_once() {
    pintail_store::override_side_index(Some(true));
    let count = 131_089_u64;
    let directory = tempfile::tempdir().expect("directory");
    let schema = TableSchema::new(
        1,
        [
            DataType::UInt64,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Date32,
            DataType::Boolean,
            DataType::Binary,
        ]
        .into_iter()
        .enumerate()
        .map(|(i, ty)| {
            Column::new(
                u32::try_from(i + 1).expect("id"),
                format!("field_{i}"),
                ty,
                i != 0,
            )
        })
        .collect(),
    )
    .expect("schema");
    let mut table = TableStore::open(
        directory.path(),
        schema,
        StoreOptions {
            wal_sync: WalSync::Off,
            background_compaction: false,
            ..StoreOptions::default()
        },
    )
    .expect("open");
    table
        .bulk_ingest_snapshot(
            (0..count)
                .map(|id| {
                    StoredRow::new(
                        PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                        values(id),
                        0,
                        false,
                    )
                })
                .collect(),
        )
        .expect("seed");
    let snapshot = table.snapshot();
    let (first, last) = snapshot.key_bounds().expect("bounds");
    let blocks = usize::try_from(count).expect("count").div_ceil(16_384);
    // Predicates in their own order; the projection reorders them among
    // columns the predicate does not read.
    let predicate = [4_u32, 1];
    let projection = [2_u32, 4, 6, 1, 8];
    let kept_id = |id: u64| id.is_multiple_of(8);
    for mode in 0..4 {
        let mut stream = snapshot
            .scan_projected_range_stream(&first, &last, &projection)
            .expect("scan")
            .expect("stream");
        if mode == 3 {
            stream.set_index_lookup(pintail_store::IndexLookup {
                column_id: 1,
                key: pintail_store::IndexKey::Integer,
                probe: pintail_store::IndexProbe::Values(
                    (0..count)
                        .filter(|id| kept_id(*id))
                        .map(i128::from)
                        .collect(),
                ),
            });
        }
        let select = |columns: &[DecodedColumn],
                      rows: usize|
         -> Result<Option<pintail_store::PrewhereRanges>, String> {
            let kept = |row: &usize| matches!(columns[1].value_at(*row), Some(Value::UInt64(id)) if kept_id(id));
            Ok(match mode {
                0 => None,
                1 | 3 => Some((0..rows).filter(kept).map(|row| row..row + 1).collect()),
                _ => Some(Vec::new()),
            }
            .map(Into::into))
        };
        let mut actual = Vec::new();
        let mut decoded = 0;
        loop {
            let chunks = stream
                .next_column_chunks_filtered(2, 128 * 1024 * 1024, &predicate, &select)
                .expect("filtered chunks");
            if chunks.is_empty() {
                break;
            }
            for chunk in chunks {
                decoded += chunk.stats().blocks_decoded();
                let rows = chunk.row_count();
                let columns = chunk.into_columns();
                for row in 0..rows {
                    actual.push(
                        columns
                            .iter()
                            .map(|column| column[row].clone())
                            .collect::<Vec<_>>(),
                    );
                }
            }
        }
        let expected: Vec<_> = (0..count)
            .filter(|id| mode == 0 || (mode != 2 && kept_id(*id)))
            .map(|id| {
                let row = values(id);
                projection
                    .iter()
                    .map(|column| row[*column as usize - 1].clone())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(actual, expected, "mode {mode}");
        let columns_decoded = if mode == 2 {
            predicate.len()
        } else {
            projection.len()
        };
        assert_eq!(
            decoded,
            blocks * columns_decoded,
            "mode {mode}: each block of each column decoded once"
        );
    }
    pintail_store::override_side_index(None);
}
