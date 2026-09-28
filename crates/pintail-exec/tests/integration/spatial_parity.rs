//! Spatial functions over replicated geometry columns and constructed
//! geometries answer as `MySQL` does.
//!
//! Every answer in `spatial_parity.pairs` was read from `MySQL` 8.4 over the
//! same rows. A geometry column holds `MySQL`'s internal format - the SRID,
//! then little-endian WKB - with SRID 4326 coordinates stored longitude
//! first.

use pintail_types::{Column, DataType, KeyPart, PrimaryKey, StoredRow, TableSchema, Value};

use crate::pairs_fixture::assert_pairs;

fn geometry(srid: u32, kind: u32, body: &[f64], counts: &[u32]) -> Value {
    let mut bytes = srid.to_le_bytes().to_vec();
    bytes.push(1);
    bytes.extend_from_slice(&kind.to_le_bytes());
    for count in counts {
        bytes.extend_from_slice(&count.to_le_bytes());
    }
    for coordinate in body {
        bytes.extend_from_slice(&coordinate.to_le_bytes());
    }
    Value::Binary(bytes)
}

fn point(srid: u32, x: f64, y: f64) -> Value {
    geometry(srid, 1, &[x, y], &[])
}

fn line(srid: u32, points: &[f64]) -> Value {
    let count = u32::try_from(points.len() / 2).expect("count");
    geometry(srid, 2, points, &[count])
}

fn square() -> Value {
    geometry(
        0,
        3,
        &[0.0, 0.0, 10.0, 0.0, 10.0, 10.0, 0.0, 10.0, 0.0, 0.0],
        &[1, 5],
    )
}

fn schema() -> TableSchema {
    TableSchema::new(
        1,
        vec![
            Column::new(1, "id", DataType::UInt64, false),
            Column::new(2, "g", DataType::Binary, true),
            Column::new(3, "p", DataType::Binary, true),
        ],
    )
    .expect("schema")
}

#[test]
fn spatial_functions_answer_as_mysql_does() {
    let geometries = [
        [line(0, &[0.0, 0.0, 3.0, 4.0, 6.0, 0.0]), point(0, 3.0, 4.0)],
        [square(), point(0, 5.0, 5.0)],
        [square(), point(0, 15.0, 5.0)],
        [point(4326, 2.0, 1.0), point(4326, 90.0, 45.0)],
        [
            line(4326, &[20.0, 10.0, 21.0, 11.0]),
            point(4326, 151.25, -33.5),
        ],
        [Value::Null, Value::Null],
    ];
    let rows = geometries
        .into_iter()
        .zip(1_u64..)
        .map(|([g, p], id)| {
            StoredRow::new(
                PrimaryKey::new(vec![KeyPart::UInt64(id)]).expect("key"),
                vec![Value::UInt64(id), g, p],
                id,
                false,
            )
        })
        .collect();
    assert_pairs(
        include_str!("spatial_parity.pairs"),
        "s",
        &schema(),
        rows,
        51,
    );
}
