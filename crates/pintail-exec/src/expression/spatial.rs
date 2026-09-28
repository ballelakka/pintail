//! Spatial functions over `MySQL`'s internal geometry format.
//!
//! A geometry is a four-byte little-endian SRID followed by little-endian
//! WKB. That is what a replicated geometry column holds, byte for byte, so a
//! column read and a constructed geometry are one kind of value.
//!
//! Two spatial reference systems are understood. SRID 0 is the Cartesian
//! plane, where every function here answers. SRID 4326 is WGS 84: its
//! coordinates are stored longitude first but read and written latitude
//! first, as `MySQL` presents them, and only the functions that need no
//! ellipsoid - reading, writing, the accessors and the spherical distance -
//! answer over it. Any other SRID is refused rather than guessed at, since
//! its axis order and units are not known here.

// Coordinates compare exactly, as the stored doubles do in `MySQL`, and the
// planar formulas read best in the textbook's one-letter names.
#![allow(
    clippy::float_cmp,
    clippy::many_single_char_names,
    clippy::similar_names
)]

use pintail_sql::{GeometryKind, SpatialFunction};
use pintail_types::{Float64, Value};

use crate::ExecError;
use crate::execution::SpatialError;

use super::{mysql_f64, mysql_i64, scalar_string};

/// `MySQL`'s default sphere for `ST_Distance_Sphere` over SRID 0, in metres.
const SPHERE_RADIUS: f64 = 6_370_986.0;

/// SRID 4326's degree in radians, as its definition states it - a few
/// units in the last place from `pi / 180`, which shows in the distances.
const WGS84_DEGREE: f64 = 0.017_453_292_519_943_278;

/// The default over SRID 4326: the WGS 84 ellipsoid's mean radius,
/// `(2a + b) / 3`.
const WGS84_MEAN_RADIUS: f64 = (2.0 * 6_378_137.0 + 6_356_752.314_245_179) / 3.0;

type Coord = [f64; 2];
type Ring = Vec<Coord>;

#[derive(Clone, Debug, PartialEq)]
enum Shape {
    Point(Coord),
    LineString(Vec<Coord>),
    Polygon(Vec<Ring>),
    MultiPoint(Vec<Coord>),
    MultiLineString(Vec<Vec<Coord>>),
    MultiPolygon(Vec<Vec<Ring>>),
    Collection(Vec<Self>),
}

#[derive(Clone, Debug, PartialEq)]
struct Geometry {
    srid: u32,
    shape: Shape,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Srs {
    Cartesian,
    Geographic,
}

fn failure(kind: SpatialError, message: String) -> ExecError {
    ExecError::Spatial { kind, message }
}

fn invalid(function: SpatialFunction) -> ExecError {
    failure(
        SpatialError::InvalidData,
        format!("Invalid GIS data provided to function {}.", function.name()),
    )
}

fn unexpected(expected: &str, found: &Shape, function: SpatialFunction) -> ExecError {
    failure(
        SpatialError::UnexpectedType,
        format!(
            "{expected} value is a geometry of unexpected type {} in {}.",
            found.type_name(),
            function.name()
        ),
    )
}

fn unsupported(function: SpatialFunction, srid: u32) -> ExecError {
    failure(
        SpatialError::Unsupported,
        format!(
            "{} is not supported over SRID {srid}: only SRID 0, and SRID 4326 for reading, \
             writing and ST_Distance_Sphere, are",
            function.name()
        ),
    )
}

fn srs_of(srid: u32, function: SpatialFunction) -> Result<Srs, ExecError> {
    match srid {
        0 => Ok(Srs::Cartesian),
        4326 => Ok(Srs::Geographic),
        _ => Err(unsupported(function, srid)),
    }
}

/// The plane the metric and relational functions need.
fn cartesian(geometry: &Geometry, function: SpatialFunction) -> Result<(), ExecError> {
    match srs_of(geometry.srid, function)? {
        Srs::Cartesian => Ok(()),
        Srs::Geographic => Err(unsupported(function, geometry.srid)),
    }
}

fn same_srid(
    left: &Geometry,
    right: &Geometry,
    function: SpatialFunction,
) -> Result<(), ExecError> {
    if left.srid == right.srid {
        return Ok(());
    }
    Err(failure(
        SpatialError::DifferentSrids,
        format!(
            "Binary geometry function {} given two geometries of different srids: {} and {}, \
             which should have been identical.",
            function.name(),
            left.srid,
            right.srid
        ),
    ))
}

impl Shape {
    const fn type_name(&self) -> &'static str {
        match self {
            Self::Point(_) => "POINT",
            Self::LineString(_) => "LINESTRING",
            Self::Polygon(_) => "POLYGON",
            Self::MultiPoint(_) => "MULTIPOINT",
            Self::MultiLineString(_) => "MULTILINESTRING",
            Self::MultiPolygon(_) => "MULTIPOLYGON",
            Self::Collection(_) => "GEOMCOLLECTION",
        }
    }

    const fn kind(&self) -> GeometryKind {
        match self {
            Self::Point(_) => GeometryKind::Point,
            Self::LineString(_) => GeometryKind::LineString,
            Self::Polygon(_) => GeometryKind::Polygon,
            Self::MultiPoint(_) => GeometryKind::MultiPoint,
            Self::MultiLineString(_) => GeometryKind::MultiLineString,
            Self::MultiPolygon(_) => GeometryKind::MultiPolygon,
            Self::Collection(_) => GeometryKind::GeometryCollection,
        }
    }

    fn map(&self, f: &impl Fn(Coord) -> Coord) -> Self {
        let line = |points: &Vec<Coord>| points.iter().map(|point| f(*point)).collect();
        let polygon = |rings: &Vec<Ring>| rings.iter().map(line).collect();
        match self {
            Self::Point(point) => Self::Point(f(*point)),
            Self::LineString(points) => Self::LineString(line(points)),
            Self::Polygon(rings) => Self::Polygon(polygon(rings)),
            Self::MultiPoint(points) => Self::MultiPoint(line(points)),
            Self::MultiLineString(lines) => Self::MultiLineString(lines.iter().map(line).collect()),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.iter().map(polygon).collect())
            }
            Self::Collection(members) => {
                Self::Collection(members.iter().map(|member| member.map(f)).collect())
            }
        }
    }

    fn coords(&self, visit: &mut impl FnMut(Coord)) {
        match self {
            Self::Point(point) => visit(*point),
            Self::LineString(points) | Self::MultiPoint(points) => {
                for point in points {
                    visit(*point);
                }
            }
            Self::Polygon(rings) | Self::MultiLineString(rings) => {
                rings.iter().flatten().for_each(|point| visit(*point));
            }
            Self::MultiPolygon(polygons) => {
                polygons
                    .iter()
                    .flatten()
                    .flatten()
                    .for_each(|point| visit(*point));
            }
            Self::Collection(members) => members.iter().for_each(|member| member.coords(visit)),
        }
    }

    /// Whether the shape is one `MySQL` would accept: finite coordinates,
    /// lines of two points or more, closed rings of four or more, and no
    /// empty multi-geometry.
    fn is_valid(&self) -> bool {
        let mut finite = true;
        self.coords(&mut |[x, y]| finite &= x.is_finite() && y.is_finite());
        let ring = |ring: &Ring| ring.len() >= 4 && ring.first() == ring.last();
        let polygon = |rings: &Vec<Ring>| !rings.is_empty() && rings.iter().all(ring);
        finite
            && match self {
                Self::Point(_) => true,
                Self::LineString(points) => points.len() >= 2,
                Self::Polygon(rings) => polygon(rings),
                Self::MultiPoint(points) => !points.is_empty(),
                Self::MultiLineString(lines) => {
                    !lines.is_empty() && lines.iter().all(|line| line.len() >= 2)
                }
                Self::MultiPolygon(polygons) => {
                    !polygons.is_empty() && polygons.iter().all(polygon)
                }
                Self::Collection(members) => members.iter().all(Self::is_valid),
            }
    }
}

/// Reading and writing axis order: SRID 4326 is stored longitude first and
/// presented latitude first.
fn presented(shape: &Shape, srs: Srs) -> Shape {
    match srs {
        Srs::Cartesian => shape.clone(),
        Srs::Geographic => shape.map(&|[x, y]| [y, x]),
    }
}

/// A shape written in `srs`'s presentation order, validated and brought to
/// storage order.
fn stored(shape: &Shape, srs: Srs, function: SpatialFunction) -> Result<Shape, ExecError> {
    if srs == Srs::Cartesian {
        return Ok(shape.clone());
    }
    let mut range = Ok(());
    shape.coords(&mut |[latitude, longitude]| {
        if range.is_err() {
            return;
        }
        range = check_geographic(latitude, longitude, function);
    });
    range?;
    Ok(presented(shape, srs))
}

fn check_geographic(
    latitude: f64,
    longitude: f64,
    function: SpatialFunction,
) -> Result<(), ExecError> {
    if !(-90.0..=90.0).contains(&latitude) {
        return Err(failure(
            SpatialError::LatitudeRange,
            format!(
                "Latitude {latitude:.6} is out of range in function {}. It must be within \
                 [-90.000000, 90.000000].",
                function.name()
            ),
        ));
    }
    if !(longitude > -180.0 && longitude <= 180.0) {
        return Err(failure(
            SpatialError::LongitudeRange,
            format!(
                "Longitude {longitude:.6} is out of range in function {}. It must be within \
                 (-180.000000, 180.000000].",
                function.name()
            ),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------- WKB

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let bytes = self.bytes.get(self.at..self.at.checked_add(N)?)?;
        self.at += N;
        bytes.try_into().ok()
    }

    fn u32(&mut self, little: bool) -> Option<u32> {
        let bytes = self.take::<4>()?;
        Some(if little {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        })
    }

    fn f64(&mut self, little: bool) -> Option<f64> {
        let bytes = self.take::<8>()?;
        Some(if little {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }

    fn coord(&mut self, little: bool) -> Option<Coord> {
        Some([self.f64(little)?, self.f64(little)?])
    }

    /// A count, refused when the bytes left cannot hold that many items of
    /// at least `item` bytes - a corrupt count must not allocate.
    fn count(&mut self, little: bool, item: usize) -> Option<usize> {
        let count = usize::try_from(self.u32(little)?).ok()?;
        (count.checked_mul(item)? <= self.bytes.len() - self.at).then_some(count)
    }

    fn points(&mut self, little: bool) -> Option<Vec<Coord>> {
        let count = self.count(little, 16)?;
        (0..count).map(|_| self.coord(little)).collect()
    }

    fn rings(&mut self, little: bool) -> Option<Vec<Ring>> {
        let count = self.count(little, 4)?;
        (0..count).map(|_| self.points(little)).collect()
    }

    fn shape(&mut self, depth: usize) -> Option<Shape> {
        if depth > 64 {
            return None;
        }
        let little = match self.take::<1>()? {
            [0] => false,
            [1] => true,
            _ => return None,
        };
        Some(match self.u32(little)? {
            1 => Shape::Point(self.coord(little)?),
            2 => Shape::LineString(self.points(little)?),
            3 => Shape::Polygon(self.rings(little)?),
            4 => {
                let count = self.count(little, 21)?;
                let points = (0..count)
                    .map(|_| match self.shape(depth + 1)? {
                        Shape::Point(point) => Some(point),
                        _ => None,
                    })
                    .collect::<Option<_>>()?;
                Shape::MultiPoint(points)
            }
            5 => {
                let count = self.count(little, 9)?;
                let lines = (0..count)
                    .map(|_| match self.shape(depth + 1)? {
                        Shape::LineString(points) => Some(points),
                        _ => None,
                    })
                    .collect::<Option<_>>()?;
                Shape::MultiLineString(lines)
            }
            6 => {
                let count = self.count(little, 9)?;
                let polygons = (0..count)
                    .map(|_| match self.shape(depth + 1)? {
                        Shape::Polygon(rings) => Some(rings),
                        _ => None,
                    })
                    .collect::<Option<_>>()?;
                Shape::MultiPolygon(polygons)
            }
            7 => {
                let count = self.count(little, 5)?;
                let members = (0..count)
                    .map(|_| self.shape(depth + 1))
                    .collect::<Option<_>>()?;
                Shape::Collection(members)
            }
            _ => return None,
        })
    }
}

fn read_wkb(bytes: &[u8]) -> Option<Shape> {
    let mut reader = Reader { bytes, at: 0 };
    let shape = reader.shape(0)?;
    (reader.at == bytes.len() && shape.is_valid()).then_some(shape)
}

fn write_wkb(shape: &Shape, out: &mut Vec<u8>) {
    fn header(out: &mut Vec<u8>, kind: u32) {
        out.push(1);
        out.extend_from_slice(&kind.to_le_bytes());
    }
    fn count(out: &mut Vec<u8>, count: usize) {
        let count = u32::try_from(count).expect("a decoded geometry counts within u32");
        out.extend_from_slice(&count.to_le_bytes());
    }
    fn coord(out: &mut Vec<u8>, [x, y]: Coord) {
        out.extend_from_slice(&x.to_le_bytes());
        out.extend_from_slice(&y.to_le_bytes());
    }
    fn points(out: &mut Vec<u8>, points: &[Coord]) {
        count(out, points.len());
        for point in points {
            coord(out, *point);
        }
    }
    fn rings(out: &mut Vec<u8>, rings: &[Ring]) {
        count(out, rings.len());
        for ring in rings {
            points(out, ring);
        }
    }
    match shape {
        Shape::Point(point) => {
            header(out, 1);
            coord(out, *point);
        }
        Shape::LineString(line) => {
            header(out, 2);
            points(out, line);
        }
        Shape::Polygon(polygon) => {
            header(out, 3);
            rings(out, polygon);
        }
        Shape::MultiPoint(members) => {
            header(out, 4);
            count(out, members.len());
            for point in members {
                header(out, 1);
                coord(out, *point);
            }
        }
        Shape::MultiLineString(lines) => {
            header(out, 5);
            count(out, lines.len());
            for line in lines {
                header(out, 2);
                points(out, line);
            }
        }
        Shape::MultiPolygon(polygons) => {
            header(out, 6);
            count(out, polygons.len());
            for polygon in polygons {
                header(out, 3);
                rings(out, polygon);
            }
        }
        Shape::Collection(members) => {
            header(out, 7);
            count(out, members.len());
            for member in members {
                write_wkb(member, out);
            }
        }
    }
}

fn decode(value: &Value, function: SpatialFunction) -> Result<Geometry, ExecError> {
    let Value::Binary(bytes) = value else {
        return Err(invalid(function));
    };
    let (srid, wkb) = bytes
        .split_first_chunk::<4>()
        .ok_or_else(|| invalid(function))?;
    let shape = read_wkb(wkb).ok_or_else(|| invalid(function))?;
    Ok(Geometry {
        srid: u32::from_le_bytes(*srid),
        shape,
    })
}

fn encode(srid: u32, shape: &Shape) -> Value {
    let mut out = srid.to_le_bytes().to_vec();
    write_wkb(shape, &mut out);
    Value::Binary(out)
}

// ---------------------------------------------------------------- WKT

fn number(value: f64) -> String {
    Float64::new(value).mysql_text()
}

fn write_wkt(shape: &Shape, out: &mut String) {
    fn coord(out: &mut String, [x, y]: Coord) {
        out.push_str(&number(x));
        out.push(' ');
        out.push_str(&number(y));
    }
    fn list<T>(out: &mut String, items: &[T], mut item: impl FnMut(&mut String, &T)) {
        out.push('(');
        for (index, value) in items.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            item(out, value);
        }
        out.push(')');
    }
    fn points(out: &mut String, line: &[Coord]) {
        list(out, line, |out, point| coord(out, *point));
    }
    fn rings(out: &mut String, polygon: &[Ring]) {
        list(out, polygon, |out, ring| points(out, ring));
    }
    out.push_str(match shape {
        Shape::Collection(_) => "GEOMETRYCOLLECTION",
        other => other.type_name(),
    });
    match shape {
        Shape::Point(point) => list(out, &[*point], |out, point| coord(out, *point)),
        Shape::LineString(line) => points(out, line),
        Shape::Polygon(polygon) => rings(out, polygon),
        Shape::MultiPoint(members) => list(out, members, |out, point| {
            list(out, &[*point], |out, point| coord(out, *point));
        }),
        Shape::MultiLineString(lines) => list(out, lines, |out, line| points(out, line)),
        Shape::MultiPolygon(polygons) => list(out, polygons, |out, polygon| rings(out, polygon)),
        Shape::Collection(members) if members.is_empty() => out.push_str(" EMPTY"),
        Shape::Collection(members) => list(out, members, |out, member| write_wkt(member, out)),
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Word(String),
    Number(f64),
    Open,
    Close,
    Comma,
}

fn tokens(text: &str) -> Option<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some(&(start, character)) = chars.peek() {
        match character {
            _ if character.is_whitespace() => {
                chars.next();
            }
            '(' | ')' | ',' => {
                chars.next();
                tokens.push(match character {
                    '(' => Token::Open,
                    ')' => Token::Close,
                    _ => Token::Comma,
                });
            }
            _ if character.is_ascii_alphabetic() => {
                let mut end = start;
                while let Some(&(at, next)) = chars.peek() {
                    if !next.is_ascii_alphabetic() {
                        break;
                    }
                    end = at + next.len_utf8();
                    chars.next();
                }
                tokens.push(Token::Word(text[start..end].to_ascii_uppercase()));
            }
            _ if character.is_ascii_digit() || matches!(character, '+' | '-' | '.') => {
                let mut end = start;
                let mut previous = ' ';
                while let Some(&(at, next)) = chars.peek() {
                    let exponent_sign = matches!(next, '+' | '-') && matches!(previous, 'e' | 'E');
                    let leading_sign = matches!(next, '+' | '-') && at == start;
                    if !(next.is_ascii_digit()
                        || matches!(next, '.' | 'e' | 'E')
                        || exponent_sign
                        || leading_sign)
                    {
                        break;
                    }
                    previous = next;
                    end = at + next.len_utf8();
                    chars.next();
                }
                tokens.push(Token::Number(text[start..end].parse().ok()?));
            }
            _ => return None,
        }
    }
    Some(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
}

impl Parser {
    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.at).cloned();
        self.at += 1;
        token
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn expect(&mut self, token: &Token) -> Option<()> {
        (self.next()? == *token).then_some(())
    }

    fn coord(&mut self) -> Option<Coord> {
        let (Some(Token::Number(x)), Some(Token::Number(y))) = (self.next(), self.next()) else {
            return None;
        };
        Some([x, y])
    }

    /// `( item , item ... )`.
    fn list<T>(&mut self, mut item: impl FnMut(&mut Self) -> Option<T>) -> Option<Vec<T>> {
        self.expect(&Token::Open)?;
        let mut items = vec![item(self)?];
        loop {
            match self.next()? {
                Token::Comma => items.push(item(self)?),
                Token::Close => return Some(items),
                _ => return None,
            }
        }
    }

    fn points(&mut self) -> Option<Vec<Coord>> {
        self.list(Self::coord)
    }

    fn rings(&mut self) -> Option<Vec<Ring>> {
        self.list(Self::points)
    }

    fn shape(&mut self, depth: usize) -> Option<Shape> {
        if depth > 64 {
            return None;
        }
        let Token::Word(word) = self.next()? else {
            return None;
        };
        Some(match word.as_str() {
            "POINT" => {
                self.expect(&Token::Open)?;
                let point = self.coord()?;
                self.expect(&Token::Close)?;
                Shape::Point(point)
            }
            "LINESTRING" => Shape::LineString(self.points()?),
            "POLYGON" => Shape::Polygon(self.rings()?),
            // A member may be written bare or in its own parentheses.
            "MULTIPOINT" => Shape::MultiPoint(self.list(|parser| {
                if parser.peek() == Some(&Token::Open) {
                    parser.next();
                    let point = parser.coord()?;
                    parser.expect(&Token::Close)?;
                    Some(point)
                } else {
                    parser.coord()
                }
            })?),
            "MULTILINESTRING" => Shape::MultiLineString(self.list(Self::points)?),
            "MULTIPOLYGON" => Shape::MultiPolygon(self.list(Self::rings)?),
            "GEOMETRYCOLLECTION" | "GEOMCOLLECTION" => {
                if self.peek() == Some(&Token::Word("EMPTY".to_owned())) {
                    self.next();
                    Shape::Collection(Vec::new())
                } else if self.tokens.get(self.at..self.at + 2)
                    == Some(&[Token::Open, Token::Close])
                {
                    self.at += 2;
                    Shape::Collection(Vec::new())
                } else {
                    Shape::Collection(self.list(|parser| parser.shape(depth + 1))?)
                }
            }
            _ => return None,
        })
    }
}

fn read_wkt(text: &str) -> Option<Shape> {
    let mut parser = Parser {
        tokens: tokens(text)?,
        at: 0,
    };
    let shape = parser.shape(0)?;
    (parser.at == parser.tokens.len() && shape.is_valid()).then_some(shape)
}

// ---------------------------------------------------------------- GeoJSON

fn geojson(shape: &Shape) -> serde_json::Value {
    use serde_json::{Value as Json, json};
    let coord = |[x, y]: Coord| json!([x, y]);
    let line =
        |points: &Vec<Coord>| Json::Array(points.iter().map(|point| coord(*point)).collect());
    let polygon = |rings: &Vec<Ring>| Json::Array(rings.iter().map(line).collect());
    let (kind, coordinates) = match shape {
        Shape::Point(point) => ("Point", coord(*point)),
        Shape::LineString(points) => ("LineString", line(points)),
        Shape::Polygon(rings) => ("Polygon", polygon(rings)),
        Shape::MultiPoint(points) => ("MultiPoint", line(points)),
        Shape::MultiLineString(lines) => (
            "MultiLineString",
            Json::Array(lines.iter().map(line).collect()),
        ),
        Shape::MultiPolygon(polygons) => (
            "MultiPolygon",
            Json::Array(polygons.iter().map(polygon).collect()),
        ),
        Shape::Collection(members) => {
            return json!({
                "type": "GeometryCollection",
                "geometries": members.iter().map(geojson).collect::<Vec<_>>(),
            });
        }
    };
    json!({ "type": kind, "coordinates": coordinates })
}

// ---------------------------------------------------------------- planar geometry

fn cross(o: Coord, a: Coord, b: Coord) -> f64 {
    (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
}

fn distance(a: Coord, b: Coord) -> f64 {
    let (dx, dy) = (a[0] - b[0], a[1] - b[1]);
    (dx * dx + dy * dy).sqrt()
}

fn within_box(p: Coord, a: Coord, b: Coord) -> bool {
    p[0] >= a[0].min(b[0])
        && p[0] <= a[0].max(b[0])
        && p[1] >= a[1].min(b[1])
        && p[1] <= a[1].max(b[1])
}

fn on_segment(p: Coord, a: Coord, b: Coord) -> bool {
    cross(a, b, p) == 0.0 && within_box(p, a, b)
}

fn segments_intersect(a: Coord, b: Coord, c: Coord, d: Coord) -> bool {
    let (d1, d2) = (cross(c, d, a), cross(c, d, b));
    let (d3, d4) = (cross(a, b, c), cross(a, b, d));
    if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    {
        return true;
    }
    on_segment(a, c, d) || on_segment(b, c, d) || on_segment(c, a, b) || on_segment(d, a, b)
}

fn point_segment_distance(p: Coord, a: Coord, b: Coord) -> f64 {
    let v = [b[0] - a[0], b[1] - a[1]];
    let w = [p[0] - a[0], p[1] - a[1]];
    let along = w[0] * v[0] + w[1] * v[1];
    if along <= 0.0 {
        return distance(p, a);
    }
    let length = v[0] * v[0] + v[1] * v[1];
    if length <= along {
        return distance(p, b);
    }
    let t = along / length;
    distance(p, [a[0] + t * v[0], a[1] + t * v[1]])
}

fn segment_distance(a: Coord, b: Coord, c: Coord, d: Coord) -> f64 {
    if segments_intersect(a, b, c, d) {
        return 0.0;
    }
    point_segment_distance(a, c, d)
        .min(point_segment_distance(b, c, d))
        .min(point_segment_distance(c, a, b))
        .min(point_segment_distance(d, a, b))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Location {
    Interior,
    Boundary,
    Exterior,
}

fn ring_location(p: Coord, ring: &Ring) -> Location {
    let mut inside = false;
    for edge in ring.windows(2) {
        let (a, b) = (edge[0], edge[1]);
        if on_segment(p, a, b) {
            return Location::Boundary;
        }
        if (a[1] > p[1]) != (b[1] > p[1]) {
            let x = a[0] + (p[1] - a[1]) * (b[0] - a[0]) / (b[1] - a[1]);
            if p[0] < x {
                inside = !inside;
            }
        }
    }
    if inside {
        Location::Interior
    } else {
        Location::Exterior
    }
}

fn polygon_location(p: Coord, rings: &[Ring]) -> Location {
    let Some((exterior, holes)) = rings.split_first() else {
        return Location::Exterior;
    };
    match ring_location(p, exterior) {
        Location::Interior => {}
        other => return other,
    }
    for hole in holes {
        match ring_location(p, hole) {
            Location::Interior => return Location::Exterior,
            Location::Boundary => return Location::Boundary,
            Location::Exterior => {}
        }
    }
    Location::Interior
}

/// A shape taken apart into what the planar predicates work on.
#[derive(Default)]
struct Parts {
    points: Vec<Coord>,
    lines: Vec<Vec<Coord>>,
    polygons: Vec<Vec<Ring>>,
}

impl Parts {
    fn of(shape: &Shape) -> Self {
        let mut parts = Self::default();
        parts.add(shape);
        parts
    }

    fn add(&mut self, shape: &Shape) {
        match shape {
            Shape::Point(point) => self.points.push(*point),
            Shape::MultiPoint(points) => self.points.extend(points),
            Shape::LineString(line) => self.lines.push(line.clone()),
            Shape::MultiLineString(lines) => self.lines.extend(lines.iter().cloned()),
            Shape::Polygon(rings) => self.polygons.push(rings.clone()),
            Shape::MultiPolygon(polygons) => self.polygons.extend(polygons.iter().cloned()),
            Shape::Collection(members) => members.iter().for_each(|member| self.add(member)),
        }
    }

    fn is_empty(&self) -> bool {
        self.points.is_empty() && self.lines.is_empty() && self.polygons.is_empty()
    }

    /// Every segment: line pieces and polygon ring edges.
    fn segments(&self) -> impl Iterator<Item = (Coord, Coord)> + '_ {
        self.lines
            .iter()
            .chain(self.polygons.iter().flatten())
            .flat_map(|line| line.windows(2).map(|edge| (edge[0], edge[1])))
    }

    fn area_location(&self, p: Coord) -> Location {
        let mut location = Location::Exterior;
        for polygon in &self.polygons {
            match polygon_location(p, polygon) {
                Location::Interior => return Location::Interior,
                Location::Boundary => location = Location::Boundary,
                Location::Exterior => {}
            }
        }
        location
    }

    /// The lineal boundary: the endpoints of unclosed lines, counted by the
    /// mod-2 rule a multi-line uses.
    fn line_boundary(&self) -> Vec<Coord> {
        let mut ends: Vec<Coord> = Vec::new();
        for line in &self.lines {
            if line.first() == line.last() {
                continue;
            }
            for end in [line[0], line[line.len() - 1]] {
                if let Some(at) = ends.iter().position(|seen| *seen == end) {
                    ends.remove(at);
                } else {
                    ends.push(end);
                }
            }
        }
        ends
    }

    fn touches_point(&self, p: Coord) -> bool {
        self.points.contains(&p)
            || self.segments().any(|(a, b)| on_segment(p, a, b))
            || self.area_location(p) != Location::Exterior
    }
}

fn intersects(left: &Parts, right: &Parts) -> bool {
    if left.points.iter().any(|p| right.touches_point(*p))
        || right.points.iter().any(|p| left.touches_point(*p))
    {
        return true;
    }
    for (a, b) in left.segments() {
        if right
            .segments()
            .any(|(c, d)| segments_intersect(a, b, c, d))
        {
            return true;
        }
    }
    // One wholly inside the other, with no edge crossing.
    let first = |parts: &Parts| {
        parts
            .lines
            .iter()
            .chain(parts.polygons.iter().flatten())
            .find_map(|line| line.first().copied())
    };
    first(left).is_some_and(|p| right.area_location(p) != Location::Exterior)
        || first(right).is_some_and(|p| left.area_location(p) != Location::Exterior)
}

fn parts_distance(left: &Parts, right: &Parts) -> f64 {
    if intersects(left, right) {
        return 0.0;
    }
    let mut best = f64::INFINITY;
    for p in &left.points {
        for q in &right.points {
            best = best.min(distance(*p, *q));
        }
        for (c, d) in right.segments() {
            best = best.min(point_segment_distance(*p, c, d));
        }
    }
    for q in &right.points {
        for (a, b) in left.segments() {
            best = best.min(point_segment_distance(*q, a, b));
        }
    }
    for (a, b) in left.segments() {
        for (c, d) in right.segments() {
            best = best.min(segment_distance(a, b, c, d));
        }
    }
    best
}

/// The points along `a`-`b` where it meets any of `edges`, as fractions of
/// its length, with both ends - the pieces between them lie wholly on one
/// side of every edge.
fn cuts(a: Coord, b: Coord, edges: &[(Coord, Coord)]) -> Vec<f64> {
    let mut at = vec![0.0, 1.0];
    let length = [b[0] - a[0], b[1] - a[1]];
    let project = |p: Coord| {
        let squared = length[0] * length[0] + length[1] * length[1];
        if squared == 0.0 {
            0.0
        } else {
            ((p[0] - a[0]) * length[0] + (p[1] - a[1]) * length[1]) / squared
        }
    };
    for (c, d) in edges {
        if !segments_intersect(a, b, *c, *d) {
            continue;
        }
        let denominator = length[0] * (d[1] - c[1]) - length[1] * (d[0] - c[0]);
        if denominator == 0.0 {
            // Collinear overlap: its ends bound the shared stretch.
            at.extend([project(*c), project(*d)]);
        } else {
            at.push(((c[0] - a[0]) * (d[1] - c[1]) - (c[1] - a[1]) * (d[0] - c[0])) / denominator);
        }
    }
    at.retain(|t| (0.0..=1.0).contains(t));
    at.sort_by(f64::total_cmp);
    at.dedup();
    at
}

/// Where the pieces of `line` fall against the areal `area`: whether any
/// piece leaves it, and whether any runs through its interior.
fn line_against_area(line: &[Coord], area: &Parts) -> (bool, bool) {
    let edges: Vec<(Coord, Coord)> = area.segments().collect();
    let (mut outside, mut inside) = (false, false);
    for edge in line.windows(2) {
        let (a, b) = (edge[0], edge[1]);
        let at = cuts(a, b, &edges);
        for pair in at.windows(2) {
            let t = f64::midpoint(pair[0], pair[1]);
            let middle = [a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])];
            match area.area_location(middle) {
                Location::Exterior => outside = true,
                Location::Interior => inside = true,
                Location::Boundary => {}
            }
        }
    }
    (outside, inside)
}

/// `container` contains `contained`: nothing of it outside, and some of its
/// interior in the container's interior. `None` for pairs this does not
/// decide.
fn contains(container: &Parts, contained: &Parts) -> Option<bool> {
    if contained.is_empty() || container.is_empty() {
        return Some(false);
    }
    let areal = container.points.is_empty() && container.lines.is_empty();
    if areal {
        let mut interior = false;
        for p in &contained.points {
            match container.area_location(*p) {
                Location::Exterior => return Some(false),
                Location::Interior => interior = true,
                Location::Boundary => {}
            }
        }
        for line in &contained.lines {
            let (outside, inside) = line_against_area(line, container);
            if outside {
                return Some(false);
            }
            interior |= inside;
        }
        if !contained.polygons.is_empty() {
            let rings = Parts {
                polygons: contained.polygons.clone(),
                ..Parts::default()
            };
            for ring in contained.polygons.iter().flatten() {
                if line_against_area(ring, container).0 {
                    return Some(false);
                }
            }
            // The container's own boundary must not run through the
            // contained area's interior: that would put the container's
            // outside inside it.
            for ring in container.polygons.iter().flatten() {
                if line_against_area(ring, &rings).1 {
                    return Some(false);
                }
            }
            interior = true;
        }
        return Some(interior);
    }
    if !contained.lines.is_empty() || !contained.polygons.is_empty() {
        // A point set contains no line or area; a line would need the
        // lineal overlay, which is not here.
        return if container.lines.is_empty() && container.polygons.is_empty() {
            Some(false)
        } else {
            None
        };
    }
    if !container.polygons.is_empty() {
        return None;
    }
    if container.lines.is_empty() {
        return Some(
            contained
                .points
                .iter()
                .all(|p| container.points.contains(p)),
        );
    }
    if !container.points.is_empty() {
        return None;
    }
    let boundary = container.line_boundary();
    let mut interior = false;
    for p in &contained.points {
        if !container.segments().any(|(a, b)| on_segment(*p, a, b)) {
            return Some(false);
        }
        interior |= !boundary.contains(p);
    }
    Some(interior)
}

fn envelope(shape: &Shape) -> Option<[f64; 4]> {
    let mut bounds: Option<[f64; 4]> = None;
    shape.coords(&mut |[x, y]| {
        bounds = Some(match bounds {
            None => [x, y, x, y],
            Some([min_x, min_y, max_x, max_y]) => {
                [min_x.min(x), min_y.min(y), max_x.max(x), max_y.max(y)]
            }
        });
    });
    bounds
}

/// A bounding box as the geometry `MySQL` gives it: a point, a line or a
/// rectangle, as its extent allows.
fn envelope_shape([min_x, min_y, max_x, max_y]: [f64; 4]) -> Shape {
    if min_x == max_x && min_y == max_y {
        Shape::Point([min_x, min_y])
    } else if min_x == max_x || min_y == max_y {
        Shape::LineString(vec![[min_x, min_y], [max_x, max_y]])
    } else {
        Shape::Polygon(vec![vec![
            [min_x, min_y],
            [max_x, min_y],
            [max_x, max_y],
            [min_x, max_y],
            [min_x, min_y],
        ]])
    }
}

fn ring_area_centroid(ring: &Ring) -> (f64, Coord) {
    let (mut area, mut cx, mut cy) = (0.0, 0.0, 0.0);
    for edge in ring.windows(2) {
        let (a, b) = (edge[0], edge[1]);
        let step = a[0] * b[1] - b[0] * a[1];
        area += step;
        cx += (a[0] + b[0]) * step;
        cy += (a[1] + b[1]) * step;
    }
    let area = area / 2.0;
    if area == 0.0 {
        return (0.0, ring[0]);
    }
    (area.abs(), [cx / (6.0 * area), cy / (6.0 * area)])
}

fn polygon_area(rings: &[Ring]) -> f64 {
    let Some((exterior, holes)) = rings.split_first() else {
        return 0.0;
    };
    ring_area_centroid(exterior).0
        - holes
            .iter()
            .map(|hole| ring_area_centroid(hole).0)
            .sum::<f64>()
}

fn line_length(line: &[Coord]) -> f64 {
    line.windows(2).map(|edge| distance(edge[0], edge[1])).sum()
}

/// The centroid of the shape's highest-dimension parts, as `MySQL` weighs
/// them: by area, else by length, else by count.
fn centroid(parts: &Parts) -> Option<Coord> {
    let mut weight = 0.0;
    let mut sum = [0.0, 0.0];
    let mut add = |w: f64, c: Coord| {
        weight += w;
        sum[0] += w * c[0];
        sum[1] += w * c[1];
    };
    if !parts.polygons.is_empty() {
        for polygon in &parts.polygons {
            for (index, ring) in polygon.iter().enumerate() {
                let (area, center) = ring_area_centroid(ring);
                add(if index == 0 { area } else { -area }, center);
            }
        }
    } else if !parts.lines.is_empty() {
        for edge in parts.lines.iter().flat_map(|line| line.windows(2)) {
            let (a, b) = (edge[0], edge[1]);
            add(
                distance(a, b),
                [f64::midpoint(a[0], b[0]), f64::midpoint(a[1], b[1])],
            );
        }
    } else {
        for point in &parts.points {
            add(1.0, *point);
        }
    }
    (weight != 0.0).then(|| [sum[0] / weight, sum[1] / weight])
}

/// The great-circle distance between two (longitude, latitude) points
/// given in radians.
fn haversine(from: Coord, to: Coord, radius: f64) -> f64 {
    let ([from_long, from_lat], [to_long, to_lat]) = (from, to);
    let half_lat = (to_lat - from_lat) / 2.0;
    let half_long = (to_long - from_long) / 2.0;
    let a = half_lat.sin().powi(2) + from_lat.cos() * to_lat.cos() * half_long.sin().powi(2);
    2.0 * radius * a.sqrt().asin()
}

// ---------------------------------------------------------------- functions

fn srid_argument(value: Option<&Value>) -> Result<u32, ExecError> {
    value.map_or(Ok(0), |value| {
        u32::try_from(mysql_i64(value)?).map_err(|_| ExecError::NumericOverflow)
    })
}

fn index_argument(value: &Value) -> Result<Option<usize>, ExecError> {
    Ok(usize::try_from(mysql_i64(value)?)
        .ok()
        .and_then(|index| index.checked_sub(1)))
}

fn boolean(value: bool) -> Value {
    Value::Int64(i64::from(value))
}

fn decided(answer: Option<bool>, function: SpatialFunction, srid: u32) -> Result<Value, ExecError> {
    answer.map(boolean).ok_or_else(|| {
        failure(
            SpatialError::Unsupported,
            format!(
                "{} is not supported for this combination of geometry types (SRID {srid})",
                function.name()
            ),
        )
    })
}

fn point_of(geometry: &Geometry, function: SpatialFunction) -> Result<Coord, ExecError> {
    match geometry.shape {
        Shape::Point(point) => Ok(point),
        ref other => Err(unexpected("POINT", other, function)),
    }
}

/// Evaluates one spatial function; a NULL argument answers NULL.
#[allow(clippy::too_many_lines)] // one arm per function reads as the catalogue
pub(super) fn evaluate(function: SpatialFunction, values: &[Value]) -> Result<Value, ExecError> {
    use SpatialFunction as F;
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    let geometry = |index: usize| decode(&values[index], function);
    let pair = || -> Result<(Geometry, Geometry), ExecError> {
        let (left, right) = (geometry(0)?, geometry(1)?);
        same_srid(&left, &right, function)?;
        cartesian(&left, function)?;
        Ok((left, right))
    };
    let child = |geometry: &Geometry, shape: Option<Shape>| {
        shape.map_or(Value::Null, |shape| encode(geometry.srid, &shape))
    };
    Ok(match function {
        F::AsText => {
            let geometry = geometry(0)?;
            let mut text = String::new();
            write_wkt(
                &presented(&geometry.shape, srs_of(geometry.srid, function)?),
                &mut text,
            );
            Value::Utf8(text)
        }
        F::AsBinary => {
            let geometry = geometry(0)?;
            let mut out = Vec::new();
            write_wkb(
                &presented(&geometry.shape, srs_of(geometry.srid, function)?),
                &mut out,
            );
            Value::Binary(out)
        }
        F::AsGeoJson => {
            let geometry = geometry(0)?;
            srs_of(geometry.srid, function)?;
            Value::Utf8(pintail_types::mysql_json_text(&geojson(&geometry.shape)))
        }
        F::FromText(kind) | F::FromWkb(kind) => {
            let srid = srid_argument(values.get(1))?;
            let srs = srs_of(srid, function)?;
            let shape = if matches!(function, F::FromText(_)) {
                read_wkt(&scalar_string(&values[0])?)
            } else {
                match &values[0] {
                    Value::Binary(bytes) => read_wkb(bytes),
                    _ => None,
                }
            }
            .filter(|shape| kind.is_none_or(|kind| shape.kind() == kind))
            .ok_or_else(|| invalid(function))?;
            encode(srid, &stored(&shape, srs, function)?)
        }
        F::Point => encode(
            0,
            &Shape::Point([mysql_f64(&values[0])?, mysql_f64(&values[1])?]),
        ),
        F::Srid => Value::UInt64(u64::from(geometry(0)?.srid)),
        F::WithSrid => {
            let geometry = geometry(0)?;
            let srid = srid_argument(values.get(1))?;
            if srs_of(srid, function)? == Srs::Geographic {
                stored(
                    &presented(&geometry.shape, Srs::Geographic),
                    Srs::Geographic,
                    function,
                )?;
            }
            encode(srid, &geometry.shape)
        }
        F::X | F::Y => {
            let geometry = geometry(0)?;
            let point = point_of(&geometry, function)?;
            let [first, second] = match srs_of(geometry.srid, function)? {
                Srs::Cartesian => point,
                Srs::Geographic => [point[1], point[0]],
            };
            Value::float64(if function == F::X { first } else { second })
        }
        F::Latitude | F::Longitude => {
            let geometry = geometry(0)?;
            if srs_of(geometry.srid, function)? != Srs::Geographic {
                return Err(failure(
                    SpatialError::NotGeographic,
                    format!(
                        "Function {} is only defined for geographic spatial reference systems, \
                         but one of its arguments is in SRID {}, which is not geographic.",
                        function.name(),
                        geometry.srid
                    ),
                ));
            }
            let [longitude, latitude] = point_of(&geometry, function)?;
            Value::float64(if function == F::Latitude {
                latitude
            } else {
                longitude
            })
        }
        F::GeometryType => Value::Utf8(geometry(0)?.shape.type_name().to_owned()),
        F::IsEmpty => boolean(
            matches!(geometry(0)?.shape, Shape::Collection(ref members) if members.is_empty()),
        ),
        F::IsClosed => match geometry(0)?.shape {
            Shape::LineString(line) => boolean(line.first() == line.last()),
            Shape::MultiLineString(lines) => {
                boolean(lines.iter().all(|line| line.first() == line.last()))
            }
            _ => Value::Null,
        },
        F::Dimension => {
            fn dimension(shape: &Shape) -> Option<i64> {
                match shape {
                    Shape::Point(_) | Shape::MultiPoint(_) => Some(0),
                    Shape::LineString(_) | Shape::MultiLineString(_) => Some(1),
                    Shape::Polygon(_) | Shape::MultiPolygon(_) => Some(2),
                    Shape::Collection(members) => members.iter().filter_map(dimension).max(),
                }
            }
            dimension(&geometry(0)?.shape).map_or(Value::Null, Value::Int64)
        }
        F::NumPoints => match geometry(0)?.shape {
            Shape::LineString(line) => Value::Int64(i64::try_from(line.len()).unwrap_or(i64::MAX)),
            _ => Value::Null,
        },
        F::NumGeometries => {
            let count = match geometry(0)?.shape {
                Shape::MultiPoint(members) => members.len(),
                Shape::MultiLineString(members) => members.len(),
                Shape::MultiPolygon(members) => members.len(),
                Shape::Collection(members) => members.len(),
                _ => return Ok(Value::Null),
            };
            Value::Int64(i64::try_from(count).unwrap_or(i64::MAX))
        }
        F::NumInteriorRings => match geometry(0)?.shape {
            Shape::Polygon(rings) => Value::Int64(i64::try_from(rings.len() - 1).unwrap_or(0)),
            _ => Value::Null,
        },
        F::StartPoint | F::EndPoint | F::PointN => {
            let geometry = geometry(0)?;
            let Shape::LineString(line) = &geometry.shape else {
                return Ok(Value::Null);
            };
            let index = match function {
                F::StartPoint => Some(0),
                F::EndPoint => Some(line.len() - 1),
                _ => index_argument(&values[1])?,
            };
            child(
                &geometry,
                index
                    .and_then(|index| line.get(index))
                    .map(|p| Shape::Point(*p)),
            )
        }
        F::GeometryN => {
            let geometry = geometry(0)?;
            let index = index_argument(&values[1])?;
            let member = index.and_then(|index| match &geometry.shape {
                Shape::MultiPoint(members) => members.get(index).map(|p| Shape::Point(*p)),
                Shape::MultiLineString(members) => members
                    .get(index)
                    .map(|line| Shape::LineString(line.clone())),
                Shape::MultiPolygon(members) => members
                    .get(index)
                    .map(|rings| Shape::Polygon(rings.clone())),
                Shape::Collection(members) => members.get(index).cloned(),
                _ => None,
            });
            child(&geometry, member)
        }
        F::ExteriorRing | F::InteriorRingN => {
            let geometry = geometry(0)?;
            let Shape::Polygon(rings) = &geometry.shape else {
                return Ok(Value::Null);
            };
            let index = if function == F::ExteriorRing {
                Some(0)
            } else {
                index_argument(&values[1])?.map(|index| index + 1)
            };
            child(
                &geometry,
                index
                    .and_then(|index| rings.get(index))
                    .map(|ring| Shape::LineString(ring.clone())),
            )
        }
        F::Envelope => {
            let geometry = geometry(0)?;
            cartesian(&geometry, function)?;
            let shape =
                envelope(&geometry.shape).map_or(Shape::Collection(Vec::new()), envelope_shape);
            encode(geometry.srid, &shape)
        }
        F::Centroid => {
            let geometry = geometry(0)?;
            cartesian(&geometry, function)?;
            let shape = centroid(&Parts::of(&geometry.shape))
                .map_or(Shape::Collection(Vec::new()), Shape::Point);
            encode(geometry.srid, &shape)
        }
        F::Distance => {
            let (left, right) = pair()?;
            let (left, right) = (Parts::of(&left.shape), Parts::of(&right.shape));
            if left.is_empty() || right.is_empty() {
                return Ok(Value::Null);
            }
            Value::float64(parts_distance(&left, &right))
        }
        F::DistanceSphere => {
            let (from, to) = (geometry(0)?, geometry(1)?);
            same_srid(&from, &to, function)?;
            let (default_radius, degree) = match srs_of(from.srid, function)? {
                Srs::Cartesian => (SPHERE_RADIUS, None),
                Srs::Geographic => (WGS84_MEAN_RADIUS, Some(WGS84_DEGREE)),
            };
            let (from, to) = (point_of(&from, function)?, point_of(&to, function)?);
            for [longitude, latitude] in [from, to] {
                check_geographic(latitude, longitude, function)?;
            }
            let radius = values.get(2).map_or(Ok(default_radius), mysql_f64)?;
            if radius.is_nan() || radius <= 0.0 {
                return Err(invalid(function));
            }
            let radians = |point: Coord| {
                point.map(|angle| degree.map_or_else(|| angle.to_radians(), |unit| angle * unit))
            };
            Value::float64(haversine(radians(from), radians(to), radius))
        }
        F::Length => {
            let geometry = geometry(0)?;
            cartesian(&geometry, function)?;
            match &geometry.shape {
                Shape::LineString(line) => Value::float64(line_length(line)),
                Shape::MultiLineString(lines) => {
                    Value::float64(lines.iter().map(|line| line_length(line)).sum())
                }
                _ => Value::Null,
            }
        }
        F::Area => {
            let geometry = geometry(0)?;
            cartesian(&geometry, function)?;
            match &geometry.shape {
                Shape::Polygon(rings) => Value::float64(polygon_area(rings)),
                Shape::MultiPolygon(polygons) => {
                    Value::float64(polygons.iter().map(|rings| polygon_area(rings)).sum())
                }
                other => return Err(unexpected("POLYGON/MULTIPOLYGON", other, function)),
            }
        }
        F::Contains | F::Within => {
            let (left, right) = pair()?;
            let (outer, inner) = if function == F::Contains {
                (&left, &right)
            } else {
                (&right, &left)
            };
            decided(
                contains(&Parts::of(&outer.shape), &Parts::of(&inner.shape)),
                function,
                left.srid,
            )?
        }
        F::Intersects | F::Disjoint => {
            let (left, right) = pair()?;
            let meets = intersects(&Parts::of(&left.shape), &Parts::of(&right.shape));
            boolean(meets == (function == F::Intersects))
        }
        F::MbrContains | F::MbrWithin | F::MbrIntersects | F::MbrDisjoint | F::MbrEquals => {
            let (left, right) = pair()?;
            let (Some(left_box), Some(right_box)) = (envelope(&left.shape), envelope(&right.shape))
            else {
                return Ok(boolean(function == F::MbrDisjoint));
            };
            let boxed = |bounds| Parts::of(&envelope_shape(bounds));
            let overlap = left_box[0] <= right_box[2]
                && right_box[0] <= left_box[2]
                && left_box[1] <= right_box[3]
                && right_box[1] <= left_box[3];
            match function {
                F::MbrContains => decided(
                    contains(&boxed(left_box), &boxed(right_box)),
                    function,
                    left.srid,
                )?,
                F::MbrWithin => decided(
                    contains(&boxed(right_box), &boxed(left_box)),
                    function,
                    left.srid,
                )?,
                F::MbrIntersects => boolean(overlap),
                F::MbrDisjoint => boolean(!overlap),
                _ => boolean(left_box == right_box),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{Shape, read_wkt, write_wkt};

    fn round_trip(text: &str) -> String {
        let mut out = String::new();
        write_wkt(&read_wkt(text).expect("valid WKT"), &mut out);
        out
    }

    #[test]
    fn wkt_reads_as_mysql_writes_it() {
        assert_eq!(round_trip(" point ( 1   2 ) "), "POINT(1 2)");
        assert_eq!(round_trip("MULTIPOINT(1 2,3 4)"), "MULTIPOINT((1 2),(3 4))");
        assert_eq!(
            round_trip("GEOMETRYCOLLECTION EMPTY"),
            "GEOMETRYCOLLECTION EMPTY"
        );
        assert_eq!(round_trip("POINT(-1.5e3 +2)"), "POINT(-1500 2)");
        assert_eq!(read_wkt("LINESTRING(0 0)"), None);
        assert_eq!(read_wkt("POLYGON((0 0,1 0,1 1,0 1))"), None);
        assert_eq!(read_wkt("POINT(1)"), None);
        assert!(matches!(
            read_wkt("GEOMCOLLECTION(POINT(1 1))"),
            Some(Shape::Collection(_))
        ));
    }
}
