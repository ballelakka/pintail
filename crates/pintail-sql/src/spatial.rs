//! The spatial functions the executor answers, and the types they return.
//!
//! A geometry travels as `MySQL`'s internal format - a four-byte little-endian
//! SRID followed by little-endian WKB - which is what a replicated geometry
//! column already holds, so a column and a constructed geometry are the same
//! kind of value.

use pintail_types::DataType;

/// The geometry type a typed constructor (`ST_PointFromText`, ...) insists
/// on; `None` accepts any.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeometryKind {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

/// One spatial function, resolved from its name and argument count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpatialFunction {
    AsText,
    AsBinary,
    AsGeoJson,
    FromText(Option<GeometryKind>),
    FromWkb(Option<GeometryKind>),
    /// `POINT(x, y)`: SRID 0, coordinates as written.
    Point,
    Srid,
    /// `ST_SRID(g, srid)`: the same coordinates under another SRID.
    WithSrid,
    X,
    Y,
    Latitude,
    Longitude,
    GeometryType,
    IsEmpty,
    IsClosed,
    Dimension,
    NumPoints,
    NumGeometries,
    NumInteriorRings,
    StartPoint,
    EndPoint,
    PointN,
    GeometryN,
    ExteriorRing,
    InteriorRingN,
    Envelope,
    Centroid,
    Distance,
    DistanceSphere,
    Length,
    Area,
    Contains,
    Within,
    Intersects,
    Disjoint,
    MbrContains,
    MbrWithin,
    MbrIntersects,
    MbrDisjoint,
    MbrEquals,
}

impl SpatialFunction {
    /// The function `name` (upper-case) names when called with `arguments`
    /// arguments.
    #[must_use]
    pub fn from_name(name: &str, arguments: usize) -> Option<Self> {
        use GeometryKind as Kind;
        let function = match (name, arguments) {
            ("ST_ASTEXT" | "ST_ASWKT", 1) => Self::AsText,
            ("ST_ASBINARY" | "ST_ASWKB", 1) => Self::AsBinary,
            ("ST_ASGEOJSON", 1) => Self::AsGeoJson,
            ("ST_GEOMFROMTEXT" | "ST_GEOMETRYFROMTEXT", 1 | 2) => Self::FromText(None),
            ("ST_POINTFROMTEXT", 1 | 2) => Self::FromText(Some(Kind::Point)),
            ("ST_LINEFROMTEXT" | "ST_LINESTRINGFROMTEXT", 1 | 2) => {
                Self::FromText(Some(Kind::LineString))
            }
            ("ST_POLYFROMTEXT" | "ST_POLYGONFROMTEXT", 1 | 2) => {
                Self::FromText(Some(Kind::Polygon))
            }
            ("ST_MPOINTFROMTEXT" | "ST_MULTIPOINTFROMTEXT", 1 | 2) => {
                Self::FromText(Some(Kind::MultiPoint))
            }
            ("ST_MLINEFROMTEXT" | "ST_MULTILINESTRINGFROMTEXT", 1 | 2) => {
                Self::FromText(Some(Kind::MultiLineString))
            }
            ("ST_MPOLYFROMTEXT" | "ST_MULTIPOLYGONFROMTEXT", 1 | 2) => {
                Self::FromText(Some(Kind::MultiPolygon))
            }
            (
                "ST_GEOMCOLLFROMTEXT" | "ST_GEOMETRYCOLLECTIONFROMTEXT" | "ST_GEOMCOLLFROMTXT",
                1 | 2,
            ) => Self::FromText(Some(Kind::GeometryCollection)),
            ("ST_GEOMFROMWKB" | "ST_GEOMETRYFROMWKB", 1 | 2) => Self::FromWkb(None),
            ("ST_POINTFROMWKB", 1 | 2) => Self::FromWkb(Some(Kind::Point)),
            ("ST_LINEFROMWKB" | "ST_LINESTRINGFROMWKB", 1 | 2) => {
                Self::FromWkb(Some(Kind::LineString))
            }
            ("ST_POLYFROMWKB" | "ST_POLYGONFROMWKB", 1 | 2) => Self::FromWkb(Some(Kind::Polygon)),
            ("POINT", 2) => Self::Point,
            ("ST_SRID", 1) => Self::Srid,
            ("ST_SRID", 2) => Self::WithSrid,
            ("ST_X", 1) => Self::X,
            ("ST_Y", 1) => Self::Y,
            ("ST_LATITUDE", 1) => Self::Latitude,
            ("ST_LONGITUDE", 1) => Self::Longitude,
            ("ST_GEOMETRYTYPE", 1) => Self::GeometryType,
            ("ST_ISEMPTY", 1) => Self::IsEmpty,
            ("ST_ISCLOSED", 1) => Self::IsClosed,
            ("ST_DIMENSION", 1) => Self::Dimension,
            ("ST_NUMPOINTS", 1) => Self::NumPoints,
            ("ST_NUMGEOMETRIES", 1) => Self::NumGeometries,
            ("ST_NUMINTERIORRINGS" | "ST_NUMINTERIORRING", 1) => Self::NumInteriorRings,
            ("ST_STARTPOINT", 1) => Self::StartPoint,
            ("ST_ENDPOINT", 1) => Self::EndPoint,
            ("ST_POINTN", 2) => Self::PointN,
            ("ST_GEOMETRYN", 2) => Self::GeometryN,
            ("ST_EXTERIORRING", 1) => Self::ExteriorRing,
            ("ST_INTERIORRINGN", 2) => Self::InteriorRingN,
            ("ST_ENVELOPE", 1) => Self::Envelope,
            ("ST_CENTROID", 1) => Self::Centroid,
            ("ST_DISTANCE", 2) => Self::Distance,
            ("ST_DISTANCE_SPHERE", 2 | 3) => Self::DistanceSphere,
            ("ST_LENGTH", 1) => Self::Length,
            ("ST_AREA", 1) => Self::Area,
            ("ST_CONTAINS", 2) => Self::Contains,
            ("ST_WITHIN", 2) => Self::Within,
            ("ST_INTERSECTS", 2) => Self::Intersects,
            ("ST_DISJOINT", 2) => Self::Disjoint,
            ("MBRCONTAINS", 2) => Self::MbrContains,
            ("MBRWITHIN", 2) => Self::MbrWithin,
            ("MBRINTERSECTS", 2) => Self::MbrIntersects,
            ("MBRDISJOINT", 2) => Self::MbrDisjoint,
            ("MBREQUALS", 2) => Self::MbrEquals,
            _ => return None,
        };
        Some(function)
    }

    /// The type of the value this function answers.
    #[must_use]
    pub const fn result_type(self) -> DataType {
        match self {
            Self::AsText | Self::AsGeoJson | Self::GeometryType => DataType::Utf8,
            Self::AsBinary
            | Self::FromText(_)
            | Self::FromWkb(_)
            | Self::Point
            | Self::WithSrid
            | Self::StartPoint
            | Self::EndPoint
            | Self::PointN
            | Self::GeometryN
            | Self::ExteriorRing
            | Self::InteriorRingN
            | Self::Envelope
            | Self::Centroid => DataType::Binary,
            Self::Srid => DataType::UInt64,
            Self::X
            | Self::Y
            | Self::Latitude
            | Self::Longitude
            | Self::Distance
            | Self::DistanceSphere
            | Self::Length
            | Self::Area => DataType::Float64,
            Self::IsEmpty
            | Self::IsClosed
            | Self::Dimension
            | Self::NumPoints
            | Self::NumGeometries
            | Self::NumInteriorRings
            | Self::Contains
            | Self::Within
            | Self::Intersects
            | Self::Disjoint
            | Self::MbrContains
            | Self::MbrWithin
            | Self::MbrIntersects
            | Self::MbrDisjoint
            | Self::MbrEquals => DataType::Int64,
        }
    }

    /// Whether the answer is itself a geometry, which a client is told is a
    /// GEOMETRY column rather than a blob.
    #[must_use]
    pub const fn returns_geometry(self) -> bool {
        matches!(self.result_type(), DataType::Binary) && !matches!(self, Self::AsBinary)
    }

    /// The name `MySQL`'s messages give this function.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::AsText => "st_astext",
            Self::AsBinary => "st_asbinary",
            Self::AsGeoJson => "st_asgeojson",
            Self::FromText(_) => "st_geomfromtext",
            Self::FromWkb(_) => "st_geomfromwkb",
            Self::Point => "point",
            Self::Srid | Self::WithSrid => "st_srid",
            Self::X => "st_x",
            Self::Y => "st_y",
            Self::Latitude => "st_latitude",
            Self::Longitude => "st_longitude",
            Self::GeometryType => "st_geometrytype",
            Self::IsEmpty => "st_isempty",
            Self::IsClosed => "st_isclosed",
            Self::Dimension => "st_dimension",
            Self::NumPoints => "st_numpoints",
            Self::NumGeometries => "st_numgeometries",
            Self::NumInteriorRings => "st_numinteriorrings",
            Self::StartPoint => "st_startpoint",
            Self::EndPoint => "st_endpoint",
            Self::PointN => "st_pointn",
            Self::GeometryN => "st_geometryn",
            Self::ExteriorRing => "st_exteriorring",
            Self::InteriorRingN => "st_interiorringn",
            Self::Envelope => "st_envelope",
            Self::Centroid => "st_centroid",
            Self::Distance => "st_distance",
            Self::DistanceSphere => "st_distance_sphere",
            Self::Length => "st_length",
            Self::Area => "st_area",
            Self::Contains => "st_contains",
            Self::Within => "st_within",
            Self::Intersects => "st_intersects",
            Self::Disjoint => "st_disjoint",
            Self::MbrContains => "mbrcontains",
            Self::MbrWithin => "mbrwithin",
            Self::MbrIntersects => "mbrintersects",
            Self::MbrDisjoint => "mbrdisjoint",
            Self::MbrEquals => "mbrequals",
        }
    }
}
