use crate::sql::error::DataflowError;
use datafusion::common::ScalarValue;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Serialize, ser::SerializeSeq};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Index;
use std::sync::Arc;

/// Canonical row container used inside Timely/Differential operators.
///
/// DataFusion expresses scalar SQL values with the `ScalarValue` enum: every Arrow
/// logical type (ints, timestamps, binary, etc.) is represented as a tagged value
/// so the planner can move between scalar expressions and columnar Arrow arrays.
/// Differential Dataflow, however, expects each record in the dataflow graph to be
/// an owned Rust type that implements `Clone + Ord + Hash + Serialize + 'static`.
///
/// `Row` therefore wraps an immutable slice of `ScalarValue`s and supplies the
/// missing pieces:
/// - A stable ordering and hashing scheme (`ScalarKind`) so heterogeneous rows can
///   participate in Timely/Differential joins, reductions and hash maps.
/// - Serde support by projecting each `ScalarValue` through a serializable mirror,
///   which Arrow/DataFusion do not provide out of the box.
/// - Convenience accessors (`as_slice`, `iter`, indexing) for Arrow compute kernels
///   or bridge code that still needs to inspect individual scalars.
///
/// The result is a compact, Arc-backed row that preserves Arrow semantics while
/// being cheap to clone and safe to move across worker boundaries.

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Row {
    data: Arc<[ScalarValue]>,
}

impl Row {
    /// Create from owned values (moves and freezes the slice).
    pub fn new(values: Vec<ScalarValue>) -> Self {
        Self {
            data: values.into(),
        }
    }

    /// Borrowing constructor (clones the slice once).
    pub fn from_slice(values: &[ScalarValue]) -> Self {
        Self::new(values.to_vec())
    }

    /// From any iterator of ScalarValue.
    pub fn from_iter<I: IntoIterator<Item = ScalarValue>>(iter: I) -> Self {
        let v: Vec<_> = iter.into_iter().collect();
        Self::new(v)
    }

    /// Accessors
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn get(&self, i: usize) -> Option<&ScalarValue> {
        self.data.get(i)
    }
    pub fn as_slice(&self) -> &[ScalarValue] {
        &self.data
    }
    pub fn iter(&self) -> std::slice::Iter<'_, ScalarValue> {
        self.data.iter()
    }
}

impl Serialize for Row {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.data.len()))?;
        for v in self.data.iter() {
            let sv = SerializableValue::try_from(v).map_err(serde::ser::Error::custom)?;
            seq.serialize_element(&sv)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RowVisitor;

        impl<'de> Visitor<'de> for RowVisitor {
            type Value = Row;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a sequence of SerializableValue")
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Row, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut out: Vec<ScalarValue> = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(sv) = seq.next_element::<SerializableValue>()? {
                    out.push(ScalarValue::try_from(sv).map_err(serde::de::Error::custom)?);
                }
                Ok(Row { data: out.into() })
            }
        }

        deserializer.deserialize_seq(RowVisitor)
    }
}

impl From<Vec<ScalarValue>> for Row {
    fn from(v: Vec<ScalarValue>) -> Self {
        Row::new(v)
    }
}

impl From<Box<[ScalarValue]>> for Row {
    fn from(b: Box<[ScalarValue]>) -> Self {
        Row { data: Arc::from(b) }
    }
}

impl FromIterator<ScalarValue> for Row {
    fn from_iter<T: IntoIterator<Item = ScalarValue>>(iter: T) -> Self {
        let v: Vec<_> = iter.into_iter().collect();
        Row::from(v)
    }
}

impl Index<usize> for Row {
    type Output = ScalarValue;
    fn index(&self, i: usize) -> &Self::Output {
        &self.data[i]
    }
}

impl Hash for Row {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.len().hash(state);
        for value in self.data.iter() {
            hash_scalar(value, state);
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum ScalarKind {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal128,
    Utf8,
    Utf8View,
    LargeUtf8,
    Binary,
    BinaryView,
    FixedSizeBinary,
    LargeBinary,
    Date32,
    Date64,
    Time32Second,
    Time32Millisecond,
    Time64Microsecond,
    Time64Nanosecond,
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
    IntervalYearMonth,
    DurationSecond,
    DurationMillisecond,
    DurationMicrosecond,
    DurationNanosecond,
    Unsupported,
}

fn scalar_kind(value: &ScalarValue) -> ScalarKind {
    match value {
        ScalarValue::Null => ScalarKind::Null,
        ScalarValue::Boolean(_) => ScalarKind::Boolean,
        ScalarValue::Int8(_) => ScalarKind::Int8,
        ScalarValue::Int16(_) => ScalarKind::Int16,
        ScalarValue::Int32(_) => ScalarKind::Int32,
        ScalarValue::Int64(_) => ScalarKind::Int64,
        ScalarValue::UInt8(_) => ScalarKind::UInt8,
        ScalarValue::UInt16(_) => ScalarKind::UInt16,
        ScalarValue::UInt32(_) => ScalarKind::UInt32,
        ScalarValue::UInt64(_) => ScalarKind::UInt64,
        ScalarValue::Float32(_) => ScalarKind::Float32,
        ScalarValue::Float64(_) => ScalarKind::Float64,
        ScalarValue::Decimal128(_, _, _) => ScalarKind::Decimal128,
        ScalarValue::Utf8(_) => ScalarKind::Utf8,
        ScalarValue::Utf8View(_) => ScalarKind::Utf8View,
        ScalarValue::LargeUtf8(_) => ScalarKind::LargeUtf8,
        ScalarValue::Binary(_) => ScalarKind::Binary,
        ScalarValue::BinaryView(_) => ScalarKind::BinaryView,
        ScalarValue::FixedSizeBinary(_, _) => ScalarKind::FixedSizeBinary,
        ScalarValue::LargeBinary(_) => ScalarKind::LargeBinary,
        ScalarValue::Date32(_) => ScalarKind::Date32,
        ScalarValue::Date64(_) => ScalarKind::Date64,
        ScalarValue::Time32Second(_) => ScalarKind::Time32Second,
        ScalarValue::Time32Millisecond(_) => ScalarKind::Time32Millisecond,
        ScalarValue::Time64Microsecond(_) => ScalarKind::Time64Microsecond,
        ScalarValue::Time64Nanosecond(_) => ScalarKind::Time64Nanosecond,
        ScalarValue::TimestampSecond(_, _) => ScalarKind::TimestampSecond,
        ScalarValue::TimestampMillisecond(_, _) => ScalarKind::TimestampMillisecond,
        ScalarValue::TimestampMicrosecond(_, _) => ScalarKind::TimestampMicrosecond,
        ScalarValue::TimestampNanosecond(_, _) => ScalarKind::TimestampNanosecond,
        ScalarValue::IntervalYearMonth(_) => ScalarKind::IntervalYearMonth,
        ScalarValue::DurationSecond(_) => ScalarKind::DurationSecond,
        ScalarValue::DurationMillisecond(_) => ScalarKind::DurationMillisecond,
        ScalarValue::DurationMicrosecond(_) => ScalarKind::DurationMicrosecond,
        ScalarValue::DurationNanosecond(_) => ScalarKind::DurationNanosecond,
        _ => ScalarKind::Unsupported,
    }
}

fn hash_scalar<H: Hasher>(value: &ScalarValue, state: &mut H) {
    scalar_kind(value).hash(state);
    value.hash(state);
}

fn cmp_scalar(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    use std::cmp::Ordering::*;
    let ta = scalar_kind(a);
    let tb = scalar_kind(b);
    match ta.cmp(&tb) {
        Equal => {
            // same variant family: compare payloads deterministically
            use ScalarValue::*;
            match (a, b) {
                (Null, Null) => Equal,
                (Boolean(x), Boolean(y)) => x.cmp(y),
                (Int8(x), Int8(y)) => x.cmp(y),
                (Int16(x), Int16(y)) => x.cmp(y),
                (Int32(x), Int32(y)) => x.cmp(y),
                (Int64(x), Int64(y)) => x.cmp(y),
                (UInt8(x), UInt8(y)) => x.cmp(y),
                (UInt16(x), UInt16(y)) => x.cmp(y),
                (UInt32(x), UInt32(y)) => x.cmp(y),
                (UInt64(x), UInt64(y)) => x.cmp(y),
                (Float32(x), Float32(y)) => {
                    // Option<f32>: None < Some; use total_cmp for NaN/-0
                    match (x, y) {
                        (None, None) => Equal,
                        (None, Some(_)) => Less,
                        (Some(_), None) => Greater,
                        (Some(xf), Some(yf)) => xf.total_cmp(yf),
                    }
                }
                (Float64(x), Float64(y)) => match (x, y) {
                    (None, None) => Equal,
                    (None, Some(_)) => Less,
                    (Some(_), None) => Greater,
                    (Some(xf), Some(yf)) => xf.total_cmp(yf),
                },
                (Utf8(x), Utf8(y)) => x.cmp(y),
                (Utf8View(x), Utf8View(y)) => x.cmp(y),
                (LargeUtf8(x), LargeUtf8(y)) => x.cmp(y),
                (Binary(x), Binary(y)) => x.cmp(y),
                (BinaryView(x), BinaryView(y)) => x.cmp(y),
                (FixedSizeBinary(xs, xv), FixedSizeBinary(ys, yv)) => (xs, xv).cmp(&(ys, yv)),
                (LargeBinary(x), LargeBinary(y)) => x.cmp(y),
                (Date32(x), Date32(y)) => x.cmp(y),
                (Date64(x), Date64(y)) => x.cmp(y),
                (Time32Second(x), Time32Second(y)) => x.cmp(y),
                (Time32Millisecond(x), Time32Millisecond(y)) => x.cmp(y),
                (Time64Microsecond(x), Time64Microsecond(y)) => x.cmp(y),
                (Time64Nanosecond(x), Time64Nanosecond(y)) => x.cmp(y),
                (TimestampSecond(x, tzx), TimestampSecond(y, tzy)) => (x, tzx).cmp(&(y, tzy)),
                (TimestampMillisecond(x, tzx), TimestampMillisecond(y, tzy)) => {
                    (x, tzx).cmp(&(y, tzy))
                }
                (TimestampMicrosecond(x, tzx), TimestampMicrosecond(y, tzy)) => {
                    (x, tzx).cmp(&(y, tzy))
                }
                (TimestampNanosecond(x, tzx), TimestampNanosecond(y, tzy)) => {
                    (x, tzx).cmp(&(y, tzy))
                }
                (Decimal128(x, px, sx), Decimal128(y, py, sy)) => (px, sx, x).cmp(&(py, sy, y)),
                (IntervalYearMonth(x), IntervalYearMonth(y)) => x.cmp(y),
                (DurationSecond(x), DurationSecond(y)) => x.cmp(y),
                (DurationMillisecond(x), DurationMillisecond(y)) => x.cmp(y),
                (DurationMicrosecond(x), DurationMicrosecond(y)) => x.cmp(y),
                (DurationNanosecond(x), DurationNanosecond(y)) => x.cmp(y),
                _ => Equal, // different shapes inside same tag shouldn't happen
            }
        }
        other => other,
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        for (a, b) in self.data.iter().zip(other.data.iter()) {
            let ord = cmp_scalar(a, b);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        self.data.len().cmp(&other.data.len())
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Serializable representation of a ScalarValue
/// https://docs.rs/datafusion/latest/datafusion/scalar/enum.ScalarValue.html
#[derive(Debug, Clone, Serialize, Deserialize)]
enum SerializableValue {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    // Float16(Option<half::f16>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Decimal32(Option<i32>, u8, i8),
    Decimal64(Option<i64>, u8, i8),
    Decimal128(Option<i128>, u8, i8),
    // Decimal256(Option<arrow::datatypes::i256>, u8, i8),
    Utf8(Option<String>),
    Utf8View(Option<String>),
    LargeUtf8(Option<String>),
    Binary(Option<Vec<u8>>),
    BinaryView(Option<Vec<u8>>),
    FixedSizeBinary(i32, Option<Vec<u8>>),
    LargeBinary(Option<Vec<u8>>),
    // FixedSizeList(Arc<arrow::array::FixedSizeListArray>),
    // List(Arc<arrow::array::GenericListArray<i32>>),
    // LargeList(Arc<arrow::array::GenericListArray<i64>>),
    // Struct(Arc<arrow::array::StructArray>),
    // Map(Arc<arrow::array::MapArray>),
    Date32(Option<i32>),
    Date64(Option<i64>),
    Time32Second(Option<i32>),
    Time32Millisecond(Option<i32>),
    Time64Microsecond(Option<i64>),
    Time64Nanosecond(Option<i64>),
    TimestampSecond(Option<i64>, Option<Arc<str>>),
    TimestampMillisecond(Option<i64>, Option<Arc<str>>),
    TimestampMicrosecond(Option<i64>, Option<Arc<str>>),
    TimestampNanosecond(Option<i64>, Option<Arc<str>>),
    IntervalYearMonth(Option<i32>),
    // IntervalDayTime(Option<arrow::datatypes::IntervalDayTime>),
    // IntervalMonthDayNano(Option<arrow::datatypes::IntervalMonthDayNano>),
    DurationSecond(Option<i64>),
    DurationMillisecond(Option<i64>),
    DurationMicrosecond(Option<i64>),
    DurationNanosecond(Option<i64>),
    // Union(Option<(i8, Box<ScalarValue>)>, arrow::datatypes::UnionFields, arrow::datatypes::UnionMode),
    // Dictionary(Box<arrow::datatypes::DataType>, Box<ScalarValue>),
}

const DECIMAL32_MAX_PRECISION: u8 = 9;
const DECIMAL64_MAX_PRECISION: u8 = 18;

fn serialize_decimal(value: Option<i128>, precision: u8, scale: i8) -> SerializableValue {
    let fits_i32 = precision <= DECIMAL32_MAX_PRECISION;
    if fits_i32 {
        match value {
            Some(raw) => {
                if let Ok(narrow) = i32::try_from(raw) {
                    return SerializableValue::Decimal32(Some(narrow), precision, scale);
                }
            }
            None => return SerializableValue::Decimal32(None, precision, scale),
        }
    }

    let fits_i64 = precision <= DECIMAL64_MAX_PRECISION;
    if fits_i64 {
        match value {
            Some(raw) => {
                if let Ok(narrow) = i64::try_from(raw) {
                    return SerializableValue::Decimal64(Some(narrow), precision, scale);
                }
            }
            None => return SerializableValue::Decimal64(None, precision, scale),
        }
    }

    SerializableValue::Decimal128(value, precision, scale)
}

impl TryFrom<&ScalarValue> for SerializableValue {
    type Error = DataflowError;
    fn try_from(value: &ScalarValue) -> Result<Self, Self::Error> {
        use ScalarValue::*;
        Ok(match value {
            Null => Self::Null,
            Boolean(v) => Self::Boolean(*v),
            Int8(v) => Self::Int8(*v),
            Int16(v) => Self::Int16(*v),
            Int32(v) => Self::Int32(*v),
            Int64(v) => Self::Int64(*v),
            UInt8(v) => Self::UInt8(*v),
            UInt16(v) => Self::UInt16(*v),
            UInt32(v) => Self::UInt32(*v),
            UInt64(v) => Self::UInt64(*v),
            Float32(v) => Self::Float32(*v),
            Float64(v) => Self::Float64(*v),
            Decimal128(v, p, s) => serialize_decimal(*v, *p, *s),
            Utf8(v) => Self::Utf8(v.clone()),
            Utf8View(v) => Self::Utf8View(v.clone()),
            LargeUtf8(v) => Self::LargeUtf8(v.clone()),
            Binary(v) => Self::Binary(v.clone()),
            BinaryView(v) => Self::BinaryView(v.clone()),
            FixedSizeBinary(size, v) => Self::FixedSizeBinary(*size, v.clone()),
            LargeBinary(v) => Self::LargeBinary(v.clone()),
            Date32(v) => Self::Date32(*v),
            Date64(v) => Self::Date64(*v),
            Time32Second(v) => Self::Time32Second(*v),
            Time32Millisecond(v) => Self::Time32Millisecond(*v),
            Time64Microsecond(v) => Self::Time64Microsecond(*v),
            Time64Nanosecond(v) => Self::Time64Nanosecond(*v),
            TimestampSecond(v, tz) => Self::TimestampSecond(*v, tz.clone()),
            TimestampMillisecond(v, tz) => Self::TimestampMillisecond(*v, tz.clone()),
            TimestampMicrosecond(v, tz) => Self::TimestampMicrosecond(*v, tz.clone()),
            TimestampNanosecond(v, tz) => Self::TimestampNanosecond(*v, tz.clone()),
            IntervalYearMonth(v) => Self::IntervalYearMonth(*v),
            DurationSecond(v) => Self::DurationSecond(*v),
            DurationMillisecond(v) => Self::DurationMillisecond(*v),
            DurationMicrosecond(v) => Self::DurationMicrosecond(*v),
            DurationNanosecond(v) => Self::DurationNanosecond(*v),
            // Unsupported variants (commented out in SerializableValue)
            Float16(_)
            | Decimal256(_, _, _)
            | IntervalDayTime(_)
            | IntervalMonthDayNano(_)
            | FixedSizeList(_)
            | List(_)
            | LargeList(_)
            | Struct(_)
            | Map(_)
            | Union(_, _, _)
            | Dictionary(_, _) => {
                return Err(DataflowError::UnsupportedScalarType(format!(
                    "{value:?} cannot be serialized as Row"
                )));
            }
        })
    }
}

impl TryFrom<SerializableValue> for ScalarValue {
    type Error = DataflowError;
    fn try_from(v: SerializableValue) -> Result<Self, Self::Error> {
        use SerializableValue::*;
        Ok(match v {
            Null => ScalarValue::Null,
            Boolean(v) => ScalarValue::Boolean(v),
            Int8(v) => ScalarValue::Int8(v),
            Int16(v) => ScalarValue::Int16(v),
            Int32(v) => ScalarValue::Int32(v),
            Int64(v) => ScalarValue::Int64(v),
            UInt8(v) => ScalarValue::UInt8(v),
            UInt16(v) => ScalarValue::UInt16(v),
            UInt32(v) => ScalarValue::UInt32(v),
            UInt64(v) => ScalarValue::UInt64(v),
            Float32(v) => ScalarValue::Float32(v),
            Float64(v) => ScalarValue::Float64(v),
            Decimal32(v, p, s) => ScalarValue::Decimal128(v.map(|x| x as i128), p, s),
            Decimal64(v, p, s) => ScalarValue::Decimal128(v.map(|x| x as i128), p, s),
            Decimal128(v, p, s) => ScalarValue::Decimal128(v, p, s),
            Utf8(v) => ScalarValue::Utf8(v),
            Utf8View(v) => ScalarValue::Utf8View(v),
            LargeUtf8(v) => ScalarValue::LargeUtf8(v),
            Binary(v) => ScalarValue::Binary(v),
            BinaryView(v) => ScalarValue::BinaryView(v),
            FixedSizeBinary(size, v) => ScalarValue::FixedSizeBinary(size, v),
            LargeBinary(v) => ScalarValue::LargeBinary(v),
            Date32(v) => ScalarValue::Date32(v),
            Date64(v) => ScalarValue::Date64(v),
            Time32Second(v) => ScalarValue::Time32Second(v),
            Time32Millisecond(v) => ScalarValue::Time32Millisecond(v),
            Time64Microsecond(v) => ScalarValue::Time64Microsecond(v),
            Time64Nanosecond(v) => ScalarValue::Time64Nanosecond(v),
            TimestampSecond(v, tz) => ScalarValue::TimestampSecond(v, tz),
            TimestampMillisecond(v, tz) => ScalarValue::TimestampMillisecond(v, tz),
            TimestampMicrosecond(v, tz) => ScalarValue::TimestampMicrosecond(v, tz),
            TimestampNanosecond(v, tz) => ScalarValue::TimestampNanosecond(v, tz),
            IntervalYearMonth(v) => ScalarValue::IntervalYearMonth(v),
            DurationSecond(v) => ScalarValue::DurationSecond(v),
            DurationMillisecond(v) => ScalarValue::DurationMillisecond(v),
            DurationMicrosecond(v) => ScalarValue::DurationMicrosecond(v),
            DurationNanosecond(v) => ScalarValue::DurationNanosecond(v),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{
        collection::vec as prop_vec,
        option::of as prop_option_of,
        prelude::*,
        strategy::{BoxedStrategy, Strategy},
    };
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    const MAX_TEXT_LEN: usize = 8;
    const MAX_LARGE_TEXT_LEN: usize = 16;
    const MAX_BINARY_LEN: usize = 16;
    const MAX_ROW_WIDTH: usize = 6;
    const DEFAULT_DECIMAL_PRECISION: u8 = 12;
    const DEFAULT_DECIMAL_SCALE: i8 = 3;

    fn hash_row(row: &Row) -> u64 {
        let mut hasher = DefaultHasher::new();
        row.hash(&mut hasher);
        hasher.finish()
    }

    fn option_string_strategy(max_len: usize) -> BoxedStrategy<Option<String>> {
        prop_option_of(
            prop_vec(any::<char>(), 0..=max_len)
                .prop_map(|chars| chars.into_iter().collect::<String>()),
        )
        .boxed()
    }

    fn option_binary_strategy(max_len: usize) -> BoxedStrategy<Option<Vec<u8>>> {
        prop_option_of(prop_vec(any::<u8>(), 0..=max_len)).boxed()
    }

    fn scalar_value_strategy() -> BoxedStrategy<ScalarValue> {
        prop_oneof![
            Just(ScalarValue::Null),
            prop_option_of(any::<bool>()).prop_map(ScalarValue::Boolean),
            prop_option_of(any::<i8>()).prop_map(ScalarValue::Int8),
            prop_option_of(any::<i16>()).prop_map(ScalarValue::Int16),
            prop_option_of(any::<i32>()).prop_map(ScalarValue::Int32),
            prop_option_of(any::<i64>()).prop_map(ScalarValue::Int64),
            prop_option_of(any::<u8>()).prop_map(ScalarValue::UInt8),
            prop_option_of(any::<u16>()).prop_map(ScalarValue::UInt16),
            prop_option_of(any::<u32>()).prop_map(ScalarValue::UInt32),
            prop_option_of(any::<u64>()).prop_map(ScalarValue::UInt64),
            prop_option_of(any::<f32>()).prop_map(ScalarValue::Float32),
            prop_option_of(any::<f64>()).prop_map(ScalarValue::Float64),
            prop_option_of(any::<i128>()).prop_map(|v| ScalarValue::Decimal128(
                v,
                DEFAULT_DECIMAL_PRECISION,
                DEFAULT_DECIMAL_SCALE
            )),
            option_string_strategy(MAX_TEXT_LEN).prop_map(ScalarValue::Utf8),
            option_string_strategy(MAX_LARGE_TEXT_LEN).prop_map(ScalarValue::LargeUtf8),
            option_binary_strategy(MAX_BINARY_LEN).prop_map(ScalarValue::Binary),
            option_binary_strategy(MAX_BINARY_LEN * 2).prop_map(ScalarValue::LargeBinary),
            prop_option_of(any::<i32>()).prop_map(ScalarValue::Date32),
            prop_option_of(any::<i64>()).prop_map(ScalarValue::Date64),
            prop_option_of(any::<i32>()).prop_map(ScalarValue::Time32Second),
            prop_option_of(any::<i32>()).prop_map(ScalarValue::Time32Millisecond),
            prop_option_of(any::<i64>()).prop_map(ScalarValue::Time64Microsecond),
            prop_option_of(any::<i64>()).prop_map(ScalarValue::Time64Nanosecond),
            prop_option_of(any::<i64>()).prop_map(|ts| ScalarValue::TimestampSecond(ts, None)),
            prop_option_of(any::<i64>()).prop_map(|ts| ScalarValue::TimestampMillisecond(ts, None)),
            prop_option_of(any::<i64>()).prop_map(|ts| ScalarValue::TimestampMicrosecond(ts, None)),
            prop_option_of(any::<i64>()).prop_map(|ts| ScalarValue::TimestampNanosecond(ts, None)),
            prop_option_of(any::<i32>()).prop_map(ScalarValue::IntervalYearMonth),
            prop_option_of(any::<i64>()).prop_map(ScalarValue::DurationSecond),
            prop_option_of(any::<i64>()).prop_map(ScalarValue::DurationMillisecond),
            prop_option_of(any::<i64>()).prop_map(ScalarValue::DurationMicrosecond),
            prop_option_of(any::<i64>()).prop_map(ScalarValue::DurationNanosecond),
        ]
        .boxed()
    }

    fn row_strategy() -> BoxedStrategy<Row> {
        prop_vec(scalar_value_strategy(), 0..=MAX_ROW_WIDTH)
            .prop_map(Row::new)
            .boxed()
    }

    #[test]
    fn test_row_creation() {
        let row = Row::new(vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Utf8(Some("hello".to_string())),
        ]);
        assert_eq!(row.len(), 2);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_row_equality() {
        let row1 = Row::new(vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Utf8(Some("hello".to_string())),
        ]);
        let row2 = Row::new(vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Utf8(Some("hello".to_string())),
        ]);
        let row3 = Row::new(vec![
            ScalarValue::Int64(Some(43)),
            ScalarValue::Utf8(Some("hello".to_string())),
        ]);
        let null_row1 = Row::new(vec![
            ScalarValue::Int64(None),
            ScalarValue::Utf8(Some("test".to_string())),
        ]);
        let null_row2 = Row::new(vec![
            ScalarValue::Int64(None),
            ScalarValue::Utf8(Some("test".to_string())),
        ]);

        assert_eq!(row1, row2);
        assert_ne!(row1, row3);
        assert_eq!(null_row1, null_row2);
    }

    #[test]
    fn test_row_ordering() {
        let row1 = Row::new(vec![ScalarValue::Int64(Some(1))]);
        let row2 = Row::new(vec![ScalarValue::Int64(Some(2))]);
        let row3 = Row::new(vec![ScalarValue::Int64(Some(1))]);
        let prefix = Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
        ]);
        let bool_row = Row::new(vec![ScalarValue::Boolean(Some(false))]);
        let int_row = Row::new(vec![ScalarValue::Int8(Some(0))]);
        let string_row = Row::new(vec![ScalarValue::Utf8(Some("a".to_string()))]);

        assert!(row1 < row2);
        assert!(row2 > row1);
        assert_eq!(row1.cmp(&row3), Ordering::Equal);
        assert!(row1 < prefix);
        assert!(bool_row < int_row);
        assert!(int_row < string_row);
    }

    #[test]
    fn test_row_with_nan() {
        // DataFusion's ScalarValue provides a total ordering for floats
        // NaN is treated as greater than all other values
        let nan_row = Row::new(vec![ScalarValue::Float64(Some(f64::NAN))]);
        let normal_row = Row::new(vec![ScalarValue::Float64(Some(1.0))]);

        // This should not panic - NaN has a defined ordering in ScalarValue
        assert!(nan_row > normal_row);
    }

    #[test]
    fn test_row_serialization() {
        let row = Row::new(vec![
            ScalarValue::Int64(Some(42)),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Float64(Some(3.14)),
        ]);

        // Serialize to JSON
        let serialized = serde_json::to_string(&row).unwrap();

        // Deserialize back
        let deserialized: Row = serde_json::from_str(&serialized).unwrap();

        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_row_serialization_binary_variants() {
        let row = Row::new(vec![
            ScalarValue::BinaryView(Some(vec![1, 2, 3])),
            ScalarValue::FixedSizeBinary(2, Some(vec![9, 9])),
            ScalarValue::LargeBinary(Some(vec![4, 5, 6])),
            ScalarValue::Time64Nanosecond(Some(42)),
            ScalarValue::DurationNanosecond(Some(7)),
        ]);

        let serialized = serde_json::to_string(&row).unwrap();
        let deserialized: Row = serde_json::from_str(&serialized).unwrap();

        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_row_extended_ordering() {
        let row1 = Row::new(vec![
            ScalarValue::BinaryView(Some(vec![0])),
            ScalarValue::FixedSizeBinary(2, Some(vec![0, 1])),
            ScalarValue::LargeBinary(Some(vec![7])),
            ScalarValue::Time32Millisecond(Some(1)),
            ScalarValue::IntervalYearMonth(Some(1)),
            ScalarValue::DurationMicrosecond(Some(10)),
        ]);
        let row2 = Row::new(vec![
            ScalarValue::BinaryView(Some(vec![1])),
            ScalarValue::FixedSizeBinary(2, Some(vec![0, 2])),
            ScalarValue::LargeBinary(Some(vec![7])),
            ScalarValue::Time32Millisecond(Some(2)),
            ScalarValue::IntervalYearMonth(Some(1)),
            ScalarValue::DurationMicrosecond(Some(20)),
        ]);

        assert!(row1 < row2);
    }

    #[test]
    fn test_row_hash_determinism() {
        let row = Row::new(vec![
            ScalarValue::Int32(Some(7)),
            ScalarValue::Utf8(Some("hash".to_string())),
            ScalarValue::DurationSecond(Some(9)),
        ]);
        let same = Row::new(vec![
            ScalarValue::Int32(Some(7)),
            ScalarValue::Utf8(Some("hash".to_string())),
            ScalarValue::DurationSecond(Some(9)),
        ]);
        let different = Row::new(vec![
            ScalarValue::Int64(Some(7)),
            ScalarValue::Utf8(Some("hash".to_string())),
            ScalarValue::DurationSecond(Some(9)),
        ]);

        assert_eq!(hash_row(&row), hash_row(&same));
        assert_ne!(hash_row(&row), hash_row(&different));
    }

    proptest! {
        #[test]
        fn equal_rows_share_hash_and_cmp(a in row_strategy(), b in row_strategy()) {
            if a == b {
                prop_assert_eq!(hash_row(&a), hash_row(&b));
                prop_assert_eq!(Ordering::Equal, a.cmp(&b));
            }
        }
    }

    proptest! {
        #[test]
        fn cmp_is_total_antisymmetric_and_transitive(
            a in row_strategy(),
            b in row_strategy(),
            c in row_strategy(),
        ) {
            let ab = a.cmp(&b);
            let ba = b.cmp(&a);
            let bc = b.cmp(&c);
            let ac = a.cmp(&c);

            prop_assert_eq!(ab, ba.reverse());

            if ab != Ordering::Greater && bc != Ordering::Greater {
                prop_assert!(ac != Ordering::Greater);
            }
            if ab != Ordering::Less && bc != Ordering::Less {
                prop_assert!(ac != Ordering::Less);
            }

            prop_assert!(ab != Ordering::Greater || ba != Ordering::Greater);
            prop_assert!(ab != Ordering::Less || ba != Ordering::Less);
        }
    }

    proptest! {
        #[test]
        fn cross_type_ordering_matches_scalar_kind(
            a in scalar_value_strategy(),
            b in scalar_value_strategy(),
        ) {
            let kind_cmp = scalar_kind(&a).cmp(&scalar_kind(&b));
            if kind_cmp != Ordering::Equal {
                let row_a = Row::new(vec![a.clone()]);
                let row_b = Row::new(vec![b.clone()]);
                prop_assert_eq!(kind_cmp, row_a.cmp(&row_b));
            }
        }
    }

    #[test]
    fn test_decimal_downcast_serialization() {
        let small_decimal = ScalarValue::Decimal128(Some(12_345), 5, 2);
        match SerializableValue::try_from(&small_decimal).unwrap() {
            SerializableValue::Decimal32(Some(v), p, s) => {
                assert_eq!(v, 12_345);
                assert_eq!(p, 5);
                assert_eq!(s, 2);
            }
            other => panic!("expected Decimal32, got {:?}", other),
        }

        let medium_decimal = ScalarValue::Decimal128(Some(123_456_789_012_345_678), 18, 4);
        match SerializableValue::try_from(&medium_decimal).unwrap() {
            SerializableValue::Decimal64(Some(v), p, s) => {
                assert_eq!(v, 123_456_789_012_345_678i64);
                assert_eq!(p, 18);
                assert_eq!(s, 4);
            }
            other => panic!("expected Decimal64, got {:?}", other),
        }

        let large_decimal = ScalarValue::Decimal128(Some(1_000_000_000_000_000_000_000), 22, 6);
        match SerializableValue::try_from(&large_decimal).unwrap() {
            SerializableValue::Decimal128(Some(v), p, s) => {
                assert_eq!(v, 1_000_000_000_000_000_000_000);
                assert_eq!(p, 22);
                assert_eq!(s, 6);
            }
            other => panic!("expected Decimal128, got {:?}", other),
        }

        let none_decimal = ScalarValue::Decimal128(None, 8, 2);
        match SerializableValue::try_from(&none_decimal).unwrap() {
            SerializableValue::Decimal32(None, p, s) => {
                assert_eq!(p, 8);
                assert_eq!(s, 2);
            }
            other => panic!("expected Decimal32 None, got {:?}", other),
        }
    }

    #[test]
    fn test_serialization_rejects_unsupported_variants() {
        use datafusion::arrow::datatypes::i256;

        let unsupported = ScalarValue::Decimal256(Some(i256::from_i128(1)), 38, 10);
        let err = SerializableValue::try_from(&unsupported).unwrap_err();

        match err {
            DataflowError::UnsupportedScalarType(msg) => {
                assert!(msg.contains("cannot be serialized"));
            }
            other => panic!("expected UnsupportedScalarType, got {other:?}"),
        }
    }
}
