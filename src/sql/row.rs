use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// A generic row representation for Differential Dataflow
///
/// This struct wraps a vector of DataFusion `ScalarValue`s to provide
/// the necessary traits for Timely/Differential Dataflow.
///
/// The Row type acts as the universal data carrier flowing through the Timely graph,
/// allowing us to handle SQL queries with dynamic schemas at runtime while satisfying
/// Rust's static type requirements.
///
/// ## Trait Requirements
///
/// Differential Dataflow requires: `Clone + 'static + Ord + Debug`
/// - `Clone`: Automatically derived
/// - `'static`: Satisfied (no non-static references)
/// - `Ord`: Manually implemented (lexicographical comparison)
/// - `Debug`: Automatically derived
/// - `Send + Sync`: Required for thread safety (ScalarValue implements these)
/// - `Serialize + Deserialize`: Manually implemented to support ExchangeData
///
/// Note: ScalarValue from DataFusion doesn't implement Serialize/Deserialize,
/// so we manually convert to/from a serializable intermediate format.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<ScalarValue>,
}

/// Serializable representation of a ScalarValue
/// This is a simplified enum that can be serialized with serde
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
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<String>),
    LargeUtf8(Option<String>),
    Binary(Option<Vec<u8>>),
    // Add more variants as needed for your use cases
}

impl From<&ScalarValue> for SerializableValue {
    fn from(value: &ScalarValue) -> Self {
        match value {
            ScalarValue::Null => SerializableValue::Null,
            ScalarValue::Boolean(v) => SerializableValue::Boolean(*v),
            ScalarValue::Int8(v) => SerializableValue::Int8(*v),
            ScalarValue::Int16(v) => SerializableValue::Int16(*v),
            ScalarValue::Int32(v) => SerializableValue::Int32(*v),
            ScalarValue::Int64(v) => SerializableValue::Int64(*v),
            ScalarValue::UInt8(v) => SerializableValue::UInt8(*v),
            ScalarValue::UInt16(v) => SerializableValue::UInt16(*v),
            ScalarValue::UInt32(v) => SerializableValue::UInt32(*v),
            ScalarValue::UInt64(v) => SerializableValue::UInt64(*v),
            ScalarValue::Float32(v) => SerializableValue::Float32(*v),
            ScalarValue::Float64(v) => SerializableValue::Float64(*v),
            ScalarValue::Utf8(v) => SerializableValue::Utf8(v.clone()),
            ScalarValue::LargeUtf8(v) => SerializableValue::LargeUtf8(v.clone()),
            ScalarValue::Binary(v) => SerializableValue::Binary(v.clone()),
            // For unsupported types, convert to string representation
            _ => SerializableValue::Utf8(Some(format!("{:?}", value))),
        }
    }
}

impl From<SerializableValue> for ScalarValue {
    fn from(value: SerializableValue) -> Self {
        match value {
            SerializableValue::Null => ScalarValue::Null,
            SerializableValue::Boolean(v) => ScalarValue::Boolean(v),
            SerializableValue::Int8(v) => ScalarValue::Int8(v),
            SerializableValue::Int16(v) => ScalarValue::Int16(v),
            SerializableValue::Int32(v) => ScalarValue::Int32(v),
            SerializableValue::Int64(v) => ScalarValue::Int64(v),
            SerializableValue::UInt8(v) => ScalarValue::UInt8(v),
            SerializableValue::UInt16(v) => ScalarValue::UInt16(v),
            SerializableValue::UInt32(v) => ScalarValue::UInt32(v),
            SerializableValue::UInt64(v) => ScalarValue::UInt64(v),
            SerializableValue::Float32(v) => ScalarValue::Float32(v),
            SerializableValue::Float64(v) => ScalarValue::Float64(v),
            SerializableValue::Utf8(v) => ScalarValue::Utf8(v),
            SerializableValue::LargeUtf8(v) => ScalarValue::LargeUtf8(v),
            SerializableValue::Binary(v) => ScalarValue::Binary(v),
        }
    }
}

impl Serialize for Row {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serializable: Vec<SerializableValue> =
            self.values.iter().map(|v| v.into()).collect();
        serializable.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serializable: Vec<SerializableValue> = Vec::deserialize(deserializer)?;
        let values: Vec<ScalarValue> = serializable.into_iter().map(|v| v.into()).collect();
        Ok(Row { values })
    }
}

impl Row {
    /// Create a new Row from a vector of ScalarValues
    pub fn new(values: Vec<ScalarValue>) -> Self {
        Self { values }
    }

    /// Create an empty Row
    pub fn empty() -> Self {
        Self { values: Vec::new() }
    }

    /// Get a value at a specific index
    pub fn get(&self, index: usize) -> Option<&ScalarValue> {
        self.values.get(index)
    }

    /// Get the number of columns in this row
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Eq for Row {}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Delegate to Ord's cmp since we have a total ordering
        Some(self.cmp(other))
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        // Lexicographical comparison of the values vector
        // Note: ScalarValue provides a total ordering even for floats (NaN is treated as greater than all values)
        // so partial_cmp will always return Some(...).
        self.values
            .iter()
            .zip(&other.values)
            .find_map(|(a, b)| {
                match a.partial_cmp(b) {
                    Some(Ordering::Equal) => None, // Continue searching
                    Some(ord) => Some(ord), // Found non-equal ordering
                    None => Some(Ordering::Equal), // Treat None as equal (should never happen)
                }
            })
            .unwrap_or_else(|| self.values.len().cmp(&other.values.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        assert_eq!(row1, row2);
        assert_ne!(row1, row3);
    }

    #[test]
    fn test_row_ordering() {
        let row1 = Row::new(vec![ScalarValue::Int64(Some(1))]);
        let row2 = Row::new(vec![ScalarValue::Int64(Some(2))]);
        let row3 = Row::new(vec![ScalarValue::Int64(Some(1))]);

        assert!(row1 < row2);
        assert!(row2 > row1);
        assert_eq!(row1.cmp(&row3), Ordering::Equal);
    }

    #[test]
    fn test_row_with_nulls() {
        let row1 = Row::new(vec![
            ScalarValue::Int64(None),
            ScalarValue::Utf8(Some("test".to_string())),
        ]);
        let row2 = Row::new(vec![
            ScalarValue::Int64(None),
            ScalarValue::Utf8(Some("test".to_string())),
        ]);

        assert_eq!(row1, row2);
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
}
