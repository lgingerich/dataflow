use datafusion::common::ScalarValue;
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
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<ScalarValue>,
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
        // so partial_cmp will always return Some(...). We use expect() as a safety check.
        self.values
            .iter()
            .zip(&other.values)
            .map(|(a, b)| {
                a.partial_cmp(b)
                    .expect("ScalarValue partial_cmp returned None - this should never happen")
            })
            .find(|&ord| ord != Ordering::Equal)
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
}
