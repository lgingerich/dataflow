use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Operator;
use thiserror::Error;

/// Errors that can occur during SQL query translation and execution
#[derive(Error, Debug)]
pub enum DataflowError {
    /// Column not found in schema
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// Unsupported SQL expression
    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(String),

    /// Type mismatch in binary operation
    #[error("Type mismatch in binary operation {op:?}: left is {left:?}, right is {right:?}. Use explicit CAST.")]
    TypeMismatch {
        op: Operator,
        left: DataType,
        right: DataType,
    },

    /// Failed to convert ScalarValue to Arrow array
    #[error("Failed to convert ScalarValue to array: {0}")]
    ScalarToArrayConversion(String),

    /// Failed to convert Arrow array result back to ScalarValue
    #[error("Failed to convert result array to ScalarValue: {0}")]
    ArrayToScalarConversion(String),

    /// Unsupported binary operation
    #[error("Unsupported operation: {0:?}")]
    UnsupportedOperation(Operator),

    /// Unsupported logical operation
    #[error("Unsupported logical operation: {0:?}")]
    UnsupportedLogicalOperation(Operator),

    /// Arrow compute kernel error
    #[error("Arrow compute error: {0}")]
    ArrowComputeError(String),

    /// Table not found in provided tables map
    #[error("Table '{0}' not found in provided tables")]
    TableNotFound(String),

    /// Unsupported logical plan operator
    #[error("Unsupported logical plan operator: {0}")]
    UnsupportedLogicalPlan(String),

    /// DataFusion error wrapper
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, DataflowError>;

