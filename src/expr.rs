use crate::{error::DataflowError, row::Row};
use datafusion::arrow::array::Scalar;
use datafusion::arrow::compute::kernels::{cmp, numeric};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

/// Compile a DataFusion expression into a closure that operates on Row
///
/// This is the "interpreter" that converts DataFusion's AST into executable code.
/// The closure can be called repeatedly on different rows to evaluate the expression.
///
/// Returns a closure that returns Result to allow proper error propagation during evaluation.
pub fn compile_expr(
    expr: &Expr,
    schema: &DFSchema,
) -> Result<Box<dyn Fn(&Row) -> Result<ScalarValue, DataflowError>>, DataflowError> {
    match expr {
        // Column reference: look up by index in schema
        Expr::Column(col) => compile_column(col, schema),

        // Literal: return constant value
        Expr::Literal(scalar, _metadata) => Ok(compile_literal(scalar)),

        // Binary operation: recursively compile and combine
        Expr::BinaryExpr(bin) => compile_binary(bin, schema),

        // Alias: just compile the inner expression
        Expr::Alias(alias) => compile_expr(&alias.expr, schema),

        _ => Err(DataflowError::UnsupportedExpression(format!("{:?}", expr))),
    }
}

/// Compile a column reference
fn compile_column(
    col: &Column,
    schema: &DFSchema,
) -> Result<Box<dyn Fn(&Row) -> Result<ScalarValue, DataflowError>>, DataflowError> {
    let idx = schema
        .index_of_column(col)
        .map_err(|e| DataflowError::ColumnNotFound(format!("{:?}", e)))?;

    Ok(Box::new(move |row| {
        row.get(idx).cloned().ok_or_else(|| {
            DataflowError::ColumnNotFound(format!(
                "Row index {} out of bounds (row has {} columns)",
                idx,
                row.len()
            ))
        })
    }))
}

/// Compile a literal value
fn compile_literal(
    scalar: &ScalarValue,
) -> Box<dyn Fn(&Row) -> Result<ScalarValue, DataflowError>> {
    let val = scalar.clone();
    Box::new(move |_row| Ok(val.clone()))
}

/// Compile a binary expression (arithmetic or comparison)
fn compile_binary(
    bin: &BinaryExpr,
    schema: &DFSchema,
) -> Result<Box<dyn Fn(&Row) -> Result<ScalarValue, DataflowError>>, DataflowError> {
    let left_fn = compile_expr(&bin.left, schema)?;
    let right_fn = compile_expr(&bin.right, schema)?;
    let op = bin.op;

    Ok(Box::new(move |row| {
        let l = left_fn(row)?;
        let r = right_fn(row)?;
        apply_binary_op(&op, l, r)
    }))
}

/// Apply a binary operation to two ScalarValues using Arrow's compute kernels
fn apply_binary_op(
    op: &Operator,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, DataflowError> {
    // Convert ScalarValues to Arrow arrays
    let left_array = left
        .to_array()
        .map_err(|e| DataflowError::ScalarToArrayConversion(format!("{:?}", e)))?;
    let right_array = right
        .to_array()
        .map_err(|e| DataflowError::ScalarToArrayConversion(format!("{:?}", e)))?;

    // Require exact type match (i.e. no automatic coercion)
    // TODO: Implement automatic type coercion
    if left_array.data_type() != right_array.data_type() {
        return Err(DataflowError::TypeMismatch {
            op: *op,
            left: left_array.data_type().clone(),
            right: right_array.data_type().clone(),
        });
    }

    let left_scalar = Scalar::new(left_array);
    let right_scalar = Scalar::new(right_array);

    // Apply the operation using Arrow's compute kernels
    let result_array = match op {
        // Arithmetic operations
        Operator::Plus => numeric::add(&left_scalar, &right_scalar),
        Operator::Minus => numeric::sub(&left_scalar, &right_scalar),
        Operator::Multiply => numeric::mul(&left_scalar, &right_scalar),
        Operator::Divide => numeric::div(&left_scalar, &right_scalar),
        Operator::Modulo => numeric::rem(&left_scalar, &right_scalar),

        // Comparison operations (return BooleanArray)
        Operator::Eq => cmp::eq(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _),
        Operator::NotEq => {
            cmp::neq(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _)
        }
        Operator::Lt => cmp::lt(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _),
        Operator::LtEq => {
            cmp::lt_eq(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _)
        }
        Operator::Gt => cmp::gt(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _),
        Operator::GtEq => {
            cmp::gt_eq(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _)
        }

        // Logical operations (handle separately as they work on booleans)
        Operator::And | Operator::Or => {
            return apply_logical_op(op, left, right);
        }

        // Unsupported operations
        _ => return Err(DataflowError::UnsupportedOperation(*op)),
    };

    // Convert result back to ScalarValue
    let arr = result_array.map_err(|e| DataflowError::ArrowComputeError(format!("{:?}", e)))?;
    ScalarValue::try_from_array(&arr, 0).map_err(|e| {
        DataflowError::ArrayToScalarConversion(format!(
            "Failed to convert result array to ScalarValue at index 0: {:?}",
            e
        ))
    })
}

/// Handle logical operations (AND, OR) separately
///
/// These operate on booleans and don't go through Arrow's numeric kernels
fn apply_logical_op(
    op: &Operator,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, DataflowError> {
    match (op, left, right) {
        (Operator::And, ScalarValue::Boolean(Some(a)), ScalarValue::Boolean(Some(b))) => {
            Ok(ScalarValue::Boolean(Some(a && b)))
        }
        (Operator::Or, ScalarValue::Boolean(Some(a)), ScalarValue::Boolean(Some(b))) => {
            Ok(ScalarValue::Boolean(Some(a || b)))
        }
        _ => Err(DataflowError::UnsupportedLogicalOperation(*op)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> DFSchema {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]);
        DFSchema::try_from(schema).unwrap()
    }

    #[test]
    fn test_compile_literal() {
        let schema = test_schema();
        let expr = Expr::Literal(ScalarValue::Int64(Some(42)), None);
        let compiled = compile_expr(&expr, &schema).unwrap();

        let row = Row::new(vec![]);
        let result = compiled(&row).unwrap();

        assert_eq!(result, ScalarValue::Int64(Some(42)));
    }

    #[test]
    fn test_compile_column() {
        let schema = test_schema();
        let expr = Expr::Column(Column::from_name("amount"));
        let compiled = compile_expr(&expr, &schema).unwrap();

        let row = Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
        ]);
        let result = compiled(&row).unwrap();

        assert_eq!(result, ScalarValue::Int64(Some(100)));
    }

    #[test]
    fn test_binary_add() {
        let schema = test_schema();
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("amount"))),
            Operator::Plus,
            Box::new(Expr::Literal(ScalarValue::Int64(Some(10)), None)),
        ));
        let compiled = compile_expr(&expr, &schema).unwrap();

        let row = Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
        ]);
        let result = compiled(&row).unwrap();

        assert_eq!(result, ScalarValue::Int64(Some(110)));
    }

    #[test]
    fn test_comparison() {
        let schema = test_schema();
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("amount"))),
            Operator::Gt,
            Box::new(Expr::Literal(ScalarValue::Int64(Some(50)), None)),
        ));
        let compiled = compile_expr(&expr, &schema).unwrap();

        let row = Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
        ]);
        let result = compiled(&row).unwrap();

        assert_eq!(result, ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn test_type_mismatch_error() {
        let schema = test_schema();
        // Try to add Int64 and Float64 - should error due to type mismatch
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("amount"))),
            Operator::Plus,
            Box::new(Expr::Literal(ScalarValue::Float64(Some(10.5)), None)),
        ));
        let compiled = compile_expr(&expr, &schema).unwrap();

        let row = Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
        ]);

        // Should return an error, not NULL
        let result = compiled(&row);
        assert!(result.is_err());
        assert!(matches!(result, Err(DataflowError::TypeMismatch { .. })));
    }

    #[test]
    fn test_column_out_of_bounds_error() {
        let schema = test_schema();
        let expr = Expr::Column(Column::from_name("amount"));
        let compiled = compile_expr(&expr, &schema).unwrap();

        // Row has only 1 column, but we're trying to access column index 1 (amount)
        let row = Row::new(vec![ScalarValue::Int64(Some(1))]);

        // Should return an error, not NULL
        let result = compiled(&row);
        assert!(result.is_err());
        assert!(matches!(result, Err(DataflowError::ColumnNotFound(_))));
    }
}
