use datafusion::common::{ScalarValue, DFSchema, Column};
use datafusion::logical_expr::{Expr, Operator, BinaryExpr};
use datafusion::arrow::array::Scalar;
use datafusion::arrow::compute::kernels::{numeric, cmp};
use crate::sql::Row;

/// Compile a DataFusion expression into a closure that operates on Row
/// 
/// This is the "interpreter" that converts DataFusion's AST into executable code.
/// The closure can be called repeatedly on different rows to evaluate the expression.
pub fn compile_expr(
    expr: &Expr,
    schema: &DFSchema,
) -> Result<Box<dyn Fn(&Row) -> ScalarValue>, String> {
    match expr {
        // Column reference: look up by index in schema
        Expr::Column(col) => compile_column(col, schema),
        
        // Literal: return constant value
        Expr::Literal(scalar, _metadata) => Ok(compile_literal(scalar)),
        
        // Binary operation: recursively compile and combine
        Expr::BinaryExpr(bin) => compile_binary(bin, schema),
        
        // Alias: just compile the inner expression
        Expr::Alias(alias) => compile_expr(&alias.expr, schema),
        
        _ => Err(format!("Unsupported expression: {:?}", expr)),
    }
}

/// Compile a column reference
fn compile_column(col: &Column, schema: &DFSchema) -> Result<Box<dyn Fn(&Row) -> ScalarValue>, String> {
    let idx = schema.index_of_column(col)
        .map_err(|e| format!("Column not found: {:?}", e))?;
    
    Ok(Box::new(move |row| {
        row.get(idx).cloned().unwrap_or(ScalarValue::Null)
    }))
}

/// Compile a literal value
fn compile_literal(scalar: &ScalarValue) -> Box<dyn Fn(&Row) -> ScalarValue> {
    let val = scalar.clone();
    Box::new(move |_row| val.clone())
}

/// Compile a binary expression (arithmetic or comparison)
fn compile_binary(
    bin: &BinaryExpr,
    schema: &DFSchema,
) -> Result<Box<dyn Fn(&Row) -> ScalarValue>, String> {
    let left_fn = compile_expr(&bin.left, schema)?;
    let right_fn = compile_expr(&bin.right, schema)?;
    let op = bin.op.clone();
    
    Ok(Box::new(move |row| {
        let l = left_fn(row);
        let r = right_fn(row);
        apply_binary_op(&op, l, r)
    }))
}

/// Apply a binary operation to two ScalarValues using Arrow's compute kernels
fn apply_binary_op(op: &Operator, left: ScalarValue, right: ScalarValue) -> ScalarValue {
    // Convert ScalarValues to Arrow arrays
    let left_array = match left.to_array() {
        Ok(arr) => arr,
        Err(e) => panic!("Failed to convert ScalarValue to array: {:?}", e),
    };
    let right_array = match right.to_array() {
        Ok(arr) => arr,
        Err(e) => panic!("Failed to convert ScalarValue to array: {:?}", e),
    };
    
    // Type checking: require exact type match (no automatic coercion)
    // 
    // TODO: Future optimization - use DataFusion's type coercion logic:
    // `datafusion::optimizer::type_coercion` provides SQL-compliant type coercion rules.
    // This would allow queries like `SELECT amount + 10` where amount is Float64 and 10 is Int64.
    // For now, we require explicit CAST in SQL: `SELECT amount + CAST(10 AS DOUBLE)`
    if left_array.data_type() != right_array.data_type() {
        panic!(
            "Type mismatch in binary operation {:?}: left is {:?}, right is {:?}. Use explicit CAST.",
            op,
            left_array.data_type(),
            right_array.data_type()
        );
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
        Operator::NotEq => cmp::neq(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _),
        Operator::Lt => cmp::lt(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _),
        Operator::LtEq => cmp::lt_eq(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _),
        Operator::Gt => cmp::gt(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _),
        Operator::GtEq => cmp::gt_eq(&left_scalar, &right_scalar).map(|b| std::sync::Arc::new(b) as _),
        
        // Logical operations (handle separately as they work on booleans)
        Operator::And | Operator::Or => {
            return apply_logical_op(op, left, right);
        }
        
        // Unsupported operations
        _ => panic!("Unsupported operation: {:?}", op),
    };
    
    // Convert result back to ScalarValue
    match result_array {
        Ok(arr) => ScalarValue::try_from_array(&arr, 0).unwrap_or(ScalarValue::Null),
        Err(e) => panic!("Failed to convert result array to ScalarValue: {:?}", e),
    }
}

/// Handle logical operations (AND, OR) separately
/// 
/// These operate on booleans and don't go through Arrow's numeric kernels
fn apply_logical_op(op: &Operator, left: ScalarValue, right: ScalarValue) -> ScalarValue {
    match (op, left, right) {
        (Operator::And, ScalarValue::Boolean(Some(a)), ScalarValue::Boolean(Some(b))) => {
            ScalarValue::Boolean(Some(a && b))
        }
        (Operator::Or, ScalarValue::Boolean(Some(a)), ScalarValue::Boolean(Some(b))) => {
            ScalarValue::Boolean(Some(a || b))
        }
        _ => panic!("Unsupported logical operation: {:?}", op),
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
        let result = compiled(&row);
        
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
        let result = compiled(&row);
        
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
        let result = compiled(&row);
        
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
        let result = compiled(&row);
        
        assert_eq!(result, ScalarValue::Boolean(Some(true)));
    }
}

