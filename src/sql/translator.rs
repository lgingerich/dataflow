use crate::sql::{error::DataflowError, expr::compile_expr, Row};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, Operator};
use differential_dataflow::collection::VecCollection;
use differential_dataflow::operators::Join;
use std::collections::HashMap;
use timely::dataflow::Scope;

/// Translate a DataFusion LogicalPlan to a Differential Dataflow Collection
///
/// This is the main entry point for translation. It recursively walks the
/// LogicalPlan tree and builds a Differential Dataflow computation graph.
///
/// # Arguments
/// * `plan` - The DataFusion logical plan to translate
/// * `scope` - The Timely dataflow scope to build the graph in
/// * `tables` - Map of table names to their input Collections (provided by user)
///
/// # Returns
/// A Collection representing the query result
pub fn translate_query<G: Scope<Timestamp = usize>>(
    plan: &LogicalPlan,
    _scope: &G,
    tables: &HashMap<String, VecCollection<G, Row, isize>>,
) -> Result<VecCollection<G, Row, isize>, DataflowError> {
    match plan {
        // TableScan: lookup the table in the provided HashMap
        LogicalPlan::TableScan(scan) => {
            let table_name = scan.table_name.to_string();
            tables
                .get(&table_name)
                .cloned()
                .ok_or_else(|| DataflowError::TableNotFound(table_name))
        }

        // Projection: compile expressions and map each row
        LogicalPlan::Projection(proj) => {
            // Recursively translate the input
            let input_collection = translate_query(&proj.input, _scope, tables)?;
            let input_schema = proj.input.schema();

            // Compile all projection expressions
            let compiled_exprs: Result<Vec<_>, _> = proj
                .expr
                .iter()
                .map(|e| compile_expr(e, input_schema))
                .collect();
            let compiled_exprs = compiled_exprs?;

            // Map: evaluate each expression on the row
            // SQL semantics: runtime evaluation errors produce NULL values
            Ok(input_collection.map(move |row| {
                let values: Vec<_> = compiled_exprs
                    .iter()
                    .map(|f| match f(&row) {
                        Ok(val) => val,
                        Err(_err) => {
                            // In SQL, runtime errors in expression evaluation typically produce NULL
                            // Future enhancement: Add logging/tracing here for debugging
                            ScalarValue::Null
                        }
                    })
                    .collect();
                Row::new(values)
            }))
        }

        // Filter: compile predicate expression and filter the input collection
        LogicalPlan::Filter(filter) => {
            // Recursively translate the input
            let input_collection = translate_query(&filter.input, _scope, tables)?;
            let input_schema = filter.input.schema();
            
            // Compile the predicate expression (e.g., amount > 50)
            let predicate_fn = compile_expr(&filter.predicate, input_schema)?;
            
            // Apply filter: keep rows where predicate evaluates to true
            // Errors and non-boolean results are treated as false (filter out)
            Ok(input_collection.filter(move |row| {
                match predicate_fn(row) {
                    Ok(ScalarValue::Boolean(Some(true))) => true,  // Keep row
                    Ok(ScalarValue::Boolean(Some(false))) => false, // Filter out
                    Ok(ScalarValue::Boolean(None)) => false,       // NULL = false in SQL
                    Ok(_) => false, // Non-boolean result = filter out
                    Err(_) => false, // Error during evaluation = filter out
                }
            }))
        }

        // Join: combine two tables based on join condition
        LogicalPlan::Join(join) => {
            // Only support INNER JOIN for now
            if !matches!(join.join_type, JoinType::Inner) {
                return Err(DataflowError::UnsupportedLogicalPlan(format!(
                    "Only INNER JOIN is currently supported, got: {:?}",
                    join.join_type
                )));
            }

            // Recursively translate both input plans
            let left_collection = translate_query(&join.left, _scope, tables)?;
            let right_collection = translate_query(&join.right, _scope, tables)?;
            
            let left_schema = join.left.schema();
            let right_schema = join.right.schema();
            
            // Extract join keys from the join condition
            // DataFusion provides join conditions in two formats (see extract_join_keys)
            let (left_key_indices, right_key_indices) = 
                extract_join_keys(&join.on, &join.filter, left_schema, right_schema)?;
            
            // Map left collection to (key, row) pairs
            // Use String representation of key values for ExchangeData compatibility
            let left_keyed = left_collection.map(move |row| {
                let key_parts: Vec<String> = left_key_indices
                    .iter()
                    .map(|&idx| {
                        row.get(idx)
                            .map(|v| format!("{:?}", v))
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect();
                let key = key_parts.join("||"); // Use || as separator
                (key, row)
            });
            
            // Map right collection to (key, row) pairs
            let right_keyed = right_collection.map(move |row| {
                let key_parts: Vec<String> = right_key_indices
                    .iter()
                    .map(|&idx| {
                        row.get(idx)
                            .map(|v| format!("{:?}", v))
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect();
                let key = key_parts.join("||");
                (key, row)
            });
            
            // Perform the join using Differential Dataflow's join operator
            // The join produces (key, (left_row, right_row)) tuples
            // We map them back to combined rows
            Ok(left_keyed.join(&right_keyed).map(|(_key, (left_row, right_row))| {
                // Combine left and right rows by concatenating their values
                let mut combined_values = left_row.values.clone();
                combined_values.extend(right_row.values.clone());
                Row::new(combined_values)
            }))
        }

        _ => Err(DataflowError::UnsupportedLogicalPlan(format!("{:?}", plan))),
    }
}

/// Extract join keys from DataFusion's Join representation
///
/// DataFusion represents join conditions in two ways depending on optimization:
/// - `on`: Direct column pairs [(left_col, right_col)] - used when the optimizer
///         recognizes pure equi-joins or when using the DataFrame API directly
/// - `filter`: Expression tree (BinaryExpr) - used after optimization passes,
///         for implicit joins (FROM a, b WHERE...), or complex conditions
///
/// Both represent the same join semantics, just different internal representations
/// after DataFusion's query planning and optimization phases.
///
/// # Arguments
/// * `join_on` - List of (left_expr, right_expr) pairs from ON clause
/// * `filter` - Optional filter expression tree
/// * `left_schema` - Schema of the left table
/// * `right_schema` - Schema of the right table
///
/// # Returns
/// Tuple of (left_key_indices, right_key_indices) for join key columns
fn extract_join_keys(
    join_on: &[(Expr, Expr)],
    filter: &Option<Expr>,
    left_schema: &datafusion::common::DFSchema,
    right_schema: &datafusion::common::DFSchema,
) -> Result<(Vec<usize>, Vec<usize>), DataflowError> {
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();

    // Handle ON clause: direct column pairs
    if !join_on.is_empty() {
        for (left_expr, right_expr) in join_on {
            // Extract column from left expression
            let left_col = match left_expr {
                Expr::Column(col) => col,
                _ => {
                    return Err(DataflowError::UnsupportedExpression(format!(
                        "Join key must be a column reference, got: {:?}",
                        left_expr
                    )))
                }
            };

            // Extract column from right expression
            let right_col = match right_expr {
                Expr::Column(col) => col,
                _ => {
                    return Err(DataflowError::UnsupportedExpression(format!(
                        "Join key must be a column reference, got: {:?}",
                        right_expr
                    )))
                }
            };

            // Find column indices in respective schemas
            let left_idx = left_schema
                .index_of_column(left_col)
                .map_err(|e| DataflowError::ColumnNotFound(format!("Left join key: {:?}", e)))?;

            let right_idx = right_schema
                .index_of_column(right_col)
                .map_err(|e| DataflowError::ColumnNotFound(format!("Right join key: {:?}", e)))?;

            left_indices.push(left_idx);
            right_indices.push(right_idx);
        }
        
        return Ok((left_indices, right_indices));
    }

    // Handle filter expression: parse expression tree
    if let Some(filter_expr) = filter {
        parse_join_filter(
            filter_expr,
            &mut left_indices,
            &mut right_indices,
            left_schema,
            right_schema,
        )?;
        
        if left_indices.is_empty() {
            return Err(DataflowError::UnsupportedExpression(
                "Join must have at least one join key".to_string(),
            ));
        }
        
        return Ok((left_indices, right_indices));
    }

    Err(DataflowError::UnsupportedExpression(
        "Join must have either ON clause or filter condition".to_string(),
    ))
}

/// Recursively parse filter expression to extract join key columns
///
/// Handles equality comparisons (a.x = b.y) and AND compositions to support
/// multi-column joins like: a.x = b.x AND a.y = b.y
fn parse_join_filter(
    expr: &Expr,
    left_indices: &mut Vec<usize>,
    right_indices: &mut Vec<usize>,
    left_schema: &datafusion::common::DFSchema,
    right_schema: &datafusion::common::DFSchema,
) -> Result<(), DataflowError> {
    match expr {
        // Equality: left.col = right.col
        Expr::BinaryExpr(bin) if bin.op == Operator::Eq => {
            let (left_expr, right_expr) = (&bin.left, &bin.right);
            
            // Extract both columns from the equality
            if let (Expr::Column(left_col), Expr::Column(right_col)) = 
                (left_expr.as_ref(), right_expr.as_ref()) {
                
                // Try matching: left table column = right table column
                if let (Ok(left_idx), Ok(right_idx)) = (
                    left_schema.index_of_column(left_col),
                    right_schema.index_of_column(right_col),
                ) {
                    left_indices.push(left_idx);
                    right_indices.push(right_idx);
                    return Ok(());
                }
                
                // Try swapped: right table column = left table column
                if let (Ok(left_idx), Ok(right_idx)) = (
                    left_schema.index_of_column(right_col),
                    right_schema.index_of_column(left_col),
                ) {
                    left_indices.push(left_idx);
                    right_indices.push(right_idx);
                    return Ok(());
                }
            }
            
            Err(DataflowError::UnsupportedExpression(format!(
                "Could not extract join keys from equality: {:?}",
                expr
            )))
        }
        
        // AND: recursively extract from both sides
        Expr::BinaryExpr(bin) if bin.op == Operator::And => {
            parse_join_filter(&bin.left, left_indices, right_indices, left_schema, right_schema)?;
            parse_join_filter(&bin.right, left_indices, right_indices, left_schema, right_schema)?;
            Ok(())
        }
        
        _ => Err(DataflowError::UnsupportedExpression(format!(
            "Unsupported join filter expression: {:?}",
            expr
        ))),
    }
}
