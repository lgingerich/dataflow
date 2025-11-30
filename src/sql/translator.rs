use crate::sql::{error::DataflowError, expr::compile_expr, Row};
use datafusion::logical_expr::LogicalPlan;
use differential_dataflow::collection::VecCollection;
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
            Ok(input_collection.map(move |row| {
                let values: Vec<_> = compiled_exprs.iter().map(|f| f(&row)).collect();
                Row::new(values)
            }))
        }

        _ => Err(DataflowError::UnsupportedLogicalPlan(format!("{:?}", plan))),
    }
}
