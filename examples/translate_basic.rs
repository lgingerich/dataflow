use dataflow::sql::{Row, translate_query};
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use differential_dataflow::input::InputSession;
use std::collections::HashMap;

/// Example: Basic translation with TableScan and Projection
///
/// This demonstrates the core translation: SQL → LogicalPlan → Differential Dataflow
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Step 1: Parse SQL with DataFusion
    let ctx = SessionContext::new();
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new())
        .await?;

    let sql = "SELECT order_id, cust_id, 100 as constant FROM orders";
    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan().clone();

    println!("=== Logical Plan ===");
    println!("{:?}\n", &logical_plan);

    // Step 2: Execute with Differential Dataflow
    println!("=== Executing with Differential Dataflow ===");

    timely::execute(timely::Config::thread(), move |worker| {
        // Create input for the "orders" table
        let mut orders_input = InputSession::new();

        worker.dataflow::<usize, _, _>(|scope| {
            let orders_collection = orders_input.to_collection(scope);

            // Map table names to collections
            let mut tables = HashMap::new();
            tables.insert("orders".to_string(), orders_collection);

            // Translate the query
            let result =
                translate_query(&logical_plan, scope, &tables).expect("Translation failed");

            // Inspect the results
            result.inspect(|x| println!("Result: {:?}", x));
        });

        // Feed some test data
        println!("\n=== Feeding Data ===");
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
            ScalarValue::Float64(Some(50.0)),
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(101)),
            ScalarValue::Float64(Some(75.5)),
        ]));
        orders_input.flush();
        orders_input.advance_to(1);

        // Step the worker to process
        for _ in 0..10 {
            worker.step();
        }
    })
    .unwrap();

    Ok(())
}
