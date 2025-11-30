use dataflow::sql::{Row, translate_query};
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use differential_dataflow::input::InputSession;
use std::collections::HashMap;

/// Example: Filter operations (WHERE clause)
///
/// This demonstrates: SELECT * FROM orders WHERE amount > 50
/// Shows how Filter operator works with incremental updates
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Step 1: Parse SQL with DataFusion
    let ctx = SessionContext::new();
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new())
        .await?;

    let sql = "SELECT order_id, cust_id, amount FROM orders WHERE amount > 50.0";
    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan().clone();

    println!("=== SQL Query ===");
    println!("{}\n", sql);
    println!("=== Logical Plan ===");
    println!("{:#?}\n", logical_plan);

    // Step 2: Execute with Differential Dataflow
    println!("=== Executing with Differential Dataflow ===");

    timely::execute(timely::Config::thread(), move |worker| {
        let mut orders_input = InputSession::new();

        worker.dataflow::<usize, _, _>(|scope| {
            let orders_collection = orders_input.to_collection(scope);

            let mut tables = HashMap::new();
            tables.insert("orders".to_string(), orders_collection);

            let result = translate_query(&logical_plan, scope, &tables)
                .expect("Translation failed");

            result.inspect(|(row, time, diff)| {
                // Helper to extract value from ScalarValue
                let extract_value = |idx: usize| -> String {
                    match row.get(idx) {
                        Some(ScalarValue::Int64(Some(v))) => format!("{}", v),
                        Some(ScalarValue::Float64(Some(v))) => format!("{}", v),
                        Some(ScalarValue::Utf8(Some(v))) => v.clone(),
                        _ => "NULL".to_string(),
                    }
                };

                let sign = if *diff > 0 { "+" } else { "-" };
                println!(
                    "[Time {}] {} order_id={}, cust_id={}, amount={}",
                    time,
                    sign,
                    extract_value(0),
                    extract_value(1),
                    extract_value(2)
                );
            });
        });

        // Feed initial test data
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(1)),      // order_id
            ScalarValue::Int64(Some(100)),    // cust_id
            ScalarValue::Float64(Some(50.0)), // amount (filtered out: <= 50)
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(101)),
            ScalarValue::Float64(Some(75.5)), // amount (passes: > 50)
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(3)),
            ScalarValue::Int64(Some(102)),
            ScalarValue::Float64(Some(100.0)), // amount (passes: > 50)
        ]));
        orders_input.flush();
        orders_input.advance_to(1);

        for _ in 0..10 {
            worker.step();
        }

        // Incremental update: modify a row
        // Update = delete old + insert new (Differential Dataflow represents updates this way)
        orders_input.remove(Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
            ScalarValue::Float64(Some(50.0)), // Old value (was filtered out)
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
            ScalarValue::Float64(Some(60.0)), // New value (now passes filter)
        ]));
        orders_input.flush();
        orders_input.advance_to(2);

        for _ in 0..10 {
            worker.step();
        }

        // Incremental update: delete a row
        orders_input.remove(Row::new(vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(101)),
            ScalarValue::Float64(Some(75.5)),
        ]));
        orders_input.flush();
        orders_input.advance_to(3);

        for _ in 0..10 {
            worker.step();
        }
    })
    .unwrap();

    Ok(())
}

