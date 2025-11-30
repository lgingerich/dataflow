use dataflow::{Row, translate_query};
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use differential_dataflow::input::InputSession;
use std::collections::HashMap;

/// Example: DISTINCT operation
///
/// Demonstrates: SELECT DISTINCT customer_id FROM orders
/// Shows how Differential Dataflow efficiently maintains unique rows incrementally
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new())
        .await?;

    let sql = "SELECT DISTINCT cust_id FROM orders";

    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan().clone();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ DISTINCT Example                                          ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nQuery: {}\n", sql);

    timely::execute(timely::Config::thread(), move |worker| {
        let mut orders_input = InputSession::new();

        worker.dataflow::<usize, _, _>(|scope| {
            let orders_collection = orders_input.to_collection(scope);

            let mut tables = HashMap::new();
            tables.insert("orders".to_string(), orders_collection);

            let result =
                translate_query(&logical_plan, scope, &tables).expect("Translation failed");

            // Track distinct results
            result.inspect(move |(row, time, diff)| {
                let cust_id = match row.get(0) {
                    Some(ScalarValue::Int64(Some(v))) => *v,
                    _ => 0,
                };

                let op = if *diff > 0 { "[+]" } else { "[-]" };
                println!("{} Time {} | Customer ID: {}", op, time, cust_id);
            });
        });

        println!("--- Loading orders (multiple per customer) ---");
        // Orders: (order_id, cust_id, amount)
        // Multiple orders for same customers
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)), // Alice
            ScalarValue::Float64(Some(50.0)),
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(101)), // Bob
            ScalarValue::Float64(Some(75.0)),
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(3)),
            ScalarValue::Int64(Some(100)), // Alice again (duplicate cust_id)
            ScalarValue::Float64(Some(100.0)),
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(4)),
            ScalarValue::Int64(Some(100)), // Alice again (duplicate cust_id)
            ScalarValue::Float64(Some(25.0)),
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(102)), // Charlie
            ScalarValue::Float64(Some(200.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(1);

        for _ in 0..20 {
            worker.step();
        }

        println!("\n--- Adding order for existing customer (Alice=100) ---");
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(6)),
            ScalarValue::Int64(Some(100)), // Alice again (duplicate)
            ScalarValue::Float64(Some(150.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(2);

        for _ in 0..20 {
            worker.step();
        }

        println!("\n--- Adding order for new customer (David=103) ---");
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(7)),
            ScalarValue::Int64(Some(103)), // David (new customer)
            ScalarValue::Float64(Some(300.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(3);

        for _ in 0..20 {
            worker.step();
        }

        println!("\n--- Deleting all orders for Bob (101) ---");
        orders_input.remove(Row::new(vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(101)),
            ScalarValue::Float64(Some(75.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(4);

        for _ in 0..20 {
            worker.step();
        }

        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ Key Observations:                                         ║");
        println!("╠════════════════════════════════════════════════════════════╣");
        println!("║ ✓ 5 orders loaded, but only 3 DISTINCT customer IDs      ║");
        println!("║ ✓ Adding order for existing customer = no new output     ║");
        println!("║ ✓ Adding order for new customer = 1 new distinct ID      ║");
        println!("║ ✓ Deleting last order for Bob removes ID from output     ║");
        println!("║ ✓ Differential Dataflow tracks reference counts!         ║");
        println!("╚════════════════════════════════════════════════════════════╝");
    })
    .unwrap();

    Ok(())
}
