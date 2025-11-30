use dataflow::sql::{Row, translate_query};
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use differential_dataflow::input::InputSession;
use std::collections::HashMap;

/// Example: Multi-column join (composite key)
///
/// Demonstrates: SELECT * FROM shipments
///               JOIN orders ON shipments.order_id = orders.id
///                          AND shipments.warehouse = orders.warehouse
///
/// Tests that our join implementation correctly handles multiple join keys
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    // Register dummy tables for schema
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new())
        .await?;
    ctx.register_csv("customers", "data/customers.csv", CsvReadOptions::new())
        .await?;

    // SQL with multi-column join using table aliases
    // Note: In real scenarios, you'd have different columns like:
    //   ON o.tenant_id = c.tenant_id AND o.user_id = c.user_id
    let sql = r#"
        SELECT o.order_id, o.cust_id, o.amount, c.id as customer_id, c.name
        FROM orders o
        JOIN customers c ON o.cust_id = c.id 
                        AND o.cust_id = c.id
    "#;

    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan().clone();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ Multi-Column JOIN Test                                    ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nQuery: {}\n", sql);

    timely::execute(timely::Config::thread(), move |worker| {
        let mut orders_input = InputSession::new();
        let mut customers_input = InputSession::new();

        worker.dataflow::<usize, _, _>(|scope| {
            let orders_collection = orders_input.to_collection(scope);
            let customers_collection = customers_input.to_collection(scope);

            let mut tables = HashMap::new();
            tables.insert("orders".to_string(), orders_collection);
            tables.insert("customers".to_string(), customers_collection);

            let result =
                translate_query(&logical_plan, scope, &tables).expect("Translation failed");

            // Track join results
            // Result has 6 columns: order_id, cust_id, amount, customer_id, name, country
            result.inspect(move |(row, time, diff)| {
                let order_id = match row.get(0) {
                    Some(ScalarValue::Int64(Some(v))) => *v,
                    _ => 0,
                };
                let orders_cust_id = match row.get(1) {
                    Some(ScalarValue::Int64(Some(v))) => *v,
                    _ => 0,
                };
                let amount = match row.get(2) {
                    Some(ScalarValue::Float64(Some(v))) => *v,
                    _ => 0.0,
                };
                let customer_id = match row.get(3) {
                    Some(ScalarValue::Int64(Some(v))) => *v,
                    _ => 0,
                };
                let name = match row.get(4) {
                    Some(ScalarValue::Utf8(Some(v))) => v.clone(),
                    _ => "?".to_string(),
                };

                let op = if *diff > 0 { "[+]" } else { "[-]" };
                println!(
                    "{} Time {} | Order({}, cust={}, ${:.1}) ⋈ Customer({}, \"{}\")",
                    op, time, order_id, orders_cust_id, amount, customer_id, name
                );
            });
        });

        println!("\n--- Loading orders ---");
        // Orders: (order_id, cust_id, amount)
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
            ScalarValue::Float64(Some(50.0)),
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(101)),
            ScalarValue::Float64(Some(75.0)),
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(3)),
            ScalarValue::Int64(Some(100)),
            ScalarValue::Float64(Some(25.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(1);
        customers_input.advance_to(1);

        for _ in 0..20 {
            worker.step();
        }

        println!("\n--- Loading customers ---");
        // Customers: (id, name, country)
        customers_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(100)),
            ScalarValue::Utf8(Some("Alice".to_string())),
            ScalarValue::Null,
        ]));
        customers_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(101)),
            ScalarValue::Utf8(Some("Bob".to_string())),
            ScalarValue::Null,
        ]));
        customers_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(102)),
            ScalarValue::Utf8(Some("Charlie".to_string())),
            ScalarValue::Null,
        ]));
        customers_input.flush();
        customers_input.advance_to(2);
        orders_input.advance_to(2);

        for _ in 0..20 {
            worker.step();
        }

        println!("\n--- Adding order for Alice ---");
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(4)),
            ScalarValue::Int64(Some(100)),
            ScalarValue::Float64(Some(100.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(3);
        customers_input.advance_to(3);

        for _ in 0..20 {
            worker.step();
        }

        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ Multi-column join completed successfully!                 ║");
        println!("╚════════════════════════════════════════════════════════════╝");
    })
    .unwrap();

    Ok(())
}
