use dataflow::sql::{Row, translate_query};
use datafusion::common::ScalarValue;
use datafusion::prelude::*;
use differential_dataflow::input::InputSession;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Example: INNER JOIN operation with dynamic table tracking
///
/// This demonstrates: SELECT * FROM orders INNER JOIN customers ON orders.cust_id = customers.id
/// Shows actual table state at each step and how JOIN grows columns (left cols + right cols)
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Step 1: Parse SQL with DataFusion
    let ctx = SessionContext::new();

    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new())
        .await?;
    ctx.register_csv("customers", "data/customers.csv", CsvReadOptions::new())
        .await?;

    let sql = "SELECT o.order_id, o.cust_id, o.amount, c.id, c.name 
                FROM orders o 
                INNER JOIN customers c ON o.cust_id = c.id";
    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan().clone();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ SQL INNER JOIN Example                                    ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nQuery: {}\n", sql);

    // Track actual data in tables
    let orders_data: Arc<Mutex<Vec<(i64, i64, f64)>>> = Arc::new(Mutex::new(Vec::new()));
    let customers_data: Arc<Mutex<Vec<(i64, String)>>> = Arc::new(Mutex::new(Vec::new()));

    let orders_clone = orders_data.clone();
    let customers_clone = customers_data.clone();

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

            // Print join results as they happen
            result.inspect(move |(row, time, diff)| {
                let order_id = match row.get(0) {
                    Some(ScalarValue::Int64(Some(v))) => *v,
                    _ => 0,
                };
                let cust_id = match row.get(1) {
                    Some(ScalarValue::Int64(Some(v))) => *v,
                    _ => 0,
                };
                let amount = match row.get(2) {
                    Some(ScalarValue::Float64(Some(v))) => *v,
                    _ => 0.0,
                };
                let cust_id_joined = match row.get(3) {
                    Some(ScalarValue::Int64(Some(v))) => *v,
                    _ => 0,
                };
                let customer_name = match row.get(4) {
                    Some(ScalarValue::Utf8(Some(v))) => v.clone(),
                    _ => "?".to_string(),
                };

                let operation = if *diff > 0 { "[JOIN +]" } else { "[JOIN -]" };
                println!(
                    "{} Time {} | Order({}, cid={}, ${:.1}) ⋈ Customer({}, \"{}\")",
                    operation, time, order_id, cust_id, amount, cust_id_joined, customer_name
                );
            });
        });

        // Helper to print current tables
        let print_tables = || {
            let orders = orders_clone.lock().unwrap();
            let customers = customers_clone.lock().unwrap();

            println!("\n📊 ORDERS (3 columns):");
            println!("┌──────────┬─────────┬────────┐");
            println!("│ order_id │ cust_id │ amount │");
            println!("├──────────┼─────────┼────────┤");
            if orders.is_empty() {
                println!("│      (empty table)          │");
            } else {
                for (id, cust, amt) in orders.iter() {
                    println!("│ {:^8} │ {:^7} │ {:>6.1} │", id, cust, amt);
                }
            }
            println!("└──────────┴─────────┴────────┘");

            println!("\n👥 CUSTOMERS (2 columns):");
            println!("┌─────┬─────────┐");
            println!("│ id  │  name   │");
            println!("├─────┼─────────┤");
            if customers.is_empty() {
                println!("│  (empty table) │");
            } else {
                for (id, name) in customers.iter() {
                    println!("│ {:^3} │ {:^7} │", id, name);
                }
            }
            println!("└─────┴─────────┘");
            println!("\n→ JOIN results (3 left cols + 2 right cols = 5 total):");
        };

        // ============================================================
        // STEP 1: Load orders
        // ============================================================
        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ STEP 1: Load orders (no customers yet → no joins)         ║");
        println!("╚════════════════════════════════════════════════════════════╝");

        let order_data = vec![
            (1, 100, 50.0),
            (2, 101, 75.5),
            (3, 100, 100.0),
            (4, 999, 25.0), // Will not match any customer
        ];

        for (id, cust, amt) in &order_data {
            orders_clone.lock().unwrap().push((*id, *cust, *amt));
            orders_input.insert(Row::new(vec![
                ScalarValue::Int64(Some(*id)),
                ScalarValue::Int64(Some(*cust)),
                ScalarValue::Float64(Some(*amt)),
            ]));
        }
        orders_input.flush();
        orders_input.advance_to(1);
        customers_input.advance_to(1);

        for _ in 0..20 {
            worker.step();
        }
        print_tables();

        // ============================================================
        // STEP 2: Load customers
        // ============================================================
        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ STEP 2: Load customers → JOIN computes matches!           ║");
        println!("╚════════════════════════════════════════════════════════════╝");

        let customer_data = vec![
            (100, "Alice"),
            (101, "Bob"),
            (102, "Charlie"), // Will not match any order
        ];

        for (id, name) in &customer_data {
            customers_clone
                .lock()
                .unwrap()
                .push((*id, name.to_string()));
            customers_input.insert(Row::new(vec![
                ScalarValue::Int64(Some(*id)),
                ScalarValue::Utf8(Some(name.to_string())),
                ScalarValue::Null,
            ]));
        }
        customers_input.flush();
        customers_input.advance_to(2);
        orders_input.advance_to(2);

        for _ in 0..20 {
            worker.step();
        }
        print_tables();
        println!("↑ INNER JOIN only shows matches (order 4 and customer 102 filtered out)");

        // ============================================================
        // STEP 3: Add order for existing customer
        // ============================================================
        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ STEP 3: Add order for Alice (incremental update)          ║");
        println!("╚════════════════════════════════════════════════════════════╝");

        orders_clone.lock().unwrap().push((5, 100, 200.0));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(100)),
            ScalarValue::Float64(Some(200.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(3);
        customers_input.advance_to(3);

        for _ in 0..20 {
            worker.step();
        }
        print_tables();
        println!("↑ Only 1 new join result (efficient incremental computation)");

        // ============================================================
        // STEP 4: Add new customer (no matches yet)
        // ============================================================
        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ STEP 4: Add David (no orders yet → no new joins)          ║");
        println!("╚════════════════════════════════════════════════════════════╝");

        customers_clone
            .lock()
            .unwrap()
            .push((103, "David".to_string()));
        customers_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(103)),
            ScalarValue::Utf8(Some("David".to_string())),
            ScalarValue::Null,
        ]));
        customers_input.flush();
        customers_input.advance_to(4);
        orders_input.advance_to(4);

        for _ in 0..20 {
            worker.step();
        }
        print_tables();
        println!("↑ No new join results (David has no orders)");

        // ============================================================
        // STEP 5: Add order for new customer
        // ============================================================
        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ STEP 5: Add order for David → new match!                  ║");
        println!("╚════════════════════════════════════════════════════════════╝");

        orders_clone.lock().unwrap().push((6, 103, 150.0));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(6)),
            ScalarValue::Int64(Some(103)),
            ScalarValue::Float64(Some(150.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(5);
        customers_input.advance_to(5);

        for _ in 0..20 {
            worker.step();
        }
        print_tables();

        // ============================================================
        // STEP 6: Delete an order
        // ============================================================
        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ STEP 6: Delete order 1 → remove its join result           ║");
        println!("╚════════════════════════════════════════════════════════════╝");

        orders_clone.lock().unwrap().retain(|o| o.0 != 1);
        orders_input.remove(Row::new(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(100)),
            ScalarValue::Float64(Some(50.0)),
        ]));
        orders_input.flush();
        orders_input.advance_to(6);
        customers_input.advance_to(6);

        for _ in 0..20 {
            worker.step();
        }
        print_tables();

        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!("║ Summary: How INNER JOIN Works                             ║");
        println!("╠════════════════════════════════════════════════════════════╣");
        println!("║ ✓ Concatenates columns: LEFT (3) + RIGHT (2) = 5 columns  ║");
        println!("║ ✓ Only matching rows appear (non-matches filtered out)    ║");
        println!("║ ✓ Updates are incremental (only +1 or -1 per change)      ║");
        println!("║ ✓ Differential Dataflow maintains indexed join state      ║");
        println!("╚════════════════════════════════════════════════════════════╝");
    })
    .unwrap();

    Ok(())
}
