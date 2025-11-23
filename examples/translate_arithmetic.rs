use datafusion::prelude::*;
use datafusion::common::ScalarValue;
use dataflow::sql::{Row, translate_query};
use differential_dataflow::input::InputSession;
use std::collections::HashMap;

/// Example: Arithmetic operations in projection
/// 
/// This demonstrates: SELECT amount + 10, amount * 2 FROM orders
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Step 1: Parse SQL with DataFusion
    let ctx = SessionContext::new();
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new()).await?;
    
    let sql = "SELECT amount + 10.0 as amount_plus_10, amount * 2.0 as amount_times_2 FROM orders";
    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan().clone();
    
    println!("=== SQL: {} ===\n", sql);
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
            
            result.inspect(|x| println!("Result: {:?}", x));
        });
        
        // Feed test data - note: amount is column index 2 (Float64)
        println!("\n=== Feeding Data ===");
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(1)),      // order_id
            ScalarValue::Int64(Some(100)),    // cust_id
            ScalarValue::Float64(Some(50.0)), // amount
        ]));
        orders_input.insert(Row::new(vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(101)),
            ScalarValue::Float64(Some(75.5)),
        ]));
        orders_input.flush();
        orders_input.advance_to(1);
        
        for _ in 0..10 {
            worker.step();
        }
    })
    .unwrap();
    
    Ok(())
}

