use datafusion::prelude::*;
use dataflow::sql::describe_translation;

/// Example: Simple projection with arithmetic expressions
/// 
/// This demonstrates translation of:
/// SELECT order_id, cust_id, amount + 10, amount - 10 FROM orders ORDER BY order_id
/// 
/// This should translate to: Input -> Map -> Sort
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create DataFusion context
    let ctx = SessionContext::new();

    // Register the CSV file as a table
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new()).await?;

    // Parse SQL query
    println!("=== Analyzing projection with arithmetic ===");
    let sql = r#"
        SELECT
            order_id,
            cust_id,
            amount + 10 as amount_plus_10,
            amount - 10 as amount_minus_10
        FROM orders
        ORDER BY order_id ASC
    "#;
    
    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan();

    // Print the logical plan structure
    println!("\nLogical Plan Structure:");
    println!("{:?}", logical_plan);

    // Print schema
    println!("\nOutput Schema:");
    for field in logical_plan.schema().fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }

    // Translate to dataflow operators
    println!("\n=== Translation to Dataflow ===");
    match describe_translation(logical_plan) {
        Ok(description) => println!("{}", description),
        Err(e) => println!("Translation error: {}", e),
    }

    Ok(())
}

