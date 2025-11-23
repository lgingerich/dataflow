use datafusion::prelude::*;
use dataflow::sql::describe_translation;

/// Example: Simple table scan (no-op query)
/// 
/// This demonstrates the basic SQL integration - just reading a table
/// without any transformations. This is the simplest case.
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create DataFusion context
    let ctx = SessionContext::new();

    // Register the CSV file as a table
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new()).await?;

    // Parse SQL query
    println!("=== Analyzing simple table scan ===");
    let sql = "SELECT * FROM orders";
    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan();

    // Print the logical plan
    println!("\nLogical Plan:");
    println!("{:?}", logical_plan);

    // Print the schema
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

