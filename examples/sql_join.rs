use datafusion::prelude::*;

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

    // Register the CSV files as tables
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new())
        .await?;
    ctx.register_csv("customers", "data/customers.csv", CsvReadOptions::new())
        .await?;

    // Parse SQL query
    println!("=== Analyzing projection with arithmetic ===");
    let sql = r#"
        SELECT
            *
        FROM orders INNER JOIN customers ON orders.cust_id = customers.id
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

    Ok(())
}
