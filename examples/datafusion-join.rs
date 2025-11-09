use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;
use datafusion::arrow::util::pretty;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a new DataFusion session
    let ctx = SessionContext::new();

    // Register CSV files as tables
    ctx.register_csv("customers", "data/customers.csv", CsvReadOptions::new()).await?;
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new()).await?;
    ctx.register_csv("order_items", "data/order_items.csv", CsvReadOptions::new()).await?;
    ctx.register_csv("products", "data/products.csv", CsvReadOptions::new()).await?;

    // Write a SQL query that joins them together
    let sql = r#"
        SELECT
            c.name AS customer,
            c.country,
            p.name AS product,
            SUM(oi.quantity * p.price) AS total_spent
        FROM orders o
        JOIN customers c ON o.cust_id = c.id
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY c.name, c.country, p.name
        ORDER BY total_spent DESC
    "#;

    // Create the DataFrame from SQL
    let df = ctx.sql(sql).await?;

    // Get the schema
    let output_schema = df.schema();

    // Collect the results into Arrow record batches
    let results = df.clone().collect().await?;

    // Print the results and the schema
    pretty::print_batches(&results)?;
    println!("{:?}", output_schema);
    
    Ok(())
}
