use dataflow::sql::{QueryAnalyzer, describe_translation};

/// Example: Simple projection with arithmetic expressions
/// 
/// This demonstrates translation of:
/// SELECT order_id, cust_id, amount + 10, amount - 10 FROM orders ORDER BY order_id
/// 
/// This should translate to: Input -> Map -> Sort
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a query analyzer
    let mut analyzer = QueryAnalyzer::new();

    // Register the CSV file as a table
    analyzer.register_csv("orders", "data/orders.csv").await?;

    // Analyze the query with arithmetic expressions
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
    
    let query_info = analyzer.analyze(sql).await?;

    // Print the logical plan structure
    println!("\nLogical Plan Structure:");
    println!("{:?}", query_info.logical_plan());

    // Print schema
    println!("\nOutput Schema:");
    for field in query_info.schema().fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }

    // Translate to dataflow operators
    println!("\n=== Translation to Dataflow ===");
    match describe_translation(query_info.logical_plan()) {
        Ok(description) => println!("{}", description),
        Err(e) => println!("Translation error: {}", e),
    }

    Ok(())
}

