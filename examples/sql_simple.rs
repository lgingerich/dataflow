use dataflow::sql::{QueryAnalyzer, describe_translation};

/// Example: Simple table scan (no-op query)
/// 
/// This demonstrates the basic SQL integration - just reading a table
/// without any transformations. This is the simplest case.
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a query analyzer
    let mut analyzer = QueryAnalyzer::new();

    // Register the CSV file as a table
    analyzer.register_csv("orders", "data/orders.csv").await?;

    // Analyze a simple table scan (SELECT * FROM orders)
    println!("=== Analyzing simple table scan ===");
    let sql = "SELECT * FROM orders";
    let query_info = analyzer.analyze(sql).await?;

    // Print the logical plan
    println!("\nLogical Plan:");
    println!("{:?}", query_info.logical_plan());

    // Print the schema
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

