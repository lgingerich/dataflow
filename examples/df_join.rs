use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    
    ctx.register_csv("orders", "data/orders.csv", CsvReadOptions::new()).await?;
    ctx.register_csv("customers", "data/customers.csv", CsvReadOptions::new()).await?;
    
    // Using DataFrame API
    let orders = ctx.table("orders").await?;
    let customers = ctx.table("customers").await?;
    
    let joined = orders.join(
        customers,
        JoinType::Inner,
        &["cust_id"],  // Left join keys
        &["id"],       // Right join keys
        None,          // No additional filter
    )?;
    
    println!("{:?}", joined.logical_plan());
    
    Ok(())
}