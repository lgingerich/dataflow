use datafusion::prelude::*;
use datafusion::arrow::util::pretty;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a new DataFusion session
    let ctx = SessionContext::new();

    let df = ctx.read_csv("data/orders.csv", CsvReadOptions::new()).await?;
    let results = df.clone().collect().await?;
    pretty::print_batches(&results)?;

    let output_schema = df.schema();
    println!("{:?}", output_schema);

    Ok(())

}
