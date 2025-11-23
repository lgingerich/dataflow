use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;
use datafusion::common::DFSchema;

/// Analyzes SQL queries using DataFusion
/// 
/// This struct wraps DataFusion's SessionContext and provides
/// a clean interface for parsing SQL and extracting query information.
pub struct QueryAnalyzer {
    ctx: SessionContext,
}

impl QueryAnalyzer {
    /// Create a new QueryAnalyzer
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Register a CSV file as a table
    pub async fn register_csv(
        &mut self,
        table_name: &str,
        path: &str,
    ) -> datafusion::error::Result<()> {
        self.ctx
            .register_csv(table_name, path, CsvReadOptions::new())
            .await
    }

    /// Analyze a SQL query and return query information
    /// 
    /// This parses the SQL, builds a logical plan, and extracts
    /// schema and type information.
    pub async fn analyze(&self, sql: &str) -> datafusion::error::Result<QueryInfo> {
        let df = self.ctx.sql(sql).await?;
        let logical_plan = df.logical_plan().clone();
        // Get the schema from the logical plan (which has Arc<DFSchema>)
        let schema = logical_plan.schema().as_ref().clone();
        
        Ok(QueryInfo {
            logical_plan,
            schema,
        })
    }

    /// Get a reference to the underlying SessionContext
    /// 
    /// Useful for advanced operations not covered by this API
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get a mutable reference to the underlying SessionContext
    pub fn context_mut(&mut self) -> &mut SessionContext {
        &mut self.ctx
    }
}

impl Default for QueryAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Information extracted from a SQL query
/// 
/// Contains the logical plan and schema information needed
/// to translate the query to differential dataflow.
pub struct QueryInfo {
    /// The logical plan representing the query
    pub logical_plan: LogicalPlan,
    
    /// The output schema of the query
    pub schema: DFSchema,
}

impl QueryInfo {
    /// Get the logical plan
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }

    /// Get the output schema
    pub fn schema(&self) -> &DFSchema {
        &self.schema
    }
}

