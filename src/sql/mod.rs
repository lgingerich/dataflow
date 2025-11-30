//! SQL integration module
//!
//! Provides translation from DataFusion LogicalPlan to Differential Dataflow.
//! Users interact with DataFusion directly for SQL parsing.

pub mod expr;
pub mod row;
pub mod translator;

// Re-export commonly used types
pub use row::Row;
pub use translator::translate_query;
