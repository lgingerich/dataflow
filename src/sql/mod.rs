/// SQL integration module for DataFusion
/// 
/// This module provides a clean interface to DataFusion for parsing SQL,
/// analyzing queries, and translating them to Differential Dataflow.

pub mod query;
pub mod row;
pub mod translator;

// Re-export commonly used types
pub use query::{QueryAnalyzer, QueryInfo};
pub use row::Row;
pub use translator::describe_translation;
