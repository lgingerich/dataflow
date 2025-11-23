//! # Dataflow: SQL-to-Differential Dataflow Runtime
//!
//! This library provides a runtime that executes SQL queries using Differential Dataflow,
//! enabling incremental, streaming computation over relational data.
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                             SQL QUERY                                   │
//! │                    "SELECT a + 10 FROM table"                           │
//! └─────────────────────────────────┬───────────────────────────────────────┘
//!                                   │
//!                                   ▼
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │  1. PARSE & ANALYZE (DataFusion)                                        │
//! │     QueryAnalyzer::analyze(sql) → LogicalPlan                           │
//! │     - Parses SQL, validates schema, builds AST                          │
//! └─────────────────────────────────┬───────────────────────────────────────┘
//!                                   │
//!                                   ▼
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │  2. TRANSLATE TO PHYSICAL PLAN                                          │
//! │     translate_plan(logical_plan) → Differential Dataflow Graph          │
//! │     - Maps LogicalPlan operators to Differential Dataflow operators     │
//! │     - Compiles Expr trees into closures over Row                        │
//! │     - Converts Arrow RecordBatch → Vec<Row>                             │
//! └─────────────────────────────────┬───────────────────────────────────────┘
//!                                   │
//!                                   ▼
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │  3. EXECUTE (Timely Dataflow)                                           │
//! │     timely::execute(|worker| { ... })                                   │
//! │                                                                         │
//! │     a) Create InputSession, feed Row data                               │
//! │     b) Build Collection<G, Row, isize> from input                       │
//! │     c) Apply operators: .map(), .filter(), .join(), .reduce()           │
//! │     d) Attach .inspect() or .consolidate() to observe results           │
//! │                                                                         │
//! │     Data flows as: (Row, Time, Diff)                                    │
//! └─────────────────────────────────┬───────────────────────────────────────┘
//!                                   │
//!                                   ▼
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │  4. OUTPUT                                                              │
//! │     Stream of (Row, Time, Diff) updates emitted incrementally           │
//! └─────────────────────────────────────────────────────────────────────────┘
//!
//! ## Data Flow Example
//!
//! ```text
//! SQL: "SELECT amount + 10 FROM orders"
//!
//! 1. QueryAnalyzer parses → LogicalPlan:
//!    Sort(Projection(TableScan("orders")))
//!
//! 2. Translator builds graph:
//!    InputSession → Collection<Row> → .map(|row| row + 10) → Output
//!
//! 3. Data flows as Row:
//!    Row([Int64(100)]) → Row([Int64(110)])
//!
//! 4. Differential tracking:
//!    (Row([Int64(110)]), Time(0), +1)  // Insert
//!    (Row([Int64(110)]), Time(1), -1)  // Delete (if source changes)
//! ```

pub mod sql;