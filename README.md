# Architecture Overview

```markdown
┌─────────────────────────────────────────────────────────────────────────┐
│                             SQL QUERY                                   │
│                    "SELECT a + 10 FROM table"                           │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  1. PARSE & ANALYZE (DataFusion)                                        │
│     QueryAnalyzer::analyze(sql) → LogicalPlan                           │
│     - Parses SQL, validates schema, builds AST                          │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  2. TRANSLATE TO PHYSICAL PLAN                                          │
│     translate_plan(logical_plan) → Differential Dataflow Graph          │
│     - Maps LogicalPlan operators to Differential Dataflow operators     │
│     - Compiles Expr trees into closures over Row                        │
│     - Converts Arrow RecordBatch → Vec<Row>                             │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  3. EXECUTE (Timely Dataflow)                                           │
│     timely::execute(|worker| { ... })                                   │
│                                                                         │
│     a) Create InputSession, feed Row data                               │
│     b) Build Collection<G, Row, isize> from input                       │
│     c) Apply operators: .map(), .filter(), .join(), .reduce()           │
│     d) Attach .inspect() or .consolidate() to observe results           │
│                                                                         │
│     Data flows as: (Row, Time, Diff)                                    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  4. OUTPUT                                                              │
│     Stream of (Row, Time, Diff) updates emitted incrementally           │
└─────────────────────────────────────────────────────────────────────────┘