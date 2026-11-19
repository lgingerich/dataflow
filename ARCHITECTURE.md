# Architecture

Keep this file short (~1 page). Project-specific invariants live here; universal Rust workflow lives in `AGENTS.md`.

## Purpose
`dataflow` is a library that runs SQL incrementally on Differential Dataflow. DataFusion parses and plans the SQL into a `LogicalPlan`; `translate_query` walks that plan and builds a `VecCollection<G, Row, isize>` graph in a caller-supplied Timely scope, so results are emitted as `(Row, Time, Diff)` updates. The pipeline diagram is in the `src/lib.rs` crate docs and `README.md`. It does NOT own the Timely worker, input sessions, or table registration (callers do that; see `examples/`), and it does not execute plans with DataFusion itself.

## Module Boundaries
- `src/lib.rs`: declares `core`, `error`, `row`; re-exports `DataflowError`, `Result`, `Row` and `core::translator::translate_query`.
- `src/error.rs`: `DataflowError` (`thiserror`) and the `Result` alias. Depends only on DataFusion types.
- `src/row.rs`: `Row`, an `Arc<[ScalarValue]>` wrapper with `Ord`, `Hash` and serde (via a private serializable mirror of `ScalarValue`) so it can be a Differential record. Depends only on `error`.
- `src/core/expr.rs`: `compile_expr` turns a DataFusion `Expr` into a boxed closure over `&Row` (supports column, literal, alias, binary expressions); `apply_binary_op` runs Arrow compute kernels. Depends on `error` and `row`.
- `src/core/translator.rs`: `translate_query`, which handles `TableScan`, `Projection`, `Filter`, `Join` (inner, left, right, full), `SubqueryAlias` and `Distinct`. Depends on `expr`, `error`, `row`. **Gap: this file is not in HEAD.** `src/core/mod.rs` declares `pub mod translator;` and `lib.rs` re-exports from it, but the only committed translator is the stale `src/translator.rs`, which is not a declared module and still imports `crate::expr`. This section describes that older file's behavior.
- Direction: `error` and `row` are leaves; `core::expr` depends on them; `core::translator` depends on `core::expr`. Nothing in `core`, `error` or `row` depends on `examples/`.

## Core Invariants
- Records flowing through Differential operators are `Row`; `Row` ordering and hashing go through its `ScalarKind`/`hash_scalar`/`cmp_scalar` helpers in `src/row.rs` and must stay consistent with `Eq`.
- `ScalarValue` types without a `SerializableValue` mapping fail with `DataflowError::UnsupportedScalarType`; extend the enum rather than bypassing it.
- No implicit type coercion in binary ops: mismatched operand types return `DataflowError::TypeMismatch` ("use explicit CAST").
- Unsupported expressions, operators and plan nodes return a typed `DataflowError` variant (`UnsupportedExpression`, `UnsupportedOperation`, `UnsupportedLogicalPlan`, ...) at translation time, not a panic.
- Runtime expression-evaluation errors inside a projection become `ScalarValue::Null` (SQL semantics); compile-time errors still propagate.
- `translate_query` is generic over `Scope<Timestamp = usize>` and takes tables as `HashMap<String, VecCollection<G, Row, isize>>`; a missing table is `DataflowError::TableNotFound`.

## Forbidden Patterns / Dependencies
- `unwrap_used` and `expect_used` are `deny` in `Cargo.toml` (tests may use them via `clippy.toml`); use `DataflowError`.
- Integration tests: HEAD has no `tests/` directory; runnable demos live in `examples/` (`translate_*.rs`, `sql_*.rs`, `df_join.rs`) and read CSVs from `data/`.
- Otherwise none beyond `AGENTS.md`.
