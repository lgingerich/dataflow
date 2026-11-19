# AGENTS.md

Read `ARCHITECTURE.md` before non-trivial changes.

## Workflow & Behavior
- Ask before assuming when requirements, architecture, or intent are unclear.
- Think before coding on design or debugging; surface tradeoffs first.
- Stay strictly in scope: only edit files required by the task. Mention unrelated follow-ups instead of making them.
- Ask before restructuring code, changing public APIs, weakening lints/CI/profiles, adding dependencies, or running destructive actions.
- Admit uncertainty instead of guessing.
- No filler openers ("Great question", "Certainly"). Start with the answer or action; end with what changed and how it was verified.

## Minimum Code That Works
Understand the problem and trace the real flow first, then stop at the first rung that holds:
1. Does this need to be built at all? (YAGNI)
2. Does a helper or pattern already exist in this repo? Reuse it.
3. Does `std`, the platform, or an existing dependency cover it? Use it.
4. Can it be one line? Make it one line.
5. Otherwise, write the minimum code that works.

- Deletion over addition; boring over clever; fewest files possible.
- Fix root causes, not symptoms: grep all callers of any shared function you touch and fix it once.
- Question overbuilt requests ("Do you need X, or does Y cover it?").
- When two stdlib options are the same size, pick the edge-case-correct one.
- Mark deliberate simplifications that cut a corner with a known ceiling (e.g., global lock, O(n^2) scan) with a comment naming the ceiling and upgrade path.
- Never cut corners on trust-boundary validation, data-loss prevention, security, or explicitly requested behavior. Non-trivial logic must leave one minimal runnable check behind.

## Rust & API Rules
`Cargo.toml` and `clippy.toml` enforce formatting, `unsafe` bans, docs, panics, complexity, and common perf lints. Beyond those:
- **No internal back-compat**: when internal semantics change, update the method/type and migrate callers. Never add parallel `*_trusted`/`*_unchecked` helpers or duplicate constructors.
- **Conversions on the type**: implement `Display`/`FromStr`, `From`/`TryFrom`/`AsRef`/`Borrow` rather than one-off `to_*`/`from_*` free functions.
- **Public APIs**: private fields with `pub fn` accessors; accept borrowed inputs (`&str`, `&[T]`) and return owned values unless zero-copy is explicit; keep external/wire types at crate boundaries; use identical accessor names across sibling types.
- **Errors**: typed `thiserror` in libraries, `anyhow` in binaries/tests/examples, no `Box<dyn Error>`. Layer bottom-up with `#[from]` (`leaf -> domain -> boundary`); leaf errors name the bad input/constraint. Avoid `map_err` unless injecting boundary-only context or crossing orphan-rule limits.
- **Performance**: measure a release-with-symbols build before optimizing, and keep the benchmark justifying any `#[inline]`, `#[cold]`, or custom hasher. Keep hot paths zero-allocation, bounded, contiguous, and statically dispatched (`dyn Trait`/`Arc`/`Box` at boundaries). Never add `unsafe` for speed without measurements and sign-off.

## Tests
- Group integration tests under `tests/integration/main.rs` (plus `tests/live/main.rs` only for network/credentials) with submodules. Never place `.rs` files directly in `tests/` and never set `autotests = false`. Build fixture paths from `env!("CARGO_MANIFEST_DIR")`.
- Test observable contracts, invariants, edge cases, and failure isolation—not implementation details or incidental formatting.
- Prefer real fixtures for formats/parsers and small local fakes over mocks. Write a behavior-named regression test before fixing a reproducible bug; use property tests for parsers and round-trips.
- No arbitrary sleeps in async tests; use explicit synchronization or time control.

## Verification
After code changes, run the CI-equivalent checks (prefix with `RUSTC_WRAPPER=""` if `kache` is not installed):

```bash
cargo fmt --check
CARGO_BUILD_WARNINGS=deny cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo test --doc
cargo nextest run --all-targets --all-features --no-tests pass # or: cargo test --all-targets --all-features
```
