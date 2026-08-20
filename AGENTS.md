# Hermes RS development guide

This repository is an experimental Rust kernel for Hermes. Preserve these
constraints in every change:

- Do not transliterate the Python module tree. Model domain invariants first.
- Do not add live providers or real tool effects until the offline contract
  corpus passes.
- Keep `serde_json::Value` at captured protocol/test boundaries. Domain state
  uses explicit types.
- A session lineage has one immutable engine and prompt manifest.
- Every dispatched effect has a durable invocation and exactly one terminal or
  reconciliation disposition.
- Every authoritative mutation carries an expected owner generation. Worker
  task mutations additionally carry a fencing token.
- There is one writer per authority scope. Never dual-write Python and Rust
  stores.
- Background delivery occurs at a legal new-turn boundary and is idempotent by
  event ID.
- Keep adapters below traits in `ports`; kernel crates do not depend on
  transports, databases, plugins, or the application package.
- Create crates only for a demonstrated reuse, executable, optional-dependency,
  or distribution boundary. Use modules for internal subsystem organization.
- Prefer behavior and property tests over snapshots of evolving catalogs.
- No unsafe Rust without a separately reviewed architectural reason. The
  workspace currently forbids it.

Before handing off a change, run formatting, Clippy with warnings denied, and
the complete workspace test suite.
