# Implementation checkpoints

## Completed

- A pinned, checksummed Python-oracle corpus and typed Rust reader
- A provider-neutral agent loop that reproduces every captured agent-turn
  outcome
- OpenAI-compatible streaming, root-confined read-only tools, and an end-to-end
  live-edge test
- Immutable session manifests and semantic conversation projection
- SQLite sessions with owner-generation compare-and-swap
- A write-ahead tool-effect ledger with duplicate-dispatch protection
- Operator visibility into effects left pending by an interrupted process
- Multi-process CLI session resume against the real runtime

## Current package decisions

1. `domain`, `protocol`, `ports`, and `runtime` are genuine reusable kernel
   boundaries.
2. Provider, local-tool, SQLite, CLI, and contract-support implementations are
   modules in `hermesd`, not one crate per directory.
3. Create another crate only for a demonstrated second consumer,
   optional-dependency boundary, separately deployed executable, or independent
   distribution requirement.
4. Keep provider replay data on semantic messages until a concrete store or
   transport constraint requires a sidecar.
5. Persist only complete replayable turns; failures never leave an unresolved
   assistant tool request in session history.
6. Keep task and lease types provisional until coordination is the active
   milestone.

## Deferred

- Reconciliation policy for effects left pending by a crash
- Branching a new lineage when a user intentionally changes model or manifest
- Public gateway/JSON-RPC integration with existing Hermes clients
- Task claims, leases, fencing, inbox/outbox delivery, and coordinator recovery
- Reading, writing, or migrating an existing Python Hermes database
- A native plugin ABI
