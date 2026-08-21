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
- Leaf-only delegation through an isolated nested runtime, with child effects
  journaled under their own scopes
- Durable delegation claims with owner generations, worker fencing, lease-expiry
  reconciliation, and an atomically written completion outbox
- A minimal long-lived stdio JSON-RPC gateway with an end-to-end
  create/prompt/event/persist/resume proof
- Live runtime event observation with fixture-wide sequence equality and a
  pre-terminal delivery proof
- Cooperative foreground interruption with cleanup-before-terminal ordering
  and durable preservation of any unresolved tool-effect plan
- Generation-guarded foreground claims, atomic session/turn completion, and a
  real process-death restart proof that reconciles to `outcome_unknown` without
  replay
- Durable gateway delegation with atomic child/spec acceptance, leased and
  fenced workers, heartbeats, atomic child/outbox completion, exactly-once
  next-turn delivery, and real normal/crash process proofs

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
6. Do not retain Kanban-shaped task states without a runtime consumer; the
   first durable subagent supervisor will define its lifecycle from observed
   parent/child behavior.

## Deferred

- Reconciliation policy for effects left pending by a crash
- Steering and an explicitly proven replay-safe restart policy
- Branching a new lineage when a user intentionally changes model or manifest
- A polished `hermes --tui` launcher/config path for Rust (the companion Ink
  fork now has a direct developer process-selection seam)
- Reading, writing, or migrating an existing Python Hermes database
- A native plugin ABI
