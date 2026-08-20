# Hermes RS

An experimental, contract-first Rust sketch of the Hermes agent backend.

This is not currently a drop-in replacement for `hermes-agent`, and parity is
not the first milestone. The project exists to test whether a typed Rust kernel
can make Hermes sessions, tool effects, durable coordination, and recovery
materially easier to reason about.

The existing Python project remains the behavioral oracle. This repository
consumes versioned, effect-free contract fixtures and does not import or execute
Python at runtime.

## Initial boundary

Rust is expected to own the invariant-heavy narrow waist:

- semantic conversations and provider projection;
- session and run state machines;
- prompt and tool-catalog manifests;
- approval and tool-effect ledgers;
- persistence ownership and migration;
- task leases, fencing, inbox/outbox delivery, and recovery;
- backend protocol validation and process supervision.

TypeScript clients remain TypeScript. Python remains a valid supervised edge
for niche providers, platform adapters, media/ML integrations, and plugins that
cannot mutate authoritative state directly.

## Workspace

- `domain`: pure identifiers, semantic messages, and state transitions
- `protocol`: versioned serializable boundary types
- `ports`: traits owned by the kernel rather than adapters
- `runtime`: effect-free agent loop over provider and tool ports
- `testkit`: readers and helpers for the shared contract corpus
- `apps/hermes`: offline developer CLI; no live model or tool effects yet

## Development

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p hermes-cli -- contract check contracts/hermes-v1
```

The first go/no-go proof is a bounded, effect-free single-agent turn. The Rust
runtime must execute every pinned `agent_turn` fixture and reproduce its
provider requests, semantic conversation, persistence intents, public events,
usage, and terminal outcome. The next milestone adds one live provider and
read-only local tools; durable coordination follows only after the single-agent
runtime is usable.
