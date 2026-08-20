# Hermes RS

An experimental, contract-first Rust implementation of the Hermes agent backend.

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
- `runtime`: provider-neutral agent loop over provider and tool ports
- `hermesd`: executable, CLI, SQLite state, contract support, and concrete
  OpenAI-compatible and root-confined tool adapters

Crates are created only for real dependency boundaries. Concrete adapters stay
as modules in `hermesd` until another executable, an optional dependency, or an
independently distributed component needs them. Subsystem names alone are not
a reason to create a package.

## Development

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p hermesd -- contract check contracts/hermes-v1
```

## Try a live turn

The live path supports OpenAI, OpenRouter, and custom OpenAI-compatible
endpoints. Credentials are read only from the selected environment variable;
they are never accepted as command-line arguments. The only enabled tools are
read-only and confined to `--root`; common credential paths such as `.env`,
`.ssh`, and `.git` are denied even when they are beneath that root.

```bash
export OPENROUTER_API_KEY="..."
cargo run -p hermesd -- chat \
  --provider openrouter \
  --model your-model-id \
  --root . \
  "Read the project README and explain the architecture."
```

For a local server that needs no credential:

```bash
cargo run -p hermesd -- chat \
  --provider custom \
  --base-url http://127.0.0.1:11434/v1 \
  --model your-model \
  "Say hello."
```

Add `--session NAME` to create a durable lineage or resume it in a later
process. Its provider, model, prompt, tool catalog, and root are immutable:

```bash
cargo run -p hermesd -- chat --session demo \
  --provider openrouter --model your-model-id --root . \
  "Inspect this repository."
cargo run -p hermesd -- chat --session demo "What did you find?"
cargo run -p hermesd -- session list
```

State defaults to `~/.hermes-rs/state.db`; use the global `--state PATH`
option for an isolated database. The runtime writes every tool plan to its
effect ledger before dispatch and records its terminal result afterward.

The bounded, deterministic single-agent proof and the live durable path use
the same runtime. Recovery of interrupted pending effects and fenced
multi-agent coordination are the next milestones.
