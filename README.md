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
- `providers`: live OpenAI-compatible HTTP/SSE adapter
- `local-tools`: root-confined `read_file` and `search_files` broker
- `testkit`: readers and helpers for the shared contract corpus
- `apps/hermes`: contract and live single-turn developer CLI

## Development

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p hermes-cli -- contract check contracts/hermes-v1
```

## Try a live turn

The live path supports OpenAI, OpenRouter, and custom OpenAI-compatible
endpoints. Credentials are read only from the selected environment variable;
they are never accepted as command-line arguments. The only enabled tools are
read-only and confined to `--root`; common credential paths such as `.env`,
`.ssh`, and `.git` are denied even when they are beneath that root.

```bash
export OPENROUTER_API_KEY="..."
cargo run -p hermes-cli -- chat \
  --provider openrouter \
  --model your-model-id \
  --root . \
  "Read the project README and explain the architecture."
```

For a local server that needs no credential:

```bash
cargo run -p hermes-cli -- chat \
  --provider custom \
  --base-url http://127.0.0.1:11434/v1 \
  --model your-model \
  "Say hello."
```

The first go/no-go proof remains a bounded, deterministic single-agent turn.
The Rust runtime executes every pinned `agent_turn` fixture and reproduces its
provider requests, semantic conversation, persistence intents, public events,
usage, and terminal outcome. The live edge now exercises that same runtime;
durable sessions and coordination come next.
