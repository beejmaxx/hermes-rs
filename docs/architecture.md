# Architecture sketch

## Thesis

Hermes RS is a Rust kernel with replaceable edges, not a zero-Python goal. Rust
owns behavior whose failure can corrupt a conversation, duplicate an effect,
create a second writer, or lose task ownership. Integrations may use any
language behind versioned, capability-limited boundaries.

## Dependency direction

```text
apps/hermes
    |
    +--> runtime --> ports ------> domain
    |       |
    |       +-----> protocol ----> domain
    |
    +--> testkit --> protocol
```

Future provider, store, tool, gateway, and coordination implementations depend
on `ports`. The domain never imports an implementation crate.

## First proof

The initial implementation remains offline. It reads the same deterministic
fixtures as the Python reference oracle and validates semantic conversation,
provider request, persistence intent, public event, usage, and terminal outcome
records. No test calls a live provider or performs a filesystem, process,
credential, or network effect.

The first executable proof is the effect-free single-agent loop:

1. consume a scripted provider stream;
2. assemble fragmented text, reasoning, and tool calls;
3. plan and execute scripted tools without real effects;
4. retain terminal tool outcomes in completion order;
5. project result batches in original model call order;
6. apply fallback only before visible output;
7. classify cancellation, malformed, and truncated streams; and
8. reproduce the pinned Python oracle outcomes exactly.

The later durable proof will add SQLite-backed sessions and coordination:

1. create an immutable session lineage and prompt manifest;
2. persist a planned tool invocation before dispatch;
3. persist each terminal outcome in actual completion order;
4. materialize the provider result batch in original call order;
5. delegate a child using a run-scoped lease and fencing token;
6. terminate and recover the coordinator;
7. reject writes from the stale worker; and
8. emit one foreground result or one deduplicated background delivery.

## Explicit non-goals for the sketch

- Full provider, platform, or plugin parity
- Desktop, web, or Ink rewrites
- Reading or writing a user's existing Hermes database
- A public native Rust plugin ABI
- General workflow-engine abstractions without a Hermes consumer
