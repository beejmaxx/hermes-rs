# Architecture sketch

## Thesis

Hermes RS is a Rust kernel with replaceable edges, not a zero-Python goal. Rust
owns behavior whose failure can corrupt a conversation, duplicate an effect,
create a second writer, or lose task ownership. Integrations may use any
language behind versioned, capability-limited boundaries.

## Dependency direction

```text
domain
  ^
  |
protocol
  ^
  |
ports <--------- runtime
  ^                 ^
  |                 |
  +---------- hermesd
               ├── CLI
               ├── contract corpus support
               └── adapters: OpenAI-compatible HTTP, local tools, SQLite
```

`domain`, `protocol`, `ports`, and `runtime` are separate because they are
kernel APIs that future executables and adapter packages can consume without
linking HTTP, filesystem, SQLite, or CLI dependencies. Concrete adapters are
ordinary modules in `hermesd`; they become crates only when a real second
consumer, optional-dependency boundary, or distribution boundary appears. The
domain never imports transport or implementation code.

## First proof

The conformance suite remains offline. It reads the same deterministic fixtures
as the Python reference oracle and validates semantic conversation, provider
request, persistence intent, public event, usage, and terminal outcome records.
No test calls a live provider or performs a filesystem, process, credential, or
network effect.

The first executable proof is the effect-free single-agent loop:

1. consume a scripted provider stream;
2. assemble fragmented text, reasoning, and tool calls;
3. plan and execute scripted tools without real effects;
4. retain terminal tool outcomes in completion order;
5. project result batches in original model call order;
6. apply fallback only before visible output;
7. classify cancellation, malformed, and truncated streams; and
8. reproduce the pinned Python oracle outcomes exactly.

## First live edge

The developer CLI can run that same loop against an OpenAI-compatible streaming
endpoint. Provider JSON and SSE normalization live in the `hermesd` adapter
module, below the kernel-owned `Provider` trait. The provider adapter strips
internal replay and execution metadata before sending tool-result messages
over the wire.

The only direct workspace tools are `read_file` and `search_files`. The
local-tools adapter
canonicalizes every requested path under one immutable root, blocks common
credential and repository-internal paths, does not follow directory symlinks
while walking, bounds input and output sizes, and always classifies plans as
`read_only`. Unknown or invalid calls become typed failed terminals so the
model can recover; they are never dynamically dispatched.

## Durable single-agent state

`hermesd` now stores sessions and tool-effect records in SQLite. A session
freezes its engine, provider, model, system prompt, ordered tool catalog, and
tool root. Only complete user/assistant turns are appended. Every append uses
an expected owner generation in one immediate transaction, so stale writers
cannot partially extend the conversation.

The journaled tool broker records an entire planned batch before executing any
call, then records exactly one terminal result for each invocation. A process
crash may therefore leave a visible `planned` record, but it cannot silently
dispatch an unrecorded effect. `hermesd effect pending` exposes those records
without guessing whether they are safe to retry. The API key value is never
persisted; only the name of its credential environment variable is part of
session configuration.

The durable proof currently covers:

1. create an immutable session lineage and prompt manifest;
2. persist a planned tool invocation before dispatch;
3. persist each terminal outcome in actual completion order;
4. materialize the provider result batch in original call order; and
5. resume a session from another CLI process without rebuilding its manifest.

The next durable proof will reconcile interrupted pending effects. Durable
multi-agent work then adds run-scoped leases and fencing, stale-worker
rejection, and one deduplicated foreground or background delivery.

## Leaf delegation

New parent sessions advertise one additional `delegate_task` tool. It launches
a focused child turn through the same provider-neutral runtime, but with a
fresh conversation, a task-specific system prompt, the parent's frozen
provider/model and workspace root, and only `read_file`/`search_files`. The
child cannot delegate, interact with the user, or mutate the workspace.

The delegation invocation is journaled as `model_inference`; any child tool
calls are separately journaled beneath a deterministic child execution scope.
The parent receives only a bounded final summary. Multiple delegation calls in
one provider response are polled concurrently, while the runtime still
projects their tool results back to the provider in original call order.

This is deliberately synchronous and leaf-only. Background delivery,
steering, cancellation, reconnect-after-restart, and durable worker leases are
not inferred from the old Kanban workflow; they will be added around this
concrete child lifecycle.

## Explicit non-goals for the sketch

- Full provider, platform, or plugin parity
- Desktop, web, or Ink rewrites
- Reading or writing a user's existing Hermes database
- A public native Rust plugin ABI
- General workflow-engine abstractions without a Hermes consumer
