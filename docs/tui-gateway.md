# Minimal TUI gateway contract

`hermesd gateway` is the first long-lived host for the Rust runtime. It speaks
the newline-delimited JSON-RPC 2.0 framing already used between Hermes's Ink
TUI and its Python gateway:

```text
Ink client  -- stdin requests / stdout responses and events -->  hermesd
                                                               ├── runtime
                                                               ├── provider adapter
                                                               └── SQLite state
```

The transport is an adapter. JSON values are accepted at this boundary, then
session identities, immutable runtime settings, semantic messages, generation
checks, provider behavior, tool effects, and persistence continue through the
same typed kernel used by `hermesd chat`.

## Framing

A client request is one JSON object followed by a newline:

```json
{"jsonrpc":"2.0","id":"r1","method":"setup.status","params":{}}
```

A correlated response contains either `result` or `error`. Uncorrelated events
use the existing Hermes envelope:

```json
{"jsonrpc":"2.0","method":"event","params":{"type":"gateway.ready","payload":{"backend":"hermes-rs"}}}
```

The wire structs live in `protocol`. `serde_json::Value` is intentionally
confined to this compatibility edge rather than propagated into domain state.

## Supported slice

The initial contract implements only the methods needed to create a TUI chat,
submit a prompt, and reopen its durable transcript:

| Area | Methods |
| --- | --- |
| Startup | `setup.status`, `config.get`, `commands.catalog`, `wake.start` |
| Sessions | `session.create`, `session.resume`, `session.activate`, `session.close`, `session.list`, `session.active_list`, `session.most_recent`, `session.usage` |
| Input | `input.detect_drop`, `complete.slash`, `complete.path`, `terminal.resize`, `system.battery` |
| Turns | `prompt.submit` |

`prompt.submit` acknowledges with `{"status":"streaming"}` and emits
`message.start`, zero or more `message.delta`, tool/reasoning events,
`message.complete`, and `session.info`. The completed semantic turn is committed
with the session's expected owner generation before `message.complete` is
emitted. Provider deltas and tool lifecycle events cross a synchronous runtime
observer boundary as they occur; the runtime also retains that exact sequence
in its returned conformance log.

The child-process integration test drives this entire path through real stdio,
a local mocked OpenAI-compatible streaming endpoint, the actual runtime, and a
real SQLite database. It then resumes the session and checks the reconstructed
transcript rather than merely snapshotting response JSON.

## Run it directly

The gateway owns one immutable provider/model/root configuration for its
lifetime:

```bash
export OPENROUTER_API_KEY="..."
cargo run -p hermesd -- gateway \
  --provider openrouter \
  --model your-model-id \
  --root .
```

It writes protocol frames—not a human interface—to stdout and reserves stderr
for diagnostics. A normal user should launch it through a compatible client.

## Use it with the Ink TUI

The companion fork at
[`beejmaxx/hermes-agent`](https://github.com/beejmaxx/hermes-agent) adds an
explicit stdio process seam to the existing Ink client. Build `hermesd`, then
run the TUI from the sibling checkout:

```bash
cd /absolute/path/to/hermes-rs
cargo build -p hermesd

cd /absolute/path/to/hermes-agent
npm install --workspace ui-tui
export OPENROUTER_API_KEY="..."
npm start --workspace ui-tui -- \
  --gateway-command /absolute/path/to/hermes-rs/target/debug/hermesd \
  gateway \
  --provider openrouter \
  --model your-model-id \
  --root /absolute/path/to/your/workspace
```

Everything after the executable path is forwarded as an exact argument token
to `hermesd`; no shell parses the command. Add the global `--state PATH` before
the `gateway` subcommand when an isolated database is useful. Omitting
`--gateway-command` preserves the client's existing Python gateway behavior.

This developer path already has a cross-repository smoke proof covering the
real TypeScript client, the real Rust child process, a provider stream, a
terminal event, SQLite persistence, and session resume. A polished
`hermes --tui` configuration/launcher flow remains separate product work.

## Deliberate limitations

This is a usable vertical slice, not a claim of full gateway parity:

- Upstream Hermes still hard-codes `python -m tui_gateway.entry`; the companion
  fork has the explicit process seam, but the normal `hermes --tui` launcher
  does not expose it yet.
- Session creation currently persists an empty frozen lineage immediately;
  Python delays that row until the first prompt.
- Interrupt, steer/queue, approvals, image attachment, shell/slash execution,
  configuration mutation, and desktop/WebSocket methods are not implemented.
- A process death during an in-flight foreground turn preserves journaled tool
  effects but does not yet materialize a resumable interrupted-turn marker.

Unsupported methods return JSON-RPC `-32601`; the gateway does not silently
pretend that a capability exists. These gaps define follow-up behavior slices
and should be added only with an end-to-end consumer test.
