# Terminal approval contract

New `hermesd gateway` sessions freeze a `terminal` tool in addition to the
read-only workspace and delegation tools. The one-shot `hermesd chat` catalog
remains read-only. Existing durable sessions never gain the tool after their
manifest is created.

Every terminal call follows one ordered boundary:

```text
model call
  -> durable effect plan (approval = pending)
  -> approval.request event
  -> approval.respond (once | deny)
  -> durable final decision
  -> tool.start and process dispatch, or rejected terminal
  -> durable effect terminal
  -> provider tool result
```

The effect plan therefore exists before the client sees the prompt, and an
allow decision exists before the shell starts. A denial never spawns a
process. If the gateway is interrupted while waiting, the foreground turn is
terminalized without dispatch and the unresolved plan remains visible through
`hermesd effect pending`. A crash after allow but before the effect terminal
also leaves that plan visible; Hermes RS does not guess whether it is safe to
repeat.

The stdio event and response match the existing Ink client contract:

```json
{"method":"event","params":{"type":"approval.request","session_id":"...","payload":{"command":"cargo test","choices":["once","deny"],"allow_permanent":false}}}
{"id":"a1","jsonrpc":"2.0","method":"approval.respond","params":{"session_id":"...","choice":"once"}}
```

Only `once` and `deny` are advertised. Session-wide and permanent grants need
a durable policy model and are intentionally not inferred from the Python
implementation. One provider response may contain at most one terminal call,
so the current single-overlay Ink interaction cannot hide competing prompts.

## Security boundary

Approval is authorization, not an operating-system sandbox. The exact command
is shown before dispatch and starts in the frozen workspace root, but an
approved shell command can traverse outside that directory, mutate files,
start processes, and use the network. Credential-shaped environment variables
(`*_TOKEN`, `*_SECRET`, `*_API_KEY`, passwords, and similar names) are removed
from the child environment. This does not make arbitrary local files or
credential helpers inaccessible.

Commands receive no interactive stdin. Stdout and stderr are drained with a
fixed retained-memory budget, independently truncated, and returned to the
model. A bounded timeout defaults to two minutes. On Unix the shell and its
descendants run in a dedicated process group that is killed on timeout,
foreground interruption, or adapter drop; normally completed commands cannot
leave background descendants behind.
