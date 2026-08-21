# Native TUI milestone

This document bounds the first native Rust client. It records UX patterns from
the Codex TUI, T3 Code, and the Hermes Ink TUI, then maps those behaviors to the
existing `hermesd gateway` protocol.

The TUI is a disposable projection of kernel state. It must never become the
source of truth for sessions, turns, effects, approvals, or worker state.

## UX findings

The three clients converge on a small set of useful behaviors:

- Render assistant text incrementally, but reconstruct committed conversation
  history from `session.resume` after a turn completes or is interrupted.
- Show tool activity inline with the conversation. Present a compact tool name,
  summary, and terminal status instead of raw event JSON.
- Put approval requests next to the composer, keep the complete command or
  action reviewable, and make allow-once and deny unambiguous.
- While a turn is running, make interrupt the primary action and show that an
  interrupt is pending after it is requested.
- Keep the persistent layout small: scrollable conversation, one-line composer,
  and a status/footer line. Use a temporary overlay for session selection and
  approvals rather than permanent dashboards.
- Normalize engine-specific activity into shared session, message, tool,
  approval, and status views. The client must not branch on Codex.

The first client intentionally does not expose raw event streams, worker
topology, engine metadata, agent graphs, memory, budgets, or workflow design.
Those surfaces require demonstrated user needs.

## Keyboard behavior

- `Enter`: submit a non-empty draft.
- `Esc`: interrupt a running turn; deny a pending approval.
- `Ctrl-C`: clear a non-empty draft, interrupt while busy, or quit while idle.
- `Up` / `Down`, then `Enter`: choose an approval or a session.
- `PageUp` / `PageDown`: scroll the conversation.
- `F2`: reopen the session picker while idle.
- `n`: create a session from the session picker.
- `q`: quit from the session picker.

## Protocol inventory

| Screen behavior | Existing gateway surface | Decision |
| --- | --- | --- |
| List sessions | `session.list` | Use as-is |
| Create a session | `session.create` | Use as-is |
| Resume canonical history | `session.resume` | Replace local transcript projection |
| Submit input | `prompt.submit` | Use as-is |
| Stream assistant text | `message.start`, `message.delta`, `message.complete` | Render a temporary streaming item |
| Show reasoning activity | `reasoning.delta` | Show only a compact activity indication |
| Show tool activity | `tool.start`, `tool.complete` | Render compact inline rows |
| Request approval | `approval.request` | Render an approval overlay |
| Resolve approval | `approval.respond` | First client supports allow-once and deny |
| Interrupt a turn | `session.interrupt` | Harden Codex lifecycle before TUI work |
| Reflect lifecycle | `session.info` | Drive busy/interrupted/status projection |
| Recover after restart | `session.resume` recovery and inflight fields | Kernel remains authoritative |

No application-protocol addition is required for this milestone. The gateway
already serializes one foreground turn per session, so the first client does
not need a turn identifier in event frames to render one active session. A new
protocol object is justified only when a concrete screen behavior cannot be
implemented correctly from these surfaces.

## First layout

```text
+----------------------------------------------------------+
| conversation                                             |
|                                                          |
| user and assistant messages                              |
|  - tool: terminal.execute (running/completed)             |
|                                                          |
| [approval request replaces this area when present]       |
+----------------------------------------------------------+
| > single-line composer                                   |
| session | engine-neutral status | keys                   |
+----------------------------------------------------------+
```

The client may cache drafts, scroll position, and temporary streaming text.
Everything durable must be reloadable from a fresh gateway process and
`session.resume`.
