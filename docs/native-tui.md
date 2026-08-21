# Native TUI milestone

This document defines the first native Rust client. It records UX patterns from
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
| Interrupt a turn | `session.interrupt` | Use cooperative Codex interruption |
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

## Implemented proof

`hermesd tui` launches `hermesd gateway` as an exact-argv child and consumes
only the protocol above. Its reducer and renderer have behavior tests. A
PTY-level process test creates a session, submits a turn, renders a terminal
approval, denies it, verifies the command did not run, reloads the committed
transcript, exits, starts an entirely new TUI and gateway process, and resumes
the same kernel-owned history.

## Real dogfood result

The client was also exercised against the locally authenticated Codex app
server rather than a fake worker:

- A running turn was interrupted from the TUI. The worker stopped and the
  canonical session remained resumable without an uncommitted binding.
- A subsequent turn streamed to completion and was reconstructed from the
  canonical transcript.
- Codex proposed an exact terminal command through the Hermes-hosted dynamic
  tool. The TUI rendered the command and denied it; the target file was absent
  and the effect ledger had no pending entry afterward.
- A fresh TUI and gateway process listed the four committed messages and
  resumed the same transcript from SQLite.

Dogfooding exposed two client integration defects, both fixed in the first
milestone: long drafts now scroll and clamp the terminal cursor, and the frozen
Codex prompt now distinguishes its read-only worker sandbox from an approved
Hermes-hosted terminal action. No new protocol object was needed.
