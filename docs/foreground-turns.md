# Durable foreground-turn contract

A foreground prompt has two independently durable consequences: a complete
semantic conversation turn and zero or more tool effects. Process death can
happen between any two writes, so `hermesd gateway` claims every accepted turn
before starting provider work and never infers that an abandoned claim is safe
to replay.

```text
                         complete(expected session generation)
running claim ───────────────────────────────────────────────────> completed
      │                                                                  │
      │ interrupt / observed failure                                     │
      ├──────────────────────────────> interrupted / failed               │
      │                                                                  │
      └──── owning process disappears ── startup reconciliation ──> outcome_unknown
```

## Identity and authority

Every attempt has a unique `ForegroundTurnId` and an immutable
`ForegroundTurnSpec` containing its exact session and user prompt. The claim
also freezes the session owner generation that authorized it. A session may
have only one `running` attempt, but an interrupted attempt does not advance the
conversation generation, so a later explicit prompt can create a new attempt
against that same generation.

The prompt is kept outside semantic conversation history until completion.
That preserves strict role alternation and avoids presenting an unresolved user
tail to the next provider request.

## Atomic completion

Successful completion performs these writes in one immediate SQLite
transaction:

1. validate the claim is still `running` at its frozen owner generation;
2. validate and append the complete user-through-assistant semantic turn;
3. advance the session owner generation; and
4. terminalize the claim as `completed`.

The transaction commits all four facts or none. A client therefore never sees
`message.complete` before the session and claim are durable.

Interrupt and observed failure terminalize the claim without adding session
messages or advancing its generation. Tool effects remain governed by their
separate write-ahead ledger: a dispatched effect interrupted before its
terminal remains visibly `planned`, not silently retried or discarded.

## Crash reconciliation

The gateway holds an OS-released exclusive lease beside its state database, so
a second gateway cannot reconcile work still owned by the first. A crash
releases that lease without requiring lock-file cleanup. On startup the new
owner converts claims still marked `running` to `outcome_unknown` before emitting
`gateway.ready`. It does not submit the prompt again. `session.resume` returns:

- the unchanged committed semantic transcript;
- display-only rows containing the abandoned prompt and recovery warning; and
- structured `recovery` metadata with the turn ID, timestamps, reason, and
  `auto_replayed: false`.

This is deliberately conservative. A provider request may have incurred cost,
and a tool may have changed an external system even when its response never
reached SQLite. Any future automatic retry policy must prove the entire frozen
capability set is replay-safe; it cannot reinterpret `outcome_unknown` as
`failed`.

The end-to-end proof starts a real delayed HTTP provider request, kills the
`hermesd` process, starts a fresh gateway against the same database, and checks
that the prompt is surfaced but neither committed nor replayed.
