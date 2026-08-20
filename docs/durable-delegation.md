# Durable delegation contract

Hermes RS separates three facts that the Python implementation currently
combines in one background registry:

1. a child was durably accepted;
2. a particular worker owns its current execution; and
3. a terminal result is waiting to enter the parent at a legal new-turn
   boundary.

Those facts have different replay rules and therefore live in different state
transitions.

```text
                    claim(expected generation)
pending ─────────────────────────────────────────> running
   │                                                 │
   │ safe to start after restart                     │ finish(generation + fence)
   │                                                 │
   │                                  ┌──────────────┴──────────────┐
   │                                  │                             │
   └──────────────────────────────> terminal + completion outbox <──┘
                                      outcome_unknown on lease expiry

completion pending ── claim ──> claimed ── acknowledge ──> delivered
                         └──── release/expiry ────┘
```

## Immutable identity

A `DelegationSpec` binds one delegation ID and one completion event ID to an
exact parent session and a dedicated child session. Both sessions already have
immutable engine and prompt manifests. The task goal and optional context are
captured with that relationship; parent conversation history is not copied
into the child.

The dedicated child session is the future replay boundary. Restarting a child
against newly generated prompt bytes, a changed model, or a different tool
catalog is not permitted.

## Ownership

Creation writes `pending` at owner generation one before any worker may run.
A worker claims that exact generation, advances it, records its worker ID,
mints a nonzero fencing token, and receives a lease. Every heartbeat and
terminal write supplies both the expected current generation and that fencing
token. A stale worker can therefore neither extend nor finish a run after
another authoritative transition.

The first implementation does not reclaim an expired `running` child. Model
inference may be billable, and later capability sets may include effects whose
outcome cannot be inferred after a crash. Lease expiry therefore records
`outcome_unknown`. Only work still in `pending` is automatically safe to start
after a restart. Replay of a running child will require an explicit policy
proving its complete frozen capability set is replay-safe.

## Terminal and delivery atomicity

A worker terminal or lease reconciliation updates the delegation and inserts
its one `DelegationCompletion` in the same SQLite transaction. A crash can
leave both absent or both committed, never a terminal child with a lost
completion.

Completion delivery is a separate leased claim keyed by the immutable event
ID. Competing CLI, gateway, or daemon consumers may inspect the same outbox,
but only the current claim holder can acknowledge it. A failed consumer
releases its claim; an abandoned claim becomes available after its deadline.
Acknowledgement means a host accepted the event at a legal new-turn boundary,
not merely that it read the row.

## Current integration boundary

The SQLite state machine, fencing, reconciliation, schema migration, and
completion outbox are implemented. The existing model-facing delegation path
remains synchronous and leaf-only until a long-lived host can:

- create the dedicated child session and durable spec before returning a
  dispatch handle;
- claim and heartbeat the child while it runs;
- stop heartbeats before committing one terminal; and
- claim the completion and inject it as one idempotent new turn for the exact
  parent session.

Steering, cancellation requests, nested orchestrators, and retry of an expired
running child remain later state transitions. They will extend this concrete
lifecycle instead of introducing a generic workflow or Kanban task model.
