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
   │ cancel(expected generation)                     │ cancel intent (same generation)
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

Cancellation follows the same ownership rules. Cancelling `pending` work
atomically writes a cancelled terminal and completion before it can be
dispatched. Cancelling `running` work records the reason and request timestamp
without transferring worker ownership or advancing its generation. From that
commit onward, the fenced worker's only legal terminal is `cancelled` with the
exact persisted reason; success, failure, and child-transcript writes are
rejected. The gateway then sends a best-effort live signal, while each worker
also polls the durable record so a registration race cannot lose the request.
If the owner disappears after that commit, startup or lease reconciliation
finishes the persisted cancellation instead of degrading it to
`outcome_unknown`.

The supervisor never reclaims an expired `running` child. Model inference may
be billable, and later capability sets may include effects whose outcome cannot
be inferred after a crash. Lease expiry therefore records `outcome_unknown`.
The gateway's exclusive OS lease also proves at startup that every owner left
by the prior process is abandoned, so restart reconciliation does not wait for
the wall-clock deadline. An abandoned run with durable cancellation intent
becomes `cancelled`; every other abandoned run becomes `outcome_unknown`. Only
work still in `pending` is automatically safe to start after a restart. Replay
of a running child requires an explicit policy proving its complete frozen
capability set is replay-safe.

## Terminal and delivery atomicity

A successful worker appends the complete child turn, terminalizes its fenced
delegation, and inserts its one `DelegationCompletion` in the same SQLite
transaction. Known failures and reconciliation atomically write the terminal
and completion. A crash can leave the entire transition absent or committed,
never a completed child with a lost outbox event.

Completion delivery is a separate leased claim keyed by the immutable event
ID. Competing CLI, gateway, or daemon consumers may inspect the same outbox,
but only the current claim holder can acknowledge it. A failed consumer
releases its claim; an abandoned claim becomes available after its deadline.
Acknowledgement means a host accepted the event at a legal new-turn boundary,
not merely that it read the row. The gateway renders every claimed completion
into the next explicit user-role provider prompt. Its foreground transaction
captures those exact prompt bytes and acknowledges all included event claims
together, so neither half can commit alone. The system prompt and all prior
messages remain byte-stable.

## Gateway integration

New gateway sessions freeze a background form of `delegate_task`. A call:

- atomically creates a dedicated immutable child session and `pending` spec;
- returns its deterministic durable handle to the parent immediately;
- is claimed by the process supervisor under an owner generation, worker ID,
  fencing token, and renewable lease;
- runs through the same provider-neutral runtime with only read-only tools;
- atomically commits the child turn, terminal, and completion outbox; and
- is delivered exactly once with the next explicit parent prompt.

The gateway exposes `delegation.list`, `delegation.status`, and
`delegation.cancel` for one exact parent session. These methods operate on the
same persisted snapshots used by the supervisor. A cancellation completion is
delivered through the ordinary outbox path, so control does not create a
second notification or prompt-injection mechanism.

The ordinary `hermesd chat` command retains its synchronous leaf behavior; its
frozen tool schema describes that different contract. The gateway never swaps
either catalog during a lineage.

Steering, nested orchestrators, and retry of an expired running child remain
later state transitions. They will extend this concrete lifecycle instead of
introducing a generic workflow or Kanban task model.
