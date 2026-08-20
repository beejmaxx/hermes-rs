# Foundation review checkpoint

This checkpoint is resolved for the first offline runtime milestone. The
decisions below keep the foundation narrow while allowing behavior work to
begin.

## Included

- Workspace and dependency direction
- Opaque profile/session/lineage/run/task/worker/board/event/tool-call IDs
- Nonzero owner-generation and fence-token types
- Provider-neutral semantic messages and conversation-order validation
- Tool result dispositions including `outcome_unknown`
- Immutable engine, system-prompt digest, and ordered-tool-catalog digest
- Provisional durable task states, transition validation, and worker leases
- Typed v1 contract headers/outcomes and an exact-byte bundle reader
- Offline CLI shell for contract verification

## Deliberately not included

- Vendored Python-oracle fixtures
- Agent loop or scripted provider/tool execution
- Provider, tool, database, gateway, plugin, or process implementations
- Async runtime selection
- SQLite schema
- Public JSON-RPC API
- Migration or coexistence code

## Decisions

1. Keep `domain`, `protocol`, `ports`, and `testkit`; the offline runtime is the
   first concrete consumer that will populate `ports`.
2. Keep opaque provider replay data attached to semantic messages until a real
   store proves a sidecar is necessary.
3. Keep `Conversation` as the replayable view and emit in-flight execution
   ledger operations separately as persistence intents.
4. A user-tail conversation is valid after cancellation or failure; an
   unresolved assistant tool request is never replayable.
5. Task and lease types remain provisional and receive no further work before
   the single-agent runtime is usable.
6. Database-clock lease expiry is provisional until the store becomes a real
   consumer.
7. Add opaque identifiers only when a concrete invariant requires them; do not
   wrap every string preemptively.
8. `serde_json::Value` remains limited to tool arguments, provider replay, and
   recorded protocol/test payloads.
9. Keep `ports`, but define only the provider and tool traits consumed by the
   offline runtime.
10. Keep terse private package names; diagnostics and dependency declarations
    are clear within this workspace.

## Review gate

The written dispositions above open the gate for pinned fixtures, scripted
adapters, and the offline agent kernel. Live providers, real effects, storage,
gateway work, and coordination remain outside this checkpoint.
