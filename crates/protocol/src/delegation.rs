//! Serializable durable-delegation and completion-outbox records.

use domain::{
    CompletionEventId, DelegationId, DelegationSpec, DelegationState, DelegationTerminal,
    OwnerGeneration,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Fully materialized durable background-delegation record.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationSnapshot {
    /// Immutable relationship and task input.
    pub spec: DelegationSpec,
    /// Optimistic generation guarding every authoritative mutation.
    pub owner_generation: OwnerGeneration,
    /// Current leased or terminal lifecycle.
    pub state: DelegationState,
    /// Wall-clock creation timestamp in Unix milliseconds.
    pub created_at_ms: u64,
    /// Wall-clock last-mutation timestamp in Unix milliseconds.
    pub updated_at_ms: u64,
}

/// One durable, idempotent completion waiting for a legal new-turn delivery.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationCompletion {
    /// Idempotency key used by competing delivery consumers.
    pub event_id: CompletionEventId,
    /// Background run that produced this event.
    pub delegation_id: DelegationId,
    /// Self-contained immutable task and routing relationship.
    pub spec: DelegationSpec,
    /// Terminal child disposition.
    pub outcome: DelegationTerminal,
    /// Wall-clock completion timestamp in Unix milliseconds.
    pub completed_at_ms: u64,
}
