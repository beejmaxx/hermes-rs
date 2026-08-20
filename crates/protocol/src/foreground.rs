//! Serializable durable foreground-turn records.

use domain::{ForegroundTurnSpec, ForegroundTurnState, OwnerGeneration};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Fully materialized durable foreground-turn attempt.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForegroundTurnSnapshot {
    /// Immutable attempt identity and user input.
    pub spec: ForegroundTurnSpec,
    /// Session authority generation frozen when the attempt was accepted.
    pub owner_generation: OwnerGeneration,
    /// Running or terminal lifecycle state.
    pub state: ForegroundTurnState,
    /// Wall-clock acceptance timestamp in Unix milliseconds.
    pub started_at_ms: u64,
    /// Wall-clock last-mutation timestamp in Unix milliseconds.
    pub updated_at_ms: u64,
}
