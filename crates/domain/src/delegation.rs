//! Durable background-delegation identities and lifecycle states.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    CompletionEventId, DelegationId, DelegationWorkerId, FencingToken, OwnerGeneration, SessionId,
};

/// Generation and fencing proof held by one active delegation worker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationAuthority {
    /// Current authoritative lifecycle generation.
    pub owner_generation: OwnerGeneration,
    /// Token fencing every earlier worker owner.
    pub fencing_token: FencingToken,
}

/// Durable operator intent that constrains the current worker's only legal terminal.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationCancellation {
    /// Stable human-readable cancellation reason.
    pub reason: String,
    /// Wall-clock request timestamp in Unix milliseconds.
    pub requested_at_ms: u64,
}

/// Immutable relationship and task input captured before background dispatch.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationSpec {
    /// Stable identity for the background unit.
    pub delegation_id: DelegationId,
    /// Stable idempotency identity reserved for this run's one completion.
    pub completion_event_id: CompletionEventId,
    /// Exact durable conversation that commissioned the work.
    pub parent_session_id: SessionId,
    /// Dedicated immutable child session used to execute the work.
    pub child_session_id: SessionId,
    /// Self-contained child objective.
    pub goal: String,
    /// Optional additional context; parent history is never inherited.
    pub context: Option<String>,
}

/// Terminal disposition of one background child.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DelegationTerminal {
    /// The child returned its final bounded summary.
    Completed {
        /// Summary delivered back to the parent at a legal new-turn boundary.
        summary: String,
    },
    /// The child terminated with a known failure.
    Failed {
        /// Stable human-readable failure description.
        error: String,
    },
    /// An authorized operator or supervisor cancelled the child.
    Cancelled {
        /// Stable cancellation reason.
        reason: String,
    },
    /// The owner disappeared after dispatch, so safe replay cannot be inferred.
    OutcomeUnknown {
        /// Reconciliation evidence explaining why the outcome is unknown.
        reason: String,
    },
}

impl DelegationTerminal {
    /// Stable persistence and display name for this disposition.
    #[must_use]
    pub const fn status_name(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
            Self::OutcomeUnknown { .. } => "outcome_unknown",
        }
    }
}

/// Current lifecycle of one durable background delegation.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DelegationState {
    /// Durably accepted but not yet dispatched to a worker.
    Pending,
    /// Owned by one leased worker generation.
    Running {
        /// Process-scoped worker identity.
        worker_id: DelegationWorkerId,
        /// Token every worker mutation must present.
        fencing_token: FencingToken,
        /// Wall-clock lease deadline in Unix milliseconds.
        lease_expires_at_ms: u64,
        /// Persisted cancellation intent, when an operator has requested shutdown.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cancellation: Option<DelegationCancellation>,
    },
    /// Final outcome; no worker may mutate the run after this transition.
    Terminal {
        /// Complete terminal disposition.
        outcome: DelegationTerminal,
        /// Wall-clock terminal timestamp in Unix milliseconds.
        completed_at_ms: u64,
    },
}

impl DelegationState {
    /// Whether the lifecycle can no longer accept worker mutations.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal { .. })
    }
}
