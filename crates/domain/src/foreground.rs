//! Durable foreground-turn identity and lifecycle states.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{ForegroundTurnId, SessionId};

/// Immutable user intent captured before a foreground provider request begins.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForegroundTurnSpec {
    /// Stable identity for this particular attempt.
    pub turn_id: ForegroundTurnId,
    /// Exact durable session whose generation authorizes the attempt.
    pub session_id: SessionId,
    /// User input needed to explain or explicitly recover abandoned work.
    pub prompt: String,
}

/// Final disposition of one foreground attempt.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ForegroundTurnTerminal {
    /// The semantic user/assistant turn was atomically committed to its session.
    Completed,
    /// An authorized client cancelled the attempt.
    Interrupted {
        /// Stable human-readable cancellation reason.
        reason: String,
    },
    /// The host observed a known failure before a complete turn could commit.
    Failed {
        /// Stable human-readable failure reason.
        reason: String,
    },
    /// The owning process disappeared, so safe replay cannot be inferred.
    OutcomeUnknown {
        /// Reconciliation evidence explaining why the outcome is unknown.
        reason: String,
    },
}

impl ForegroundTurnTerminal {
    /// Stable persistence and display name for this disposition.
    #[must_use]
    pub const fn status_name(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Interrupted { .. } => "interrupted",
            Self::Failed { .. } => "failed",
            Self::OutcomeUnknown { .. } => "outcome_unknown",
        }
    }
}

/// Current lifecycle of one durable foreground attempt.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ForegroundTurnState {
    /// The owning gateway may still be running provider or tool work.
    Running,
    /// Final outcome; no later mutation may rewrite this attempt.
    Terminal {
        /// Complete terminal disposition.
        outcome: ForegroundTurnTerminal,
        /// Wall-clock terminal timestamp in Unix milliseconds.
        completed_at_ms: u64,
    },
}

impl ForegroundTurnState {
    /// Whether the lifecycle can no longer accept host mutations.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Terminal { .. })
    }
}
