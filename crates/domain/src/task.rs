//! Durable coordination state and fencing leases.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{FenceToken, RunId, TaskId, WorkerId};

/// Durable lifecycle of a coordination task.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// Submitted but not yet admitted for execution.
    Proposed,
    /// Eligible for a worker claim.
    Ready,
    /// Claimed under a lease but not yet executing.
    Claimed,
    /// Actively executing.
    Running,
    /// Waiting for an explicit input response.
    WaitingForInput,
    /// Awaiting review.
    Review,
    /// Blocked on a declared dependency or condition.
    Blocked,
    /// Successfully completed.
    Completed,
    /// Failed with a recorded disposition.
    Failed,
    /// Cancelled.
    Cancelled,
    /// Retained only as history.
    Archived,
}

/// A requested durable task transition is not legal.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("illegal task transition from {from:?} to {to:?}")]
pub struct TaskTransitionError {
    /// Current durable state.
    pub from: TaskState,
    /// Requested durable state.
    pub to: TaskState,
}

impl TaskState {
    /// Validate a transition without mutating durable state.
    pub fn transition_to(self, next: Self) -> Result<Self, TaskTransitionError> {
        let legal = matches!(
            (self, next),
            (Self::Proposed, Self::Ready | Self::Cancelled | Self::Archived)
                | (Self::Ready, Self::Claimed | Self::Cancelled | Self::Archived)
                | (Self::Claimed, Self::Running | Self::Ready | Self::Cancelled)
                | (
                    Self::Running,
                    Self::WaitingForInput
                        | Self::Review
                        | Self::Blocked
                        | Self::Completed
                        | Self::Failed
                        | Self::Cancelled
                )
                | (Self::WaitingForInput, Self::Running | Self::Failed | Self::Cancelled)
                | (Self::Review, Self::Running | Self::Completed | Self::Failed | Self::Cancelled)
                | (Self::Blocked, Self::Ready | Self::Cancelled | Self::Archived)
                | (Self::Failed, Self::Ready | Self::Archived)
                | (Self::Completed | Self::Cancelled, Self::Archived)
        );
        if legal { Ok(next) } else { Err(TaskTransitionError { from: self, to: next }) }
    }

    /// Whether the task can no longer execute without an explicit retry transition.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled | Self::Archived)
    }
}

/// The only permit with which a task worker may mutate coordination state.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TaskLease {
    /// Durable task being executed.
    pub task_id: TaskId,
    /// Particular attempt to execute the task.
    pub run_id: RunId,
    /// Supervised worker holding the lease.
    pub worker_id: WorkerId,
    /// Monotonic token checked by every worker mutation.
    pub fence: FenceToken,
    /// Expiry according to the authoritative database clock.
    pub expires_at_unix_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::TaskState;

    #[test]
    fn ordinary_task_path_is_legal() {
        let states = [
            TaskState::Ready,
            TaskState::Claimed,
            TaskState::Running,
            TaskState::Review,
            TaskState::Completed,
        ];
        let mut current = TaskState::Proposed;
        for next in states {
            current = current
                .transition_to(next)
                .unwrap_or_else(|error| unreachable!("known legal transition: {error}"));
        }
        assert!(current.is_terminal());
    }

    #[test]
    fn terminal_task_cannot_be_reclaimed() {
        assert!(TaskState::Completed.transition_to(TaskState::Claimed).is_err());
        assert!(TaskState::Archived.transition_to(TaskState::Ready).is_err());
    }

    #[test]
    fn retry_is_explicitly_a_new_ready_transition() {
        assert_eq!(TaskState::Failed.transition_to(TaskState::Ready), Ok(TaskState::Ready));
        assert!(TaskState::Failed.transition_to(TaskState::Running).is_err());
    }
}
