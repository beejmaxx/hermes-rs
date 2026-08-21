//! Process-local waiters for gateway approvals.
//!
//! The durable effect ledger owns plans and decisions. This module only pairs
//! an already-journaled pending decision with the live JSON-RPC response that
//! can unblock its current process.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use domain::{SessionId, ToolCallId};
use thiserror::Error;
use tokio::sync::oneshot;

/// Final decision supplied by the connected session principal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalDecision {
    /// Dispatch this one planned invocation.
    Allow,
    /// Return a rejected terminal without dispatch.
    Deny,
}

impl ApprovalDecision {
    /// Stable domain decision name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

/// A live approval registry operation violated its routing contract.
#[derive(Debug, Error)]
pub enum ApprovalControlError {
    /// The process-local registry lock was poisoned.
    #[error("approval registry lock poisoned")]
    Poisoned,
    /// The same call was registered twice for one session.
    #[error("approval is already pending for tool call {0}")]
    AlreadyPending(ToolCallId),
    /// The client sent a choice this backend does not advertise.
    #[error("unsupported approval choice {0:?}; expected once or deny")]
    InvalidChoice(String),
}

struct PendingApproval {
    call_id: ToolCallId,
    sender: oneshot::Sender<ApprovalDecision>,
}

/// Cloneable gateway control plane for pending per-session decisions.
#[derive(Clone, Default)]
pub struct ApprovalControl {
    pending: Arc<Mutex<HashMap<String, VecDeque<PendingApproval>>>>,
}

impl ApprovalControl {
    /// Register one waiter before its durable plan is exposed to the client.
    pub fn register(
        &self,
        session_id: &SessionId,
        call_id: ToolCallId,
    ) -> Result<oneshot::Receiver<ApprovalDecision>, ApprovalControlError> {
        let mut pending = self.pending.lock().map_err(|_| ApprovalControlError::Poisoned)?;
        let queue = pending.entry(session_id.as_str().into()).or_default();
        if queue.iter().any(|approval| approval.call_id == call_id) {
            return Err(ApprovalControlError::AlreadyPending(call_id));
        }
        let (sender, receiver) = oneshot::channel();
        queue.push_back(PendingApproval { call_id, sender });
        Ok(receiver)
    }

    /// Resolve the oldest approval shown for one session.
    pub fn respond(
        &self,
        session_id: &SessionId,
        choice: &str,
    ) -> Result<bool, ApprovalControlError> {
        let decision = match choice {
            "once" => ApprovalDecision::Allow,
            "deny" => ApprovalDecision::Deny,
            other => return Err(ApprovalControlError::InvalidChoice(other.into())),
        };
        let sender = {
            let mut pending = self.pending.lock().map_err(|_| ApprovalControlError::Poisoned)?;
            let Some(queue) = pending.get_mut(session_id.as_str()) else {
                return Ok(false);
            };
            let sender = queue.pop_front().map(|approval| approval.sender);
            if queue.is_empty() {
                pending.remove(session_id.as_str());
            }
            sender
        };
        Ok(sender.is_some_and(|sender| sender.send(decision).is_ok()))
    }

    /// Remove a waiter whose turn ended before a response arrived.
    pub fn remove(&self, session_id: &SessionId, call_id: &ToolCallId) {
        if let Ok(mut pending) = self.pending.lock()
            && let Some(queue) = pending.get_mut(session_id.as_str())
        {
            queue.retain(|approval| approval.call_id != *call_id);
            if queue.is_empty() {
                pending.remove(session_id.as_str());
            }
        }
    }

    /// Deny and release every live waiter for one interrupted session.
    pub fn deny_session(&self, session_id: &SessionId) {
        let approvals =
            self.pending.lock().ok().and_then(|mut pending| pending.remove(session_id.as_str()));
        if let Some(approvals) = approvals {
            for approval in approvals {
                let _ = approval.sender.send(ApprovalDecision::Deny);
            }
        }
    }

    /// Deny every waiter before the gateway stops reading responses.
    pub fn deny_all(&self) {
        let approvals = self.pending.lock().ok().map(|mut pending| std::mem::take(&mut *pending));
        if let Some(approvals) = approvals {
            for queue in approvals.into_values() {
                for approval in queue {
                    let _ = approval.sender.send(ApprovalDecision::Deny);
                }
            }
        }
    }
}
