//! Tool-effect planning and terminal dispositions.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{ToolArguments, ToolCallId, ToolResultStatus};

/// Observable side-effect class used by approval and replay policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    /// Reads state without changing it.
    ReadOnly,
    /// Performs a potentially billable model inference without granting child mutation rights.
    ModelInference,
    /// Mutates only the local machine or workspace.
    LocalMutation,
    /// Mutates a remote system.
    ExternalMutation,
    /// Starts, stops, or otherwise controls a process.
    ProcessControl,
    /// Uses a credential to access a protected capability.
    CredentialUse,
}

/// User or policy decision attached to a planned invocation.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRecord {
    /// Whether policy required an explicit decision.
    pub required: bool,
    /// Stable decision name, currently `allow` or `deny`.
    pub decision: String,
    /// Principal that supplied the decision.
    pub principal: String,
}

impl ApprovalRecord {
    /// Whether this decision forbids dispatching the effect.
    #[must_use]
    pub fn denied(&self) -> bool {
        self.decision == "deny"
    }
}

/// A validated tool call plus its execution policy and deduplication identity.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedToolCall {
    /// Provider-visible tool-call identity.
    pub call_id: ToolCallId,
    /// Frozen catalog name.
    pub name: String,
    /// Validated call arguments.
    pub arguments: ToolArguments,
    /// Stable per-invocation deduplication identity.
    pub execution_key: String,
    /// Effect classification used by approval and replay policy.
    pub effect: ToolEffect,
    /// Approval record when policy required a decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<ApprovalRecord>,
}

/// Final observed or reconciliatory outcome of one planned tool call.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolTerminal {
    /// Provider-visible tool-call identity.
    pub call_id: ToolCallId,
    /// Frozen catalog name.
    pub name: String,
    /// Terminal or reconciliation status.
    pub status: ToolResultStatus,
    /// Human- and provider-visible result payload.
    pub content: String,
    /// Stable per-invocation deduplication identity.
    pub execution_key: String,
    /// Effect classification copied from the plan.
    pub effect: ToolEffect,
    /// Optional receipt supplied by a successful implementation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<String>,
}
