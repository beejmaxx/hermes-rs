//! Pure types and state transitions for the Hermes Rust kernel.
//!
//! This crate has no transport, database, process, or async-runtime dependency.

mod conversation;
mod delegation;
mod effect;
mod foreground;
mod id;
mod prompt;

pub use conversation::{
    Conversation, ConversationError, SemanticMessage, ToolArguments, ToolCall, ToolResult,
    ToolResultStatus,
};
pub use delegation::{
    DelegationAuthority, DelegationCancellation, DelegationSpec, DelegationState,
    DelegationTerminal,
};
pub use effect::{ApprovalRecord, PlannedToolCall, ToolEffect, ToolTerminal};
pub use foreground::{ForegroundTurnSpec, ForegroundTurnState, ForegroundTurnTerminal};
pub use id::{
    CompletionEventId, DelegationId, DelegationWorkerId, DeliveryClaimId, FencingToken,
    ForegroundTurnId, IdError, LineageId, OwnerGeneration, SessionId, ToolCallId,
};
pub use prompt::{EngineId, ManifestDigest, PromptManifest, PromptManifestError};
