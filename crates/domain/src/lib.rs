//! Pure types and state transitions for the Hermes Rust kernel.
//!
//! This crate has no transport, database, process, or async-runtime dependency.

mod conversation;
mod effect;
mod id;
mod prompt;
mod task;

pub use conversation::{
    Conversation, ConversationError, SemanticMessage, ToolArguments, ToolCall, ToolResult,
    ToolResultStatus,
};
pub use effect::{ApprovalRecord, PlannedToolCall, ToolEffect, ToolTerminal};
pub use id::{
    BoardId, EventId, FenceToken, IdError, LineageId, OwnerGeneration, ProfileId, RunId, SessionId,
    TaskId, ToolCallId, WorkerId,
};
pub use prompt::{EngineId, ManifestDigest, PromptManifest, PromptManifestError};
pub use task::{TaskLease, TaskState, TaskTransitionError};
