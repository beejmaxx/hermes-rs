//! Provider-neutral semantic conversation records.

use std::collections::{BTreeMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::ToolCallId;

/// A JSON object containing model-provided tool arguments.
///
/// The payload remains structurally open because each frozen tool schema owns
/// its validation. It is the only dynamic value admitted to this domain model.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ToolArguments(pub BTreeMap<String, Value>);

/// One tool invocation requested by the model.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    /// Provider-visible call identity.
    pub id: ToolCallId,
    /// Frozen catalog name of the selected tool.
    pub name: String,
    /// Arguments validated against that frozen tool schema before dispatch.
    pub arguments: ToolArguments,
}

/// A terminal or reconciliatory disposition for one tool call.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    /// The tool returned successfully.
    Succeeded,
    /// The tool failed before a successful result was observed.
    Failed,
    /// The tool was cancelled.
    Cancelled,
    /// Required approval was denied.
    Rejected,
    /// An external effect may have occurred and must not be retried blindly.
    OutcomeUnknown,
    /// A trusted provider projection observed a result without executing it.
    Observed,
}

/// One provider-facing tool result.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    /// Identity of the assistant tool call this result satisfies.
    pub call_id: ToolCallId,
    /// Terminal or reconciliatory disposition.
    pub status: ToolResultStatus,
    /// Provider-facing result text.
    pub content: String,
    /// Stable key used to deduplicate an execution, when Hermes dispatched it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_key: Option<String>,
}

/// A provider-neutral, replayable conversation record.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SemanticMessage {
    /// User input at a legal turn boundary.
    User {
        /// Exact user-role content projected to the provider.
        content: String,
        /// Human-authored text when durable context was prepended to `content`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_content: Option<String>,
    },
    /// Assistant request for one or more tools.
    AssistantToolRequest {
        /// Optional assistant text accompanying the calls.
        content: Option<String>,
        /// Calls in provider order.
        calls: Vec<ToolCall>,
        /// Provider-visible reasoning text when the transport exposes it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning: Option<String>,
        /// Opaque provider replay data retained only at the protocol boundary.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_replay: Option<Value>,
    },
    /// A complete result batch ordered like the preceding assistant calls.
    ToolResultBatch {
        /// Results in original assistant-call order.
        results: Vec<ToolResult>,
    },
    /// A terminal assistant response for the current user turn.
    Assistant {
        /// User-visible response text.
        content: String,
        /// Provider-visible reasoning text when the transport exposes it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning: Option<String>,
        /// Opaque provider replay data retained only at the protocol boundary.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_replay: Option<Value>,
    },
}

/// A replayable semantic conversation whose ordering invariants were checked.
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct Conversation(Vec<SemanticMessage>);

/// A semantic conversation violates provider-independent ordering rules.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConversationError {
    /// A message kind was not legal at its position.
    #[error("message {index} has kind {actual}; expected {expected}")]
    UnexpectedMessage {
        /// Zero-based message position.
        index: usize,
        /// Human-readable expected state.
        expected: &'static str,
        /// Human-readable actual message kind.
        actual: &'static str,
    },
    /// A tool request contained no calls.
    #[error("assistant tool request at message {index} contains no calls")]
    EmptyToolRequest {
        /// Zero-based message position.
        index: usize,
    },
    /// A tool request contained a duplicate call identity.
    #[error("assistant tool request at message {index} contains duplicate call id {call_id}")]
    DuplicateToolCall {
        /// Zero-based message position.
        index: usize,
        /// Duplicated call identity.
        call_id: ToolCallId,
    },
    /// A tool name was empty.
    #[error("assistant tool request at message {index} contains an empty tool name")]
    EmptyToolName {
        /// Zero-based message position.
        index: usize,
    },
    /// A result batch did not exactly match its assistant request.
    #[error("tool result batch at message {index} does not match assistant call order")]
    ToolResultOrderMismatch {
        /// Zero-based message position.
        index: usize,
    },
    /// The conversation ended before an outstanding tool request was satisfied.
    #[error("conversation ends with an unresolved assistant tool request")]
    MissingToolResultBatch,
}

#[derive(Clone, Debug)]
enum ValidationState {
    Start,
    AwaitingAssistant,
    AwaitingToolResults(Vec<ToolCallId>),
    AfterToolResults,
    AfterAssistant,
}

impl Conversation {
    /// Validate and construct a semantic conversation.
    pub fn new(messages: Vec<SemanticMessage>) -> Result<Self, ConversationError> {
        let mut state = ValidationState::Start;

        for (index, message) in messages.iter().enumerate() {
            state = match (&state, message) {
                (
                    ValidationState::Start | ValidationState::AfterAssistant,
                    SemanticMessage::User { .. },
                ) => ValidationState::AwaitingAssistant,
                (
                    ValidationState::AwaitingAssistant | ValidationState::AfterToolResults,
                    SemanticMessage::Assistant { .. },
                ) => ValidationState::AfterAssistant,
                (
                    ValidationState::AwaitingAssistant | ValidationState::AfterToolResults,
                    SemanticMessage::AssistantToolRequest { calls, .. },
                ) => {
                    Self::validate_calls(index, calls)?;
                    ValidationState::AwaitingToolResults(
                        calls.iter().map(|call| call.id.clone()).collect(),
                    )
                }
                (
                    ValidationState::AwaitingToolResults(expected),
                    SemanticMessage::ToolResultBatch { results },
                ) => {
                    if results.iter().map(|result| &result.call_id).ne(expected.iter()) {
                        return Err(ConversationError::ToolResultOrderMismatch { index });
                    }
                    ValidationState::AfterToolResults
                }
                (current, actual) => {
                    return Err(ConversationError::UnexpectedMessage {
                        index,
                        expected: expected_message(current),
                        actual: message_kind(actual),
                    });
                }
            };
        }

        if matches!(state, ValidationState::AwaitingToolResults(_)) {
            return Err(ConversationError::MissingToolResultBatch);
        }
        Ok(Self(messages))
    }

    fn validate_calls(index: usize, calls: &[ToolCall]) -> Result<(), ConversationError> {
        if calls.is_empty() {
            return Err(ConversationError::EmptyToolRequest { index });
        }
        let mut seen = HashSet::with_capacity(calls.len());
        for call in calls {
            if call.name.is_empty() {
                return Err(ConversationError::EmptyToolName { index });
            }
            if !seen.insert(&call.id) {
                return Err(ConversationError::DuplicateToolCall {
                    index,
                    call_id: call.id.clone(),
                });
            }
        }
        Ok(())
    }

    /// Borrow the validated messages.
    #[must_use]
    pub fn messages(&self) -> &[SemanticMessage] {
        &self.0
    }

    /// Consume the wrapper and return its messages.
    #[must_use]
    pub fn into_messages(self) -> Vec<SemanticMessage> {
        self.0
    }
}

fn message_kind(message: &SemanticMessage) -> &'static str {
    match message {
        SemanticMessage::User { .. } => "user",
        SemanticMessage::AssistantToolRequest { .. } => "assistant_tool_request",
        SemanticMessage::ToolResultBatch { .. } => "tool_result_batch",
        SemanticMessage::Assistant { .. } => "assistant",
    }
}

fn expected_message(state: &ValidationState) -> &'static str {
    match state {
        ValidationState::Start | ValidationState::AfterAssistant => "user",
        ValidationState::AwaitingAssistant | ValidationState::AfterToolResults => {
            "assistant or assistant_tool_request"
        }
        ValidationState::AwaitingToolResults(_) => "tool_result_batch",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        Conversation, ConversationError, SemanticMessage, ToolArguments, ToolCall, ToolResult,
        ToolResultStatus,
    };
    use crate::ToolCallId;

    fn call(id: &str) -> ToolCall {
        ToolCall {
            id: ToolCallId::new(id).unwrap_or_else(|error| unreachable!("test id: {error}")),
            name: "read_file".into(),
            arguments: ToolArguments(BTreeMap::new()),
        }
    }

    fn result(id: &str) -> ToolResult {
        ToolResult {
            call_id: ToolCallId::new(id).unwrap_or_else(|error| unreachable!("test id: {error}")),
            status: ToolResultStatus::Succeeded,
            content: "ok".into(),
            execution_key: Some(format!("scenario:{id}")),
        }
    }

    #[test]
    fn accepts_parallel_calls_when_results_keep_call_order() {
        let messages = vec![
            SemanticMessage::User { content: "compare".into(), display_content: None },
            SemanticMessage::AssistantToolRequest {
                content: None,
                calls: vec![call("a"), call("b")],
                reasoning: None,
                provider_replay: None,
            },
            SemanticMessage::ToolResultBatch { results: vec![result("a"), result("b")] },
            SemanticMessage::Assistant {
                content: "done".into(),
                reasoning: None,
                provider_replay: None,
            },
        ];

        assert!(Conversation::new(messages).is_ok());
    }

    #[test]
    fn rejects_results_in_completion_order_when_call_order_differs() {
        let messages = vec![
            SemanticMessage::User { content: "compare".into(), display_content: None },
            SemanticMessage::AssistantToolRequest {
                content: None,
                calls: vec![call("a"), call("b")],
                reasoning: None,
                provider_replay: None,
            },
            SemanticMessage::ToolResultBatch { results: vec![result("b"), result("a")] },
        ];

        assert!(matches!(
            Conversation::new(messages),
            Err(ConversationError::ToolResultOrderMismatch { index: 2 })
        ));
    }

    #[test]
    fn permits_a_user_tail_after_an_interrupted_turn() {
        assert!(
            Conversation::new(vec![SemanticMessage::User {
                content: "hello".into(),
                display_content: None,
            }])
            .is_ok()
        );
    }

    #[test]
    fn rejects_unresolved_tool_calls() {
        let messages = vec![
            SemanticMessage::User { content: "read".into(), display_content: None },
            SemanticMessage::AssistantToolRequest {
                content: None,
                calls: vec![call("a")],
                reasoning: None,
                provider_replay: None,
            },
        ];

        assert_eq!(Conversation::new(messages), Err(ConversationError::MissingToolResultBatch));
    }
}
