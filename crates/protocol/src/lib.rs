//! Versioned wire and contract-corpus types.
//!
//! Dynamic JSON is permitted here only where a provider-specific or recorded
//! test payload is intentionally opaque to the domain.

mod delegation;
mod foreground;
mod gateway;

use domain::{LineageId, OwnerGeneration, PromptManifest, SemanticMessage, SessionId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use delegation::{DelegationCompletion, DelegationSnapshot};
pub use foreground::ForegroundTurnSnapshot;
pub use gateway::{
    GatewayErrorBody, GatewayEvent, GatewayEventFrame, GatewayFailure, GatewayRequest,
    GatewaySuccess, JSON_RPC_VERSION,
};

/// Schema marker shared by the Python oracle and this Rust reader.
pub const CONTRACT_SCHEMA_V1: &str = "hermes-rewrite-contract/v1";

/// A deterministic contract scenario family.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractKind {
    /// An offline agent turn driven by scripted provider and tool events.
    AgentTurn,
    /// Projection of an Anthropic request and its cache-control policy.
    AnthropicRequest,
    /// Projection of Codex app-server notifications.
    CodexProjection,
}

/// A provider transport family represented in the v1 corpus.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    /// OpenAI-compatible Chat Completions.
    ChatCompletions,
    /// Native Anthropic Messages.
    AnthropicMessages,
    /// OpenAI Responses-compatible transport.
    CodexResponses,
    /// Codex app-server notifications and commands.
    CodexAppServer,
    /// AWS Bedrock Converse.
    BedrockConverse,
    /// Native Gemini generateContent.
    GeminiNative,
}

/// Model reasoning budget frozen for the lifetime of a session lineage.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelReasoningEffort {
    /// Disable deliberate reasoning when the selected model supports it.
    None,
    /// Use the smallest model-supported reasoning budget.
    Minimal,
    /// Prefer low latency over extended deliberation.
    Low,
    /// Use a balanced reasoning budget.
    Medium,
    /// Use an extended reasoning budget.
    High,
    /// Use an extra-high reasoning budget.
    Xhigh,
    /// Use the model's maximum reasoning budget.
    Max,
    /// Use the model's ultra reasoning budget.
    Ultra,
}

impl ModelReasoningEffort {
    /// Exact value sent to a cognitive engine.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
            Self::Ultra => "ultra",
        }
    }
}

/// Versioned authority contract applied to a supervised Codex worker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexAuthorityProfile {
    /// Codex owns cognition while every model-visible effect is hosted by Hermes.
    HermesOwnedEffectsV1,
}

impl CodexAuthorityProfile {
    /// Stable identity included in the immutable engine manifest.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HermesOwnedEffectsV1 => "hermes_owned_effects_v1",
        }
    }
}

/// Engine-specific settings that must not drift during a session lineage.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "engine", rename_all = "snake_case")]
pub enum EngineConfig {
    /// Hermes owns the complete model-and-tool loop.
    Direct,
    /// Hermes supervises an external Codex app-server reasoning loop.
    CodexAppServer {
        /// Explicit reasoning effort, independent of mutable user-level Codex config.
        reasoning_effort: ModelReasoningEffort,
        /// Versioned worker authority policy applied to every thread and turn.
        authority_profile: CodexAuthorityProfile,
    },
}

/// OpenAI-compatible function payload nested inside a tool call.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderFunctionCall {
    /// Frozen tool name.
    pub name: String,
    /// Canonical JSON arguments accumulated from the provider stream.
    pub arguments: String,
}

/// OpenAI-compatible assistant tool-call record.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderToolCall {
    /// Provider-visible call identity.
    pub id: String,
    /// Wire discriminator, currently always `function`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Function name and canonical arguments.
    pub function: ProviderFunctionCall,
}

/// Provider-facing conversation message for Chat Completions.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum ProviderMessage {
    /// Frozen system prompt.
    System {
        /// Exact prompt bytes represented as UTF-8 text.
        content: String,
    },
    /// User input.
    User {
        /// User content for the current contract corpus.
        content: String,
    },
    /// Assistant text or tool request.
    Assistant {
        /// Text accompanying the response; `null` for a tool-only request.
        content: Option<String>,
        /// Provider-visible reasoning when exposed by the transport.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning: Option<String>,
        /// Complete tool calls in provider order.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tool_calls: Vec<ProviderToolCall>,
        /// Opaque provider continuation data.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_replay: Option<Value>,
    },
    /// Result satisfying one preceding assistant tool call.
    Tool {
        /// Provider-visible call identity.
        tool_call_id: String,
        /// Stable terminal or reconciliation status.
        status: String,
        /// Provider-facing result text.
        content: String,
        /// Stable execution deduplication key.
        execution_key: String,
    },
}

impl ProviderMessage {
    /// Return the OpenAI role name used by persistence-intent projection.
    #[must_use]
    pub const fn role(&self) -> &'static str {
        match self {
            Self::System { .. } => "system",
            Self::User { .. } => "user",
            Self::Assistant { .. } => "assistant",
            Self::Tool { .. } => "tool",
        }
    }
}

/// One OpenAI-compatible Chat Completions request.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChatCompletionsRequest {
    /// Provider model identifier.
    pub model: String,
    /// Complete provider-facing conversation projection.
    pub messages: Vec<ProviderMessage>,
    /// Ordered frozen tool schemas.
    pub tools: Vec<Value>,
}

/// Input required to execute one provider-neutral agent turn.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentTurnRequest {
    /// Stable execution scope used by the v1 kernel invocation-ID allocator.
    pub execution_scope: String,
    /// Provider transport selected for the turn.
    pub transport: TransportKind,
    /// Provider model identifier.
    pub model: String,
    /// Frozen system prompt, when present.
    pub system_prompt: Option<String>,
    /// Replayable provider-facing history, ending in a user message.
    pub conversation: Vec<ProviderMessage>,
    /// Ordered frozen tool schemas.
    pub tools: Vec<Value>,
}

/// Immutable configuration captured when a durable session is created.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    /// Stable user-facing session identity.
    pub session_id: SessionId,
    /// Immutable lineage identity; equal to the session ID until explicit branching exists.
    pub lineage_id: LineageId,
    /// Frozen engine and prompt/tool byte identity.
    pub prompt_manifest: PromptManifest,
    /// Frozen engine-specific behavior and authority settings.
    pub engine_config: EngineConfig,
    /// Provider transport used for every turn in this lineage.
    pub transport: TransportKind,
    /// Stable provider-adapter name such as `openai` or `openrouter`.
    pub provider_adapter: String,
    /// Non-secret provider endpoint; empty for a local-process cognitive engine.
    pub base_url: String,
    /// Name of the environment variable containing the credential, when required.
    pub api_key_env: Option<String>,
    /// Immutable provider model identifier.
    pub model: String,
    /// Canonical filesystem root visible to session tools.
    pub tool_root: String,
    /// Exact frozen system-prompt bytes.
    pub system_prompt: String,
    /// Exact ordered frozen tool catalog.
    pub tools: Vec<Value>,
}

/// One fully loaded durable session.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    /// Immutable configuration.
    pub config: SessionConfig,
    /// Current optimistic write-authority generation.
    pub owner_generation: OwnerGeneration,
    /// Validated provider-neutral conversation.
    pub conversation: Vec<SemanticMessage>,
}

/// Compact durable-session listing record.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSummary {
    /// Stable session identity.
    pub session_id: SessionId,
    /// Provider adapter frozen for the session.
    pub provider_adapter: String,
    /// Provider model frozen for the session.
    pub model: String,
    /// Current optimistic write-authority generation.
    pub owner_generation: OwnerGeneration,
    /// Number of persisted semantic messages.
    pub message_count: usize,
}

/// A durably planned tool invocation that has no terminal disposition yet.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingEffect {
    /// Stable execution scope in which the invocation was planned.
    pub execution_scope: String,
    /// Complete frozen invocation plan.
    pub plan: domain::PlannedToolCall,
}

/// A normalized event emitted by a model-provider transport.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderEvent {
    /// Provider began an assistant message.
    MessageStart,
    /// Visible assistant text fragment.
    TextDelta {
        /// Delta text.
        text: String,
    },
    /// Visible reasoning fragment.
    ReasoningDelta {
        /// Delta text.
        text: String,
    },
    /// Fragment of one indexed assistant tool call.
    ToolCallDelta {
        /// Zero-based call position.
        index: usize,
        /// Call identity, present on the first or repeated fragments.
        #[serde(default)]
        id: Option<String>,
        /// Tool name, present on the first or repeated fragments.
        #[serde(default)]
        name: Option<String>,
        /// JSON argument fragment.
        #[serde(default)]
        arguments_delta: String,
    },
    /// Token usage reported by one provider attempt.
    Usage {
        /// Input tokens.
        #[serde(default)]
        prompt_tokens: u64,
        /// Output tokens.
        #[serde(default)]
        completion_tokens: u64,
        /// Provider total, or zero when unavailable.
        #[serde(default)]
        total_tokens: u64,
        /// Cached input tokens.
        #[serde(default)]
        cached_tokens: u64,
    },
    /// Provider completed the attempt normally.
    Completed {
        /// Provider finish reason.
        #[serde(default)]
        finish_reason: Option<String>,
        /// Opaque provider continuation data.
        #[serde(default)]
        provider_data: Option<Value>,
    },
    /// Provider failed the attempt.
    Error {
        /// Stable failure classification.
        #[serde(default)]
        reason: Option<String>,
    },
    /// Caller or runtime cancelled the attempt.
    Cancelled {
        /// Stable cancellation classification.
        #[serde(default)]
        reason: Option<String>,
    },
    /// Provider emitted an invalid protocol sequence.
    Malformed {
        /// Stable protocol-error classification.
        #[serde(default)]
        reason: Option<String>,
    },
}

/// One outbound provider request recorded by the oracle.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequestRecord {
    /// Stable attempt identifier within the fixture.
    pub attempt_id: String,
    /// Provider protocol family.
    pub transport: TransportKind,
    /// Transport-specific request payload.
    pub request: Value,
}

/// Normalized token accounting.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Usage {
    /// Input tokens submitted across attempts.
    pub prompt_tokens: u64,
    /// Output tokens generated across attempts.
    pub completion_tokens: u64,
    /// Provider-reported or derived total tokens.
    pub total_tokens: u64,
    /// Input tokens served from provider cache.
    pub cached_tokens: u64,
}

/// Terminal status of a deterministic contract scenario.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalStatus {
    /// The turn completed normally.
    Completed,
    /// A valid but nonterminal provider finish reason ended the turn.
    Incomplete,
    /// The turn failed before it could commit a final assistant response.
    Failed,
    /// The caller or runtime cancelled the turn.
    Cancelled,
    /// Visible output was interrupted and cannot be transparently retried.
    Interrupted,
    /// A request-only fixture was projected without executing a turn.
    FixtureProjected,
}

/// Normalized terminal outcome of a fixture.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalOutcome {
    /// Scenario status.
    pub status: TerminalStatus,
    /// Final user-visible response, when one was durably completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_response: Option<String>,
    /// Provider finish reason, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    /// Stable failure or cancellation classification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Whether the failed attempt emitted content that forbids transparent fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_output: Option<bool>,
}

/// Canonical output produced by a contract runner.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractOutcome {
    /// Outbound requests in attempt order.
    pub provider_requests: Vec<ProviderRequestRecord>,
    /// Provider-neutral replay records.
    pub semantic_conversation: Vec<SemanticMessage>,
    /// Durable storage commands; typed variants will replace raw capture as the kernel lands.
    pub persistence_intents: Vec<Value>,
    /// User/client-visible streaming and lifecycle events.
    pub public_events: Vec<Value>,
    /// Aggregate usage.
    pub usage: Usage,
    /// Terminal scenario classification.
    pub terminal_outcome: TerminalOutcome,
}

/// One language-neutral deterministic fixture.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContractFixture {
    /// Versioned contract schema marker.
    #[serde(rename = "$schema")]
    pub schema: String,
    /// Stable scenario identity.
    pub id: String,
    /// Fixture scenario family.
    pub kind: ContractKind,
    /// Human-readable behavior described by the fixture.
    pub description: String,
    /// Current Python source/tests that justify the scenario.
    pub evidence: Vec<String>,
    /// Script or projection input owned by the fixture kind.
    pub input: Value,
    /// Explicit normalization rules; there is no global ignore list.
    pub normalization: Value,
    /// Canonical behavior another implementation must reproduce.
    pub expected: ContractOutcome,
}
