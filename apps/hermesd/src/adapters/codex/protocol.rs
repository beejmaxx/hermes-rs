//! Typed subset of the Codex app-server protocol used by the supervised worker.

use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC request identity accepted by Codex app-server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum CodexRequestId {
    /// A textual request identity supplied by the server.
    String(String),
    /// A numeric request identity supplied by either peer.
    Integer(i64),
}

/// Client identity and capability negotiation sent during initialization.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexInitializeParams {
    client_info: CodexClientInfo,
    capabilities: CodexInitializeCapabilities,
}

impl CodexInitializeParams {
    /// Construct the Hermes app-server client identity.
    #[must_use]
    pub fn hermes(version: impl Into<String>) -> Self {
        Self {
            client_info: CodexClientInfo {
                name: "hermes-rs".into(),
                title: Some("Hermes RS".into()),
                version: version.into(),
            },
            capabilities: CodexInitializeCapabilities::default(),
        }
    }

    /// Opt into the experimental app-server surface required by dynamic tools.
    #[must_use]
    pub const fn with_experimental_api(mut self, enabled: bool) -> Self {
        self.capabilities.experimental_api = enabled;
        self
    }
}

#[derive(Clone, Debug, Serialize)]
struct CodexClientInfo {
    name: String,
    title: Option<String>,
    version: String,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexInitializeCapabilities {
    experimental_api: bool,
}

/// Server metadata returned by `initialize`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodexInitializeResponse {
    user_agent: String,
    codex_home: PathBuf,
    platform_family: String,
    platform_os: String,
}

impl CodexInitializeResponse {
    /// App-server user agent string.
    #[must_use]
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    /// Absolute Codex home reported by the worker.
    #[must_use]
    pub fn codex_home(&self) -> &std::path::Path {
        &self.codex_home
    }

    /// Broad worker platform family, such as `unix`.
    #[must_use]
    pub fn platform_family(&self) -> &str {
        &self.platform_family
    }

    /// Worker operating system, such as `macos`.
    #[must_use]
    pub fn platform_os(&self) -> &str {
        &self.platform_os
    }
}

/// Parameters for reading the worker's effective configuration without layers or secrets output.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexConfigReadParams {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    include_layers: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
}

impl CodexConfigReadParams {
    /// Read effective configuration for the selected working directory.
    #[must_use]
    pub fn for_cwd(cwd: impl Into<String>) -> Self {
        Self { include_layers: false, cwd: Some(cwd.into()) }
    }
}

/// Effective configuration returned by `config/read`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct CodexConfigReadResponse {
    config: BTreeMap<String, Value>,
}

impl CodexConfigReadResponse {
    /// Return one effective top-level configuration value.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.config.get(key)
    }
}

/// Codex approval behavior selected for a thread.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexApprovalPolicy {
    /// Only commands classified as trusted may run without approval.
    Untrusted,
    /// The worker asks for approval when it judges that necessary.
    OnRequest,
    /// The worker never asks for an approval escalation.
    Never,
}

/// Legacy app-server sandbox mode selected for a thread.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexSandboxMode {
    /// Permit reads while denying workspace mutation and network access.
    ReadOnly,
    /// Permit writes within the selected workspace.
    WorkspaceWrite,
    /// Do not apply a Codex filesystem sandbox.
    DangerFullAccess,
}

/// Parameters used to create a new Codex thread.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadStartParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval_policy: Option<CodexApprovalPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sandbox: Option<CodexSandboxMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    developer_instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<BTreeMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dynamic_tools: Option<Vec<CodexDynamicToolSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    environments: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ephemeral: Option<bool>,
}

impl CodexThreadStartParams {
    /// Start with no overrides, allowing app-server configuration to supply defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Select the immutable model requested for this thread.
    #[must_use]
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Select the working directory visible to the worker.
    #[must_use]
    pub fn with_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Select the worker's approval policy.
    #[must_use]
    pub const fn with_approval_policy(mut self, policy: CodexApprovalPolicy) -> Self {
        self.approval_policy = Some(policy);
        self
    }

    /// Select the worker's legacy sandbox mode.
    #[must_use]
    pub const fn with_sandbox(mut self, sandbox: CodexSandboxMode) -> Self {
        self.sandbox = Some(sandbox);
        self
    }

    /// Replace the thread's base instructions.
    #[must_use]
    pub fn with_base_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.base_instructions = Some(instructions.into());
        self
    }

    /// Add client-owned developer instructions to the thread.
    #[must_use]
    pub fn with_developer_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.developer_instructions = Some(instructions.into());
        self
    }

    /// Apply explicit per-thread configuration overrides.
    #[must_use]
    pub fn with_config(mut self, config: BTreeMap<String, Value>) -> Self {
        self.config = Some(config);
        self
    }

    /// Expose only the client-hosted dynamic tools selected for this thread.
    #[must_use]
    pub fn with_dynamic_tools(mut self, tools: Vec<CodexDynamicToolSpec>) -> Self {
        self.dynamic_tools = Some(tools);
        self
    }

    /// Disable every Codex-owned execution environment for this thread.
    #[must_use]
    pub fn without_environments(mut self) -> Self {
        self.environments = Some(Vec::new());
        self
    }

    /// Control whether Codex persists the new thread in its own store.
    #[must_use]
    pub const fn with_ephemeral(mut self, ephemeral: bool) -> Self {
        self.ephemeral = Some(ephemeral);
        self
    }
}

/// Client-hosted tool specification accepted by Codex app-server.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CodexDynamicToolSpec {
    /// One standalone function implemented by the Hermes host.
    Function(CodexDynamicToolFunctionSpec),
}

impl CodexDynamicToolSpec {
    /// Model-visible tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Function(function) => function.name(),
        }
    }
}

/// Schema and model-facing documentation for one client-hosted function.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexDynamicToolFunctionSpec {
    name: String,
    description: String,
    input_schema: Value,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    defer_loading: bool,
}

impl CodexDynamicToolFunctionSpec {
    /// Define one eagerly visible client-hosted function.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
            defer_loading: false,
        }
    }

    /// Defer loading this tool until the worker's own discovery flow selects it.
    #[must_use]
    pub const fn with_deferred_loading(mut self, defer_loading: bool) -> Self {
        self.defer_loading = defer_loading;
        self
    }

    /// Client-hosted function name visible to the worker.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Parameters used to reopen an existing Codex thread.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadResumeParams {
    thread_id: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    exclude_turns: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval_policy: Option<CodexApprovalPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sandbox: Option<CodexSandboxMode>,
}

impl CodexThreadResumeParams {
    /// Resume the identified worker thread.
    #[must_use]
    pub fn new(thread_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            exclude_turns: false,
            model: None,
            cwd: None,
            approval_policy: None,
            sandbox: None,
        }
    }

    /// Use Codex's experimental compact response that omits historical turns.
    #[must_use]
    pub const fn without_historical_turns(mut self) -> Self {
        self.exclude_turns = true;
        self
    }

    /// Select the model expected by the immutable Hermes engine binding.
    #[must_use]
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Restore the working directory frozen by the Hermes session.
    #[must_use]
    pub fn with_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Restore the worker approval policy frozen by the Hermes engine.
    #[must_use]
    pub const fn with_approval_policy(mut self, policy: CodexApprovalPolicy) -> Self {
        self.approval_policy = Some(policy);
        self
    }

    /// Restore the worker sandbox frozen by the Hermes engine.
    #[must_use]
    pub const fn with_sandbox(mut self, sandbox: CodexSandboxMode) -> Self {
        self.sandbox = Some(sandbox);
        self
    }
}

/// Minimal opaque Codex thread identity returned to the Hermes binding layer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct CodexThread {
    id: String,
}

impl CodexThread {
    /// Opaque app-server thread identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Shared response shape returned by `thread/start` and `thread/resume`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadOpenResponse {
    thread: CodexThread,
    model: String,
    model_provider: String,
    cwd: PathBuf,
    approval_policy: Value,
    sandbox: Value,
}

impl CodexThreadOpenResponse {
    /// Worker thread identity to persist in the Hermes-owned binding.
    #[must_use]
    pub const fn thread(&self) -> &CodexThread {
        &self.thread
    }

    /// Model selected by the worker after resolving configuration.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Model-provider identifier selected by the worker.
    #[must_use]
    pub fn model_provider(&self) -> &str {
        &self.model_provider
    }

    /// Working directory accepted by the worker.
    #[must_use]
    pub fn cwd(&self) -> &std::path::Path {
        &self.cwd
    }

    /// Effective approval policy in the app-server protocol representation.
    #[must_use]
    pub const fn approval_policy(&self) -> &Value {
        &self.approval_policy
    }

    /// Effective sandbox in the app-server protocol representation.
    #[must_use]
    pub const fn sandbox(&self) -> &Value {
        &self.sandbox
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum CodexUserInput {
    Text { text: String },
}

/// Parameters used to begin one Codex turn.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexTurnStartParams {
    thread_id: String,
    input: Vec<CodexUserInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_user_message_id: Option<String>,
}

impl CodexTurnStartParams {
    /// Construct a text-only turn for the identified worker thread.
    #[must_use]
    pub fn text(thread_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            input: vec![CodexUserInput::Text { text: text.into() }],
            model: None,
            effort: None,
            client_user_message_id: None,
        }
    }

    /// Override the model for this and subsequent worker turns.
    #[must_use]
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    /// Override reasoning effort for this and subsequent worker turns.
    #[must_use]
    pub fn with_effort(mut self, effort: impl Into<String>) -> Self {
        self.effort = Some(effort.into());
        self
    }

    /// Attach the Hermes foreground-turn identity as client correlation metadata.
    #[must_use]
    pub fn with_client_user_message_id(mut self, id: impl Into<String>) -> Self {
        self.client_user_message_id = Some(id.into());
        self
    }
}

/// Lifecycle status reported for a Codex turn.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum CodexTurnStatus {
    /// The turn finished successfully.
    Completed,
    /// The client interrupted the turn.
    Interrupted,
    /// The turn terminated with an error.
    Failed,
    /// The turn is currently running.
    InProgress,
}

/// Minimal typed Codex turn state needed by the supervising engine.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodexTurn {
    id: String,
    status: CodexTurnStatus,
    #[serde(default)]
    items: Vec<Value>,
    #[serde(default)]
    error: Option<Value>,
}

impl CodexTurn {
    /// Opaque app-server turn identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Current worker-reported lifecycle status.
    #[must_use]
    pub const fn status(&self) -> CodexTurnStatus {
        self.status
    }

    /// Worker-owned item payloads included with this lifecycle message.
    #[must_use]
    pub fn items(&self) -> &[Value] {
        &self.items
    }

    /// Worker-reported terminal error payload, when present.
    #[must_use]
    pub const fn error(&self) -> Option<&Value> {
        self.error.as_ref()
    }
}

/// Response returned immediately after `turn/start` is accepted.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct CodexTurnStartResponse {
    turn: CodexTurn,
}

impl CodexTurnStartResponse {
    /// Accepted worker turn.
    #[must_use]
    pub const fn turn(&self) -> &CodexTurn {
        &self.turn
    }
}

/// Parameters used to interrupt the currently active Codex turn.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexTurnInterruptParams {
    thread_id: String,
    turn_id: String,
}

impl CodexTurnInterruptParams {
    /// Target one active turn in one worker thread.
    #[must_use]
    pub fn new(thread_id: impl Into<String>, turn_id: impl Into<String>) -> Self {
        Self { thread_id: thread_id.into(), turn_id: turn_id.into() }
    }
}

/// Streaming assistant text emitted by a Codex turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodexAgentMessageDelta {
    thread_id: String,
    turn_id: String,
    item_id: String,
    delta: String,
}

impl CodexAgentMessageDelta {
    /// Worker thread associated with the delta.
    #[must_use]
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    /// Worker turn associated with the delta.
    #[must_use]
    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }

    /// Worker item associated with the delta.
    #[must_use]
    pub fn item_id(&self) -> &str {
        &self.item_id
    }

    /// Text fragment in transport arrival order.
    #[must_use]
    pub fn delta(&self) -> &str {
        &self.delta
    }
}

/// Notification emitted when a worker turn becomes active.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodexTurnStarted {
    thread_id: String,
    turn: CodexTurn,
}

impl CodexTurnStarted {
    /// Worker thread associated with the lifecycle event.
    #[must_use]
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    /// Worker turn state included in the event.
    #[must_use]
    pub const fn turn(&self) -> &CodexTurn {
        &self.turn
    }
}

/// Notification emitted when a worker turn reaches a terminal state.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodexTurnCompleted {
    thread_id: String,
    turn: CodexTurn,
}

impl CodexTurnCompleted {
    /// Worker thread associated with the lifecycle event.
    #[must_use]
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    /// Terminal worker turn state included in the event.
    #[must_use]
    pub const fn turn(&self) -> &CodexTurn {
        &self.turn
    }
}

/// Typed notifications used by the initial supervised-worker lifecycle.
#[derive(Clone, Debug, PartialEq)]
pub enum CodexNotification {
    /// The worker started a newly created or resumed thread.
    ThreadStarted(CodexThread),
    /// The worker accepted and started a turn.
    TurnStarted(CodexTurnStarted),
    /// The worker streamed assistant text.
    AgentMessageDelta(CodexAgentMessageDelta),
    /// The worker terminalized a turn.
    TurnCompleted(CodexTurnCompleted),
    /// A notification outside the currently typed lifecycle subset.
    Other {
        /// Exact app-server method name.
        method: String,
        /// Opaque app-server parameters retained at the adapter boundary.
        params: Value,
    },
}

/// Typed parameters for a client-hosted dynamic-tool request.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CodexDynamicToolCallParams {
    thread_id: String,
    turn_id: String,
    call_id: String,
    #[serde(default)]
    namespace: Option<String>,
    tool: String,
    arguments: Value,
}

impl CodexDynamicToolCallParams {
    /// Worker thread requesting the call.
    #[must_use]
    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    /// Worker turn requesting the call.
    #[must_use]
    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }

    /// Worker correlation identity for this call.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Optional dynamic-tool namespace.
    #[must_use]
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Client-hosted tool name requested by the worker.
    #[must_use]
    pub fn tool(&self) -> &str {
        &self.tool
    }

    /// JSON arguments proposed by the worker.
    #[must_use]
    pub const fn arguments(&self) -> &Value {
        &self.arguments
    }
}

/// Result returned by Hermes after executing a client-hosted dynamic tool.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexDynamicToolCallResponse {
    content_items: Vec<CodexDynamicToolCallOutputContentItem>,
    success: bool,
}

impl CodexDynamicToolCallResponse {
    /// Return one text result and an explicit success disposition.
    #[must_use]
    pub fn text(text: impl Into<String>, success: bool) -> Self {
        Self {
            content_items: vec![CodexDynamicToolCallOutputContentItem::InputText {
                text: text.into(),
            }],
            success,
        }
    }
}

/// Content item accepted in a dynamic-tool result.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CodexDynamicToolCallOutputContentItem {
    /// Text returned to the worker's reasoning loop.
    InputText {
        /// Tool result text.
        text: String,
    },
}

/// App-server request that requires a client response.
#[derive(Clone, Debug, PartialEq)]
pub struct CodexServerRequest {
    id: CodexRequestId,
    method: String,
    params: Value,
    dynamic_tool_call: Option<CodexDynamicToolCallParams>,
}

impl CodexServerRequest {
    /// Server-owned correlation identity that must be echoed in the response.
    #[must_use]
    pub const fn id(&self) -> &CodexRequestId {
        &self.id
    }

    /// Exact app-server request method.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Opaque request parameters retained at this external protocol boundary.
    #[must_use]
    pub const fn params(&self) -> &Value {
        &self.params
    }

    /// Typed dynamic-tool parameters when this is `item/tool/call`.
    #[must_use]
    pub const fn dynamic_tool_call(&self) -> Option<&CodexDynamicToolCallParams> {
        self.dynamic_tool_call.as_ref()
    }
}

/// One unsolicited app-server message available to a supervising engine.
#[derive(Clone, Debug, PartialEq)]
pub enum CodexAppServerEvent {
    /// A notification that does not require a client response.
    Notification(CodexNotification),
    /// A request that must be answered by the Hermes host.
    Request(CodexServerRequest),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(super) enum InboundMessage {
    Request {
        id: CodexRequestId,
        method: String,
        #[serde(default)]
        params: Value,
    },
    Notification {
        method: String,
        #[serde(default)]
        params: Value,
    },
    Response {
        id: CodexRequestId,
        result: Value,
    },
    Error {
        id: CodexRequestId,
        error: CodexRpcError,
    },
}

#[derive(Debug, Deserialize)]
pub(super) struct CodexRpcError {
    pub(super) code: i64,
    pub(super) message: String,
    #[serde(default)]
    pub(super) data: Value,
}

#[derive(Serialize)]
pub(super) struct OutboundRequest<'a, P> {
    pub(super) id: CodexRequestId,
    pub(super) method: &'a str,
    pub(super) params: &'a P,
}

#[derive(Serialize)]
pub(super) struct OutboundNotification<'a> {
    pub(super) method: &'a str,
}

#[derive(Serialize)]
pub(super) struct OutboundResponse<'a> {
    pub(super) id: &'a CodexRequestId,
    pub(super) result: &'a Value,
}

#[derive(Serialize)]
pub(super) struct OutboundErrorResponse<'a> {
    pub(super) id: &'a CodexRequestId,
    pub(super) error: OutboundRpcError<'a>,
}

#[derive(Serialize)]
pub(super) struct OutboundRpcError<'a> {
    pub(super) code: i64,
    pub(super) message: &'a str,
    pub(super) data: &'a Value,
}

pub(super) fn decode_event(message: InboundMessage) -> Result<CodexAppServerEvent, String> {
    match message {
        InboundMessage::Request { id, method, params } => {
            let dynamic_tool_call = if method == "item/tool/call" {
                Some(
                    serde_json::from_value(params.clone())
                        .map_err(|error| format!("item/tool/call parameters: {error}"))?,
                )
            } else {
                None
            };
            Ok(CodexAppServerEvent::Request(CodexServerRequest {
                id,
                method,
                params,
                dynamic_tool_call,
            }))
        }
        InboundMessage::Notification { method, params } => {
            let notification = match method.as_str() {
                "thread/started" => {
                    #[derive(Deserialize)]
                    struct Payload {
                        thread: CodexThread,
                    }
                    CodexNotification::ThreadStarted(
                        serde_json::from_value::<Payload>(params)
                            .map_err(|error| format!("thread/started parameters: {error}"))?
                            .thread,
                    )
                }
                "turn/started" => CodexNotification::TurnStarted(
                    serde_json::from_value(params)
                        .map_err(|error| format!("turn/started parameters: {error}"))?,
                ),
                "item/agentMessage/delta" => CodexNotification::AgentMessageDelta(
                    serde_json::from_value(params)
                        .map_err(|error| format!("item/agentMessage/delta parameters: {error}"))?,
                ),
                "turn/completed" => CodexNotification::TurnCompleted(
                    serde_json::from_value(params)
                        .map_err(|error| format!("turn/completed parameters: {error}"))?,
                ),
                _ => CodexNotification::Other { method, params },
            };
            Ok(CodexAppServerEvent::Notification(notification))
        }
        InboundMessage::Response { .. } | InboundMessage::Error { .. } => {
            Err("received a response where an unsolicited event was required".into())
        }
    }
}
