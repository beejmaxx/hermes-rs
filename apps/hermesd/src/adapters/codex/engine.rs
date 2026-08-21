//! Codex cognitive engine supervised by the Hermes runtime authority boundary.

use std::{collections::BTreeMap, path::PathBuf};

use domain::{
    Conversation, SemanticMessage, ToolArguments, ToolCall, ToolCallId, ToolResult,
    ToolResultStatus,
};
use ports::ToolBroker;
use protocol::{ContractOutcome, TerminalOutcome, TerminalStatus, Usage};
use runtime::{RuntimeError, RuntimeEventObserver, RuntimeEventObserverError, ToolHost};
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;

use super::{
    CodexAppServer, CodexAppServerCommand, CodexAppServerError, CodexAppServerEvent,
    CodexApprovalPolicy, CodexAuthorityError, CodexAuthorityManifest, CodexAuthorityPolicy,
    CodexConfigReadParams, CodexDynamicToolCallResponse, CodexDynamicToolFunctionSpec,
    CodexDynamicToolSpec, CodexInitializeParams, CodexNotification, CodexSandboxMode,
    CodexThreadStartParams, CodexTurnStartParams, CodexTurnStatus,
};

/// A Codex turn could not complete inside the Hermes authority boundary.
#[derive(Debug, Error)]
pub enum CodexEngineError {
    /// The supervised process or protocol failed.
    #[error(transparent)]
    AppServer(#[from] CodexAppServerError),
    /// Effective worker capabilities could not be constrained.
    #[error(transparent)]
    Authority(#[from] CodexAuthorityError),
    /// Kernel tool planning, approval, or execution failed.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// The final provider-neutral transcript violated conversation invariants.
    #[error(transparent)]
    Conversation(#[from] domain::ConversationError),
    /// The live event consumer rejected an ordered event.
    #[error(transparent)]
    Observer(#[from] RuntimeEventObserverError),
    /// Worker behavior violated the frozen engine contract.
    #[error("invalid Codex engine turn: {0}")]
    Invalid(String),
}

/// Immutable configuration for a supervised Codex cognitive engine.
#[derive(Clone, Debug)]
pub struct CodexTurnEngine {
    command: CodexAppServerCommand,
    model: String,
    cwd: PathBuf,
    base_instructions: String,
    developer_instructions: String,
    effort: Option<String>,
}

impl CodexTurnEngine {
    /// Configure a worker executable, model, workspace, and frozen instructions.
    pub fn new(
        command: CodexAppServerCommand,
        model: impl Into<String>,
        cwd: impl Into<PathBuf>,
        base_instructions: impl Into<String>,
        developer_instructions: impl Into<String>,
    ) -> Result<Self, CodexEngineError> {
        let model = model.into();
        if model.is_empty() || model.trim() != model {
            return Err(CodexEngineError::Invalid(
                "model must be non-empty and have no surrounding whitespace".into(),
            ));
        }
        let cwd = cwd.into();
        if !cwd.is_absolute() {
            return Err(CodexEngineError::Invalid("worker cwd must be an absolute path".into()));
        }
        let base_instructions = base_instructions.into();
        let developer_instructions = developer_instructions.into();
        if base_instructions.is_empty() || developer_instructions.is_empty() {
            return Err(CodexEngineError::Invalid("worker instructions must be non-empty".into()));
        }
        Ok(Self { command, model, cwd, base_instructions, developer_instructions, effort: None })
    }

    /// Select the model reasoning effort for worker turns.
    #[must_use]
    pub fn with_effort(mut self, effort: impl Into<String>) -> Self {
        self.effort = Some(effort.into());
        self
    }

    /// Execute one new ephemeral Codex thread while Hermes owns tools and transcript state.
    pub async fn run_new<T, O>(
        &self,
        request: CodexEngineTurnRequest,
        tool_catalog: &[Value],
        tools: &mut T,
        observer: &mut O,
    ) -> Result<CodexEngineOutcome, CodexEngineError>
    where
        T: ToolBroker + ?Sized,
        O: RuntimeEventObserver + ?Sized,
    {
        request.validate()?;
        let dynamic_tools = dynamic_tools_from_catalog(tool_catalog)?;
        let allowed_tools =
            dynamic_tools.iter().map(|tool| tool.name().to_owned()).collect::<Vec<_>>();
        let cwd = self
            .cwd
            .to_str()
            .ok_or_else(|| CodexEngineError::Invalid("worker cwd is not valid UTF-8".into()))?;
        let mut worker = CodexAppServer::spawn(&self.command)?;
        let initialized = worker
            .initialize(
                &CodexInitializeParams::hermes(env!("CARGO_PKG_VERSION"))
                    .with_experimental_api(true),
            )
            .await?;
        worker.initialized().await?;
        let effective = worker.read_config(&CodexConfigReadParams::for_cwd(cwd)).await?;
        let policy = CodexAuthorityPolicy::new(&effective, dynamic_tools)?;
        let opened = worker
            .start_thread(
                &policy.constrain(
                    CodexThreadStartParams::new()
                        .with_model(&self.model)
                        .with_cwd(&self.cwd)
                        .with_approval_policy(CodexApprovalPolicy::Never)
                        .with_sandbox(CodexSandboxMode::ReadOnly)
                        .with_base_instructions(&self.base_instructions)
                        .with_developer_instructions(&self.developer_instructions)
                        .with_ephemeral(true),
                ),
            )
            .await?;
        if opened.model() != self.model || opened.cwd() != self.cwd {
            return Err(CodexEngineError::Invalid(
                "worker did not preserve the frozen model and cwd".into(),
            ));
        }
        let binding = CodexWorkerBinding {
            thread_id: opened.thread().id().to_owned(),
            worker_user_agent: initialized.user_agent().to_owned(),
            model_provider: opened.model_provider().to_owned(),
            authority: policy.manifest().clone(),
        };
        let mut turn_params = CodexTurnStartParams::text(&binding.thread_id, &request.prompt);
        if let Some(effort) = &self.effort {
            turn_params = turn_params.with_effort(effort);
        }
        if let Some(id) = &request.client_user_message_id {
            turn_params = turn_params.with_client_user_message_id(id);
        }
        let started = worker.start_turn(&turn_params).await?;
        let turn_id = started.turn().id().to_owned();
        if started.turn().status() != CodexTurnStatus::InProgress {
            return Err(CodexEngineError::Invalid(
                "worker returned a non-running turn from turn/start".into(),
            ));
        }

        let mut transcript = request.semantic_history;
        transcript
            .push(SemanticMessage::User { content: request.prompt.clone(), display_content: None });
        let mut persistence_intents = vec![json!({
            "type": "append_user",
            "message": {"kind": "user", "content": request.prompt},
        })];
        let mut public_events = Vec::new();
        push_event(
            observer,
            &mut public_events,
            json!({
                "type": "provider.attempt_started",
                "attempt_id": format!("codex:{turn_id}"),
                "engine": "codex_app_server",
            }),
        )?;
        let mut host = ToolHost::new(tools, request.execution_scope)?;
        let mut response = String::new();
        let mut message_started = false;

        loop {
            match worker.next_event().await? {
                CodexAppServerEvent::Notification(notification) => match notification {
                    CodexNotification::ThreadStarted(thread) => {
                        if thread.id() != binding.thread_id {
                            return Err(CodexEngineError::Invalid(
                                "worker emitted thread/started for another thread".into(),
                            ));
                        }
                    }
                    CodexNotification::TurnStarted(started) => {
                        if started.thread_id() != binding.thread_id
                            || started.turn().id() != turn_id
                        {
                            return Err(CodexEngineError::Invalid(
                                "worker emitted turn/started for another turn".into(),
                            ));
                        }
                    }
                    CodexNotification::AgentMessageDelta(delta) => {
                        if delta.thread_id() != binding.thread_id || delta.turn_id() != turn_id {
                            return Err(CodexEngineError::Invalid(
                                "worker emitted assistant text for another turn".into(),
                            ));
                        }
                        if !message_started {
                            push_event(
                                observer,
                                &mut public_events,
                                json!({"type": "message.start", "attempt_id": format!("codex:{turn_id}")}),
                            )?;
                            message_started = true;
                        }
                        response.push_str(delta.delta());
                        push_event(
                            observer,
                            &mut public_events,
                            json!({
                                "type": "message.delta",
                                "attempt_id": format!("codex:{turn_id}"),
                                "text": delta.delta(),
                            }),
                        )?;
                    }
                    CodexNotification::TurnCompleted(completed) => {
                        if completed.thread_id() != binding.thread_id
                            || completed.turn().id() != turn_id
                        {
                            return Err(CodexEngineError::Invalid(
                                "worker completed another turn".into(),
                            ));
                        }
                        if completed.turn().status() != CodexTurnStatus::Completed {
                            return Err(CodexEngineError::Invalid(format!(
                                "worker turn ended with status {:?}",
                                completed.turn().status()
                            )));
                        }
                        break;
                    }
                    CodexNotification::Other { .. } => {}
                },
                CodexAppServerEvent::Request(server_request) => {
                    let Some(call) = server_request.dynamic_tool_call() else {
                        worker
                            .respond_error(
                                server_request.id(),
                                -32601,
                                "Hermes rejects non-dynamic worker requests",
                                &Value::Null,
                            )
                            .await?;
                        return Err(CodexEngineError::Invalid(format!(
                            "worker requested forbidden method {}",
                            server_request.method()
                        )));
                    };
                    if call.thread_id() != binding.thread_id || call.turn_id() != turn_id {
                        return Err(CodexEngineError::Invalid(
                            "dynamic tool request targeted another turn".into(),
                        ));
                    }
                    if call.namespace().is_some()
                        || !allowed_tools.iter().any(|name| name == call.tool())
                    {
                        return Err(CodexEngineError::Invalid(format!(
                            "worker requested unregistered dynamic tool {:?}",
                            call.tool()
                        )));
                    }
                    let Value::Object(arguments) = call.arguments() else {
                        return Err(CodexEngineError::Invalid(
                            "dynamic tool arguments must be a JSON object".into(),
                        ));
                    };
                    let tool_call = ToolCall {
                        id: ToolCallId::new(call.call_id().to_owned())
                            .map_err(RuntimeError::from)?,
                        name: call.tool().to_owned(),
                        arguments: ToolArguments(BTreeMap::from_iter(
                            arguments.iter().map(|(key, value)| (key.clone(), value.clone())),
                        )),
                    };
                    let planned = host.plan(std::slice::from_ref(&tool_call))?;
                    for plan in &planned {
                        if let Some(approval) = &plan.approval {
                            push_event(
                                observer,
                                &mut public_events,
                                json!({
                                    "type": "approval.request",
                                    "call_id": plan.call_id,
                                    "name": plan.name,
                                    "arguments": plan.arguments,
                                    "effect": plan.effect,
                                    "requirement": approval,
                                }),
                            )?;
                        }
                    }
                    let resolved = host.resolve_approvals(&planned).await?;
                    for plan in &resolved {
                        persistence_intents.push(json!({
                            "type": "tool_planned",
                            "call_id": plan.call_id,
                            "name": plan.name,
                            "arguments": plan.arguments,
                            "execution_key": plan.invocation_id.as_str(),
                            "effect": plan.effect,
                            "approval": plan.approval,
                        }));
                        if let Some(approval) = &plan.approval {
                            push_event(
                                observer,
                                &mut public_events,
                                json!({
                                    "type": "approval.resolved",
                                    "call_id": plan.call_id,
                                    "decision": approval.decision,
                                }),
                            )?;
                        }
                        if !plan.approval.as_ref().is_some_and(domain::ApprovalRecord::denied) {
                            push_event(
                                observer,
                                &mut public_events,
                                json!({
                                    "type": "tool.start",
                                    "call_id": plan.call_id,
                                    "name": plan.name,
                                    "execution_key": plan.invocation_id.as_str(),
                                }),
                            )?;
                        }
                    }
                    let completed = host.execute(&resolved).await?;
                    let terminal = completed.first().ok_or_else(|| {
                        CodexEngineError::Invalid("dynamic tool produced no terminal".into())
                    })?;
                    persistence_intents.push(json!({
                        "type": format!("tool_{}", status_name(terminal.status)),
                        "call_id": terminal.call_id,
                        "execution_key": terminal.invocation_id.as_str(),
                        "effect": terminal.effect,
                        "content": terminal.content,
                        "receipt": terminal.receipt,
                    }));
                    push_event(
                        observer,
                        &mut public_events,
                        json!({
                            "type": "tool.complete",
                            "call_id": terminal.call_id,
                            "name": terminal.name,
                            "status": status_name(terminal.status),
                            "execution_key": terminal.invocation_id.as_str(),
                        }),
                    )?;
                    transcript.push(SemanticMessage::AssistantToolRequest {
                        content: None,
                        calls: vec![tool_call],
                        reasoning: None,
                        provider_replay: Some(json!({
                            "engine": "codex_app_server",
                            "thread_id": binding.thread_id,
                            "turn_id": turn_id,
                        })),
                    });
                    transcript.push(SemanticMessage::ToolResultBatch {
                        results: vec![ToolResult {
                            call_id: terminal.call_id.clone(),
                            status: terminal.status,
                            content: terminal.content.clone(),
                            execution_key: Some(terminal.invocation_id.as_str().to_owned()),
                        }],
                    });
                    worker
                        .respond_dynamic_tool_call(
                            &server_request,
                            &CodexDynamicToolCallResponse::text(
                                &terminal.content,
                                terminal.status == ToolResultStatus::Succeeded,
                            ),
                        )
                        .await?;
                }
            }
        }

        if response.is_empty() {
            return Err(CodexEngineError::Invalid(
                "completed worker turn produced no assistant text".into(),
            ));
        }
        transcript.push(SemanticMessage::Assistant {
            content: response.clone(),
            reasoning: None,
            provider_replay: Some(json!({
                "engine": "codex_app_server",
                "thread_id": binding.thread_id,
                "turn_id": turn_id,
            })),
        });
        let transcript = Conversation::new(transcript)?.into_messages();
        persistence_intents.push(json!({
            "type": "append_assistant",
            "message": transcript.last(),
        }));
        push_event(
            observer,
            &mut public_events,
            json!({
                "type": "message.complete",
                "attempt_id": format!("codex:{turn_id}"),
                "content": response,
                "finish_reason": "stop",
            }),
        )?;
        worker.shutdown().await?;
        Ok(CodexEngineOutcome {
            contract: ContractOutcome {
                provider_requests: Vec::new(),
                semantic_conversation: transcript,
                persistence_intents,
                public_events,
                usage: Usage::default(),
                terminal_outcome: TerminalOutcome {
                    status: TerminalStatus::Completed,
                    final_response: Some(response),
                    finish_reason: Some("stop".into()),
                    reason: None,
                    visible_output: None,
                },
            },
            binding,
        })
    }
}

/// Inputs owned by Hermes for one externally reasoned turn.
#[derive(Clone, Debug)]
pub struct CodexEngineTurnRequest {
    /// Kernel execution scope used to issue invocation identities.
    pub execution_scope: String,
    /// Canonical semantic history owned by Hermes.
    pub semantic_history: Vec<SemanticMessage>,
    /// New user input.
    pub prompt: String,
    /// Optional durable foreground-turn correlation identity.
    pub client_user_message_id: Option<String>,
}

impl CodexEngineTurnRequest {
    fn validate(&self) -> Result<(), CodexEngineError> {
        if self.execution_scope.is_empty() {
            return Err(CodexEngineError::Invalid("execution scope must be non-empty".into()));
        }
        if self.prompt.trim().is_empty() {
            return Err(CodexEngineError::Invalid("prompt must be non-empty".into()));
        }
        let _conversation = Conversation::new(self.semantic_history.clone())?;
        Ok(())
    }
}

/// Opaque worker attachment and frozen capability evidence returned to the Hermes host.
#[derive(Clone, Debug, Serialize)]
pub struct CodexWorkerBinding {
    /// Worker-owned thread identity related to the Hermes session by the host.
    pub thread_id: String,
    /// Worker build identity returned during initialization.
    pub worker_user_agent: String,
    /// Effective model-provider adapter selected inside Codex.
    pub model_provider: String,
    /// Exact capability restrictions applied when the worker thread was opened.
    pub authority: CodexAuthorityManifest,
}

/// Complete semantic and worker-binding result of one supervised turn.
pub struct CodexEngineOutcome {
    /// Engine-neutral transcript, events, and terminal result.
    pub contract: ContractOutcome,
    /// Worker attachment to persist under Hermes session ownership.
    pub binding: CodexWorkerBinding,
}

/// Convert the frozen OpenAI-compatible function catalog into Codex dynamic-tool specs.
pub fn dynamic_tools_from_catalog(
    catalog: &[Value],
) -> Result<Vec<CodexDynamicToolSpec>, CodexEngineError> {
    catalog
        .iter()
        .map(|schema| {
            let function = schema
                .as_object()
                .filter(|object| object.get("type").and_then(Value::as_str) == Some("function"))
                .and_then(|object| object.get("function"))
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    CodexEngineError::Invalid(
                        "only OpenAI-compatible function tools can be projected to Codex".into(),
                    )
                })?;
            let name = function.get("name").and_then(Value::as_str).ok_or_else(|| {
                CodexEngineError::Invalid("tool schema has no string function.name".into())
            })?;
            let description =
                function.get("description").and_then(Value::as_str).ok_or_else(|| {
                    CodexEngineError::Invalid(
                        "tool schema has no string function.description".into(),
                    )
                })?;
            let input_schema = function.get("parameters").cloned().ok_or_else(|| {
                CodexEngineError::Invalid("tool schema has no function.parameters".into())
            })?;
            Ok(CodexDynamicToolSpec::Function(CodexDynamicToolFunctionSpec::new(
                name,
                description,
                input_schema,
            )))
        })
        .collect()
}

fn push_event<O>(
    observer: &mut O,
    events: &mut Vec<Value>,
    event: Value,
) -> Result<(), CodexEngineError>
where
    O: RuntimeEventObserver + ?Sized,
{
    observer.observe(&event)?;
    events.push(event);
    Ok(())
}

const fn status_name(status: ToolResultStatus) -> &'static str {
    match status {
        ToolResultStatus::Succeeded => "succeeded",
        ToolResultStatus::Failed => "failed",
        ToolResultStatus::Cancelled => "cancelled",
        ToolResultStatus::Rejected => "rejected",
        ToolResultStatus::OutcomeUnknown => "outcome_unknown",
        ToolResultStatus::Observed => "observed",
    }
}
