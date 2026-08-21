//! Session tool broker combining local inspection with isolated leaf delegation.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use domain::{
    ApprovalRecord, CompletionEventId, DelegationId, DelegationSpec, EngineId, InvocationId,
    LineageId, ManifestDigest, PlannedToolCall, PromptManifest, SessionId, ToolArguments, ToolCall,
    ToolCallId, ToolEffect, ToolResultStatus, ToolTerminal,
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use ports::{DelegationStore, SessionStore, ToolBroker, ToolBrokerError};
use protocol::{AgentTurnRequest, ProviderMessage, SessionConfig, TerminalStatus, TransportKind};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    approval::{ApprovalControl, ApprovalDecision},
    local_tools::{LocalToolsConfigError, ReadOnlyLocalTools},
    openai::{OpenAiCompatibleProvider, OpenAiProviderConfigError},
    sqlite::{SqliteEffectLedger, SqliteSessionStore},
    sqlite_delegation::SqliteDelegationStore,
    terminal::TerminalTool,
};
use runtime::JournaledToolBroker;

const DELEGATE_TOOL: &str = "delegate_task";
const BACKGROUND_DELEGATE_DESCRIPTION: &str = "Queue one focused, independent subtask in a durable leaf-agent session. Return immediately with a task handle. The final result is delivered exactly once with a later explicit user turn; the child cannot delegate or modify files.";
const MAX_GOAL_CHARS: usize = 16_000;
const MAX_CONTEXT_CHARS: usize = 32_000;
const MAX_RESULT_CHARS: usize = 32_000;
const CHILD_MAX_ATTEMPTS: usize = 32;

/// Invalid configuration for the session-level tool broker.
#[derive(Debug, Error)]
pub enum AgentToolsConfigError {
    /// The local filesystem boundary is invalid.
    #[error(transparent)]
    Local(#[from] LocalToolsConfigError),
    /// The delegated provider endpoint is invalid.
    #[error(transparent)]
    Provider(#[from] OpenAiProviderConfigError),
    /// A child model identifier was empty or padded.
    #[error("delegation model must be non-empty and have no surrounding whitespace")]
    InvalidModel,
}

/// Immutable provider, workspace, and persistence inputs inherited by leaf agents.
#[derive(Clone)]
pub struct AgentToolsConfig {
    base_url: String,
    api_key: Option<String>,
    model: String,
    root: PathBuf,
    state: PathBuf,
    delegation_enabled: bool,
    background_parent: Option<SessionId>,
    terminal_approval: Option<(SessionId, ApprovalControl)>,
}

impl AgentToolsConfig {
    /// Validate configuration shared by the parent broker and delegated children.
    pub fn new(
        root: impl Into<PathBuf>,
        state: impl Into<PathBuf>,
        base_url: impl Into<String>,
        api_key: Option<String>,
        model: impl Into<String>,
        delegation_enabled: bool,
    ) -> Result<Self, AgentToolsConfigError> {
        let base_url = base_url.into();
        OpenAiCompatibleProvider::validate_base_url(&base_url)?;
        let model = model.into();
        if model.is_empty() || model.trim() != model {
            return Err(AgentToolsConfigError::InvalidModel);
        }
        Ok(Self {
            base_url,
            api_key,
            model,
            root: root.into(),
            state: state.into(),
            delegation_enabled,
            background_parent: None,
            terminal_approval: None,
        })
    }

    /// Configure only workspace and approved-terminal tools for an external cognitive engine.
    pub fn without_delegation(
        root: impl Into<PathBuf>,
        state: impl Into<PathBuf>,
        model: impl Into<String>,
    ) -> Result<Self, AgentToolsConfigError> {
        let model = model.into();
        if model.is_empty() || model.trim() != model {
            return Err(AgentToolsConfigError::InvalidModel);
        }
        Ok(Self {
            base_url: String::new(),
            api_key: None,
            model,
            root: root.into(),
            state: state.into(),
            delegation_enabled: false,
            background_parent: None,
            terminal_approval: None,
        })
    }

    /// Route delegation calls into durable background work for this parent.
    #[must_use]
    pub fn with_background_parent(mut self, parent_session_id: SessionId) -> Self {
        self.background_parent = Some(parent_session_id);
        self
    }

    /// Enable the frozen terminal tool through one session-scoped approval channel.
    #[must_use]
    pub fn with_terminal_approval(
        mut self,
        session_id: SessionId,
        control: ApprovalControl,
    ) -> Self {
        self.terminal_approval = Some((session_id, control));
        self
    }
}

/// Tool broker for a parent agent session.
pub struct AgentTools {
    local: ReadOnlyLocalTools,
    config: AgentToolsConfig,
    pending_approvals: HashMap<ToolCallId, tokio::sync::oneshot::Receiver<ApprovalDecision>>,
    registered_approvals: HashSet<ToolCallId>,
}

impl AgentTools {
    /// Construct tools for one immutable execution scope.
    pub fn new(
        config: AgentToolsConfig,
        execution_scope: impl Into<String>,
    ) -> Result<Self, AgentToolsConfigError> {
        let execution_scope = execution_scope.into();
        let local = ReadOnlyLocalTools::new(&config.root, &execution_scope)?;
        Ok(Self {
            local,
            config,
            pending_approvals: HashMap::new(),
            registered_approvals: HashSet::new(),
        })
    }

    /// Ordered tool schemas advertised to new parent sessions.
    #[must_use]
    pub fn catalog() -> Vec<Value> {
        let mut tools = ReadOnlyLocalTools::catalog();
        tools.push(json!({
            "type": "function",
            "function": {
                "name": DELEGATE_TOOL,
                "description": "Run one focused, independent subtask in a fresh leaf-agent context. The child can inspect the same workspace with read-only tools but cannot delegate or modify files. Use separate calls in one response to run independent subtasks concurrently.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "goal": {
                            "type": "string",
                            "description": "Specific, self-contained task for the child.",
                            "minLength": 1,
                            "maxLength": MAX_GOAL_CHARS
                        },
                        "context": {
                            "type": "string",
                            "description": "Only the additional context the child needs; parent history is not inherited.",
                            "maxLength": MAX_CONTEXT_CHARS
                        }
                    },
                    "required": ["goal"],
                    "additionalProperties": false
                }
            }
        }));
        tools
    }

    /// Ordered tool schemas for a long-lived host with durable completion delivery.
    #[must_use]
    pub fn background_catalog() -> Vec<Value> {
        let mut tools = Self::catalog();
        if let Some(function) = tools
            .last_mut()
            .and_then(|tool| tool.get_mut("function"))
            .and_then(Value::as_object_mut)
        {
            function.insert(
                "description".into(),
                Value::String(BACKGROUND_DELEGATE_DESCRIPTION.into()),
            );
        }
        tools.push(TerminalTool::schema());
        tools
    }

    /// Ordered schemas for a supervised external engine with approved local terminal access.
    #[must_use]
    pub fn operator_catalog() -> Vec<Value> {
        let mut tools = ReadOnlyLocalTools::catalog();
        tools.push(TerminalTool::schema());
        tools
    }

    /// Whether a frozen catalog selects durable next-turn delegation delivery.
    #[must_use]
    pub fn catalog_uses_background_delivery(catalog: &[Value]) -> bool {
        catalog.iter().any(|tool| {
            tool.get("function").and_then(|function| function.get("name")).and_then(Value::as_str)
                == Some(DELEGATE_TOOL)
                && tool
                    .get("function")
                    .and_then(|function| function.get("description"))
                    .and_then(Value::as_str)
                    == Some(BACKGROUND_DELEGATE_DESCRIPTION)
        })
    }

    /// Whether an ordered catalog exposes leaf delegation.
    #[must_use]
    pub fn catalog_enables_delegation(catalog: &[Value]) -> bool {
        catalog.iter().any(|tool| {
            tool.get("function").and_then(|function| function.get("name")).and_then(Value::as_str)
                == Some(DELEGATE_TOOL)
        })
    }

    /// Whether an ordered frozen catalog exposes the approved terminal adapter.
    #[must_use]
    pub fn catalog_enables_terminal(catalog: &[Value]) -> bool {
        catalog.iter().any(|tool| {
            tool.get("function").and_then(|function| function.get("name")).and_then(Value::as_str)
                == Some(TerminalTool::NAME)
        })
    }
}

impl ToolBroker for AgentTools {
    fn plan(
        &mut self,
        calls: &[ToolCall],
        invocation_ids: &[InvocationId],
    ) -> Result<Vec<PlannedToolCall>, ToolBrokerError> {
        if calls.len() != invocation_ids.len() {
            return Err(ToolBrokerError::new(
                "kernel invocation identities must align with model calls",
            ));
        }
        let mut seen = HashSet::with_capacity(calls.len());
        let terminal_calls = calls
            .iter()
            .filter(|call| {
                self.config.terminal_approval.is_some() && call.name == TerminalTool::NAME
            })
            .count();
        if terminal_calls > 1 {
            return Err(ToolBrokerError::new(
                "only one terminal call may be approved in a provider response",
            ));
        }
        calls
            .iter()
            .zip(invocation_ids)
            .map(|(call, invocation_id)| {
                if !seen.insert(&call.id) {
                    return Err(ToolBrokerError::new(format!(
                        "duplicate tool call id {}",
                        call.id
                    )));
                }
                let (effect, approval) =
                    if self.config.delegation_enabled && call.name == DELEGATE_TOOL {
                        (ToolEffect::ModelInference, None)
                    } else if let Some((session_id, control)) = &self.config.terminal_approval
                        && call.name == TerminalTool::NAME
                    {
                        let validation_plan = PlannedToolCall {
                            call_id: call.id.clone(),
                            name: call.name.clone(),
                            arguments: call.arguments.clone(),
                            invocation_id: invocation_id.clone(),
                            effect: ToolEffect::ProcessControl,
                            approval: None,
                        };
                        TerminalTool::approval_command(&validation_plan)
                            .map_err(ToolBrokerError::new)?;
                        let receiver = control
                            .register(session_id, call.id.clone())
                            .map_err(|error| ToolBrokerError::new(error.to_string()))?;
                        self.pending_approvals.insert(call.id.clone(), receiver);
                        self.registered_approvals.insert(call.id.clone());
                        (
                            ToolEffect::ProcessControl,
                            Some(ApprovalRecord {
                                required: true,
                                decision: "pending".into(),
                                principal: "gateway_user".into(),
                            }),
                        )
                    } else {
                        (ToolEffect::ReadOnly, None)
                    };
                Ok(PlannedToolCall {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                    invocation_id: invocation_id.clone(),
                    effect,
                    approval,
                })
            })
            .collect()
    }

    fn resolve_approvals<'a>(
        &'a mut self,
        calls: &'a [PlannedToolCall],
    ) -> BoxFuture<'a, Result<Vec<PlannedToolCall>, ToolBrokerError>> {
        async move {
            let mut resolved = calls.to_vec();
            for plan in &mut resolved {
                if !plan.approval.as_ref().is_some_and(ApprovalRecord::pending) {
                    continue;
                }
                let receiver = self.pending_approvals.remove(&plan.call_id).ok_or_else(|| {
                    ToolBrokerError::new(format!(
                        "tool call {} has no live approval waiter",
                        plan.call_id
                    ))
                })?;
                let decision = receiver.await.unwrap_or(ApprovalDecision::Deny);
                let approval = plan.approval.as_mut().ok_or_else(|| {
                    ToolBrokerError::new(format!(
                        "tool call {} lost its approval requirement",
                        plan.call_id
                    ))
                })?;
                approval.decision = decision.as_str().into();
            }
            Ok(resolved)
        }
        .boxed()
    }

    fn execute<'a>(
        &'a mut self,
        calls: &'a [PlannedToolCall],
    ) -> BoxFuture<'a, Result<Vec<ToolTerminal>, ToolBrokerError>> {
        async move {
            let (delegated, remaining): (Vec<_>, Vec<_>) = calls
                .iter()
                .cloned()
                .partition(|call| self.config.delegation_enabled && call.name == DELEGATE_TOOL);
            let (terminal_calls, local): (Vec<_>, Vec<_>) =
                remaining.into_iter().partition(|call| {
                    self.config.terminal_approval.is_some() && call.name == TerminalTool::NAME
                });
            let mut completed = self.local.execute(&local).await?;
            for plan in terminal_calls {
                let decision =
                    match plan.approval.as_ref().map(|approval| approval.decision.as_str()) {
                        Some("allow") => ApprovalDecision::Allow,
                        Some("deny") => ApprovalDecision::Deny,
                        _ => {
                            return Err(ToolBrokerError::new(format!(
                                "terminal call {} reached dispatch without a final approval",
                                plan.call_id
                            )));
                        }
                    };
                completed.push(TerminalTool::execute(self.local.root(), plan, decision).await);
            }
            let mut children = delegated
                .into_iter()
                .map(|plan| {
                    let config = self.config.clone();
                    async move { execute_delegate(config, plan).await }
                })
                .collect::<FuturesUnordered<_>>();
            while let Some(terminal) = children.next().await {
                completed.push(terminal);
            }
            Ok(completed)
        }
        .boxed()
    }
}

impl Drop for AgentTools {
    fn drop(&mut self) {
        if let Some((session_id, control)) = &self.config.terminal_approval {
            for call_id in &self.registered_approvals {
                control.remove(session_id, call_id);
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DelegateArgs {
    goal: String,
    #[serde(default)]
    context: Option<String>,
}

async fn execute_delegate(config: AgentToolsConfig, plan: PlannedToolCall) -> ToolTerminal {
    let result = decode_delegate_args(&plan.arguments).and_then(validate_delegate_args);
    let outcome = match result {
        Ok(arguments) if config.background_parent.is_some() => {
            enqueue_child(&config, &plan, arguments).map(|handle| (handle.clone(), handle))
        }
        Ok(arguments) => run_child(&config, &plan, arguments).await,
        Err(error) => Err(error),
    };
    let (status, content, receipt) = match outcome {
        Ok((summary, child_scope)) => {
            (ToolResultStatus::Succeeded, bounded(summary), Some(child_scope))
        }
        Err(error) => (ToolResultStatus::Failed, bounded(error), None),
    };
    ToolTerminal {
        call_id: plan.call_id,
        name: plan.name,
        status,
        content,
        invocation_id: plan.invocation_id,
        effect: ToolEffect::ModelInference,
        receipt,
    }
}

fn enqueue_child(
    config: &AgentToolsConfig,
    plan: &PlannedToolCall,
    arguments: DelegateArgs,
) -> Result<String, String> {
    let parent_session_id = config
        .background_parent
        .clone()
        .ok_or_else(|| "background delegation has no parent session".to_owned())?;
    let identity = stable_delegation_identity(plan.invocation_id.as_str());
    let delegation_id =
        DelegationId::new(format!("delegation-{identity}")).map_err(|error| error.to_string())?;
    let completion_event_id = CompletionEventId::new(format!("completion-{identity}"))
        .map_err(|error| error.to_string())?;
    let child_session_id =
        SessionId::new(format!("child-{identity}")).map_err(|error| error.to_string())?;
    let mut sessions = SqliteSessionStore::open(&config.state)
        .map_err(|error| format!("could not open parent session: {error}"))?;
    let parent = sessions
        .load(&parent_session_id)
        .map_err(|error| format!("could not load parent session: {error}"))?;
    let system_prompt = child_system_prompt(&config.root, &arguments);
    let tools = ReadOnlyLocalTools::catalog();
    let child_config = SessionConfig {
        session_id: child_session_id.clone(),
        lineage_id: LineageId::new(child_session_id.as_str()).map_err(|error| error.to_string())?,
        prompt_manifest: PromptManifest::new(
            1,
            EngineId::new(parent.config.prompt_manifest.engine().as_str())
                .map_err(|error| error.to_string())?,
            ManifestDigest::new(digest(system_prompt.as_bytes()))
                .map_err(|error| error.to_string())?,
            ManifestDigest::new(digest(
                &serde_json::to_vec(&tools).map_err(|error| error.to_string())?,
            ))
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?,
        engine_config: parent.config.engine_config,
        transport: parent.config.transport,
        provider_adapter: parent.config.provider_adapter,
        base_url: parent.config.base_url,
        api_key_env: parent.config.api_key_env,
        model: parent.config.model,
        tool_root: parent.config.tool_root,
        system_prompt,
        tools,
    };
    let spec = DelegationSpec {
        delegation_id: delegation_id.clone(),
        completion_event_id,
        parent_session_id,
        child_session_id,
        goal: arguments.goal,
        context: arguments.context,
    };
    SqliteDelegationStore::open(&config.state)
        .and_then(|mut store| store.create_with_child(child_config, spec, unix_time_ms()?))
        .map_err(|error| format!("could not durably queue delegation: {error}"))?;
    Ok(format!(
        "Queued background delegation {delegation_id}. Its result will be delivered with a later explicit user turn."
    ))
}

async fn run_child(
    config: &AgentToolsConfig,
    plan: &PlannedToolCall,
    arguments: DelegateArgs,
) -> Result<(String, String), String> {
    let child_scope = format!("{}:child", plan.invocation_id.as_str());
    let system_prompt = child_system_prompt(&config.root, &arguments);
    let mut provider = OpenAiCompatibleProvider::new(&config.base_url, config.api_key.clone())
        .map_err(|error| format!("could not configure delegated provider: {error}"))?;
    let local = ReadOnlyLocalTools::new(&config.root, &child_scope)
        .map_err(|error| format!("could not configure delegated tools: {error}"))?;
    let ledger = SqliteEffectLedger::open(&config.state)
        .map_err(|error| format!("could not open delegated effect ledger: {error}"))?;
    let mut tools = JournaledToolBroker::new(local, ledger, &child_scope)
        .map_err(|error| format!("could not journal delegated tools: {error}"))?;
    let outcome = runtime::run_turn_with_limit(
        AgentTurnRequest {
            execution_scope: child_scope.clone(),
            transport: TransportKind::ChatCompletions,
            model: config.model.clone(),
            system_prompt: Some(system_prompt),
            conversation: vec![ProviderMessage::User {
                content:
                    "Complete the assigned task and return a concise result for the parent agent."
                        .into(),
            }],
            tools: ReadOnlyLocalTools::catalog(),
        },
        &mut provider,
        &mut tools,
        CHILD_MAX_ATTEMPTS,
    )
    .await
    .map_err(|error| format!("delegated runtime failed: {error}"))?;
    if outcome.terminal_outcome.status != TerminalStatus::Completed {
        return Err(format!(
            "delegated turn ended with status {:?}: {}",
            outcome.terminal_outcome.status,
            outcome.terminal_outcome.reason.as_deref().unwrap_or("no provider reason")
        ));
    }
    let summary = outcome
        .terminal_outcome
        .final_response
        .ok_or_else(|| "delegated turn completed without a final response".to_owned())?;
    Ok((summary, child_scope))
}

fn decode_delegate_args(arguments: &ToolArguments) -> Result<DelegateArgs, String> {
    serde_json::from_value(Value::Object(Map::from_iter(arguments.0.clone())))
        .map_err(|error| format!("invalid delegate_task arguments: {error}"))
}

fn validate_delegate_args(arguments: DelegateArgs) -> Result<DelegateArgs, String> {
    if arguments.goal.is_empty() || arguments.goal.trim() != arguments.goal {
        return Err(
            "delegate_task goal must be non-empty and have no surrounding whitespace".into()
        );
    }
    if arguments.goal.chars().count() > MAX_GOAL_CHARS {
        return Err(format!("delegate_task goal exceeds {MAX_GOAL_CHARS} characters"));
    }
    if arguments.context.as_ref().is_some_and(|context| context.chars().count() > MAX_CONTEXT_CHARS)
    {
        return Err(format!("delegate_task context exceeds {MAX_CONTEXT_CHARS} characters"));
    }
    Ok(arguments)
}

fn child_system_prompt(root: &Path, arguments: &DelegateArgs) -> String {
    let mut prompt = format!(
        "You are a focused leaf subagent. You cannot delegate, interact with the user, or modify files. Inspect the workspace at {} with read-only tools when useful.\n\nTASK:\n{}",
        root.display(),
        arguments.goal
    );
    if let Some(context) = arguments.context.as_ref().filter(|value| !value.is_empty()) {
        prompt.push_str("\n\nCONTEXT:\n");
        prompt.push_str(context);
    }
    prompt
}

fn bounded(value: String) -> String {
    if value.chars().count() <= MAX_RESULT_CHARS {
        return value;
    }
    let mut bounded = value.chars().take(MAX_RESULT_CHARS).collect::<String>();
    bounded.push_str("\n[delegated result truncated]");
    bounded
}

fn stable_delegation_identity(execution_key: &str) -> String {
    format!("{:x}", Sha256::digest(execution_key.as_bytes()))
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn unix_time_ms() -> Result<u64, ports::DelegationStoreError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ports::DelegationStoreError::Invalid(error.to_string()))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| ports::DelegationStoreError::Invalid("Unix timestamp exceeds u64".into()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use domain::{InvocationId, ToolArguments, ToolCall, ToolCallId, ToolEffect, ToolResultStatus};
    use futures_executor::block_on;
    use ports::ToolBroker;
    use serde_json::json;
    use tempfile::tempdir;

    use super::{AgentTools, AgentToolsConfig};

    #[test]
    fn parent_catalog_adds_exactly_one_leaf_delegation_tool() {
        let catalog = AgentTools::catalog();
        assert!(AgentTools::catalog_enables_delegation(&catalog));
        assert_eq!(
            catalog
                .iter()
                .filter(|tool| tool["function"]["name"] == json!("delegate_task"))
                .count(),
            1
        );
    }

    #[test]
    fn malformed_delegation_is_a_recoverable_tool_failure() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempdir()?;
        let state = root.path().join("state.db");
        let config = AgentToolsConfig::new(
            root.path(),
            state,
            "http://127.0.0.1:1/v1",
            None,
            "test-model",
            true,
        )?;
        let mut tools = AgentTools::new(config, "parent")?;
        let calls = [ToolCall {
            id: ToolCallId::new("delegate-invalid")?,
            name: "delegate_task".into(),
            arguments: ToolArguments(BTreeMap::from([("goal".into(), json!(" padded "))])),
        }];
        let plans = tools.plan(&calls, &[InvocationId::new("parent:delegate-invalid")?])?;
        assert_eq!(plans[0].effect, ToolEffect::ModelInference);
        let terminals = block_on(tools.execute(&plans))?;
        assert_eq!(terminals[0].status, ToolResultStatus::Failed);
        assert!(terminals[0].content.contains("surrounding whitespace"));
        Ok(())
    }

    #[test]
    fn frozen_catalog_without_delegation_cannot_gain_it_at_runtime()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let state = root.path().join("state.db");
        let config = AgentToolsConfig::new(
            root.path(),
            state,
            "http://127.0.0.1:1/v1",
            None,
            "test-model",
            false,
        )?;
        let mut tools = AgentTools::new(config, "parent")?;
        let calls = [ToolCall {
            id: ToolCallId::new("delegate-hidden")?,
            name: "delegate_task".into(),
            arguments: ToolArguments(BTreeMap::from([("goal".into(), json!("Inspect it"))])),
        }];
        let plans = tools.plan(&calls, &[InvocationId::new("parent:delegate-hidden")?])?;
        assert_eq!(plans[0].effect, ToolEffect::ReadOnly);
        let terminals = block_on(tools.execute(&plans))?;
        assert_eq!(terminals[0].status, ToolResultStatus::Failed);
        assert!(terminals[0].content.contains("unknown read-only tool"));
        Ok(())
    }
}
