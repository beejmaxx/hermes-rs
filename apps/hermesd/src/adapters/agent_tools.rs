//! Session tool broker combining local inspection with isolated leaf delegation.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use domain::{
    PlannedToolCall, ToolArguments, ToolCall, ToolEffect, ToolResultStatus, ToolTerminal,
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use ports::{ToolBroker, ToolBrokerError};
use protocol::{AgentTurnRequest, ProviderMessage, TerminalStatus, TransportKind};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use thiserror::Error;

use super::{
    local_tools::{LocalToolsConfigError, ReadOnlyLocalTools},
    openai::{OpenAiCompatibleProvider, OpenAiProviderConfigError},
    sqlite::SqliteEffectLedger,
};
use runtime::JournaledToolBroker;

const DELEGATE_TOOL: &str = "delegate_task";
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
        })
    }
}

/// Tool broker for a parent agent session.
pub struct AgentTools {
    local: ReadOnlyLocalTools,
    config: AgentToolsConfig,
    execution_scope: String,
}

impl AgentTools {
    /// Construct tools for one immutable execution scope.
    pub fn new(
        config: AgentToolsConfig,
        execution_scope: impl Into<String>,
    ) -> Result<Self, AgentToolsConfigError> {
        let execution_scope = execution_scope.into();
        let local = ReadOnlyLocalTools::new(&config.root, &execution_scope)?;
        Ok(Self { local, config, execution_scope })
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

    /// Whether an ordered catalog exposes leaf delegation.
    #[must_use]
    pub fn catalog_enables_delegation(catalog: &[Value]) -> bool {
        catalog.iter().any(|tool| {
            tool.get("function").and_then(|function| function.get("name")).and_then(Value::as_str)
                == Some(DELEGATE_TOOL)
        })
    }
}

impl ToolBroker for AgentTools {
    fn plan(&mut self, calls: &[ToolCall]) -> Result<Vec<PlannedToolCall>, ToolBrokerError> {
        let mut seen = HashSet::with_capacity(calls.len());
        calls
            .iter()
            .map(|call| {
                if !seen.insert(&call.id) {
                    return Err(ToolBrokerError::new(format!(
                        "duplicate tool call id {}",
                        call.id
                    )));
                }
                let effect = if self.config.delegation_enabled && call.name == DELEGATE_TOOL {
                    ToolEffect::ModelInference
                } else {
                    ToolEffect::ReadOnly
                };
                Ok(PlannedToolCall {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                    execution_key: format!("{}:{}", self.execution_scope, call.id),
                    effect,
                    approval: None,
                })
            })
            .collect()
    }

    fn execute<'a>(
        &'a mut self,
        calls: &'a [PlannedToolCall],
    ) -> BoxFuture<'a, Result<Vec<ToolTerminal>, ToolBrokerError>> {
        async move {
            let (delegated, local): (Vec<_>, Vec<_>) = calls
                .iter()
                .cloned()
                .partition(|call| self.config.delegation_enabled && call.name == DELEGATE_TOOL);
            let mut completed = self.local.execute(&local).await?;
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
        execution_key: plan.execution_key,
        effect: ToolEffect::ModelInference,
        receipt,
    }
}

async fn run_child(
    config: &AgentToolsConfig,
    plan: &PlannedToolCall,
    arguments: DelegateArgs,
) -> Result<(String, String), String> {
    let child_scope = format!("{}:child", plan.execution_key);
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use domain::{ToolArguments, ToolCall, ToolCallId, ToolEffect, ToolResultStatus};
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
        let plans = tools.plan(&calls)?;
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
        let plans = tools.plan(&calls)?;
        assert_eq!(plans[0].effect, ToolEffect::ReadOnly);
        let terminals = block_on(tools.execute(&plans))?;
        assert_eq!(terminals[0].status, ToolResultStatus::Failed);
        assert!(terminals[0].content.contains("unknown read-only tool"));
        Ok(())
    }
}
