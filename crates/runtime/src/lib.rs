//! Effect-free agent turn state machine.
//!
//! The runtime consumes normalized provider events and policy-aware scripted
//! tool outcomes through kernel-owned ports. It performs no network,
//! filesystem, process, credential, or database effects itself.

use std::collections::{BTreeMap, HashMap, HashSet};

use domain::{
    Conversation, ConversationError, IdError, PlannedToolCall, SemanticMessage, ToolArguments,
    ToolCall, ToolCallId, ToolResult, ToolResultStatus, ToolTerminal,
};
use futures_util::StreamExt;
use ports::{AttemptErrorPolicy, Provider, ProviderError, ToolBroker, ToolBrokerError};
use protocol::{
    AgentTurnRequest, ChatCompletionsRequest, ContractOutcome, ProviderEvent, ProviderFunctionCall,
    ProviderMessage, ProviderRequestRecord, ProviderToolCall, TerminalOutcome, TerminalStatus,
    TransportKind, Usage,
};
use serde_json::{Map, Value, json};
use thiserror::Error;

const MAX_ATTEMPTS_PER_TURN: usize = 128;

/// An offline turn could not produce a valid contract outcome.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Turn input or a normalized provider sequence violated a runtime contract.
    #[error("invalid turn: {0}")]
    InvalidTurn(String),
    /// Provider startup or stream delivery failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Tool planning or execution failed before a typed terminal existed.
    #[error(transparent)]
    ToolBroker(#[from] ToolBrokerError),
    /// A provider-visible identifier was invalid.
    #[error(transparent)]
    Identifier(#[from] IdError),
    /// The completed semantic conversation violated ordering invariants.
    #[error(transparent)]
    Conversation(#[from] ConversationError),
    /// A typed protocol record could not be projected to or from JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Default)]
struct PartialToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

enum AttemptTerminal {
    Completed { finish_reason: String, provider_data: Option<Value> },
    Error { reason: String },
    Cancelled { reason: String },
    Malformed { reason: String },
}

/// Execute one complete, provider-neutral agent turn.
pub async fn run_turn<P, T>(
    request: AgentTurnRequest,
    provider: &mut P,
    tools: &mut T,
) -> Result<ContractOutcome, RuntimeError>
where
    P: Provider,
    T: ToolBroker,
{
    validate_request(&request)?;

    let AgentTurnRequest {
        execution_scope: _,
        transport,
        model,
        system_prompt,
        conversation,
        tools: tool_catalog,
    } = request;
    let mut raw_messages = conversation;
    let mut provider_requests = Vec::new();
    let mut persistence_intents = raw_messages
        .iter()
        .map(|message| {
            Ok(json!({
                "type": format!("append_{}", message.role()),
                "message": serde_json::to_value(message)?,
            }))
        })
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    let mut public_events = Vec::new();
    let mut usage = Usage::default();
    let mut attempt_count = 0_usize;

    let terminal_outcome = loop {
        attempt_count += 1;
        if attempt_count > MAX_ATTEMPTS_PER_TURN {
            return Err(RuntimeError::InvalidTurn(format!(
                "turn exceeded {MAX_ATTEMPTS_PER_TURN} provider attempts"
            )));
        }

        let mut request_messages = Vec::with_capacity(raw_messages.len() + 1);
        if let Some(prompt) = &system_prompt {
            request_messages.push(ProviderMessage::System { content: prompt.clone() });
        }
        request_messages.extend(raw_messages.clone());
        let provider_request = ChatCompletionsRequest {
            model: model.clone(),
            messages: request_messages,
            tools: tool_catalog.clone(),
        };
        let mut attempt = provider.stream(provider_request.clone()).await?;
        let attempt_id = attempt.attempt_id.clone();
        provider_requests.push(ProviderRequestRecord {
            attempt_id: attempt_id.clone(),
            transport,
            request: serde_json::to_value(provider_request)?,
        });
        public_events.push(json!({
            "type": "provider.attempt_started",
            "attempt_id": attempt_id,
        }));

        let mut text = String::new();
        let mut reasoning = String::new();
        let mut partial_calls = BTreeMap::<usize, PartialToolCall>::new();
        let mut terminal = None;
        let mut visible_output = false;

        while let Some(event) = attempt.events.next().await {
            let event = event?;
            if terminal.is_some() {
                return Err(RuntimeError::InvalidTurn(format!(
                    "provider attempt {attempt_id} emitted an event after its terminal"
                )));
            }
            match event {
                ProviderEvent::MessageStart => {
                    public_events.push(json!({
                        "type": "message.start",
                        "attempt_id": attempt_id,
                    }));
                }
                ProviderEvent::TextDelta { text: delta } => {
                    visible_output = true;
                    text.push_str(&delta);
                    public_events.push(json!({
                        "type": "message.delta",
                        "attempt_id": attempt_id,
                        "text": delta,
                    }));
                }
                ProviderEvent::ReasoningDelta { text: delta } => {
                    visible_output = true;
                    reasoning.push_str(&delta);
                    public_events.push(json!({
                        "type": "reasoning.delta",
                        "attempt_id": attempt_id,
                        "text": delta,
                    }));
                }
                ProviderEvent::ToolCallDelta { index, id, name, arguments_delta } => {
                    visible_output = true;
                    let partial = partial_calls.entry(index).or_default();
                    merge_stable_fragment(&mut partial.id, id.as_deref(), index, "id")?;
                    merge_stable_fragment(&mut partial.name, name.as_deref(), index, "name")?;
                    partial.arguments.push_str(&arguments_delta);
                    public_events.push(json!({
                        "type": "tool_call.delta",
                        "attempt_id": attempt_id,
                        "index": index,
                        "id": id,
                        "name": name,
                        "arguments_delta": arguments_delta,
                    }));
                }
                ProviderEvent::Usage {
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    cached_tokens,
                } => {
                    let attempt_usage =
                        Usage { prompt_tokens, completion_tokens, total_tokens, cached_tokens };
                    usage.prompt_tokens += prompt_tokens;
                    usage.completion_tokens += completion_tokens;
                    usage.total_tokens += total_tokens;
                    usage.cached_tokens += cached_tokens;
                    public_events.push(json!({
                        "type": "usage",
                        "attempt_id": attempt_id,
                        "usage": attempt_usage,
                    }));
                }
                ProviderEvent::Completed { finish_reason, provider_data } => {
                    terminal = Some(AttemptTerminal::Completed {
                        finish_reason: finish_reason.unwrap_or_else(|| "stop".into()),
                        provider_data,
                    });
                }
                ProviderEvent::Error { reason } => {
                    terminal = Some(AttemptTerminal::Error {
                        reason: reason.unwrap_or_else(|| "provider_error".into()),
                    });
                }
                ProviderEvent::Cancelled { reason } => {
                    terminal = Some(AttemptTerminal::Cancelled {
                        reason: reason.unwrap_or_else(|| "cancelled".into()),
                    });
                }
                ProviderEvent::Malformed { reason } => {
                    terminal = Some(AttemptTerminal::Malformed {
                        reason: reason.unwrap_or_else(|| "malformed_provider_stream".into()),
                    });
                }
            }
        }

        let Some(terminal) = terminal else {
            public_events.push(json!({
                "type": "provider.stream_failed",
                "attempt_id": attempt_id,
                "reason": "truncated",
            }));
            break TerminalOutcome {
                status: TerminalStatus::Failed,
                final_response: None,
                finish_reason: None,
                reason: Some("provider_stream_truncated".into()),
                visible_output: Some(visible_output),
            };
        };

        match terminal {
            AttemptTerminal::Error { reason } => {
                public_events.push(json!({
                    "type": "provider.attempt_failed",
                    "attempt_id": attempt_id,
                    "reason": reason,
                }));
                if attempt.error_policy == AttemptErrorPolicy::FallbackBeforeVisibleOutput
                    && !visible_output
                {
                    continue;
                }
                break TerminalOutcome {
                    status: if visible_output {
                        TerminalStatus::Interrupted
                    } else {
                        TerminalStatus::Failed
                    },
                    final_response: None,
                    finish_reason: None,
                    reason: Some(reason),
                    visible_output: Some(visible_output),
                };
            }
            AttemptTerminal::Cancelled { reason } => {
                public_events.push(json!({
                    "type": "provider.cancelled",
                    "attempt_id": attempt_id,
                    "reason": reason,
                }));
                break TerminalOutcome {
                    status: TerminalStatus::Cancelled,
                    final_response: None,
                    finish_reason: None,
                    reason: Some(reason),
                    visible_output: None,
                };
            }
            AttemptTerminal::Malformed { reason } => {
                public_events.push(json!({
                    "type": "provider.protocol_error",
                    "attempt_id": attempt_id,
                    "reason": reason,
                }));
                break TerminalOutcome {
                    status: TerminalStatus::Failed,
                    final_response: None,
                    finish_reason: None,
                    reason: Some(reason),
                    visible_output: None,
                };
            }
            AttemptTerminal::Completed { finish_reason, provider_data } => {
                if partial_calls.is_empty() {
                    let assistant = ProviderMessage::Assistant {
                        content: Some(text.clone()),
                        reasoning: (!reasoning.is_empty()).then_some(reasoning),
                        tool_calls: Vec::new(),
                        provider_replay: provider_data,
                    };
                    raw_messages.push(assistant.clone());
                    persistence_intents.push(json!({
                        "type": "append_assistant",
                        "message": serde_json::to_value(&assistant)?,
                    }));
                    public_events.push(json!({
                        "type": "message.complete",
                        "attempt_id": attempt_id,
                        "content": text,
                        "finish_reason": finish_reason,
                    }));
                    break TerminalOutcome {
                        status: if finish_reason == "stop" {
                            TerminalStatus::Completed
                        } else {
                            TerminalStatus::Incomplete
                        },
                        final_response: Some(text),
                        finish_reason: Some(finish_reason),
                        reason: None,
                        visible_output: None,
                    };
                }

                let calls = finish_tool_calls(partial_calls)?;
                let assistant = provider_tool_request(&text, &reasoning, &calls, provider_data)?;
                raw_messages.push(assistant.clone());
                persistence_intents.push(json!({
                    "type": "append_assistant_tool_request",
                    "message": serde_json::to_value(&assistant)?,
                }));

                let planned = tools.plan(&calls)?;
                validate_plans(&calls, &planned)?;
                for call in &planned {
                    persistence_intents.push(planned_intent(call));
                    if let Some(approval) = &call.approval {
                        public_events.push(json!({
                            "type": "approval.request",
                            "call_id": call.call_id,
                            "requirement": approval,
                        }));
                        public_events.push(json!({
                            "type": "approval.resolved",
                            "call_id": call.call_id,
                            "decision": approval.decision,
                        }));
                    }
                    if !call.approval.as_ref().is_some_and(domain::ApprovalRecord::denied) {
                        persistence_intents.push(json!({
                            "type": "tool_started",
                            "call_id": call.call_id,
                            "execution_key": call.execution_key,
                        }));
                        public_events.push(json!({
                            "type": "tool.start",
                            "call_id": call.call_id,
                            "name": call.name,
                            "execution_key": call.execution_key,
                        }));
                    }
                }

                let completed = tools.execute(&planned).await?;
                validate_terminals(&planned, &completed)?;
                let terminals_by_id = completed
                    .iter()
                    .map(|terminal| (terminal.call_id.clone(), terminal))
                    .collect::<HashMap<_, _>>();
                for terminal in &completed {
                    persistence_intents.push(terminal_intent(terminal));
                    public_events.push(json!({
                        "type": "tool.complete",
                        "call_id": terminal.call_id,
                        "name": terminal.name,
                        "status": status_name(terminal.status),
                        "execution_key": terminal.execution_key,
                    }));
                }

                let ordered = planned
                    .iter()
                    .map(|plan| {
                        terminals_by_id.get(&plan.call_id).copied().ok_or_else(|| {
                            RuntimeError::InvalidTurn(format!(
                                "tool {} has no terminal outcome",
                                plan.call_id
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let provider_results = ordered
                    .iter()
                    .map(|terminal| provider_tool_result(terminal))
                    .collect::<Result<Vec<_>, RuntimeError>>()?;
                raw_messages.extend(provider_results);
                persistence_intents.push(json!({
                    "type": "append_tool_result_batch",
                    "results": ordered.iter().map(|terminal| json!({
                        "call_id": terminal.call_id,
                        "name": terminal.name,
                        "status": status_name(terminal.status),
                        "content": terminal.content,
                        "execution_key": terminal.execution_key,
                    })).collect::<Vec<_>>(),
                }));
                public_events.push(json!({
                    "type": "tool_result_batch.complete",
                    "call_ids": ordered.iter().map(|terminal| terminal.call_id.as_str()).collect::<Vec<_>>(),
                }));
            }
        }
    };

    if let Some(remaining) = provider.remaining_attempts()
        && remaining != 0
    {
        return Err(RuntimeError::InvalidTurn(format!(
            "terminal outcome left {remaining} unused provider attempt(s)"
        )));
    }

    let semantic_conversation = semanticize(&raw_messages)?;
    Ok(ContractOutcome {
        provider_requests,
        semantic_conversation,
        persistence_intents,
        public_events,
        usage,
        terminal_outcome,
    })
}

fn validate_request(request: &AgentTurnRequest) -> Result<(), RuntimeError> {
    if request.transport != TransportKind::ChatCompletions {
        return Err(RuntimeError::InvalidTurn(
            "the offline agent runtime currently supports chat_completions only".into(),
        ));
    }
    if request.model.is_empty() {
        return Err(RuntimeError::InvalidTurn("model must be non-empty".into()));
    }
    if request.execution_scope.is_empty() {
        return Err(RuntimeError::InvalidTurn("execution scope must be non-empty".into()));
    }
    if request.conversation.is_empty()
        || !matches!(request.conversation.last(), Some(ProviderMessage::User { .. }))
    {
        return Err(RuntimeError::InvalidTurn(
            "conversation must be non-empty and end with a user message".into(),
        ));
    }
    Ok(())
}

fn merge_stable_fragment(
    current: &mut Option<String>,
    incoming: Option<&str>,
    index: usize,
    field: &str,
) -> Result<(), RuntimeError> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    if let Some(current) = current {
        if current != incoming {
            return Err(RuntimeError::InvalidTurn(format!(
                "tool call {index} changed {field} mid-stream"
            )));
        }
    } else {
        *current = Some(incoming.to_owned());
    }
    Ok(())
}

fn finish_tool_calls(
    partial_calls: BTreeMap<usize, PartialToolCall>,
) -> Result<Vec<ToolCall>, RuntimeError> {
    if partial_calls.keys().copied().ne(0..partial_calls.len()) {
        return Err(RuntimeError::InvalidTurn(
            "tool call indexes must be contiguous from zero".into(),
        ));
    }
    partial_calls
        .into_iter()
        .map(|(index, partial)| {
            let id = partial.id.ok_or_else(|| {
                RuntimeError::InvalidTurn(format!("tool call at index {index} has no id"))
            })?;
            let name = partial
                .name
                .ok_or_else(|| RuntimeError::InvalidTurn(format!("tool call {id} has no name")))?;
            let arguments = if partial.arguments.is_empty() {
                BTreeMap::new()
            } else {
                serde_json::from_str::<BTreeMap<String, Value>>(&partial.arguments).map_err(
                    |error| {
                        RuntimeError::InvalidTurn(format!(
                            "tool call {id} has malformed JSON arguments: {error}"
                        ))
                    },
                )?
            };
            Ok(ToolCall { id: ToolCallId::new(id)?, name, arguments: ToolArguments(arguments) })
        })
        .collect()
}

fn provider_tool_request(
    content: &str,
    reasoning: &str,
    calls: &[ToolCall],
    provider_replay: Option<Value>,
) -> Result<ProviderMessage, RuntimeError> {
    let tool_calls = calls
        .iter()
        .map(|call| {
            Ok(ProviderToolCall {
                id: call.id.as_str().to_owned(),
                kind: "function".into(),
                function: ProviderFunctionCall {
                    name: call.name.clone(),
                    arguments: serde_json::to_string(&call.arguments.0)?,
                },
            })
        })
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    Ok(ProviderMessage::Assistant {
        content: (!content.is_empty()).then(|| content.to_owned()),
        reasoning: (!reasoning.is_empty()).then(|| reasoning.to_owned()),
        tool_calls,
        provider_replay,
    })
}

fn validate_plans(calls: &[ToolCall], plans: &[PlannedToolCall]) -> Result<(), RuntimeError> {
    let call_ids = calls.iter().map(|call| &call.id).collect::<Vec<_>>();
    let plan_ids = plans.iter().map(|plan| &plan.call_id).collect::<Vec<_>>();
    if call_ids != plan_ids {
        return Err(RuntimeError::InvalidTurn(
            "tool plans must be complete and ordered like model calls".into(),
        ));
    }
    Ok(())
}

fn validate_terminals(
    plans: &[PlannedToolCall],
    terminals: &[ToolTerminal],
) -> Result<(), RuntimeError> {
    let expected = plans.iter().map(|plan| &plan.call_id).collect::<HashSet<_>>();
    let actual = terminals.iter().map(|terminal| &terminal.call_id).collect::<HashSet<_>>();
    if expected != actual || terminals.len() != plans.len() {
        return Err(RuntimeError::InvalidTurn(
            "tool terminals must contain each planned call exactly once".into(),
        ));
    }
    Ok(())
}

fn planned_intent(call: &PlannedToolCall) -> Value {
    let mut value = Map::from_iter([
        ("type".into(), json!("tool_planned")),
        ("call_id".into(), json!(call.call_id)),
        ("name".into(), json!(call.name)),
        ("arguments".into(), json!(call.arguments)),
        ("execution_key".into(), json!(call.execution_key)),
        ("effect".into(), json!(call.effect)),
    ]);
    if let Some(approval) = &call.approval {
        value.insert("approval".into(), json!(approval));
    }
    Value::Object(value)
}

fn terminal_intent(terminal: &ToolTerminal) -> Value {
    let mut value = Map::from_iter([
        ("type".into(), json!(format!("tool_{}", status_name(terminal.status)))),
        ("call_id".into(), json!(terminal.call_id)),
        ("execution_key".into(), json!(terminal.execution_key)),
        ("effect".into(), json!(terminal.effect)),
        ("content".into(), json!(terminal.content)),
    ]);
    if let Some(receipt) = &terminal.receipt {
        value.insert("receipt".into(), json!(receipt));
    }
    Value::Object(value)
}

fn provider_tool_result(terminal: &ToolTerminal) -> Result<ProviderMessage, RuntimeError> {
    let content = if terminal.status == ToolResultStatus::Succeeded {
        terminal.content.clone()
    } else {
        let payload = BTreeMap::from([
            ("execution_key", json!(terminal.execution_key)),
            ("message", json!(terminal.content)),
            ("status", json!(status_name(terminal.status))),
        ]);
        serde_json::to_string(&payload)?
    };
    Ok(ProviderMessage::Tool {
        tool_call_id: terminal.call_id.as_str().to_owned(),
        status: status_name(terminal.status).into(),
        content,
        execution_key: terminal.execution_key.clone(),
    })
}

fn semanticize(messages: &[ProviderMessage]) -> Result<Vec<SemanticMessage>, RuntimeError> {
    let mut semantic = Vec::new();
    let mut index = 0;
    while index < messages.len() {
        match &messages[index] {
            ProviderMessage::System { .. } => index += 1,
            ProviderMessage::User { content } => {
                semantic.push(SemanticMessage::User { content: content.clone() });
                index += 1;
            }
            ProviderMessage::Assistant { content, reasoning, tool_calls, provider_replay }
                if tool_calls.is_empty() =>
            {
                semantic.push(SemanticMessage::Assistant {
                    content: content.clone().unwrap_or_default(),
                    reasoning: reasoning.clone(),
                    provider_replay: provider_replay.clone(),
                });
                index += 1;
            }
            ProviderMessage::Assistant { content, reasoning, tool_calls, provider_replay } => {
                let calls = tool_calls
                    .iter()
                    .map(|call| {
                        Ok(ToolCall {
                            id: ToolCallId::new(call.id.clone())?,
                            name: call.function.name.clone(),
                            arguments: ToolArguments(serde_json::from_str(
                                &call.function.arguments,
                            )?),
                        })
                    })
                    .collect::<Result<Vec<_>, RuntimeError>>()?;
                let expected_ids = calls.iter().map(|call| call.id.clone()).collect::<Vec<_>>();
                semantic.push(SemanticMessage::AssistantToolRequest {
                    content: content.clone(),
                    calls,
                    reasoning: reasoning.clone(),
                    provider_replay: provider_replay.clone(),
                });
                index += 1;

                let mut results = Vec::new();
                while let Some(ProviderMessage::Tool {
                    tool_call_id,
                    status,
                    content,
                    execution_key,
                }) = messages.get(index)
                {
                    results.push(ToolResult {
                        call_id: ToolCallId::new(tool_call_id.clone())?,
                        status: parse_status(status)?,
                        content: content.clone(),
                        execution_key: Some(execution_key.clone()),
                    });
                    index += 1;
                }
                if results.iter().map(|result| &result.call_id).ne(expected_ids.iter()) {
                    return Err(RuntimeError::InvalidTurn(
                        "tool results must be complete and ordered like assistant calls".into(),
                    ));
                }
                semantic.push(SemanticMessage::ToolResultBatch { results });
            }
            ProviderMessage::Tool { .. } => {
                return Err(RuntimeError::InvalidTurn(
                    "provider conversation contains an orphaned tool result".into(),
                ));
            }
        }
    }
    Ok(Conversation::new(semantic)?.into_messages())
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

fn parse_status(value: &str) -> Result<ToolResultStatus, RuntimeError> {
    match value {
        "succeeded" => Ok(ToolResultStatus::Succeeded),
        "failed" => Ok(ToolResultStatus::Failed),
        "cancelled" => Ok(ToolResultStatus::Cancelled),
        "rejected" => Ok(ToolResultStatus::Rejected),
        "outcome_unknown" => Ok(ToolResultStatus::OutcomeUnknown),
        "observed" => Ok(ToolResultStatus::Observed),
        other => Err(RuntimeError::InvalidTurn(format!("unknown tool terminal status {other:?}"))),
    }
}
