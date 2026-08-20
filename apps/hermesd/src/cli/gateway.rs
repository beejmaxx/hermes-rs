//! Minimal long-lived stdio JSON-RPC host for existing Hermes clients.

use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use domain::{
    ForegroundTurnId, ForegroundTurnSpec, ForegroundTurnState, ForegroundTurnTerminal,
    OwnerGeneration, SemanticMessage, SessionId,
};
use ports::{ForegroundTurnStore, ForegroundTurnStoreError, SessionStore, SessionStoreError};
use protocol::{
    ForegroundTurnSnapshot, GatewayEventFrame, GatewayFailure, GatewayRequest, GatewaySuccess,
    JSON_RPC_VERSION,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tokio::{io::AsyncBufReadExt, sync::oneshot, task::JoinHandle};

use super::{
    chat::{LiveSettings, RuntimeArgs, completed_response, execute_turn_observed},
    state::state_path,
};
use crate::adapters::{SqliteForegroundTurnStore, SqliteSessionStore};

const RESTART_RECONCILIATION_REASON: &str =
    "owning gateway exited before recording a foreground turn terminal";

/// Arguments for the long-lived stdio gateway host.
#[derive(Debug, clap::Args)]
pub struct GatewayArgs {
    /// Provider and immutable runtime settings used by every created session.
    #[command(flatten)]
    runtime: RuntimeArgs,
}

/// Serve newline-delimited JSON-RPC until stdin closes.
pub async fn run_gateway(
    arguments: GatewayArgs,
    state_override: Option<&Path>,
) -> anyhow::Result<()> {
    let settings = LiveSettings::for_new(&arguments.runtime)?;
    let state = state_path(state_override)?;
    let _lease = GatewayLease::acquire(&state)?;
    let reconciled = SqliteForegroundTurnStore::open(&state)?
        .reconcile_running(RESTART_RECONCILIATION_REASON, unix_time_ms()?)?;
    let writer = FrameWriter::new();
    writer.send(&GatewayEventFrame::global(
        "gateway.ready",
        Some(json!({
            "skin": {},
            "change_events": false,
            "backend": "hermes-rs",
            "reconciled_foreground_turns": reconciled.len(),
        })),
    ))?;

    let mut host = GatewayHost {
        settings,
        state,
        writer,
        busy: Arc::new(Mutex::new(HashSet::new())),
        controls: Arc::new(Mutex::new(HashMap::new())),
        turns: Vec::new(),
    };
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<GatewayRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                host.writer.send(&GatewayFailure::new(
                    Value::Null,
                    -32700,
                    format!("parse error: {error}"),
                ))?;
                continue;
            }
        };
        host.dispatch(request).await?;
        host.turns.retain(|turn| !turn.is_finished());
    }
    for turn in host.turns {
        let _ = turn.await;
    }
    Ok(())
}

struct GatewayHost {
    settings: LiveSettings,
    state: std::path::PathBuf,
    writer: FrameWriter,
    busy: Arc<Mutex<HashSet<String>>>,
    controls: Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>,
    turns: Vec<JoinHandle<()>>,
}

enum TurnExit {
    Completed(String),
    Failed(String),
    Interrupted,
}

struct GatewayLease {
    _file: File,
}

impl GatewayLease {
    fn acquire(state: &Path) -> anyhow::Result<Self> {
        let lock_path = gateway_lock_path(state);
        if let Some(parent) = lock_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("could not create gateway state directory {}", parent.display())
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("could not open gateway lease {}", lock_path.display()))?;
        file.try_lock().with_context(|| {
            format!("could not acquire exclusive gateway ownership for {}", state.display())
        })?;
        Ok(Self { _file: file })
    }
}

fn gateway_lock_path(state: &Path) -> PathBuf {
    let mut path = state.as_os_str().to_os_string();
    path.push(".gateway.lock");
    PathBuf::from(path)
}

impl GatewayHost {
    async fn dispatch(&mut self, request: GatewayRequest) -> anyhow::Result<()> {
        if request.jsonrpc != JSON_RPC_VERSION {
            return self.writer.send(&GatewayFailure::new(
                request.id,
                -32600,
                "invalid request: jsonrpc must be 2.0",
            ));
        }
        let params = match request.params.as_object() {
            Some(params) => params,
            None => {
                return self.writer.send(&GatewayFailure::new(
                    request.id,
                    -32602,
                    "invalid params: expected an object",
                ));
            }
        };
        if request.method == "prompt.submit" {
            return self.submit(request.id, params).await;
        }
        let result = self.call(&request.method, params);
        match result {
            Ok(result) => self.writer.send(&GatewaySuccess::new(request.id, result)),
            Err(error) => {
                self.writer.send(&GatewayFailure::new(request.id, error.code, error.message))
            }
        }
    }

    fn call(&self, method: &str, params: &Map<String, Value>) -> Result<Value, RpcError> {
        match method {
            "setup.status" => Ok(json!({
                "provider_configured": self
                    .settings
                    .api_key_env()
                    .is_none_or(|name| std::env::var_os(name).is_some())
            })),
            "config.get" => Ok(config_value(params)),
            "session.create" => self.create_session(params),
            "session.resume" | "session.activate" => self.resume_session(params),
            "session.interrupt" => self.interrupt_session(params),
            "session.close" => {
                let _ = session_param(params)?;
                Ok(json!({"closed": true}))
            }
            "session.list" => self.list_sessions(false),
            "session.active_list" => self.list_sessions(true),
            "session.most_recent" => self.most_recent_session(),
            "input.detect_drop" => {
                let _ = session_param(params)?;
                Ok(json!({"matched": false}))
            }
            "commands.catalog" => Ok(json!({
                "canon": {}, "categories": [], "pairs": [], "skill_count": 0, "sub": {}
            })),
            "complete.slash" | "complete.path" => Ok(json!({"items": []})),
            "session.usage" => Ok(zero_usage(self.settings.model())),
            "system.battery" => Ok(json!({"available": false})),
            "terminal.resize" => {
                let _ = session_param(params)?;
                Ok(json!({"ok": true}))
            }
            "wake.start" => Ok(json!({"reason": "unsupported", "started": false})),
            _ => Err(RpcError::new(-32601, format!("method not found: {method}"))),
        }
    }

    fn create_session(&self, params: &Map<String, Value>) -> Result<Value, RpcError> {
        if let Some(requested) = params.get("model").and_then(Value::as_str)
            && requested != self.settings.model()
        {
            return Err(RpcError::new(
                4091,
                format!(
                    "gateway model is frozen as {}; requested {requested}",
                    self.settings.model()
                ),
            ));
        }
        if let Some(cwd) = params.get("cwd").and_then(Value::as_str).filter(|cwd| !cwd.is_empty()) {
            let resolved = std::fs::canonicalize(cwd)
                .map_err(|error| RpcError::new(4001, format!("invalid cwd: {error}")))?;
            if resolved != self.settings.root() {
                return Err(RpcError::new(
                    4092,
                    "gateway workspace root is immutable for this process",
                ));
            }
        }
        let session_id = fresh_session_id().map_err(internal_error)?;
        let config = self.settings.session_config(session_id.clone()).map_err(internal_error)?;
        let mut store = SqliteSessionStore::open(&self.state).map_err(internal_error)?;
        store.create(config).map_err(internal_error)?;
        Ok(json!({
            "session_id": session_id,
            "stored_session_id": session_id,
            "message_count": 0,
            "messages": [],
            "info": self.session_info(),
        }))
    }

    fn resume_session(&self, params: &Map<String, Value>) -> Result<Value, RpcError> {
        let session_id = SessionId::new(session_param(params)?).map_err(internal_error)?;
        let mut store = SqliteSessionStore::open(&self.state).map_err(internal_error)?;
        let snapshot = store.load(&session_id).map_err(|error| match error {
            SessionStoreError::NotFound(_) => RpcError::new(4040, error.to_string()),
            other => internal_error(other),
        })?;
        let latest = SqliteForegroundTurnStore::open(&self.state)
            .map_err(internal_error)?
            .latest(&session_id)
            .map_err(internal_error)?;
        let busy = self.is_busy(session_id.as_str());
        let projection = resume_projection(&snapshot.conversation, latest.as_ref(), busy);
        Ok(json!({
            "session_id": session_id,
            "resumed": session_id,
            "session_key": session_id,
            "message_count": snapshot.conversation.len(),
            "messages": projection.messages,
            "inflight": projection.inflight,
            "recovery": projection.recovery,
            "running": busy,
            "status": if busy { "working" } else { "idle" },
            "started_at": 0,
            "info": self.session_info(),
        }))
    }

    fn list_sessions(&self, active_shape: bool) -> Result<Value, RpcError> {
        let mut store = SqliteSessionStore::open(&self.state).map_err(internal_error)?;
        let sessions = store.list().map_err(internal_error)?;
        if active_shape {
            return Ok(json!({
                "sessions": sessions.into_iter().map(|session| {
                    let busy = self.is_busy(session.session_id.as_str());
                    json!({
                        "id": session.session_id,
                        "session_key": session.session_id,
                        "message_count": session.message_count,
                        "model": session.model,
                        "preview": "",
                        "started_at": 0,
                        "status": if busy { "working" } else { "idle" },
                        "title": session.session_id.as_str(),
                    })
                }).collect::<Vec<_>>()
            }));
        }
        Ok(json!({
            "sessions": sessions.into_iter().map(|session| json!({
                "id": session.session_id,
                "message_count": session.message_count,
                "preview": "",
                "source": "hermes-rs",
                "started_at": 0,
                "title": session.session_id.as_str(),
            })).collect::<Vec<_>>()
        }))
    }

    fn most_recent_session(&self) -> Result<Value, RpcError> {
        let mut store = SqliteSessionStore::open(&self.state).map_err(internal_error)?;
        let latest = store.list().map_err(internal_error)?.into_iter().next();
        Ok(match latest {
            Some(session) => json!({
                "session_id": session.session_id,
                "source": "hermes-rs",
                "started_at": 0,
                "title": session.session_id.as_str(),
            }),
            None => json!({"session_id": null}),
        })
    }

    fn interrupt_session(&self, params: &Map<String, Value>) -> Result<Value, RpcError> {
        let sid = SessionId::new(session_param(params)?)
            .map_err(|error| RpcError::new(4000, error.to_string()))?;
        let mut store = SqliteSessionStore::open(&self.state).map_err(internal_error)?;
        store.load(&sid).map_err(|error| match error {
            SessionStoreError::NotFound(_) => RpcError::new(4040, error.to_string()),
            other => internal_error(other),
        })?;
        let sender = self
            .controls
            .lock()
            .map_err(|_| RpcError::new(5000, "turn-control lock poisoned"))?
            .remove(sid.as_str());
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
        Ok(json!({"ok": true, "status": "interrupted"}))
    }

    async fn submit(&mut self, id: Value, params: &Map<String, Value>) -> anyhow::Result<()> {
        let sid = match session_param(params).and_then(|sid| {
            SessionId::new(sid).map_err(|error| RpcError::new(4000, error.to_string()))
        }) {
            Ok(sid) => sid,
            Err(error) => {
                return self.writer.send(&GatewayFailure::new(id, error.code, error.message));
            }
        };
        let text = match params.get("text").and_then(Value::as_str) {
            Some(text) if !text.trim().is_empty() => text.to_owned(),
            _ => {
                return self.writer.send(&GatewayFailure::new(
                    id,
                    -32602,
                    "prompt text must be non-empty",
                ));
            }
        };
        let turn_id = fresh_turn_id()?;
        let started_at_ms = unix_time_ms()?;
        {
            let mut busy = self.busy.lock().map_err(|_| anyhow::anyhow!("busy lock poisoned"))?;
            if !busy.insert(sid.as_str().to_owned()) {
                return self.writer.send(&GatewayFailure::new(id, 4090, "session busy"));
            }
        }
        let mut store = match SqliteSessionStore::open(&self.state) {
            Ok(store) => store,
            Err(error) => {
                self.clear_busy(sid.as_str());
                return Err(error.into());
            }
        };
        let snapshot = match store.load(&sid) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.clear_busy(sid.as_str());
                return self.writer.send(&GatewayFailure::new(id, 4040, error.to_string()));
            }
        };
        let generation = snapshot.owner_generation;
        let spec = ForegroundTurnSpec {
            turn_id: turn_id.clone(),
            session_id: sid.clone(),
            prompt: text.clone(),
        };
        let claim = match SqliteForegroundTurnStore::open(&self.state)
            .and_then(|mut turns| turns.start(spec, generation, started_at_ms))
        {
            Ok(claim) => claim,
            Err(error) => {
                self.clear_busy(sid.as_str());
                let error = foreground_rpc_error(error);
                return self.writer.send(&GatewayFailure::new(id, error.code, error.message));
            }
        };

        let (cancel_sender, cancel_receiver) = oneshot::channel();
        if let Ok(mut controls) = self.controls.lock() {
            controls.insert(sid.as_str().to_owned(), cancel_sender);
        } else {
            self.clear_turn(sid.as_str());
            let _ = terminate_turn(
                &self.state,
                &claim.spec.turn_id,
                claim.owner_generation,
                ForegroundTurnTerminal::Failed { reason: "turn-control lock poisoned".into() },
                claim.started_at_ms,
            );
            anyhow::bail!("turn-control lock poisoned");
        }
        if let Err(error) =
            self.writer.send(&GatewaySuccess::new(id, json!({"status": "streaming"})))
        {
            self.clear_turn(sid.as_str());
            let _ = terminate_turn(
                &self.state,
                &claim.spec.turn_id,
                claim.owner_generation,
                ForegroundTurnTerminal::Failed {
                    reason: "gateway could not acknowledge the accepted turn".into(),
                },
                claim.started_at_ms,
            );
            return Err(error);
        }
        let settings = self.settings.clone();
        let state = self.state.clone();
        let writer = self.writer.clone();
        let busy = Arc::clone(&self.busy);
        let controls = Arc::clone(&self.controls);
        self.turns.push(tokio::spawn(async move {
            writer.event("message.start", &sid, None);
            let result = tokio::select! {
                result = run_session_turn(&settings, &state, &writer, &claim) => Some(result),
                _ = cancel_receiver => None,
            };
            let result = match result {
                Some(Ok(final_response)) => Ok(TurnExit::Completed(final_response)),
                Some(Err(error)) => terminate_turn(
                    &state,
                    &claim.spec.turn_id,
                    claim.owner_generation,
                    ForegroundTurnTerminal::Failed {
                        reason: normalized_reason(&error.to_string(), "foreground turn failed"),
                    },
                    claim.started_at_ms,
                )
                .map(|_| TurnExit::Failed(format!("Error: {error}"))),
                None => terminate_turn(
                    &state,
                    &claim.spec.turn_id,
                    claim.owner_generation,
                    ForegroundTurnTerminal::Interrupted {
                        reason: "client requested foreground interruption".into(),
                    },
                    claim.started_at_ms,
                )
                .map(|_| TurnExit::Interrupted),
            };
            if let Ok(mut active) = busy.lock() {
                active.remove(sid.as_str());
            }
            if let Ok(mut active) = controls.lock() {
                active.remove(sid.as_str());
            }
            match result {
                Ok(TurnExit::Completed(final_response)) => {
                    writer.event("message.complete", &sid, Some(json!({"text": final_response})))
                }
                Ok(TurnExit::Failed(error)) => writer.event(
                    "message.complete",
                    &sid,
                    Some(json!({"text": error, "status": "error"})),
                ),
                Ok(TurnExit::Interrupted) => writer.event(
                    "message.complete",
                    &sid,
                    Some(json!({"status": "interrupted", "text": ""})),
                ),
                Err(error) => writer.event(
                    "message.complete",
                    &sid,
                    Some(json!({
                        "text": format!("Error: foreground terminal was not persisted: {error}"),
                        "status": "error",
                    })),
                ),
            }
            writer.event("session.info", &sid, Some(session_info(&settings)));
        }));
        Ok(())
    }

    fn session_info(&self) -> Value {
        session_info(&self.settings)
    }

    fn is_busy(&self, session_id: &str) -> bool {
        self.busy.lock().is_ok_and(|busy| busy.contains(session_id))
    }

    fn clear_busy(&self, session_id: &str) {
        if let Ok(mut busy) = self.busy.lock() {
            busy.remove(session_id);
        }
    }

    fn clear_turn(&self, session_id: &str) {
        self.clear_busy(session_id);
        if let Ok(mut controls) = self.controls.lock() {
            controls.remove(session_id);
        }
    }
}

async fn run_session_turn(
    settings: &LiveSettings,
    state: &Path,
    writer: &FrameWriter,
    claim: &ForegroundTurnSnapshot,
) -> anyhow::Result<String> {
    let session_id = &claim.spec.session_id;
    let expected_generation = claim.owner_generation;
    let mut store = SqliteSessionStore::open(state)?;
    let snapshot = store.load(session_id)?;
    if snapshot.owner_generation != expected_generation {
        anyhow::bail!(
            "session generation changed before turn execution: expected {}, actual {}",
            expected_generation.get(),
            snapshot.owner_generation.get()
        );
    }
    let previous_len = snapshot.conversation.len();
    let scope = format!(
        "session:{}:generation:{}",
        snapshot.config.session_id,
        snapshot.owner_generation.get()
    );
    let mut observer = GatewayRuntimeEventObserver { writer, session_id };
    let outcome = execute_turn_observed(
        settings,
        snapshot.conversation,
        &claim.spec.prompt,
        &scope,
        state,
        &mut observer,
    )
    .await?;
    let final_response = completed_response(&outcome)?.to_owned();
    let appended = outcome.semantic_conversation.get(previous_len..).ok_or_else(|| {
        anyhow::anyhow!("runtime returned a conversation shorter than its durable prefix")
    })?;
    SqliteForegroundTurnStore::open(state)?.complete(
        &claim.spec.turn_id,
        expected_generation,
        appended,
        terminal_time_ms(claim.started_at_ms),
    )?;
    Ok(final_response)
}

fn terminate_turn(
    state: &Path,
    turn_id: &ForegroundTurnId,
    expected_generation: OwnerGeneration,
    outcome: ForegroundTurnTerminal,
    started_at_ms: u64,
) -> anyhow::Result<()> {
    SqliteForegroundTurnStore::open(state)?.terminate(
        turn_id,
        expected_generation,
        outcome,
        terminal_time_ms(started_at_ms),
    )?;
    Ok(())
}

fn session_info(settings: &LiveSettings) -> Value {
    json!({
        "cwd": settings.root(),
        "model": settings.model(),
        "skills": {},
        "tools": {
            "workspace": ["read_file", "search_files"],
            "delegation": ["delegate_task"],
        },
        "usage": zero_usage(settings.model()),
        "version": env!("CARGO_PKG_VERSION"),
    })
}

struct GatewayRuntimeEventObserver<'a> {
    writer: &'a FrameWriter,
    session_id: &'a SessionId,
}

impl runtime::RuntimeEventObserver for GatewayRuntimeEventObserver<'_> {
    fn observe(&mut self, event: &Value) -> Result<(), runtime::RuntimeEventObserverError> {
        emit_runtime_event(self.writer, self.session_id, event)
            .map_err(|error| runtime::RuntimeEventObserverError::new(error.to_string()))
    }
}

fn emit_runtime_event(
    writer: &FrameWriter,
    session_id: &SessionId,
    event: &Value,
) -> anyhow::Result<()> {
    let Some(kind) = event.get("type").and_then(Value::as_str) else {
        return Ok(());
    };
    match kind {
        "message.delta" | "reasoning.delta" => {
            if let Some(text) = event.get("text").and_then(Value::as_str) {
                writer.try_event(kind, session_id, Some(json!({"text": text})))?;
            }
            Ok(())
        }
        "tool.start" => writer.try_event(
            kind,
            session_id,
            Some(json!({
                "tool_id": event.get("call_id"),
                "name": event.get("name"),
                "args_text": "",
            })),
        ),
        "tool.complete" => writer.try_event(
            kind,
            session_id,
            Some(json!({
                "tool_id": event.get("call_id"),
                "name": event.get("name"),
                "summary": event.get("status"),
            })),
        ),
        _ => Ok(()),
    }
}

fn transcript(messages: &[SemanticMessage]) -> Vec<Value> {
    messages
        .iter()
        .map(|message| match message {
            SemanticMessage::User { content } => json!({"role": "user", "text": content}),
            SemanticMessage::Assistant { content, .. } => {
                json!({"role": "assistant", "text": content})
            }
            SemanticMessage::AssistantToolRequest { content, calls, .. } => json!({
                "role": "assistant",
                "text": content.as_deref().unwrap_or(""),
                "context": format!("{} tool call(s)", calls.len()),
            }),
            SemanticMessage::ToolResultBatch { results } => json!({
                "role": "tool",
                "text": results.iter().map(|result| result.content.as_str()).collect::<Vec<_>>().join("\n"),
            }),
        })
        .collect()
}

struct ResumeProjection {
    messages: Vec<Value>,
    inflight: Value,
    recovery: Value,
}

fn resume_projection(
    conversation: &[SemanticMessage],
    latest: Option<&ForegroundTurnSnapshot>,
    busy: bool,
) -> ResumeProjection {
    let mut messages = transcript(conversation);
    let mut inflight = Value::Null;
    let mut recovery = Value::Null;
    let Some(turn) = latest else {
        return ResumeProjection { messages, inflight, recovery };
    };
    match &turn.state {
        ForegroundTurnState::Running => {
            inflight = json!({
                "user": turn.spec.prompt,
                "assistant": "",
                "streaming": busy,
            });
        }
        ForegroundTurnState::Terminal { outcome: ForegroundTurnTerminal::Completed, .. } => {}
        ForegroundTurnState::Terminal { outcome, completed_at_ms } => {
            let (reason, note) = match outcome {
                ForegroundTurnTerminal::Interrupted { reason } => (
                    reason.as_str(),
                    "Foreground turn interrupted; it was not committed or replayed.",
                ),
                ForegroundTurnTerminal::Failed { reason } => {
                    (reason.as_str(), "Foreground turn failed before commit; it was not replayed.")
                }
                ForegroundTurnTerminal::OutcomeUnknown { reason } => (
                    reason.as_str(),
                    "Foreground turn outcome is unknown after restart; it was not replayed.",
                ),
                ForegroundTurnTerminal::Completed => {
                    return ResumeProjection { messages, inflight, recovery };
                }
            };
            messages.push(json!({"role": "user", "text": turn.spec.prompt}));
            messages.push(json!({"role": "system", "text": note}));
            recovery = json!({
                "auto_replayed": false,
                "completed_at_ms": completed_at_ms,
                "prompt": turn.spec.prompt,
                "reason": reason,
                "started_at_ms": turn.started_at_ms,
                "status": outcome.status_name(),
                "turn_id": turn.spec.turn_id,
            });
        }
    }
    ResumeProjection { messages, inflight, recovery }
}

fn config_value(params: &Map<String, Value>) -> Value {
    match params.get("key").and_then(Value::as_str).unwrap_or("full") {
        "full" => json!({
            "config": {
                "approvals": {"destructive_slash_confirm": true},
                "display": {"streaming": true, "tui_auto_resume_recent": false}
            }
        }),
        "mtime" => json!({"mtime": 0, "mcp_rev": ""}),
        _ => json!({"value": null}),
    }
}

fn zero_usage(model: &str) -> Value {
    json!({
        "active_subagents": 0,
        "calls": 0,
        "input": 0,
        "model": model,
        "output": 0,
        "total": 0,
    })
}

fn session_param(params: &Map<String, Value>) -> Result<&str, RpcError> {
    params
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RpcError::new(-32602, "session_id is required"))
}

fn fresh_session_id() -> anyhow::Result<SessionId> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    SessionId::new(format!("rs-{}-{now:x}", std::process::id())).map_err(Into::into)
}

fn fresh_turn_id() -> anyhow::Result<ForegroundTurnId> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    ForegroundTurnId::new(format!("turn-{}-{now:x}", std::process::id())).map_err(Into::into)
}

fn unix_time_ms() -> anyhow::Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("Unix timestamp exceeds u64 milliseconds")
}

fn terminal_time_ms(started_at_ms: u64) -> u64 {
    unix_time_ms().unwrap_or(started_at_ms).max(started_at_ms)
}

fn normalized_reason(message: &str, fallback: &str) -> String {
    let message = message.trim();
    if message.is_empty() { fallback.into() } else { message.into() }
}

fn foreground_rpc_error(error: ForegroundTurnStoreError) -> RpcError {
    match error {
        ForegroundTurnStoreError::SessionBusy(_) => RpcError::new(4090, error.to_string()),
        ForegroundTurnStoreError::SessionNotFound(_) => RpcError::new(4040, error.to_string()),
        ForegroundTurnStoreError::GenerationConflict { .. }
        | ForegroundTurnStoreError::AlreadyExists(_)
        | ForegroundTurnStoreError::NotRunning { .. } => RpcError::new(4093, error.to_string()),
        ForegroundTurnStoreError::Invalid(_) => RpcError::new(4002, error.to_string()),
        ForegroundTurnStoreError::NotFound(_) | ForegroundTurnStoreError::Storage(_) => {
            internal_error(error)
        }
    }
}

fn internal_error(error: impl std::fmt::Display) -> RpcError {
    RpcError::new(5000, error.to_string())
}

struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

#[derive(Clone)]
struct FrameWriter {
    inner: Arc<Mutex<BufWriter<std::io::Stdout>>>,
}

impl FrameWriter {
    fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(BufWriter::new(std::io::stdout()))) }
    }

    fn send(&self, frame: &impl Serialize) -> anyhow::Result<()> {
        let encoded = serde_json::to_string(frame)?;
        let mut writer = self.inner.lock().map_err(|_| anyhow::anyhow!("stdout lock poisoned"))?;
        writer.write_all(encoded.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    fn event(&self, kind: &str, session_id: &SessionId, payload: Option<Value>) {
        let _ = self.try_event(kind, session_id, payload);
    }

    fn try_event(
        &self,
        kind: &str,
        session_id: &SessionId,
        payload: Option<Value>,
    ) -> anyhow::Result<()> {
        self.send(&GatewayEventFrame::session(kind, session_id.as_str(), payload))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::GatewayLease;

    #[test]
    fn gateway_lease_rejects_a_second_writer_and_releases_on_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let state = directory.path().join("state.db");
        let first = GatewayLease::acquire(&state)?;
        assert!(GatewayLease::acquire(&state).is_err());
        drop(first);
        let _replacement = GatewayLease::acquire(&state)?;
        Ok(())
    }
}
