use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SessionSummary {
    pub(super) id: String,
    pub(super) message_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TranscriptMessage {
    pub(super) role: String,
    pub(super) text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ToolActivity {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) status: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ApprovalView {
    pub(super) command: String,
    pub(super) description: String,
    pub(super) selected: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Screen {
    Loading,
    Sessions,
    Chat,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ModelAction {
    Resume(String),
    ReloadActive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PendingRequest {
    List,
    Create,
    Resume,
    Submit,
    Approval,
    Interrupt,
}

pub(super) struct AppModel {
    pub(super) screen: Screen,
    pub(super) sessions: Vec<SessionSummary>,
    pub(super) selected_session: usize,
    pub(super) active_session: Option<String>,
    pub(super) messages: Vec<TranscriptMessage>,
    pub(super) tools: Vec<ToolActivity>,
    pub(super) streaming: String,
    pub(super) draft: String,
    pub(super) approval: Option<ApprovalView>,
    pub(super) busy: bool,
    pub(super) interrupting: bool,
    pub(super) status: String,
    pub(super) engine: String,
    pub(super) model: String,
    pub(super) error: Option<String>,
    pub(super) scroll: u16,
    pending: HashMap<u64, PendingRequest>,
}

impl Default for AppModel {
    fn default() -> Self {
        Self {
            screen: Screen::Loading,
            sessions: Vec::new(),
            selected_session: 0,
            active_session: None,
            messages: Vec::new(),
            tools: Vec::new(),
            streaming: String::new(),
            draft: String::new(),
            approval: None,
            busy: false,
            interrupting: false,
            status: "starting gateway".into(),
            engine: String::new(),
            model: String::new(),
            error: None,
            scroll: 0,
            pending: HashMap::new(),
        }
    }
}

impl AppModel {
    pub(super) fn track(&mut self, id: u64, request: PendingRequest) {
        self.pending.insert(id, request);
    }

    pub(super) fn begin_submit(&mut self, text: String) {
        self.messages.push(TranscriptMessage { role: "user".into(), text });
        self.streaming.clear();
        self.tools.clear();
        self.busy = true;
        self.interrupting = false;
        self.status = "starting turn".into();
        self.error = None;
    }

    pub(super) fn apply_frame(&mut self, frame: Value) -> anyhow::Result<Vec<ModelAction>> {
        if frame.get("method").and_then(Value::as_str) == Some("event") {
            return self.apply_event(serde_json::from_value(frame)?);
        }
        let envelope: ResponseEnvelope = serde_json::from_value(frame)?;
        let id = envelope.id.as_u64().ok_or_else(|| anyhow::anyhow!("response id is not u64"))?;
        let pending = self
            .pending
            .remove(&id)
            .ok_or_else(|| anyhow::anyhow!("gateway returned unknown response id {id}"))?;
        if let Some(error) = envelope.error {
            self.error = Some(format!("{} ({})", error.message, error.code));
            self.busy = false;
            self.interrupting = false;
            return Ok(Vec::new());
        }
        let result = envelope.result.ok_or_else(|| anyhow::anyhow!("response omitted result"))?;
        self.apply_response(pending, result)
    }

    fn apply_response(
        &mut self,
        pending: PendingRequest,
        result: Value,
    ) -> anyhow::Result<Vec<ModelAction>> {
        match pending {
            PendingRequest::List => {
                let result: SessionList = serde_json::from_value(result)?;
                self.sessions = result
                    .sessions
                    .into_iter()
                    .map(|session| SessionSummary {
                        id: session.id,
                        message_count: session.message_count,
                    })
                    .collect();
                self.selected_session =
                    self.selected_session.min(self.sessions.len().saturating_sub(1));
                self.screen = Screen::Sessions;
                self.status = "select or create a session".into();
                Ok(Vec::new())
            }
            PendingRequest::Create => {
                let created: CreatedSession = serde_json::from_value(result)?;
                Ok(vec![ModelAction::Resume(created.session_id)])
            }
            PendingRequest::Resume => {
                let resumed: ResumedSession = serde_json::from_value(result)?;
                self.active_session = Some(resumed.session_id);
                self.messages = resumed
                    .messages
                    .into_iter()
                    .map(|message| TranscriptMessage { role: message.role, text: message.text })
                    .collect();
                self.tools.clear();
                self.streaming.clear();
                self.approval = None;
                self.busy = resumed.running;
                self.interrupting = false;
                self.status = resumed.status;
                self.engine = resumed.info.engine;
                self.model = resumed.info.model;
                self.screen = Screen::Chat;
                self.scroll = 0;
                Ok(Vec::new())
            }
            PendingRequest::Submit => {
                self.busy = true;
                self.status = "working".into();
                Ok(Vec::new())
            }
            PendingRequest::Approval => {
                self.approval = None;
                self.status = "working".into();
                Ok(Vec::new())
            }
            PendingRequest::Interrupt => {
                self.interrupting = true;
                self.status = "interrupting".into();
                Ok(Vec::new())
            }
        }
    }

    fn apply_event(&mut self, frame: EventEnvelope) -> anyhow::Result<Vec<ModelAction>> {
        let event = frame.params;
        if let (Some(active), Some(event_session)) = (&self.active_session, &event.session_id)
            && active != event_session
        {
            return Ok(Vec::new());
        }
        let payload = event.payload.unwrap_or(Value::Null);
        match event.kind.as_str() {
            "gateway.ready" => self.status = "gateway ready".into(),
            "message.start" => {
                self.busy = true;
                self.status = "working".into();
            }
            "message.delta" => {
                let payload: TextPayload = serde_json::from_value(payload)?;
                self.streaming.push_str(&payload.text);
            }
            "reasoning.delta" => self.status = "reasoning".into(),
            "tool.start" => {
                let payload: ToolPayload = serde_json::from_value(payload)?;
                self.tools.push(ToolActivity {
                    id: payload.tool_id.unwrap_or_else(|| format!("tool-{}", self.tools.len())),
                    name: payload.name.unwrap_or_else(|| "tool".into()),
                    status: "running".into(),
                });
                self.status = "using tool".into();
            }
            "tool.complete" => {
                let payload: ToolPayload = serde_json::from_value(payload)?;
                let id = payload.tool_id.unwrap_or_default();
                if let Some(tool) = self.tools.iter_mut().rev().find(|tool| tool.id == id) {
                    tool.status = payload.summary.unwrap_or_else(|| "completed".into());
                }
            }
            "approval.request" => {
                let payload: ApprovalPayload = serde_json::from_value(payload)?;
                self.approval = Some(ApprovalView {
                    command: payload.command,
                    description: payload.description,
                    selected: 0,
                });
                self.status = "approval required".into();
            }
            "message.complete" => {
                self.busy = false;
                self.interrupting = false;
                self.approval = None;
                self.status = "idle".into();
                return Ok(vec![ModelAction::ReloadActive]);
            }
            "session.info" => {
                let payload: SessionInfo = serde_json::from_value(payload)?;
                self.engine = payload.engine;
                self.model = payload.model;
            }
            _ => {}
        }
        Ok(Vec::new())
    }
}

#[derive(Deserialize)]
struct ResponseEnvelope {
    id: Value,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ResponseError>,
}

#[derive(Deserialize)]
struct ResponseError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct EventEnvelope {
    params: EventParams,
}

#[derive(Deserialize)]
struct EventParams {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    payload: Option<Value>,
}

#[derive(Deserialize)]
struct SessionList {
    sessions: Vec<WireSessionSummary>,
}

#[derive(Deserialize)]
struct WireSessionSummary {
    id: String,
    message_count: usize,
}

#[derive(Deserialize)]
struct CreatedSession {
    session_id: String,
}

#[derive(Deserialize)]
struct ResumedSession {
    session_id: String,
    messages: Vec<WireMessage>,
    running: bool,
    status: String,
    info: SessionInfo,
}

#[derive(Deserialize)]
struct WireMessage {
    role: String,
    text: String,
}

#[derive(Deserialize)]
struct SessionInfo {
    engine: String,
    model: String,
}

#[derive(Deserialize)]
struct TextPayload {
    text: String,
}

#[derive(Deserialize)]
struct ToolPayload {
    #[serde(default)]
    tool_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    summary: Option<String>,
}

#[derive(Deserialize)]
struct ApprovalPayload {
    command: String,
    description: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{AppModel, ModelAction, PendingRequest, Screen};

    #[test]
    fn resume_replaces_disposable_projection() -> anyhow::Result<()> {
        let mut model = AppModel { streaming: "stale partial".into(), ..AppModel::default() };
        model.track(1, PendingRequest::Resume);
        model.apply_frame(json!({
            "id": 1,
            "jsonrpc": "2.0",
            "result": {
                "session_id": "session-a",
                "messages": [{"role": "user", "text": "canonical"}],
                "running": false,
                "status": "idle",
                "info": {"engine": "codex", "model": "gpt-test"}
            }
        }))?;
        assert_eq!(model.screen, Screen::Chat);
        assert_eq!(model.messages[0].text, "canonical");
        assert!(model.streaming.is_empty());
        Ok(())
    }

    #[test]
    fn terminal_event_requests_a_canonical_reload() -> anyhow::Result<()> {
        let mut model =
            AppModel { active_session: Some("session-a".into()), ..AppModel::default() };
        let actions = model.apply_frame(json!({
            "jsonrpc": "2.0",
            "method": "event",
            "params": {
                "type": "message.complete",
                "session_id": "session-a",
                "payload": {"status": "interrupted", "text": ""}
            }
        }))?;
        assert_eq!(actions, vec![ModelAction::ReloadActive]);
        assert!(!model.busy);
        Ok(())
    }

    #[test]
    fn tool_and_approval_events_are_typed_views() -> anyhow::Result<()> {
        let mut model =
            AppModel { active_session: Some("session-a".into()), ..AppModel::default() };
        for frame in [
            json!({"method":"event","params":{"type":"tool.start","session_id":"session-a","payload":{"tool_id":"inv-1","name":"terminal"}}}),
            json!({"method":"event","params":{"type":"approval.request","session_id":"session-a","payload":{"command":"cargo test","description":"terminal command requires approval"}}}),
        ] {
            model.apply_frame(frame)?;
        }
        assert_eq!(model.tools[0].status, "running");
        assert_eq!(model.approval.as_ref().map(|view| view.command.as_str()), Some("cargo test"));
        Ok(())
    }
}
