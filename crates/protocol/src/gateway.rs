//! Newline-delimited JSON-RPC frames used by Hermes terminal clients.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// JSON-RPC version accepted and emitted by the gateway.
pub const JSON_RPC_VERSION: &str = "2.0";

/// One client request read from the stdio transport.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayRequest {
    /// Exact JSON-RPC version; dispatch rejects values other than `2.0`.
    pub jsonrpc: String,
    /// Client-selected correlation identity.
    pub id: Value,
    /// Method name.
    pub method: String,
    /// Method parameters, normally an object.
    #[serde(default = "empty_object")]
    pub params: Value,
}

/// One successful correlated response.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewaySuccess {
    /// JSON-RPC version.
    pub jsonrpc: &'static str,
    /// Correlation identity copied from the request.
    pub id: Value,
    /// Method result.
    pub result: Value,
}

impl GatewaySuccess {
    /// Construct a successful response.
    #[must_use]
    pub fn new(id: Value, result: Value) -> Self {
        Self { jsonrpc: JSON_RPC_VERSION, id, result }
    }
}

/// Structured JSON-RPC error body.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayErrorBody {
    /// Stable JSON-RPC or application error code.
    pub code: i64,
    /// Human-readable error description.
    pub message: String,
    /// Optional structured diagnostic data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// One failed correlated response.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayFailure {
    /// JSON-RPC version.
    pub jsonrpc: &'static str,
    /// Correlation identity, or null when parsing could not recover one.
    pub id: Value,
    /// Structured failure.
    pub error: GatewayErrorBody,
}

impl GatewayFailure {
    /// Construct a failed response without diagnostic data.
    #[must_use]
    pub fn new(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: JSON_RPC_VERSION,
            id,
            error: GatewayErrorBody { code, message: message.into(), data: None },
        }
    }
}

/// Event parameters carried by an uncorrelated `event` notification.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayEvent {
    /// Stable event name such as `gateway.ready` or `message.delta`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Owning live session, absent for process-global events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Event-specific body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

/// Uncorrelated server-to-client event frame.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayEventFrame {
    /// JSON-RPC version.
    pub jsonrpc: &'static str,
    /// Notification method, always `event`.
    pub method: &'static str,
    /// Typed event parameters.
    pub params: GatewayEvent,
}

impl GatewayEventFrame {
    /// Construct a process-global event.
    #[must_use]
    pub fn global(kind: impl Into<String>, payload: Option<Value>) -> Self {
        Self::new(kind, None, payload)
    }

    /// Construct a session-scoped event.
    #[must_use]
    pub fn session(
        kind: impl Into<String>,
        session_id: impl Into<String>,
        payload: Option<Value>,
    ) -> Self {
        Self::new(kind, Some(session_id.into()), payload)
    }

    fn new(kind: impl Into<String>, session_id: Option<String>, payload: Option<Value>) -> Self {
        Self {
            jsonrpc: JSON_RPC_VERSION,
            method: "event",
            params: GatewayEvent { kind: kind.into(), session_id, payload },
        }
    }
}

fn empty_object() -> Value {
    json!({})
}
