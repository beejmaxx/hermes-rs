//! Fail-closed capability policy for an externally supervised Codex worker.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;

use super::{CodexConfigReadResponse, CodexDynamicToolSpec, CodexThreadStartParams};

const DISABLED_FEATURES: &[&str] = &[
    "apps",
    "code_mode",
    "code_mode_only",
    "deferred_executor",
    "enable_mcp_apps",
    "hooks",
    "image_generation",
    "memories",
    "multi_agent",
    "multi_agent_v2",
    "plugins",
    "request_permissions_tool",
    "shell_tool",
    "tool_suggest",
    "unified_exec",
    "view_image",
];

/// Effective worker configuration could not be reduced to the supervised capability set.
#[derive(Debug, Error)]
pub enum CodexAuthorityError {
    /// Effective `mcp_servers` configuration had an unexpected representation.
    #[error("Codex effective mcp_servers config must be an object or null")]
    InvalidMcpServers,
    /// The selected Hermes dynamic-tool catalog was empty or malformed.
    #[error("invalid Hermes dynamic-tool catalog: {0}")]
    InvalidDynamicTools(String),
}

/// Frozen evidence of the capabilities granted to one Codex worker thread.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CodexAuthorityManifest {
    worker: String,
    dynamic_tools: Vec<String>,
    disabled_mcp_servers: Vec<String>,
    ambient_environments: bool,
    codex_shell: bool,
    codex_web_search: bool,
    codex_plugins: bool,
    codex_apps: bool,
    codex_hooks: bool,
    codex_multi_agent: bool,
}

impl CodexAuthorityManifest {
    /// Model-visible tools implemented by the Hermes host.
    #[must_use]
    pub fn dynamic_tools(&self) -> &[String] {
        &self.dynamic_tools
    }

    /// User-configured MCP servers explicitly disabled for the worker thread.
    #[must_use]
    pub fn disabled_mcp_servers(&self) -> &[String] {
        &self.disabled_mcp_servers
    }
}

/// Immutable per-thread controls that leave cognition in Codex and effects in Hermes.
#[derive(Clone, Debug)]
pub struct CodexAuthorityPolicy {
    config_overrides: BTreeMap<String, Value>,
    dynamic_tools: Vec<CodexDynamicToolSpec>,
    manifest: CodexAuthorityManifest,
}

impl CodexAuthorityPolicy {
    /// Build a supervised policy from the worker's effective config and Hermes tool catalog.
    pub fn new(
        effective: &CodexConfigReadResponse,
        dynamic_tools: Vec<CodexDynamicToolSpec>,
    ) -> Result<Self, CodexAuthorityError> {
        let tool_names = validate_dynamic_tools(&dynamic_tools)?;
        let disabled_mcp_servers = effective_mcp_server_names(effective)?;
        let config_overrides = authority_overrides(&disabled_mcp_servers);
        let manifest = CodexAuthorityManifest {
            worker: "codex-app-server".into(),
            dynamic_tools: tool_names,
            disabled_mcp_servers,
            ambient_environments: false,
            codex_shell: false,
            codex_web_search: false,
            codex_plugins: false,
            codex_apps: false,
            codex_hooks: false,
            codex_multi_agent: false,
        };
        Ok(Self { config_overrides, dynamic_tools, manifest })
    }

    /// Freeze a worker configuration value alongside the authority restrictions.
    #[must_use]
    pub fn with_config_override(mut self, key: impl Into<String>, value: Value) -> Self {
        self.config_overrides.insert(key.into(), value);
        self
    }

    /// Apply the frozen restrictions and client-hosted catalog to a thread request.
    #[must_use]
    pub fn constrain(&self, params: CodexThreadStartParams) -> CodexThreadStartParams {
        params
            .with_config(self.config_overrides.clone())
            .with_dynamic_tools(self.dynamic_tools.clone())
            .without_environments()
    }

    /// Configuration overrides shared by new and safely forked worker threads.
    #[must_use]
    pub fn config_overrides(&self) -> BTreeMap<String, Value> {
        self.config_overrides.clone()
    }

    /// Capability evidence to persist beside the worker binding.
    #[must_use]
    pub const fn manifest(&self) -> &CodexAuthorityManifest {
        &self.manifest
    }
}

fn validate_dynamic_tools(
    tools: &[CodexDynamicToolSpec],
) -> Result<Vec<String>, CodexAuthorityError> {
    if tools.is_empty() {
        return Err(CodexAuthorityError::InvalidDynamicTools(
            "at least one Hermes-hosted tool is required".into(),
        ));
    }
    let mut seen = BTreeSet::new();
    for tool in tools {
        let name = tool.name();
        if name.is_empty() || name.trim() != name {
            return Err(CodexAuthorityError::InvalidDynamicTools(
                "tool names must be non-empty and have no surrounding whitespace".into(),
            ));
        }
        if !seen.insert(name.to_owned()) {
            return Err(CodexAuthorityError::InvalidDynamicTools(format!(
                "duplicate tool name {name:?}"
            )));
        }
    }
    Ok(seen.into_iter().collect())
}

fn effective_mcp_server_names(
    effective: &CodexConfigReadResponse,
) -> Result<Vec<String>, CodexAuthorityError> {
    match effective.get("mcp_servers") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Object(servers)) => Ok(servers.keys().cloned().collect()),
        Some(_) => Err(CodexAuthorityError::InvalidMcpServers),
    }
}

fn authority_overrides(mcp_server_names: &[String]) -> BTreeMap<String, Value> {
    let disabled_mcp = Map::from_iter(
        mcp_server_names.iter().map(|name| (name.clone(), json!({"enabled": false}))),
    );
    let disabled_features = Map::from_iter(
        DISABLED_FEATURES.iter().map(|name| ((*name).to_owned(), Value::Bool(false))),
    );
    BTreeMap::from([
        ("features".into(), Value::Object(disabled_features)),
        ("mcp_servers".into(), Value::Object(disabled_mcp)),
        (
            "tools".into(),
            json!({
                "experimental_request_user_input": {"enabled": false},
                "update_plan": {"enabled": false}
            }),
        ),
        ("web_search".into(), json!("disabled")),
    ])
}

#[cfg(test)]
mod tests {
    use super::{CodexAuthorityPolicy, CodexDynamicToolSpec};
    use crate::adapters::codex::{
        CodexConfigReadResponse, CodexDynamicToolFunctionSpec, CodexThreadStartParams,
    };
    use serde_json::json;

    #[test]
    fn policy_disables_ambient_capabilities_and_preserves_literal_mcp_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let effective: CodexConfigReadResponse = serde_json::from_value(json!({
            "config": {
                "mcp_servers": {
                    "docs": {"command": "docs"},
                    "name.with.dots": {"url": "https://example.test/mcp"}
                }
            },
            "origins": {}
        }))?;
        let tools = vec![CodexDynamicToolSpec::Function(CodexDynamicToolFunctionSpec::new(
            "hermes_read_file",
            "Read through Hermes.",
            json!({"type": "object"}),
        ))];
        let policy = CodexAuthorityPolicy::new(&effective, tools)?;
        assert_eq!(policy.manifest().dynamic_tools(), ["hermes_read_file"]);
        assert_eq!(policy.manifest().disabled_mcp_servers(), ["docs", "name.with.dots"]);

        let request = serde_json::to_value(policy.constrain(CodexThreadStartParams::new()))?;
        assert_eq!(request["environments"], json!([]));
        assert_eq!(request["config"]["features"]["shell_tool"], json!(false));
        assert_eq!(request["config"]["features"]["multi_agent"], json!(false));
        assert_eq!(request["config"]["web_search"], json!("disabled"));
        assert_eq!(request["config"]["mcp_servers"]["name.with.dots"]["enabled"], json!(false));
        assert_eq!(request["dynamicTools"][0]["name"], json!("hermes_read_file"));
        Ok(())
    }
}
