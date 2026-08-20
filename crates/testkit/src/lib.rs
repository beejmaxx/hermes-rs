//! Effect-free readers for the versioned Hermes contract corpus.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use domain::{
    ApprovalRecord, PlannedToolCall, ToolCall, ToolEffect, ToolResultStatus, ToolTerminal,
};
use futures_util::{FutureExt, future::BoxFuture, stream};
use ports::{
    AttemptErrorPolicy, Provider, ProviderAttempt, ProviderError, ToolBroker, ToolBrokerError,
};
use protocol::{
    AgentTurnRequest, CONTRACT_SCHEMA_V1, ContractFixture, ContractKind, ProviderEvent,
    ProviderMessage, TransportKind,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Metadata pinning a vendored contract bundle to exact bytes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BundleManifest {
    /// Bundle-manifest schema.
    pub schema_version: String,
    /// Repository from which the fixtures were derived.
    pub source_repository: String,
    /// Source revision on which the draft corpus was produced.
    pub source_base_revision: String,
    /// Whether the source corpus was committed or an explicitly identified draft.
    pub source_contract_state: String,
    /// SHA-256 by fixture filename.
    pub files: BTreeMap<String, String>,
}

/// A verified, parsed contract corpus.
#[derive(Clone, Debug)]
pub struct ContractCorpus {
    manifest: BundleManifest,
    fixtures: Vec<ContractFixture>,
}

/// Loading or verifying a contract bundle failed.
#[derive(Debug, Error)]
pub enum CorpusError {
    /// A bundle file could not be read.
    #[error("could not read {path}: {source}")]
    Read {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: std::io::Error,
    },
    /// A JSON document could not be decoded.
    #[error("could not decode {path}: {source}")]
    Decode {
        /// Invalid document path.
        path: PathBuf,
        /// Underlying JSON error.
        source: serde_json::Error,
    },
    /// A fixture checksum differs from the pinned manifest.
    #[error("checksum mismatch for {file}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// Fixture filename.
        file: String,
        /// Pinned SHA-256.
        expected: String,
        /// Observed SHA-256.
        actual: String,
    },
    /// A fixture declared an unsupported schema.
    #[error("fixture {fixture_id} declares unsupported schema {actual}")]
    UnsupportedSchema {
        /// Stable fixture identity.
        fixture_id: String,
        /// Observed schema marker.
        actual: String,
    },
    /// A fixture identity occurred more than once.
    #[error("duplicate fixture id {0}")]
    DuplicateFixtureId(String),
    /// The manifest did not name any fixtures.
    #[error("contract manifest contains no fixture files")]
    EmptyManifest,
}

impl ContractCorpus {
    /// Load every manifest-listed fixture and verify its exact bytes.
    pub fn load(bundle_root: impl AsRef<Path>) -> Result<Self, CorpusError> {
        let bundle_root = bundle_root.as_ref();
        let manifest_path = bundle_root.join("SOURCE.json");
        let manifest_bytes = read(&manifest_path)?;
        let manifest: BundleManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|source| CorpusError::Decode { path: manifest_path, source })?;
        if manifest.files.is_empty() {
            return Err(CorpusError::EmptyManifest);
        }

        let mut fixtures = Vec::with_capacity(manifest.files.len());
        let mut fixture_ids = HashSet::with_capacity(manifest.files.len());
        for (file, expected_checksum) in &manifest.files {
            let path = bundle_root.join("fixtures").join(file);
            let bytes = read(&path)?;
            let actual_checksum = format!("{:x}", Sha256::digest(&bytes));
            if &actual_checksum != expected_checksum {
                return Err(CorpusError::ChecksumMismatch {
                    file: file.clone(),
                    expected: expected_checksum.clone(),
                    actual: actual_checksum,
                });
            }
            let fixture: ContractFixture = serde_json::from_slice(&bytes)
                .map_err(|source| CorpusError::Decode { path: path.clone(), source })?;
            if fixture.schema != CONTRACT_SCHEMA_V1 {
                return Err(CorpusError::UnsupportedSchema {
                    fixture_id: fixture.id,
                    actual: fixture.schema,
                });
            }
            if !fixture_ids.insert(fixture.id.clone()) {
                return Err(CorpusError::DuplicateFixtureId(fixture.id));
            }
            fixtures.push(fixture);
        }

        Ok(Self { manifest, fixtures })
    }

    /// Borrow the verified source manifest.
    #[must_use]
    pub const fn manifest(&self) -> &BundleManifest {
        &self.manifest
    }

    /// Borrow fixtures in deterministic filename order.
    #[must_use]
    pub fn fixtures(&self) -> &[ContractFixture] {
        &self.fixtures
    }
}

fn read(path: &Path) -> Result<Vec<u8>, CorpusError> {
    fs::read(path).map_err(|source| CorpusError::Read { path: path.to_path_buf(), source })
}

/// Preparing a deterministic agent-turn harness failed.
#[derive(Debug, Error)]
pub enum ScenarioError {
    /// The selected fixture does not describe an agent turn.
    #[error("fixture {0} is not an agent_turn scenario")]
    WrongKind(String),
    /// The fixture input could not be decoded to the typed v1 script.
    #[error("could not decode agent_turn input for {fixture_id}: {source}")]
    Decode {
        /// Stable fixture identity.
        fixture_id: String,
        /// Invalid typed input.
        source: serde_json::Error,
    },
    /// A scripted provider attempt is structurally invalid.
    #[error("invalid provider script for {fixture_id}: {message}")]
    ProviderScript {
        /// Stable fixture identity.
        fixture_id: String,
        /// Validation failure.
        message: String,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentTurnFixtureInput {
    transport: TransportKind,
    model: String,
    #[serde(default)]
    system_prompt: Option<String>,
    conversation: Vec<ProviderMessage>,
    #[serde(default)]
    tools: Vec<Value>,
    #[serde(default)]
    tool_outcomes: HashMap<String, ScriptedToolOutcome>,
    provider_steps: Vec<ScriptedProviderStep>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptedProviderStep {
    attempt_id: String,
    #[serde(default)]
    on_error: Option<String>,
    events: Vec<ProviderEvent>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScriptedToolMode {
    Succeed,
    Fail,
    Cancel,
    Reject,
    CrashBeforeEffect,
    CrashAfterEffect,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptedToolOutcome {
    name: String,
    effect: ToolEffect,
    mode: ScriptedToolMode,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    receipt: Option<String>,
    completion_order: usize,
    #[serde(default)]
    approval: Option<ApprovalRecord>,
}

/// Fully prepared, effect-free dependencies for one agent-turn fixture.
pub struct ScriptedAgentTurn {
    /// Runtime input derived from the fixture.
    pub request: AgentTurnRequest,
    /// Deterministic provider event source.
    pub provider: ScriptedProvider,
    /// Deterministic policy-aware tool broker.
    pub tools: ScriptedToolBroker,
}

/// Decode and validate one agent-turn fixture into executable scripted ports.
pub fn scripted_agent_turn(fixture: &ContractFixture) -> Result<ScriptedAgentTurn, ScenarioError> {
    if fixture.kind != ContractKind::AgentTurn {
        return Err(ScenarioError::WrongKind(fixture.id.clone()));
    }
    let input: AgentTurnFixtureInput = serde_json::from_value(fixture.input.clone())
        .map_err(|source| ScenarioError::Decode { fixture_id: fixture.id.clone(), source })?;
    let provider = ScriptedProvider::new(&fixture.id, input.provider_steps)?;
    let tools =
        ScriptedToolBroker { scenario_id: fixture.id.clone(), outcomes: input.tool_outcomes };
    Ok(ScriptedAgentTurn {
        request: AgentTurnRequest {
            execution_scope: fixture.id.clone(),
            transport: input.transport,
            model: input.model,
            system_prompt: input.system_prompt,
            conversation: input.conversation,
            tools: input.tools,
        },
        provider,
        tools,
    })
}

/// Provider that yields one pinned event sequence per submitted request.
pub struct ScriptedProvider {
    steps: Vec<ScriptedProviderStep>,
    cursor: usize,
}

impl ScriptedProvider {
    fn new(fixture_id: &str, steps: Vec<ScriptedProviderStep>) -> Result<Self, ScenarioError> {
        if steps.is_empty() {
            return Err(ScenarioError::ProviderScript {
                fixture_id: fixture_id.into(),
                message: "provider_steps must be non-empty".into(),
            });
        }
        for step in &steps {
            if step.attempt_id.is_empty() {
                return Err(ScenarioError::ProviderScript {
                    fixture_id: fixture_id.into(),
                    message: "attempt_id must be non-empty".into(),
                });
            }
            let terminals = step
                .events
                .iter()
                .enumerate()
                .filter(|(_, event)| is_terminal_event(event))
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if terminals.len() > 1 {
                return Err(ScenarioError::ProviderScript {
                    fixture_id: fixture_id.into(),
                    message: format!("attempt {} has multiple terminals", step.attempt_id),
                });
            }
            if terminals.first().is_some_and(|index| *index + 1 != step.events.len()) {
                return Err(ScenarioError::ProviderScript {
                    fixture_id: fixture_id.into(),
                    message: format!("attempt {} emits after its terminal", step.attempt_id),
                });
            }
            if step.on_error.as_deref().is_some_and(|policy| policy != "fallback") {
                return Err(ScenarioError::ProviderScript {
                    fixture_id: fixture_id.into(),
                    message: format!("attempt {} has unsupported on_error policy", step.attempt_id),
                });
            }
        }
        Ok(Self { steps, cursor: 0 })
    }
}

impl Provider for ScriptedProvider {
    fn stream<'a>(
        &'a mut self,
        _request: protocol::ChatCompletionsRequest,
    ) -> BoxFuture<'a, Result<ProviderAttempt, ProviderError>> {
        async move {
            let step = self
                .steps
                .get(self.cursor)
                .cloned()
                .ok_or_else(|| ProviderError::new("provider script exhausted"))?;
            self.cursor += 1;
            Ok(ProviderAttempt {
                attempt_id: step.attempt_id,
                error_policy: if step.on_error.as_deref() == Some("fallback") {
                    AttemptErrorPolicy::FallbackBeforeVisibleOutput
                } else {
                    AttemptErrorPolicy::Stop
                },
                events: Box::pin(stream::iter(step.events.into_iter().map(Ok))),
            })
        }
        .boxed()
    }

    fn remaining_attempts(&self) -> Option<usize> {
        Some(self.steps.len().saturating_sub(self.cursor))
    }
}

/// Tool broker that returns pinned outcomes without executing real effects.
pub struct ScriptedToolBroker {
    scenario_id: String,
    outcomes: HashMap<String, ScriptedToolOutcome>,
}

impl ToolBroker for ScriptedToolBroker {
    fn plan(&mut self, calls: &[ToolCall]) -> Result<Vec<PlannedToolCall>, ToolBrokerError> {
        let mut seen = HashSet::with_capacity(calls.len());
        calls
            .iter()
            .map(|call| {
                if !seen.insert(call.id.clone()) {
                    return Err(ToolBrokerError::new(format!("duplicate tool call {}", call.id)));
                }
                let outcome = self.outcomes.get(call.id.as_str()).ok_or_else(|| {
                    ToolBrokerError::new(format!("tool call {} has no scripted outcome", call.id))
                })?;
                if outcome.name != call.name {
                    return Err(ToolBrokerError::new(format!(
                        "tool call {} expected {:?}, got {:?}",
                        call.id, outcome.name, call.name
                    )));
                }
                Ok(PlannedToolCall {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                    execution_key: format!("{}:{}", self.scenario_id, call.id),
                    effect: outcome.effect,
                    approval: outcome.approval.clone(),
                })
            })
            .collect()
    }

    fn execute<'a>(
        &'a mut self,
        calls: &'a [PlannedToolCall],
    ) -> BoxFuture<'a, Result<Vec<ToolTerminal>, ToolBrokerError>> {
        let result = calls
            .iter()
            .enumerate()
            .map(|(position, call)| {
                let outcome = self.outcomes.get(call.call_id.as_str()).ok_or_else(|| {
                    ToolBrokerError::new(format!(
                        "tool call {} has no scripted outcome",
                        call.call_id
                    ))
                })?;
                let (status, content) = match outcome.mode {
                    ScriptedToolMode::Succeed => {
                        (ToolResultStatus::Succeeded, outcome.result.clone().unwrap_or_default())
                    }
                    ScriptedToolMode::Fail => (
                        ToolResultStatus::Failed,
                        outcome.error.clone().unwrap_or_else(|| "tool failed".into()),
                    ),
                    ScriptedToolMode::Cancel => (
                        ToolResultStatus::Cancelled,
                        outcome.reason.clone().unwrap_or_else(|| "tool cancelled".into()),
                    ),
                    ScriptedToolMode::Reject => (
                        ToolResultStatus::Rejected,
                        outcome.reason.clone().unwrap_or_else(|| "approval denied".into()),
                    ),
                    ScriptedToolMode::CrashBeforeEffect => {
                        (ToolResultStatus::Failed, "worker crashed before effect".into())
                    }
                    ScriptedToolMode::CrashAfterEffect => (
                        ToolResultStatus::OutcomeUnknown,
                        outcome.error.clone().unwrap_or_else(|| {
                            "effect may have occurred; reconciliation required".into()
                        }),
                    ),
                };
                Ok((
                    outcome.completion_order,
                    position,
                    ToolTerminal {
                        call_id: call.call_id.clone(),
                        name: call.name.clone(),
                        status,
                        content,
                        execution_key: call.execution_key.clone(),
                        effect: call.effect,
                        receipt: outcome.receipt.clone(),
                    },
                ))
            })
            .collect::<Result<Vec<_>, ToolBrokerError>>()
            .map(|mut completed| {
                completed.sort_by_key(|(order, position, _)| (*order, *position));
                completed.into_iter().map(|(_, _, terminal)| terminal).collect()
            });
        async move { result }.boxed()
    }
}

fn is_terminal_event(event: &ProviderEvent) -> bool {
    matches!(
        event,
        ProviderEvent::Completed { .. }
            | ProviderEvent::Error { .. }
            | ProviderEvent::Cancelled { .. }
            | ProviderEvent::Malformed { .. }
    )
}
