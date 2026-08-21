//! Derived Codex thread bindings fenced by the canonical Hermes session generation.

use std::{collections::BTreeSet, path::Path};

use domain::{OwnerGeneration, SessionId};
use ports::SessionStoreError;
use protocol::{EngineConfig, SessionSnapshot, TransportKind};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{codex::CodexWorkerBinding, sqlite::SqliteSessionStore};

const WORKER_KIND: &str = "codex-app-server";

/// SQLite cache relating one Hermes session generation to one Codex thread head.
pub struct SqliteCodexBindingStore {
    connection: Connection,
}

impl SqliteCodexBindingStore {
    /// Open the shared state database and apply every schema migration.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SessionStoreError> {
        let store = SqliteSessionStore::open(path)?;
        Ok(Self { connection: store.into_connection() })
    }

    /// Load a binding only when it represents the caller's canonical session generation.
    pub fn load_current(
        &mut self,
        session_id: &SessionId,
        generation: OwnerGeneration,
    ) -> Result<Option<CodexWorkerBinding>, SessionStoreError> {
        let snapshot = super::sqlite::load_snapshot(&self.connection, session_id)?;
        if snapshot.owner_generation != generation {
            return Err(SessionStoreError::Conflict {
                session_id: session_id.clone(),
                expected: generation.get(),
                actual: snapshot.owner_generation.get(),
            });
        }
        validate_codex_session(&snapshot)?;
        let row = self
            .connection
            .query_row(
                "SELECT owner_generation, worker_kind, binding_json
                   FROM worker_bindings WHERE session_id = ?1",
                params![session_id.as_str()],
                |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                },
            )
            .optional()
            .map_err(storage_error)?;
        let Some((stored_generation, worker_kind, encoded)) = row else {
            return Ok(None);
        };
        let stored_generation = positive_u64(stored_generation, "stored binding generation")?;
        if stored_generation != generation.get() {
            return Ok(None);
        }
        if worker_kind != WORKER_KIND {
            return Err(SessionStoreError::Invalid(format!(
                "session has unsupported worker binding kind {worker_kind:?}"
            )));
        }
        let binding = serde_json::from_str::<CodexWorkerBinding>(&encoded).map_err(|error| {
            SessionStoreError::Invalid(format!("stored Codex worker binding is invalid: {error}"))
        })?;
        validate_binding(&binding)?;
        validate_catalog(&snapshot, &binding)?;
        Ok(Some(binding))
    }

    /// Replace the derived binding only if the canonical session has the represented generation.
    pub fn save(
        &mut self,
        session_id: &SessionId,
        generation: OwnerGeneration,
        binding: &CodexWorkerBinding,
    ) -> Result<(), SessionStoreError> {
        validate_binding(binding)?;
        let encoded = serde_json::to_string(binding).map_err(storage_error)?;
        let expected = i64::try_from(generation.get()).map_err(|_| {
            SessionStoreError::Invalid("worker binding generation is out of range".into())
        })?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let snapshot = super::sqlite::load_snapshot(&transaction, session_id)?;
        validate_codex_session(&snapshot)?;
        validate_catalog(&snapshot, binding)?;
        if snapshot.owner_generation != generation {
            return Err(SessionStoreError::Conflict {
                session_id: session_id.clone(),
                expected: generation.get(),
                actual: snapshot.owner_generation.get(),
            });
        }
        transaction
            .execute(
                "INSERT INTO worker_bindings (
                    session_id, owner_generation, worker_kind, binding_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, unixepoch())
                 ON CONFLICT(session_id) DO UPDATE SET
                    owner_generation = excluded.owner_generation,
                    worker_kind = excluded.worker_kind,
                    binding_json = excluded.binding_json,
                    updated_at = excluded.updated_at",
                params![session_id.as_str(), expected, WORKER_KIND, encoded],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)
    }
}

fn validate_codex_session(snapshot: &SessionSnapshot) -> Result<(), SessionStoreError> {
    if snapshot.config.transport != TransportKind::CodexAppServer
        || !matches!(snapshot.config.engine_config, EngineConfig::CodexAppServer { .. })
    {
        return Err(SessionStoreError::Invalid(
            "Codex worker bindings require a Codex app-server session".into(),
        ));
    }
    Ok(())
}

fn validate_catalog(
    snapshot: &SessionSnapshot,
    binding: &CodexWorkerBinding,
) -> Result<(), SessionStoreError> {
    let names = snapshot
        .config
        .tools
        .iter()
        .map(|schema| {
            schema
                .as_object()
                .filter(|object| {
                    object.get("type").and_then(serde_json::Value::as_str) == Some("function")
                })
                .and_then(|object| object.get("function"))
                .and_then(serde_json::Value::as_object)
                .and_then(|function| function.get("name"))
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.is_empty() && name.trim() == *name)
                .map(str::to_owned)
                .ok_or_else(|| {
                    SessionStoreError::Invalid(
                        "Codex session contains a malformed dynamic-tool schema".into(),
                    )
                })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let bound = binding.authority.dynamic_tools().iter().cloned().collect::<BTreeSet<_>>();
    if names != bound {
        return Err(SessionStoreError::Invalid(
            "Codex worker binding does not match the frozen session tool catalog".into(),
        ));
    }
    Ok(())
}

fn validate_binding(binding: &CodexWorkerBinding) -> Result<(), SessionStoreError> {
    for (name, value) in [
        ("thread ID", binding.thread_id.as_str()),
        ("last turn ID", binding.last_turn_id.as_str()),
        ("worker user agent", binding.worker_user_agent.as_str()),
        ("model provider", binding.model_provider.as_str()),
    ] {
        if value.is_empty() || value.trim() != value {
            return Err(SessionStoreError::Invalid(format!(
                "Codex worker binding {name} must be non-empty and unpadded"
            )));
        }
    }
    if binding.authority.dynamic_tools().is_empty() {
        return Err(SessionStoreError::Invalid(
            "Codex worker binding must retain at least one Hermes-hosted tool".into(),
        ));
    }
    Ok(())
}

fn positive_u64(value: i64, name: &str) -> Result<u64, SessionStoreError> {
    if value < 1 {
        return Err(SessionStoreError::Invalid(format!("{name} must be positive")));
    }
    u64::try_from(value).map_err(|_| SessionStoreError::Invalid(format!("{name} is out of range")))
}

fn storage_error(error: impl std::fmt::Display) -> SessionStoreError {
    SessionStoreError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use domain::{EngineId, LineageId, ManifestDigest, PromptManifest, SemanticMessage, SessionId};
    use ports::SessionStore;
    use protocol::{
        CodexAuthorityProfile, EngineConfig, ModelReasoningEffort, SessionConfig, TransportKind,
    };
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{CodexWorkerBinding, SqliteCodexBindingStore, SqliteSessionStore};

    #[test]
    fn binding_is_visible_only_at_its_canonical_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let database = directory.path().join("state.db");
        let config = codex_config(directory.path())?;
        let session_id = config.session_id.clone();
        let mut sessions = SqliteSessionStore::open(&database)?;
        let created = sessions.create(config)?;
        let binding = binding()?;
        let mut bindings = SqliteCodexBindingStore::open(&database)?;
        bindings.save(&session_id, created.owner_generation, &binding)?;
        assert_eq!(bindings.load_current(&session_id, created.owner_generation)?, Some(binding));

        let committed = sessions.append(
            &session_id,
            created.owner_generation,
            &[
                SemanticMessage::User { content: "hello".into(), display_content: None },
                SemanticMessage::Assistant {
                    content: "hi".into(),
                    reasoning: None,
                    provider_replay: None,
                },
            ],
        )?;
        assert!(bindings.load_current(&session_id, committed.owner_generation)?.is_none());
        assert!(bindings.load_current(&session_id, created.owner_generation).is_err());
        Ok(())
    }

    fn codex_config(root: &std::path::Path) -> Result<SessionConfig, Box<dyn std::error::Error>> {
        let tools = vec![json!({
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read one file.",
                "parameters": {"type": "object"}
            }
        })];
        let system_prompt = "Frozen prompt.".to_owned();
        Ok(SessionConfig {
            session_id: SessionId::new("codex-binding-session")?,
            lineage_id: LineageId::new("codex-binding-session")?,
            prompt_manifest: PromptManifest::new(
                1,
                EngineId::new("codex-app-server:test")?,
                ManifestDigest::new(digest(system_prompt.as_bytes()))?,
                ManifestDigest::new(digest(&serde_json::to_vec(&tools)?))?,
            )?,
            engine_config: EngineConfig::CodexAppServer {
                reasoning_effort: ModelReasoningEffort::Low,
                authority_profile: CodexAuthorityProfile::HermesOwnedEffectsV1,
            },
            transport: TransportKind::CodexAppServer,
            provider_adapter: "codex-app-server".into(),
            base_url: String::new(),
            api_key_env: None,
            model: "gpt-5.6-luna".into(),
            tool_root: fs::canonicalize(root)?.to_string_lossy().into_owned(),
            system_prompt,
            tools,
        })
    }

    fn binding() -> Result<CodexWorkerBinding, serde_json::Error> {
        serde_json::from_value(json!({
            "thread_id": "thread-1",
            "last_turn_id": "turn-1",
            "worker_user_agent": "fake-codex/test",
            "model_provider": "openai_http",
            "authority": {
                "worker": "codex-app-server",
                "dynamic_tools": ["read_file"],
                "disabled_mcp_servers": [],
                "ambient_environments": false,
                "codex_shell": false,
                "codex_web_search": false,
                "codex_plugins": false,
                "codex_apps": false,
                "codex_hooks": false,
                "codex_multi_agent": false
            }
        }))
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
}
