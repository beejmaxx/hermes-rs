//! SQLite-backed durable foreground-turn ownership and recovery state.

use std::path::Path;

use domain::{
    ForegroundTurnId, ForegroundTurnSpec, ForegroundTurnState, ForegroundTurnTerminal,
    OwnerGeneration, SemanticMessage, SessionId,
};
use ports::{ForegroundTurnStore, ForegroundTurnStoreError, SessionStoreError};
use protocol::{ForegroundTurnSnapshot, SessionSnapshot};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::sqlite::{SqliteSessionStore, append_turn_in_transaction, load_snapshot};

/// SQLite repository for foreground ownership, atomic commit, and reconciliation.
pub struct SqliteForegroundTurnStore {
    connection: Connection,
}

impl SqliteForegroundTurnStore {
    /// Open the shared state database and apply every schema migration.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ForegroundTurnStoreError> {
        let store = SqliteSessionStore::open(path).map_err(session_error)?;
        Ok(Self { connection: store.into_connection() })
    }

    /// Create an isolated in-memory repository, primarily for tests.
    pub fn in_memory() -> Result<Self, ForegroundTurnStoreError> {
        let store = SqliteSessionStore::in_memory().map_err(session_error)?;
        Ok(Self { connection: store.into_connection() })
    }
}

impl ForegroundTurnStore for SqliteForegroundTurnStore {
    fn start(
        &mut self,
        spec: ForegroundTurnSpec,
        expected_generation: OwnerGeneration,
        started_at_ms: u64,
    ) -> Result<ForegroundTurnSnapshot, ForegroundTurnStoreError> {
        validate_spec(&spec)?;
        let started_at = to_i64(started_at_ms, "start timestamp")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let session = load_snapshot(&transaction, &spec.session_id).map_err(session_error)?;
        ensure_generation(&session, expected_generation)?;
        if transaction
            .query_row(
                "SELECT 1 FROM foreground_turns WHERE turn_id = ?1",
                params![spec.turn_id.as_str()],
                |row| row.get::<_, u8>(0),
            )
            .optional()
            .map_err(storage_error)?
            .is_some()
        {
            return Err(ForegroundTurnStoreError::AlreadyExists(spec.turn_id));
        }
        if transaction
            .query_row(
                "SELECT 1 FROM foreground_turns
                 WHERE session_id = ?1 AND state = 'running'",
                params![spec.session_id.as_str()],
                |row| row.get::<_, u8>(0),
            )
            .optional()
            .map_err(storage_error)?
            .is_some()
        {
            return Err(ForegroundTurnStoreError::SessionBusy(spec.session_id));
        }
        transaction
            .execute(
                "INSERT INTO foreground_turns (
                    turn_id, session_id, owner_generation, prompt, state,
                    started_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, 'running', ?5, ?5)",
                params![
                    spec.turn_id.as_str(),
                    spec.session_id.as_str(),
                    to_i64(expected_generation.get(), "owner generation")?,
                    spec.prompt,
                    started_at,
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(ForegroundTurnSnapshot {
            spec,
            owner_generation: expected_generation,
            state: ForegroundTurnState::Running,
            started_at_ms,
            updated_at_ms: started_at_ms,
        })
    }

    fn complete(
        &mut self,
        turn_id: &ForegroundTurnId,
        expected_generation: OwnerGeneration,
        messages: &[SemanticMessage],
        completed_at_ms: u64,
    ) -> Result<SessionSnapshot, ForegroundTurnStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let raw = load_raw(&transaction, turn_id)?;
        ensure_turn_generation(&raw, expected_generation)?;
        ensure_running(&raw)?;
        ensure_monotonic_timestamp(&raw, completed_at_ms)?;
        let session_id = SessionId::new(raw.session_id.clone())
            .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?;
        let snapshot =
            append_turn_in_transaction(&transaction, &session_id, expected_generation, messages)
                .map_err(session_error)?;
        let outcome = ForegroundTurnTerminal::Completed;
        let updated = transaction
            .execute(
                "UPDATE foreground_turns
                 SET state = 'completed', terminal_json = ?1, updated_at_ms = ?2
                 WHERE turn_id = ?3 AND state = 'running' AND owner_generation = ?4",
                params![
                    serde_json::to_string(&outcome).map_err(storage_error)?,
                    to_i64(completed_at_ms, "completion timestamp")?,
                    turn_id.as_str(),
                    to_i64(expected_generation.get(), "owner generation")?,
                ],
            )
            .map_err(storage_error)?;
        if updated != 1 {
            return Err(ForegroundTurnStoreError::NotRunning {
                turn_id: turn_id.clone(),
                state: raw.state,
            });
        }
        transaction.commit().map_err(storage_error)?;
        Ok(snapshot)
    }

    fn terminate(
        &mut self,
        turn_id: &ForegroundTurnId,
        expected_generation: OwnerGeneration,
        outcome: ForegroundTurnTerminal,
        completed_at_ms: u64,
    ) -> Result<ForegroundTurnSnapshot, ForegroundTurnStoreError> {
        if matches!(outcome, ForegroundTurnTerminal::Completed) {
            return Err(ForegroundTurnStoreError::Invalid(
                "completed foreground turns must atomically append session messages".into(),
            ));
        }
        validate_terminal(&outcome)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let raw = load_raw(&transaction, turn_id)?;
        ensure_turn_generation(&raw, expected_generation)?;
        ensure_running(&raw)?;
        ensure_monotonic_timestamp(&raw, completed_at_ms)?;
        let session_id = SessionId::new(raw.session_id.clone())
            .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?;
        let session = load_snapshot(&transaction, &session_id).map_err(session_error)?;
        ensure_generation(&session, expected_generation)?;
        let updated = transaction
            .execute(
                "UPDATE foreground_turns
                 SET state = ?1, terminal_json = ?2, updated_at_ms = ?3
                 WHERE turn_id = ?4 AND state = 'running' AND owner_generation = ?5",
                params![
                    outcome.status_name(),
                    serde_json::to_string(&outcome).map_err(storage_error)?,
                    to_i64(completed_at_ms, "completion timestamp")?,
                    turn_id.as_str(),
                    to_i64(expected_generation.get(), "owner generation")?,
                ],
            )
            .map_err(storage_error)?;
        if updated != 1 {
            return Err(ForegroundTurnStoreError::NotRunning {
                turn_id: turn_id.clone(),
                state: raw.state,
            });
        }
        transaction.commit().map_err(storage_error)?;
        snapshot_from_raw(load_raw(&self.connection, turn_id)?)
    }

    fn latest(
        &mut self,
        session_id: &SessionId,
    ) -> Result<Option<ForegroundTurnSnapshot>, ForegroundTurnStoreError> {
        let id = self
            .connection
            .query_row(
                "SELECT turn_id FROM foreground_turns
                 WHERE session_id = ?1
                 ORDER BY started_at_ms DESC, turn_id DESC LIMIT 1",
                params![session_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(storage_error)?;
        id.map(|id| {
            let id = ForegroundTurnId::new(id)
                .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?;
            snapshot_from_raw(load_raw(&self.connection, &id)?)
        })
        .transpose()
    }

    fn reconcile_running(
        &mut self,
        reason: &str,
        completed_at_ms: u64,
    ) -> Result<Vec<ForegroundTurnSnapshot>, ForegroundTurnStoreError> {
        validate_reason(reason, "reconciliation reason")?;
        let completed_at = to_i64(completed_at_ms, "reconciliation timestamp")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let ids = {
            let mut statement = transaction
                .prepare(
                    "SELECT turn_id FROM foreground_turns
                     WHERE state = 'running'
                     ORDER BY started_at_ms ASC, turn_id ASC",
                )
                .map_err(storage_error)?;
            let rows =
                statement.query_map([], |row| row.get::<_, String>(0)).map_err(storage_error)?;
            rows.map(|row| row.map_err(storage_error)).collect::<Result<Vec<_>, _>>()?
        };
        let outcome = ForegroundTurnTerminal::OutcomeUnknown { reason: reason.into() };
        let terminal_json = serde_json::to_string(&outcome).map_err(storage_error)?;
        for id in &ids {
            let raw_id = ForegroundTurnId::new(id.clone())
                .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?;
            let raw = load_raw(&transaction, &raw_id)?;
            ensure_monotonic_timestamp(&raw, completed_at_ms)?;
            let updated = transaction
                .execute(
                    "UPDATE foreground_turns
                     SET state = 'outcome_unknown', terminal_json = ?1, updated_at_ms = ?2
                     WHERE turn_id = ?3 AND state = 'running' AND owner_generation = ?4",
                    params![terminal_json, completed_at, id, raw.owner_generation],
                )
                .map_err(storage_error)?;
            if updated != 1 {
                return Err(ForegroundTurnStoreError::NotRunning {
                    turn_id: raw_id,
                    state: raw.state,
                });
            }
        }
        transaction.commit().map_err(storage_error)?;
        ids.into_iter()
            .map(|id| {
                let id = ForegroundTurnId::new(id)
                    .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?;
                snapshot_from_raw(load_raw(&self.connection, &id)?)
            })
            .collect()
    }
}

struct RawForegroundTurn {
    turn_id: String,
    session_id: String,
    owner_generation: i64,
    prompt: String,
    state: String,
    terminal_json: Option<String>,
    started_at_ms: i64,
    updated_at_ms: i64,
}

fn load_raw(
    connection: &Connection,
    turn_id: &ForegroundTurnId,
) -> Result<RawForegroundTurn, ForegroundTurnStoreError> {
    connection
        .query_row(
            "SELECT turn_id, session_id, owner_generation, prompt, state, terminal_json,
                    started_at_ms, updated_at_ms
             FROM foreground_turns WHERE turn_id = ?1",
            params![turn_id.as_str()],
            |row| {
                Ok(RawForegroundTurn {
                    turn_id: row.get(0)?,
                    session_id: row.get(1)?,
                    owner_generation: row.get(2)?,
                    prompt: row.get(3)?,
                    state: row.get(4)?,
                    terminal_json: row.get(5)?,
                    started_at_ms: row.get(6)?,
                    updated_at_ms: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(storage_error)?
        .ok_or_else(|| ForegroundTurnStoreError::NotFound(turn_id.clone()))
}

fn snapshot_from_raw(
    raw: RawForegroundTurn,
) -> Result<ForegroundTurnSnapshot, ForegroundTurnStoreError> {
    let turn_id = ForegroundTurnId::new(raw.turn_id)
        .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?;
    let state = if raw.state == "running" {
        if raw.terminal_json.is_some() {
            return Err(ForegroundTurnStoreError::Invalid(format!(
                "running foreground turn {turn_id} has terminal data"
            )));
        }
        ForegroundTurnState::Running
    } else {
        let encoded = raw.terminal_json.ok_or_else(|| {
            ForegroundTurnStoreError::Invalid(format!(
                "terminal foreground turn {turn_id} has no terminal data"
            ))
        })?;
        let outcome = serde_json::from_str::<ForegroundTurnTerminal>(&encoded)
            .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?;
        if outcome.status_name() != raw.state {
            return Err(ForegroundTurnStoreError::Invalid(format!(
                "foreground turn {turn_id} state {} disagrees with terminal {}",
                raw.state,
                outcome.status_name()
            )));
        }
        ForegroundTurnState::Terminal {
            outcome,
            completed_at_ms: u64_from_i64(raw.updated_at_ms, "completion timestamp")?,
        }
    };
    Ok(ForegroundTurnSnapshot {
        spec: ForegroundTurnSpec {
            turn_id,
            session_id: SessionId::new(raw.session_id)
                .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?,
            prompt: raw.prompt,
        },
        owner_generation: generation(raw.owner_generation)?,
        state,
        started_at_ms: u64_from_i64(raw.started_at_ms, "start timestamp")?,
        updated_at_ms: u64_from_i64(raw.updated_at_ms, "update timestamp")?,
    })
}

fn validate_spec(spec: &ForegroundTurnSpec) -> Result<(), ForegroundTurnStoreError> {
    if spec.prompt.trim().is_empty() {
        return Err(ForegroundTurnStoreError::Invalid("prompt must be non-empty".into()));
    }
    Ok(())
}

fn validate_terminal(outcome: &ForegroundTurnTerminal) -> Result<(), ForegroundTurnStoreError> {
    match outcome {
        ForegroundTurnTerminal::Interrupted { reason }
        | ForegroundTurnTerminal::Failed { reason }
        | ForegroundTurnTerminal::OutcomeUnknown { reason } => {
            validate_reason(reason, "terminal reason")
        }
        ForegroundTurnTerminal::Completed => Ok(()),
    }
}

fn validate_reason(reason: &str, name: &str) -> Result<(), ForegroundTurnStoreError> {
    if reason.trim().is_empty() || reason.trim() != reason {
        return Err(ForegroundTurnStoreError::Invalid(format!(
            "{name} must be non-empty and have no surrounding whitespace"
        )));
    }
    Ok(())
}

fn ensure_generation(
    snapshot: &SessionSnapshot,
    expected: OwnerGeneration,
) -> Result<(), ForegroundTurnStoreError> {
    if snapshot.owner_generation == expected {
        Ok(())
    } else {
        Err(ForegroundTurnStoreError::GenerationConflict {
            session_id: snapshot.config.session_id.clone(),
            expected: expected.get(),
            actual: snapshot.owner_generation.get(),
        })
    }
}

fn ensure_turn_generation(
    raw: &RawForegroundTurn,
    expected: OwnerGeneration,
) -> Result<(), ForegroundTurnStoreError> {
    let actual = u64_from_i64(raw.owner_generation, "owner generation")?;
    if actual == expected.get() {
        Ok(())
    } else {
        Err(ForegroundTurnStoreError::GenerationConflict {
            session_id: SessionId::new(raw.session_id.clone())
                .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?,
            expected: expected.get(),
            actual,
        })
    }
}

fn ensure_running(raw: &RawForegroundTurn) -> Result<(), ForegroundTurnStoreError> {
    if raw.state == "running" {
        Ok(())
    } else {
        Err(ForegroundTurnStoreError::NotRunning {
            turn_id: ForegroundTurnId::new(raw.turn_id.clone())
                .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))?,
            state: raw.state.clone(),
        })
    }
}

fn ensure_monotonic_timestamp(
    raw: &RawForegroundTurn,
    completed_at_ms: u64,
) -> Result<(), ForegroundTurnStoreError> {
    if to_i64(completed_at_ms, "terminal timestamp")? < raw.updated_at_ms {
        return Err(ForegroundTurnStoreError::Invalid(
            "terminal timestamp precedes the last foreground-turn mutation".into(),
        ));
    }
    Ok(())
}

fn generation(value: i64) -> Result<OwnerGeneration, ForegroundTurnStoreError> {
    OwnerGeneration::new(u64_from_i64(value, "owner generation")?)
        .map_err(|error| ForegroundTurnStoreError::Invalid(error.to_string()))
}

fn to_i64(value: u64, name: &str) -> Result<i64, ForegroundTurnStoreError> {
    i64::try_from(value)
        .map_err(|_| ForegroundTurnStoreError::Invalid(format!("{name} exceeds SQLite range")))
}

fn u64_from_i64(value: i64, name: &str) -> Result<u64, ForegroundTurnStoreError> {
    u64::try_from(value)
        .map_err(|_| ForegroundTurnStoreError::Invalid(format!("{name} is negative")))
}

fn session_error(error: SessionStoreError) -> ForegroundTurnStoreError {
    match error {
        SessionStoreError::NotFound(session_id) => {
            ForegroundTurnStoreError::SessionNotFound(session_id)
        }
        SessionStoreError::Conflict { session_id, expected, actual } => {
            ForegroundTurnStoreError::GenerationConflict { session_id, expected, actual }
        }
        SessionStoreError::Invalid(message) => ForegroundTurnStoreError::Invalid(message),
        SessionStoreError::Storage(message) => ForegroundTurnStoreError::Storage(message),
        SessionStoreError::AlreadyExists(session_id) => ForegroundTurnStoreError::Invalid(format!(
            "unexpected duplicate session while mutating foreground turn: {session_id}"
        )),
    }
}

fn storage_error(error: impl std::fmt::Display) -> ForegroundTurnStoreError {
    ForegroundTurnStoreError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use domain::{
        EngineId, ForegroundTurnId, ForegroundTurnSpec, ForegroundTurnState,
        ForegroundTurnTerminal, LineageId, ManifestDigest, PromptManifest, SemanticMessage,
        SessionId,
    };
    use ports::{ForegroundTurnStore, ForegroundTurnStoreError, SessionStore};
    use protocol::{SessionConfig, TransportKind};
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{SqliteForegroundTurnStore, SqliteSessionStore};

    #[test]
    fn completion_atomically_advances_session_and_terminalizes_claim()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new("complete")?;
        let mut turns = SqliteForegroundTurnStore::open(&fixture.database)?;
        let started = turns.start(fixture.spec("turn-one", "hello")?, fixture.generation, 10)?;
        assert_eq!(started.state, ForegroundTurnState::Running);

        let committed =
            turns.complete(&started.spec.turn_id, fixture.generation, &turn("hello", "hi"), 20)?;
        assert_eq!(committed.owner_generation.get(), 2);
        assert_eq!(committed.conversation, turn("hello", "hi"));
        let latest = turns.latest(&fixture.session_id)?.ok_or("missing completed turn")?;
        assert!(matches!(
            latest.state,
            ForegroundTurnState::Terminal { outcome: ForegroundTurnTerminal::Completed, .. }
        ));

        let mut sessions = SqliteSessionStore::open(&fixture.database)?;
        assert_eq!(sessions.load(&fixture.session_id)?, committed);
        Ok(())
    }

    #[test]
    fn invalid_completion_rolls_back_both_session_and_turn()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new("rollback")?;
        let mut turns = SqliteForegroundTurnStore::open(&fixture.database)?;
        let started = turns.start(fixture.spec("turn-bad", "hello")?, fixture.generation, 10)?;
        let error = turns.complete(
            &started.spec.turn_id,
            fixture.generation,
            &[SemanticMessage::User { content: "hello".into() }],
            20,
        );
        assert!(matches!(error, Err(ForegroundTurnStoreError::Invalid(_))));
        assert_eq!(
            turns.latest(&fixture.session_id)?.ok_or("missing running turn")?.state,
            ForegroundTurnState::Running
        );

        let mut sessions = SqliteSessionStore::open(&fixture.database)?;
        let session = sessions.load(&fixture.session_id)?;
        assert_eq!(session.owner_generation, fixture.generation);
        assert!(session.conversation.is_empty());
        Ok(())
    }

    #[test]
    fn interruption_preserves_generation_and_allows_a_new_attempt()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new("interrupt")?;
        let mut turns = SqliteForegroundTurnStore::open(&fixture.database)?;
        let first = turns.start(fixture.spec("turn-first", "one")?, fixture.generation, 10)?;
        turns.terminate(
            &first.spec.turn_id,
            fixture.generation,
            ForegroundTurnTerminal::Interrupted { reason: "client requested stop".into() },
            20,
        )?;
        let second = turns.start(fixture.spec("turn-second", "two")?, fixture.generation, 30)?;
        assert_eq!(second.owner_generation, fixture.generation);
        assert_eq!(turns.latest(&fixture.session_id)?, Some(second));

        let mut sessions = SqliteSessionStore::open(&fixture.database)?;
        assert_eq!(sessions.load(&fixture.session_id)?.owner_generation, fixture.generation);
        Ok(())
    }

    #[test]
    fn reconciliation_marks_abandoned_claims_unknown_without_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new("reconcile")?;
        let mut turns = SqliteForegroundTurnStore::open(&fixture.database)?;
        turns.start(fixture.spec("turn-lost", "do not replay me")?, fixture.generation, 10)?;
        let reconciled = turns.reconcile_running("owning gateway restarted", 20)?;
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].spec.prompt, "do not replay me");
        assert!(matches!(
            &reconciled[0].state,
            ForegroundTurnState::Terminal {
                outcome: ForegroundTurnTerminal::OutcomeUnknown { reason },
                completed_at_ms: 20,
            } if reason == "owning gateway restarted"
        ));
        assert!(turns.reconcile_running("owning gateway restarted", 30)?.is_empty());

        let mut sessions = SqliteSessionStore::open(&fixture.database)?;
        let session = sessions.load(&fixture.session_id)?;
        assert_eq!(session.owner_generation, fixture.generation);
        assert!(session.conversation.is_empty());
        Ok(())
    }

    #[test]
    fn generation_and_single_running_owner_are_enforced() -> Result<(), Box<dyn std::error::Error>>
    {
        let fixture = Fixture::new("ownership")?;
        let mut turns = SqliteForegroundTurnStore::open(&fixture.database)?;
        let stale = domain::OwnerGeneration::new(2)?;
        assert!(matches!(
            turns.start(fixture.spec("turn-stale", "stale")?, stale, 10),
            Err(ForegroundTurnStoreError::GenerationConflict { expected: 2, actual: 1, .. })
        ));
        turns.start(fixture.spec("turn-owner", "owner")?, fixture.generation, 10)?;
        assert!(matches!(
            turns.start(fixture.spec("turn-racer", "racer")?, fixture.generation, 11),
            Err(ForegroundTurnStoreError::SessionBusy(_))
        ));
        Ok(())
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        database: std::path::PathBuf,
        session_id: SessionId,
        generation: domain::OwnerGeneration,
    }

    impl Fixture {
        fn new(label: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let directory = tempdir()?;
            let database = directory.path().join("state.db");
            let session_id = SessionId::new(format!("session-{label}"))?;
            let mut sessions = SqliteSessionStore::open(&database)?;
            let created = sessions.create(config(&session_id, directory.path())?)?;
            Ok(Self {
                _directory: directory,
                database,
                session_id,
                generation: created.owner_generation,
            })
        }

        fn spec(
            &self,
            turn_id: &str,
            prompt: &str,
        ) -> Result<ForegroundTurnSpec, Box<dyn std::error::Error>> {
            Ok(ForegroundTurnSpec {
                turn_id: ForegroundTurnId::new(turn_id)?,
                session_id: self.session_id.clone(),
                prompt: prompt.into(),
            })
        }
    }

    fn turn(user: &str, assistant: &str) -> Vec<SemanticMessage> {
        vec![
            SemanticMessage::User { content: user.into() },
            SemanticMessage::Assistant {
                content: assistant.into(),
                reasoning: None,
                provider_replay: None,
            },
        ]
    }

    fn config(
        session_id: &SessionId,
        root: &std::path::Path,
    ) -> Result<SessionConfig, Box<dyn std::error::Error>> {
        let tools = vec![json!({
            "type": "function",
            "function": {"name": "read_file", "parameters": {"type": "object"}}
        })];
        let system_prompt = "Frozen foreground test prompt.".to_owned();
        Ok(SessionConfig {
            session_id: session_id.clone(),
            lineage_id: LineageId::new(session_id.as_str())?,
            prompt_manifest: PromptManifest::new(
                1,
                EngineId::new("rust-v1:test:foreground")?,
                ManifestDigest::new(digest(system_prompt.as_bytes()))?,
                ManifestDigest::new(digest(&serde_json::to_vec(&tools)?))?,
            )?,
            transport: TransportKind::ChatCompletions,
            provider_adapter: "openai".into(),
            base_url: "https://api.openai.com/v1".into(),
            api_key_env: Some("OPENAI_API_KEY".into()),
            model: "test-model".into(),
            tool_root: fs::canonicalize(root)?.to_string_lossy().into_owned(),
            system_prompt,
            tools,
        })
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
}
