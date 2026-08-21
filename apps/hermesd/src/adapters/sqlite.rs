//! SQLite implementation of the kernel-owned durable session port.

use std::{collections::HashSet, fs, path::Path, time::Duration};

use domain::{
    Conversation, EngineId, LineageId, ManifestDigest, OwnerGeneration, PlannedToolCall,
    PromptManifest, SemanticMessage, SessionId, ToolArguments, ToolCallId, ToolEffect,
    ToolResultStatus, ToolTerminal,
};
use ports::{EffectLedger, EffectLedgerError, SessionStore, SessionStoreError};
use protocol::{PendingEffect, SessionConfig, SessionSnapshot, SessionSummary, TransportKind};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use sha2::{Digest, Sha256};

const SCHEMA_VERSION: u32 = 6;

/// SQLite-backed single-writer durable session repository.
pub struct SqliteSessionStore {
    connection: Connection,
}

/// SQLite-backed write-ahead ledger for tool effects.
pub struct SqliteEffectLedger {
    connection: Connection,
}

impl SqliteSessionStore {
    /// Open or create a durable store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SessionStoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(storage_error)?;
        }
        let connection = Connection::open(path).map_err(storage_error)?;
        Self::initialize(connection)
    }

    /// Create an isolated in-memory store, primarily for embedding and tests.
    pub fn in_memory() -> Result<Self, SessionStoreError> {
        let connection = Connection::open_in_memory().map_err(storage_error)?;
        Self::initialize(connection)
    }

    pub(super) fn into_connection(self) -> Connection {
        self.connection
    }

    fn initialize(connection: Connection) -> Result<Self, SessionStoreError> {
        connection.busy_timeout(Duration::from_secs(5)).map_err(storage_error)?;
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;",
            )
            .map_err(storage_error)?;
        let version = connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
            .map_err(storage_error)?;
        match version {
            0 => {
                connection
                    .execute_batch(
                        "BEGIN IMMEDIATE;
                         CREATE TABLE sessions (
                             session_id TEXT PRIMARY KEY NOT NULL,
                             lineage_id TEXT NOT NULL,
                             owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
                             engine_id TEXT NOT NULL,
                             prompt_revision INTEGER NOT NULL CHECK (prompt_revision > 0),
                             system_prompt_digest TEXT NOT NULL,
                             tool_catalog_digest TEXT NOT NULL,
                             transport_json TEXT NOT NULL,
                             provider_adapter TEXT NOT NULL,
                             base_url TEXT NOT NULL,
                             api_key_env TEXT,
                             model TEXT NOT NULL,
                             tool_root TEXT NOT NULL,
                             system_prompt TEXT NOT NULL,
                             tools_json TEXT NOT NULL,
                             message_count INTEGER NOT NULL DEFAULT 0 CHECK (message_count >= 0),
                             created_at INTEGER NOT NULL DEFAULT (unixepoch()),
                             updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                         );
                         CREATE TABLE session_messages (
                             session_id TEXT NOT NULL,
                             sequence INTEGER NOT NULL CHECK (sequence >= 0),
                             message_json TEXT NOT NULL,
                             PRIMARY KEY (session_id, sequence),
                             FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
                         );
                         CREATE INDEX session_updated_at ON sessions(updated_at DESC);
                         CREATE TABLE tool_effects (
                             execution_key TEXT PRIMARY KEY NOT NULL,
                             execution_scope TEXT NOT NULL,
                             call_id TEXT NOT NULL,
                             name TEXT NOT NULL,
                             arguments_json TEXT NOT NULL,
                             effect_json TEXT NOT NULL,
                             approval_json TEXT,
                             status TEXT NOT NULL,
                             content TEXT,
                             receipt TEXT,
                             planned_at INTEGER NOT NULL DEFAULT (unixepoch()),
                             completed_at INTEGER,
                             CHECK (
                                 (status = 'planned' AND content IS NULL AND completed_at IS NULL)
                                 OR (status != 'planned' AND content IS NOT NULL AND completed_at IS NOT NULL)
                             )
                         );
                         CREATE INDEX pending_tool_effects
                             ON tool_effects(planned_at, execution_key) WHERE status = 'planned';
                         PRAGMA user_version = 2;
                         COMMIT;",
                    )
                    .map_err(storage_error)?;
            }
            1 => {
                connection
                    .execute_batch(
                        "BEGIN IMMEDIATE;
                         CREATE TABLE tool_effects (
                             execution_key TEXT PRIMARY KEY NOT NULL,
                             execution_scope TEXT NOT NULL,
                             call_id TEXT NOT NULL,
                             name TEXT NOT NULL,
                             arguments_json TEXT NOT NULL,
                             effect_json TEXT NOT NULL,
                             approval_json TEXT,
                             status TEXT NOT NULL,
                             content TEXT,
                             receipt TEXT,
                             planned_at INTEGER NOT NULL DEFAULT (unixepoch()),
                             completed_at INTEGER,
                             CHECK (
                                 (status = 'planned' AND content IS NULL AND completed_at IS NULL)
                                 OR (status != 'planned' AND content IS NOT NULL AND completed_at IS NOT NULL)
                             )
                         );
                         CREATE INDEX pending_tool_effects
                             ON tool_effects(planned_at, execution_key) WHERE status = 'planned';
                         PRAGMA user_version = 2;
                         COMMIT;",
                    )
                    .map_err(storage_error)?;
            }
            2 | 3 | 4 | 5 | SCHEMA_VERSION => {}
            other => {
                return Err(SessionStoreError::Storage(format!(
                    "unsupported SQLite schema version {other}; expected {SCHEMA_VERSION}"
                )));
            }
        }
        if version <= 2 {
            connection
                .execute_batch(
                    "BEGIN IMMEDIATE;
                     CREATE TABLE delegations (
                         delegation_id TEXT PRIMARY KEY NOT NULL,
                         completion_event_id TEXT UNIQUE NOT NULL,
                         parent_session_id TEXT NOT NULL,
                         child_session_id TEXT UNIQUE NOT NULL,
                         goal TEXT NOT NULL,
                         context TEXT,
                         state TEXT NOT NULL,
                         owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
                         worker_id TEXT,
                         fencing_token INTEGER NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
                         lease_expires_at_ms INTEGER,
                         terminal_json TEXT,
                         created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
                         updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= created_at_ms),
                         FOREIGN KEY (parent_session_id) REFERENCES sessions(session_id),
                         FOREIGN KEY (child_session_id) REFERENCES sessions(session_id),
                         CHECK (state IN (
                             'pending', 'running', 'completed', 'failed', 'cancelled',
                             'outcome_unknown'
                         )),
                         CHECK (
                             (state = 'pending' AND worker_id IS NULL AND fencing_token = 0
                                 AND lease_expires_at_ms IS NULL AND terminal_json IS NULL)
                             OR (state = 'running' AND worker_id IS NOT NULL
                                 AND fencing_token > 0 AND lease_expires_at_ms IS NOT NULL
                                 AND terminal_json IS NULL)
                             OR (state IN ('completed', 'failed', 'cancelled', 'outcome_unknown')
                                 AND fencing_token > 0 AND lease_expires_at_ms IS NULL
                                 AND terminal_json IS NOT NULL)
                         )
                     );
                     CREATE INDEX pending_delegations
                         ON delegations(created_at_ms, delegation_id) WHERE state = 'pending';
                     CREATE INDEX leased_delegations
                         ON delegations(lease_expires_at_ms, delegation_id) WHERE state = 'running';
                     CREATE TABLE delegation_completions (
                         event_id TEXT PRIMARY KEY NOT NULL,
                         delegation_id TEXT UNIQUE NOT NULL,
                         payload_json TEXT NOT NULL,
                         delivery_state TEXT NOT NULL DEFAULT 'pending',
                         delivery_claim_id TEXT,
                         delivery_claim_expires_at_ms INTEGER,
                         delivery_attempts INTEGER NOT NULL DEFAULT 0
                             CHECK (delivery_attempts >= 0),
                         created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
                         delivered_at_ms INTEGER,
                         FOREIGN KEY (delegation_id) REFERENCES delegations(delegation_id),
                         CHECK (delivery_state IN ('pending', 'delivered')),
                         CHECK (
                             (delivery_claim_id IS NULL
                                 AND delivery_claim_expires_at_ms IS NULL)
                             OR (delivery_claim_id IS NOT NULL
                                 AND delivery_claim_expires_at_ms IS NOT NULL)
                         ),
                         CHECK (
                             (delivery_state = 'pending' AND delivered_at_ms IS NULL)
                             OR (delivery_state = 'delivered' AND delivered_at_ms IS NOT NULL
                                 AND delivery_claim_id IS NULL
                                 AND delivery_claim_expires_at_ms IS NULL)
                         )
                     );
                     CREATE INDEX pending_delegation_completions
                         ON delegation_completions(created_at_ms, event_id)
                         WHERE delivery_state = 'pending';
                     PRAGMA user_version = 3;
                     COMMIT;",
                )
                .map_err(storage_error)?;
        }
        if version <= 3 {
            connection
                .execute_batch(
                    "BEGIN IMMEDIATE;
                     CREATE TABLE foreground_turns (
                         turn_id TEXT PRIMARY KEY NOT NULL,
                         session_id TEXT NOT NULL,
                         owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
                         prompt TEXT NOT NULL CHECK (length(prompt) > 0),
                         state TEXT NOT NULL,
                         terminal_json TEXT,
                         started_at_ms INTEGER NOT NULL CHECK (started_at_ms >= 0),
                         updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= started_at_ms),
                         FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
                         CHECK (state IN (
                             'running', 'completed', 'interrupted', 'failed', 'outcome_unknown'
                         )),
                         CHECK (
                             (state = 'running' AND terminal_json IS NULL)
                             OR (state != 'running' AND terminal_json IS NOT NULL)
                         )
                     );
                     CREATE UNIQUE INDEX running_foreground_turn
                         ON foreground_turns(session_id) WHERE state = 'running';
                     CREATE INDEX latest_foreground_turn
                         ON foreground_turns(session_id, started_at_ms DESC, turn_id DESC);
                     PRAGMA user_version = 4;
                     COMMIT;",
                )
                .map_err(storage_error)?;
        }
        if version <= 4 {
            connection
                .execute_batch(
                    "BEGIN IMMEDIATE;
                     ALTER TABLE foreground_turns
                         ADD COLUMN provider_prompt TEXT NOT NULL DEFAULT '';
                     UPDATE foreground_turns SET provider_prompt = prompt;
                     PRAGMA user_version = 5;
                     COMMIT;",
                )
                .map_err(storage_error)?;
        }
        if version <= 5 {
            connection
                .execute_batch(
                    "BEGIN IMMEDIATE;
                     ALTER TABLE delegations ADD COLUMN cancellation_reason TEXT;
                     ALTER TABLE delegations ADD COLUMN cancellation_requested_at_ms INTEGER;
                     PRAGMA user_version = 6;
                     COMMIT;",
                )
                .map_err(storage_error)?;
        }
        Ok(Self { connection })
    }
}

impl SqliteEffectLedger {
    /// Open or create an effect ledger in the shared state database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EffectLedgerError> {
        let store = SqliteSessionStore::open(path).map_err(effect_storage_error)?;
        Ok(Self { connection: store.connection })
    }

    /// Create an isolated in-memory ledger.
    pub fn in_memory() -> Result<Self, EffectLedgerError> {
        let store = SqliteSessionStore::in_memory().map_err(effect_storage_error)?;
        Ok(Self { connection: store.connection })
    }
}

impl EffectLedger for SqliteEffectLedger {
    fn record_plans(
        &mut self,
        execution_scope: &str,
        plans: &[PlannedToolCall],
    ) -> Result<(), EffectLedgerError> {
        if execution_scope.is_empty() {
            return Err(EffectLedgerError::Invalid("execution scope must be non-empty".into()));
        }
        if plans.is_empty() {
            return Err(EffectLedgerError::Invalid("cannot record an empty plan batch".into()));
        }
        let mut keys = HashSet::with_capacity(plans.len());
        for plan in plans {
            if !keys.insert(&plan.execution_key) {
                return Err(EffectLedgerError::Invalid(format!(
                    "duplicate execution key in plan batch: {}",
                    plan.execution_key
                )));
            }
        }

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(ledger_storage_error)?;
        for plan in plans {
            let exists = transaction
                .query_row(
                    "SELECT 1 FROM tool_effects WHERE execution_key = ?1",
                    params![plan.execution_key],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(ledger_storage_error)?
                .is_some();
            if exists {
                return Err(EffectLedgerError::AlreadyRecorded(plan.execution_key.clone()));
            }
        }
        {
            let mut insert = transaction
                .prepare(
                    "INSERT INTO tool_effects (
                        execution_key, execution_scope, call_id, name, arguments_json,
                        effect_json, approval_json, status
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'planned')",
                )
                .map_err(ledger_storage_error)?;
            for plan in plans {
                insert
                    .execute(params![
                        plan.execution_key,
                        execution_scope,
                        plan.call_id.as_str(),
                        plan.name,
                        serde_json::to_string(&plan.arguments).map_err(ledger_storage_error)?,
                        serde_json::to_string(&plan.effect).map_err(ledger_storage_error)?,
                        plan.approval
                            .as_ref()
                            .map(serde_json::to_string)
                            .transpose()
                            .map_err(ledger_storage_error)?,
                    ])
                    .map_err(ledger_storage_error)?;
            }
        }
        transaction.commit().map_err(ledger_storage_error)
    }

    fn record_terminals(&mut self, terminals: &[ToolTerminal]) -> Result<(), EffectLedgerError> {
        if terminals.is_empty() {
            return Err(EffectLedgerError::Invalid("cannot record an empty terminal batch".into()));
        }
        let mut keys = HashSet::with_capacity(terminals.len());
        for terminal in terminals {
            if !keys.insert(&terminal.execution_key) {
                return Err(EffectLedgerError::Invalid(format!(
                    "duplicate execution key in terminal batch: {}",
                    terminal.execution_key
                )));
            }
        }

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(ledger_storage_error)?;
        for terminal in terminals {
            let recorded = transaction
                .query_row(
                    "SELECT status, call_id, name, effect_json
                     FROM tool_effects WHERE execution_key = ?1",
                    params![terminal.execution_key],
                    |row| {
                        Ok(RecordedPlan {
                            status: row.get(0)?,
                            call_id: row.get(1)?,
                            name: row.get(2)?,
                            effect_json: row.get(3)?,
                        })
                    },
                )
                .optional()
                .map_err(ledger_storage_error)?
                .ok_or_else(|| EffectLedgerError::MissingPlan(terminal.execution_key.clone()))?;
            if recorded.status != "planned" {
                return Err(EffectLedgerError::AlreadyTerminal(terminal.execution_key.clone()));
            }
            let effect = serde_json::from_str::<ToolEffect>(&recorded.effect_json)
                .map_err(|error| EffectLedgerError::Invalid(error.to_string()))?;
            if recorded.call_id != terminal.call_id.as_str()
                || recorded.name != terminal.name
                || effect != terminal.effect
            {
                return Err(EffectLedgerError::PlanMismatch(terminal.execution_key.clone()));
            }
        }
        {
            let mut update = transaction
                .prepare(
                    "UPDATE tool_effects
                     SET status = ?1, content = ?2, receipt = ?3, completed_at = unixepoch()
                     WHERE execution_key = ?4 AND status = 'planned'",
                )
                .map_err(ledger_storage_error)?;
            for terminal in terminals {
                let updated = update
                    .execute(params![
                        terminal_status_name(terminal.status),
                        terminal.content,
                        terminal.receipt,
                        terminal.execution_key,
                    ])
                    .map_err(ledger_storage_error)?;
                if updated != 1 {
                    return Err(EffectLedgerError::AlreadyTerminal(terminal.execution_key.clone()));
                }
            }
        }
        transaction.commit().map_err(ledger_storage_error)
    }

    fn pending(&mut self) -> Result<Vec<PendingEffect>, EffectLedgerError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT execution_scope, call_id, name, arguments_json, execution_key,
                        effect_json, approval_json
                 FROM tool_effects WHERE status = 'planned'
                 ORDER BY planned_at ASC, execution_key ASC",
            )
            .map_err(ledger_storage_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok(RawPendingEffect {
                    execution_scope: row.get(0)?,
                    call_id: row.get(1)?,
                    name: row.get(2)?,
                    arguments_json: row.get(3)?,
                    execution_key: row.get(4)?,
                    effect_json: row.get(5)?,
                    approval_json: row.get(6)?,
                })
            })
            .map_err(ledger_storage_error)?;
        rows.map(|row| {
            let row = row.map_err(ledger_storage_error)?;
            Ok(PendingEffect {
                execution_scope: row.execution_scope,
                plan: PlannedToolCall {
                    call_id: ToolCallId::new(row.call_id)
                        .map_err(|error| EffectLedgerError::Invalid(error.to_string()))?,
                    name: row.name,
                    arguments: serde_json::from_str::<ToolArguments>(&row.arguments_json)
                        .map_err(|error| EffectLedgerError::Invalid(error.to_string()))?,
                    execution_key: row.execution_key,
                    effect: serde_json::from_str::<ToolEffect>(&row.effect_json)
                        .map_err(|error| EffectLedgerError::Invalid(error.to_string()))?,
                    approval: row
                        .approval_json
                        .map(|encoded| serde_json::from_str(&encoded))
                        .transpose()
                        .map_err(|error| EffectLedgerError::Invalid(error.to_string()))?,
                },
            })
        })
        .collect()
    }
}

impl SessionStore for SqliteSessionStore {
    fn create(&mut self, config: SessionConfig) -> Result<SessionSnapshot, SessionStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let snapshot = create_session_in_transaction(&transaction, config)?;
        transaction.commit().map_err(storage_error)?;
        Ok(snapshot)
    }

    fn load(&mut self, session_id: &SessionId) -> Result<SessionSnapshot, SessionStoreError> {
        load_snapshot(&self.connection, session_id)
    }

    fn append(
        &mut self,
        session_id: &SessionId,
        expected_generation: OwnerGeneration,
        messages: &[SemanticMessage],
    ) -> Result<SessionSnapshot, SessionStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let snapshot =
            append_turn_in_transaction(&transaction, session_id, expected_generation, messages)?;
        transaction.commit().map_err(storage_error)?;
        Ok(snapshot)
    }

    fn list(&mut self) -> Result<Vec<SessionSummary>, SessionStoreError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT session_id, provider_adapter, model, owner_generation, message_count
                 FROM sessions ORDER BY updated_at DESC, session_id ASC",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok(RawSummary {
                    session_id: row.get(0)?,
                    provider_adapter: row.get(1)?,
                    model: row.get(2)?,
                    owner_generation: row.get(3)?,
                    message_count: row.get(4)?,
                })
            })
            .map_err(storage_error)?;
        rows.map(|row| {
            let row = row.map_err(storage_error)?;
            Ok(SessionSummary {
                session_id: SessionId::new(row.session_id)
                    .map_err(|error| SessionStoreError::Invalid(error.to_string()))?,
                provider_adapter: row.provider_adapter,
                model: row.model,
                owner_generation: generation_from_i64(row.owner_generation)?,
                message_count: usize_from_i64(row.message_count, "message count")?,
            })
        })
        .collect()
    }
}

pub(super) fn create_session_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    config: SessionConfig,
) -> Result<SessionSnapshot, SessionStoreError> {
    validate_config(&config)?;
    let exists = transaction
        .query_row(
            "SELECT 1 FROM sessions WHERE session_id = ?1",
            params![config.session_id.as_str()],
            |row| row.get::<_, u8>(0),
        )
        .optional()
        .map_err(storage_error)?
        .is_some();
    if exists {
        return Err(SessionStoreError::AlreadyExists(config.session_id));
    }
    let generation =
        OwnerGeneration::new(1).map_err(|error| SessionStoreError::Invalid(error.to_string()))?;
    let tools_json = serde_json::to_string(&config.tools).map_err(storage_error)?;
    let transport_json = serde_json::to_string(&config.transport).map_err(storage_error)?;
    transaction
        .execute(
            "INSERT INTO sessions (
                    session_id, lineage_id, owner_generation, engine_id, prompt_revision,
                    system_prompt_digest, tool_catalog_digest, transport_json, provider_adapter,
                    base_url, api_key_env, model, tool_root, system_prompt, tools_json
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
                 )",
            params![
                config.session_id.as_str(),
                config.lineage_id.as_str(),
                to_i64(generation.get(), "owner generation")?,
                config.prompt_manifest.engine().as_str(),
                to_i64(config.prompt_manifest.revision(), "prompt revision")?,
                config.prompt_manifest.system_prompt().as_str(),
                config.prompt_manifest.tool_catalog().as_str(),
                transport_json,
                config.provider_adapter,
                config.base_url,
                config.api_key_env,
                config.model,
                config.tool_root,
                config.system_prompt,
                tools_json,
            ],
        )
        .map_err(storage_error)?;
    Ok(SessionSnapshot { config, owner_generation: generation, conversation: Vec::new() })
}

pub(super) fn append_turn_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    expected_generation: OwnerGeneration,
    messages: &[SemanticMessage],
) -> Result<SessionSnapshot, SessionStoreError> {
    if messages.is_empty() {
        return Err(SessionStoreError::Invalid("cannot append an empty turn".into()));
    }
    if !matches!(messages.first(), Some(SemanticMessage::User { .. }))
        || !matches!(messages.last(), Some(SemanticMessage::Assistant { .. }))
    {
        return Err(SessionStoreError::Invalid(
            "an appended turn must begin with user and end with assistant".into(),
        ));
    }

    let snapshot = load_snapshot(transaction, session_id)?;
    if snapshot.owner_generation != expected_generation {
        return Err(SessionStoreError::Conflict {
            session_id: session_id.clone(),
            expected: expected_generation.get(),
            actual: snapshot.owner_generation.get(),
        });
    }
    let mut conversation = snapshot.conversation;
    let start = conversation.len();
    conversation.extend_from_slice(messages);
    Conversation::new(conversation.clone())
        .map_err(|error| SessionStoreError::Invalid(error.to_string()))?;

    {
        let mut insert = transaction
            .prepare(
                "INSERT INTO session_messages (session_id, sequence, message_json)
                 VALUES (?1, ?2, ?3)",
            )
            .map_err(storage_error)?;
        for (offset, message) in messages.iter().enumerate() {
            let sequence = start.checked_add(offset).ok_or_else(|| {
                SessionStoreError::Invalid("session message count overflowed".into())
            })?;
            let message_json = serde_json::to_string(message).map_err(storage_error)?;
            insert
                .execute(params![
                    session_id.as_str(),
                    usize_to_i64(sequence, "message sequence")?,
                    message_json,
                ])
                .map_err(storage_error)?;
        }
    }
    let next_generation = expected_generation
        .get()
        .checked_add(1)
        .ok_or_else(|| SessionStoreError::Invalid("owner generation overflowed".into()))?;
    let updated = transaction
        .execute(
            "UPDATE sessions
             SET owner_generation = ?1, message_count = ?2, updated_at = unixepoch()
             WHERE session_id = ?3 AND owner_generation = ?4",
            params![
                to_i64(next_generation, "owner generation")?,
                usize_to_i64(conversation.len(), "message count")?,
                session_id.as_str(),
                to_i64(expected_generation.get(), "owner generation")?,
            ],
        )
        .map_err(storage_error)?;
    if updated != 1 {
        return Err(SessionStoreError::Conflict {
            session_id: session_id.clone(),
            expected: expected_generation.get(),
            actual: snapshot.owner_generation.get(),
        });
    }
    Ok(SessionSnapshot {
        config: snapshot.config,
        owner_generation: OwnerGeneration::new(next_generation)
            .map_err(|error| SessionStoreError::Invalid(error.to_string()))?,
        conversation,
    })
}

pub(super) fn load_snapshot(
    connection: &Connection,
    session_id: &SessionId,
) -> Result<SessionSnapshot, SessionStoreError> {
    let raw = connection
        .query_row(
            "SELECT
                session_id, lineage_id, owner_generation, engine_id, prompt_revision,
                system_prompt_digest, tool_catalog_digest, transport_json, provider_adapter,
                base_url, api_key_env, model, tool_root, system_prompt, tools_json, message_count
             FROM sessions WHERE session_id = ?1",
            params![session_id.as_str()],
            |row| {
                Ok(RawSession {
                    session_id: row.get(0)?,
                    lineage_id: row.get(1)?,
                    owner_generation: row.get(2)?,
                    engine_id: row.get(3)?,
                    prompt_revision: row.get(4)?,
                    system_prompt_digest: row.get(5)?,
                    tool_catalog_digest: row.get(6)?,
                    transport_json: row.get(7)?,
                    provider_adapter: row.get(8)?,
                    base_url: row.get(9)?,
                    api_key_env: row.get(10)?,
                    model: row.get(11)?,
                    tool_root: row.get(12)?,
                    system_prompt: row.get(13)?,
                    tools_json: row.get(14)?,
                    message_count: row.get(15)?,
                })
            },
        )
        .optional()
        .map_err(storage_error)?
        .ok_or_else(|| SessionStoreError::NotFound(session_id.clone()))?;
    let config = config_from_raw(&raw)?;

    let mut statement = connection
        .prepare(
            "SELECT message_json FROM session_messages
             WHERE session_id = ?1 ORDER BY sequence ASC",
        )
        .map_err(storage_error)?;
    let rows = statement
        .query_map(params![session_id.as_str()], |row| row.get::<_, String>(0))
        .map_err(storage_error)?;
    let conversation = rows
        .map(|row| {
            let encoded = row.map_err(storage_error)?;
            serde_json::from_str::<SemanticMessage>(&encoded).map_err(|error| {
                SessionStoreError::Invalid(format!("stored semantic message is invalid: {error}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let stored_count = usize_from_i64(raw.message_count, "message count")?;
    if stored_count != conversation.len() {
        return Err(SessionStoreError::Invalid(format!(
            "session message count is {stored_count}, but {} messages exist",
            conversation.len()
        )));
    }
    Conversation::new(conversation.clone())
        .map_err(|error| SessionStoreError::Invalid(error.to_string()))?;
    Ok(SessionSnapshot {
        config,
        owner_generation: generation_from_i64(raw.owner_generation)?,
        conversation,
    })
}

fn config_from_raw(raw: &RawSession) -> Result<SessionConfig, SessionStoreError> {
    let config = SessionConfig {
        session_id: SessionId::new(raw.session_id.clone())
            .map_err(|error| SessionStoreError::Invalid(error.to_string()))?,
        lineage_id: LineageId::new(raw.lineage_id.clone())
            .map_err(|error| SessionStoreError::Invalid(error.to_string()))?,
        prompt_manifest: PromptManifest::new(
            u64_from_i64(raw.prompt_revision, "prompt revision")?,
            EngineId::new(raw.engine_id.clone())
                .map_err(|error| SessionStoreError::Invalid(error.to_string()))?,
            ManifestDigest::new(raw.system_prompt_digest.clone())
                .map_err(|error| SessionStoreError::Invalid(error.to_string()))?,
            ManifestDigest::new(raw.tool_catalog_digest.clone())
                .map_err(|error| SessionStoreError::Invalid(error.to_string()))?,
        )
        .map_err(|error| SessionStoreError::Invalid(error.to_string()))?,
        transport: serde_json::from_str::<TransportKind>(&raw.transport_json).map_err(|error| {
            SessionStoreError::Invalid(format!("stored transport is invalid: {error}"))
        })?,
        provider_adapter: raw.provider_adapter.clone(),
        base_url: raw.base_url.clone(),
        api_key_env: raw.api_key_env.clone(),
        model: raw.model.clone(),
        tool_root: raw.tool_root.clone(),
        system_prompt: raw.system_prompt.clone(),
        tools: serde_json::from_str::<Vec<Value>>(&raw.tools_json).map_err(|error| {
            SessionStoreError::Invalid(format!("stored tool catalog is invalid: {error}"))
        })?,
    };
    validate_config(&config)?;
    Ok(config)
}

fn validate_config(config: &SessionConfig) -> Result<(), SessionStoreError> {
    if config.lineage_id.as_str() != config.session_id.as_str() {
        return Err(SessionStoreError::Invalid(
            "lineage branching is not implemented; lineage ID must equal session ID".into(),
        ));
    }
    for (name, value) in [
        ("provider adapter", config.provider_adapter.as_str()),
        ("base URL", config.base_url.as_str()),
        ("model", config.model.as_str()),
        ("tool root", config.tool_root.as_str()),
    ] {
        if value.is_empty() || value.trim() != value {
            return Err(SessionStoreError::Invalid(format!(
                "{name} must be non-empty and have no surrounding whitespace"
            )));
        }
    }
    if !Path::new(&config.tool_root).is_absolute() {
        return Err(SessionStoreError::Invalid("tool root must be an absolute path".into()));
    }
    if config.api_key_env.as_ref().is_some_and(|name| name.is_empty() || name.trim() != name) {
        return Err(SessionStoreError::Invalid(
            "API key environment variable must be non-empty and unpadded".into(),
        ));
    }
    let system_digest = digest(config.system_prompt.as_bytes());
    if config.prompt_manifest.system_prompt().as_str() != system_digest {
        return Err(SessionStoreError::Invalid(
            "system prompt does not match its frozen manifest digest".into(),
        ));
    }
    let tools = serde_json::to_vec(&config.tools).map_err(storage_error)?;
    let tools_digest = digest(&tools);
    if config.prompt_manifest.tool_catalog().as_str() != tools_digest {
        return Err(SessionStoreError::Invalid(
            "tool catalog does not match its frozen manifest digest".into(),
        ));
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn storage_error(error: impl std::fmt::Display) -> SessionStoreError {
    SessionStoreError::Storage(error.to_string())
}

fn to_i64(value: u64, name: &str) -> Result<i64, SessionStoreError> {
    i64::try_from(value)
        .map_err(|_| SessionStoreError::Invalid(format!("{name} exceeds SQLite integer range")))
}

fn usize_to_i64(value: usize, name: &str) -> Result<i64, SessionStoreError> {
    i64::try_from(value)
        .map_err(|_| SessionStoreError::Invalid(format!("{name} exceeds SQLite integer range")))
}

fn u64_from_i64(value: i64, name: &str) -> Result<u64, SessionStoreError> {
    u64::try_from(value).map_err(|_| SessionStoreError::Invalid(format!("{name} is negative")))
}

fn usize_from_i64(value: i64, name: &str) -> Result<usize, SessionStoreError> {
    usize::try_from(value)
        .map_err(|_| SessionStoreError::Invalid(format!("{name} is negative or too large")))
}

fn generation_from_i64(value: i64) -> Result<OwnerGeneration, SessionStoreError> {
    OwnerGeneration::new(u64_from_i64(value, "owner generation")?)
        .map_err(|error| SessionStoreError::Invalid(error.to_string()))
}

fn effect_storage_error(error: impl std::fmt::Display) -> EffectLedgerError {
    EffectLedgerError::Storage(error.to_string())
}

fn ledger_storage_error(error: impl std::fmt::Display) -> EffectLedgerError {
    EffectLedgerError::Storage(error.to_string())
}

const fn terminal_status_name(status: ToolResultStatus) -> &'static str {
    match status {
        ToolResultStatus::Succeeded => "succeeded",
        ToolResultStatus::Failed => "failed",
        ToolResultStatus::Cancelled => "cancelled",
        ToolResultStatus::Rejected => "rejected",
        ToolResultStatus::OutcomeUnknown => "outcome_unknown",
        ToolResultStatus::Observed => "observed",
    }
}

struct RawSession {
    session_id: String,
    lineage_id: String,
    owner_generation: i64,
    engine_id: String,
    prompt_revision: i64,
    system_prompt_digest: String,
    tool_catalog_digest: String,
    transport_json: String,
    provider_adapter: String,
    base_url: String,
    api_key_env: Option<String>,
    model: String,
    tool_root: String,
    system_prompt: String,
    tools_json: String,
    message_count: i64,
}

struct RawSummary {
    session_id: String,
    provider_adapter: String,
    model: String,
    owner_generation: i64,
    message_count: i64,
}

struct RecordedPlan {
    status: String,
    call_id: String,
    name: String,
    effect_json: String,
}

struct RawPendingEffect {
    execution_scope: String,
    call_id: String,
    name: String,
    arguments_json: String,
    execution_key: String,
    effect_json: String,
    approval_json: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use domain::{
        EngineId, LineageId, ManifestDigest, OwnerGeneration, PlannedToolCall, PromptManifest,
        SemanticMessage, SessionId, ToolArguments, ToolCallId, ToolEffect, ToolResultStatus,
        ToolTerminal,
    };
    use ports::{EffectLedger, EffectLedgerError, SessionStore, SessionStoreError};
    use protocol::{SessionConfig, TransportKind};
    use serde_json::json;
    use tempfile::tempdir;

    use super::{SqliteEffectLedger, SqliteSessionStore, digest};

    fn config(root: &std::path::Path) -> Result<SessionConfig, Box<dyn std::error::Error>> {
        let tools = vec![json!({
            "type": "function",
            "function": {"name": "read_file", "parameters": {"type": "object"}}
        })];
        let system_prompt = "Frozen prompt.".to_owned();
        Ok(SessionConfig {
            session_id: SessionId::new("session-one")?,
            lineage_id: LineageId::new("session-one")?,
            prompt_manifest: PromptManifest::new(
                1,
                EngineId::new("rust-v1:openai:test-model")?,
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

    fn turn(user: &str, assistant: &str) -> Vec<SemanticMessage> {
        vec![
            SemanticMessage::User { content: user.into(), display_content: None },
            SemanticMessage::Assistant {
                content: assistant.into(),
                reasoning: None,
                provider_replay: None,
            },
        ]
    }

    fn plan(key: &str) -> Result<PlannedToolCall, Box<dyn std::error::Error>> {
        Ok(PlannedToolCall {
            call_id: ToolCallId::new("call-read")?,
            name: "read_file".into(),
            arguments: ToolArguments(BTreeMap::from([("path".into(), json!("README.md"))])),
            execution_key: key.into(),
            effect: ToolEffect::ReadOnly,
            approval: None,
        })
    }

    fn terminal(plan: &PlannedToolCall) -> ToolTerminal {
        ToolTerminal {
            call_id: plan.call_id.clone(),
            name: plan.name.clone(),
            status: ToolResultStatus::Succeeded,
            content: "1|hello\n".into(),
            execution_key: plan.execution_key.clone(),
            effect: plan.effect,
            receipt: None,
        }
    }

    #[test]
    fn committed_turn_survives_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let database = root.path().join("state.db");
        let session_config = config(root.path())?;
        let session_id = session_config.session_id.clone();
        {
            let mut store = SqliteSessionStore::open(&database)?;
            let created = store.create(session_config.clone())?;
            let committed =
                store.append(&session_id, created.owner_generation, &turn("hello", "hi"))?;
            assert_eq!(committed.owner_generation.get(), 2);
            assert_eq!(committed.conversation.len(), 2);
        }

        let mut reopened = SqliteSessionStore::open(&database)?;
        let loaded = reopened.load(&session_id)?;
        assert_eq!(loaded.config, session_config);
        assert_eq!(loaded.owner_generation.get(), 2);
        assert_eq!(loaded.conversation, turn("hello", "hi"));
        Ok(())
    }

    #[test]
    fn stale_generation_cannot_partially_append() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let mut store = SqliteSessionStore::in_memory()?;
        let created = store.create(config(root.path())?)?;
        let session_id = created.config.session_id.clone();
        store.append(&session_id, created.owner_generation, &turn("one", "first"))?;

        let stale = OwnerGeneration::new(1)?;
        let error = store.append(&session_id, stale, &turn("two", "second"));
        assert!(matches!(error, Err(SessionStoreError::Conflict { expected: 1, actual: 2, .. })));
        let loaded = store.load(&session_id)?;
        assert_eq!(loaded.conversation, turn("one", "first"));
        Ok(())
    }

    #[test]
    fn duplicate_create_cannot_replace_frozen_configuration()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let mut store = SqliteSessionStore::in_memory()?;
        let original = config(root.path())?;
        store.create(original.clone())?;
        let mut replacement = original.clone();
        replacement.model = "different-model".into();
        assert!(matches!(store.create(replacement), Err(SessionStoreError::AlreadyExists(_))));
        assert_eq!(store.load(&original.session_id)?.config, original);
        Ok(())
    }

    #[test]
    fn mismatched_prompt_manifest_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let mut store = SqliteSessionStore::in_memory()?;
        let mut invalid = config(root.path())?;
        invalid.system_prompt = "Changed bytes.".into();
        assert!(matches!(store.create(invalid), Err(SessionStoreError::Invalid(_))));
        assert!(store.list()?.is_empty());
        Ok(())
    }

    #[test]
    fn unsupported_lineage_branch_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let mut store = SqliteSessionStore::in_memory()?;
        let mut invalid = config(root.path())?;
        invalid.lineage_id = LineageId::new("different-lineage")?;
        assert!(matches!(store.create(invalid), Err(SessionStoreError::Invalid(_))));
        assert!(store.list()?.is_empty());
        Ok(())
    }

    #[test]
    fn list_reports_generation_and_message_count() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let mut store = SqliteSessionStore::in_memory()?;
        let created = store.create(config(root.path())?)?;
        store.append(&created.config.session_id, created.owner_generation, &turn("hello", "hi"))?;
        let summaries = store.list()?;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].owner_generation.get(), 2);
        assert_eq!(summaries[0].message_count, 2);
        Ok(())
    }

    #[test]
    fn pending_effect_survives_reopen_and_gets_one_terminal()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let database = root.path().join("state.db");
        let planned = plan("scope:call-read")?;
        {
            let mut ledger = SqliteEffectLedger::open(&database)?;
            ledger.record_plans("scope", std::slice::from_ref(&planned))?;
            assert_eq!(ledger.pending()?.len(), 1);
        }

        let mut reopened = SqliteEffectLedger::open(&database)?;
        let pending = reopened.pending()?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].execution_scope, "scope");
        assert_eq!(pending[0].plan, planned);
        let completed = terminal(&planned);
        reopened.record_terminals(std::slice::from_ref(&completed))?;
        assert!(reopened.pending()?.is_empty());
        assert!(matches!(
            reopened.record_terminals(&[completed]),
            Err(EffectLedgerError::AlreadyTerminal(_))
        ));
        Ok(())
    }

    #[test]
    fn duplicate_plan_is_rejected_before_redispatch() -> Result<(), Box<dyn std::error::Error>> {
        let mut ledger = SqliteEffectLedger::in_memory()?;
        let planned = plan("scope:call-read")?;
        ledger.record_plans("scope", std::slice::from_ref(&planned))?;
        assert!(matches!(
            ledger.record_plans("scope", &[planned]),
            Err(EffectLedgerError::AlreadyRecorded(_))
        ));
        assert_eq!(ledger.pending()?.len(), 1);
        Ok(())
    }

    #[test]
    fn terminal_batch_is_atomic_when_one_terminal_mismatches()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut ledger = SqliteEffectLedger::in_memory()?;
        let first = plan("scope:call-one")?;
        let mut second = plan("scope:call-two")?;
        second.call_id = ToolCallId::new("call-two")?;
        ledger.record_plans("scope", &[first.clone(), second.clone()])?;
        let first_terminal = terminal(&first);
        let mut mismatched = terminal(&second);
        mismatched.name = "search_files".into();
        assert!(matches!(
            ledger.record_terminals(&[first_terminal, mismatched]),
            Err(EffectLedgerError::PlanMismatch(_))
        ));
        assert_eq!(ledger.pending()?.len(), 2);
        Ok(())
    }
}
