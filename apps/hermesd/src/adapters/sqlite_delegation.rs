//! SQLite-backed durable background-delegation supervisor state.

use std::path::Path;

use domain::{
    CompletionEventId, DelegationId, DelegationSpec, DelegationState, DelegationTerminal,
    DelegationWorkerId, DeliveryClaimId, FencingToken, OwnerGeneration, SessionId,
};
use ports::{DelegationStore, DelegationStoreError};
use protocol::{DelegationCompletion, DelegationSnapshot};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::sqlite::SqliteSessionStore;

/// SQLite repository for child leases and their completion outbox.
pub struct SqliteDelegationStore {
    connection: Connection,
}

impl SqliteDelegationStore {
    /// Open the shared state database and apply every schema migration.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DelegationStoreError> {
        let store = SqliteSessionStore::open(path).map_err(storage_error)?;
        Ok(Self { connection: store.into_connection() })
    }
}

impl DelegationStore for SqliteDelegationStore {
    fn create(
        &mut self,
        spec: DelegationSpec,
        now_ms: u64,
    ) -> Result<DelegationSnapshot, DelegationStoreError> {
        validate_spec(&spec)?;
        let now = to_i64(now_ms, "creation timestamp")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let collision = transaction
            .query_row(
                "SELECT 1 FROM delegations
                 WHERE delegation_id = ?1 OR completion_event_id = ?2",
                params![spec.delegation_id.as_str(), spec.completion_event_id.as_str()],
                |row| row.get::<_, u8>(0),
            )
            .optional()
            .map_err(storage_error)?
            .is_some();
        if collision {
            return Err(DelegationStoreError::AlreadyExists(spec.delegation_id));
        }
        ensure_session_exists(&transaction, &spec.parent_session_id, "parent")?;
        ensure_session_exists(&transaction, &spec.child_session_id, "child")?;
        transaction
            .execute(
                "INSERT INTO delegations (
                    delegation_id, completion_event_id, parent_session_id, child_session_id,
                    goal, context, state, owner_generation, created_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', 1, ?7, ?7)",
                params![
                    spec.delegation_id.as_str(),
                    spec.completion_event_id.as_str(),
                    spec.parent_session_id.as_str(),
                    spec.child_session_id.as_str(),
                    spec.goal,
                    spec.context,
                    now,
                ],
            )
            .map_err(storage_error)?;
        transaction.commit().map_err(storage_error)?;
        Ok(DelegationSnapshot {
            spec,
            owner_generation: generation(1)?,
            state: DelegationState::Pending,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        })
    }

    fn load(
        &mut self,
        delegation_id: &DelegationId,
    ) -> Result<DelegationSnapshot, DelegationStoreError> {
        snapshot_from_raw(load_raw(&self.connection, delegation_id)?)
    }

    fn pending(&mut self, limit: usize) -> Result<Vec<DelegationSnapshot>, DelegationStoreError> {
        if limit == 0 {
            return Err(DelegationStoreError::Invalid(
                "pending delegation limit must be greater than zero".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT delegation_id FROM delegations
                 WHERE state = 'pending'
                 ORDER BY created_at_ms ASC, delegation_id ASC LIMIT ?1",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map(params![usize_to_i64(limit, "pending limit")?], |row| {
                row.get::<_, String>(0)
            })
            .map_err(storage_error)?;
        let ids = rows.map(|row| row.map_err(storage_error)).collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        ids.into_iter()
            .map(|id| {
                let id = DelegationId::new(id)
                    .map_err(|error| DelegationStoreError::Invalid(error.to_string()))?;
                snapshot_from_raw(load_raw(&self.connection, &id)?)
            })
            .collect()
    }

    fn claim(
        &mut self,
        delegation_id: &DelegationId,
        expected_generation: OwnerGeneration,
        worker_id: DelegationWorkerId,
        now_ms: u64,
        lease_expires_at_ms: u64,
    ) -> Result<DelegationSnapshot, DelegationStoreError> {
        validate_lease(now_ms, lease_expires_at_ms)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let raw = load_raw(&transaction, delegation_id)?;
        ensure_generation(&raw, expected_generation)?;
        ensure_monotonic_timestamp(&raw, now_ms, "claim timestamp")?;
        if raw.state != "pending" {
            return Err(DelegationStoreError::NotClaimable {
                delegation_id: delegation_id.clone(),
                state: raw.state,
            });
        }
        let next_generation = increment(raw.owner_generation, "owner generation")?;
        let next_fence = increment(raw.fencing_token, "fencing token")?;
        let updated = transaction
            .execute(
                "UPDATE delegations
                 SET state = 'running', owner_generation = ?1, worker_id = ?2,
                     fencing_token = ?3, lease_expires_at_ms = ?4, updated_at_ms = ?5
                 WHERE delegation_id = ?6 AND state = 'pending' AND owner_generation = ?7",
                params![
                    next_generation,
                    worker_id.as_str(),
                    next_fence,
                    to_i64(lease_expires_at_ms, "lease deadline")?,
                    to_i64(now_ms, "claim timestamp")?,
                    delegation_id.as_str(),
                    to_i64(expected_generation.get(), "expected generation")?,
                ],
            )
            .map_err(storage_error)?;
        if updated != 1 {
            return Err(DelegationStoreError::GenerationConflict {
                delegation_id: delegation_id.clone(),
                expected: expected_generation.get(),
                actual: u64_from_i64(raw.owner_generation, "owner generation")?,
            });
        }
        transaction.commit().map_err(storage_error)?;
        snapshot_from_raw(load_raw(&self.connection, delegation_id)?)
    }

    fn heartbeat(
        &mut self,
        delegation_id: &DelegationId,
        expected_generation: OwnerGeneration,
        fencing_token: FencingToken,
        now_ms: u64,
        lease_expires_at_ms: u64,
    ) -> Result<DelegationSnapshot, DelegationStoreError> {
        validate_lease(now_ms, lease_expires_at_ms)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let raw = load_raw(&transaction, delegation_id)?;
        ensure_generation(&raw, expected_generation)?;
        ensure_running_fence(&raw, fencing_token)?;
        ensure_monotonic_timestamp(&raw, now_ms, "heartbeat timestamp")?;
        let current_deadline = raw.lease_expires_at_ms.ok_or_else(|| {
            DelegationStoreError::Invalid("running delegation has no lease deadline".into())
        })?;
        if to_i64(lease_expires_at_ms, "lease deadline")? <= current_deadline {
            return Err(DelegationStoreError::Invalid(
                "heartbeat must extend the current delegation lease".into(),
            ));
        }
        let next_generation = increment(raw.owner_generation, "owner generation")?;
        let updated = transaction
            .execute(
                "UPDATE delegations
                 SET owner_generation = ?1, lease_expires_at_ms = ?2, updated_at_ms = ?3
                 WHERE delegation_id = ?4 AND state = 'running'
                   AND owner_generation = ?5 AND fencing_token = ?6",
                params![
                    next_generation,
                    to_i64(lease_expires_at_ms, "lease deadline")?,
                    to_i64(now_ms, "heartbeat timestamp")?,
                    delegation_id.as_str(),
                    to_i64(expected_generation.get(), "expected generation")?,
                    to_i64(fencing_token.get(), "fencing token")?,
                ],
            )
            .map_err(storage_error)?;
        if updated != 1 {
            return Err(DelegationStoreError::GenerationConflict {
                delegation_id: delegation_id.clone(),
                expected: expected_generation.get(),
                actual: u64_from_i64(raw.owner_generation, "owner generation")?,
            });
        }
        transaction.commit().map_err(storage_error)?;
        snapshot_from_raw(load_raw(&self.connection, delegation_id)?)
    }

    fn finish(
        &mut self,
        delegation_id: &DelegationId,
        expected_generation: OwnerGeneration,
        fencing_token: FencingToken,
        outcome: DelegationTerminal,
        completed_at_ms: u64,
    ) -> Result<DelegationCompletion, DelegationStoreError> {
        validate_terminal(&outcome)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let raw = load_raw(&transaction, delegation_id)?;
        ensure_generation(&raw, expected_generation)?;
        ensure_running_fence(&raw, fencing_token)?;
        ensure_monotonic_timestamp(&raw, completed_at_ms, "completion timestamp")?;
        let completion = completion_from_raw(&raw, outcome.clone(), completed_at_ms)?;
        let next_generation = increment(raw.owner_generation, "owner generation")?;
        let updated = transaction
            .execute(
                "UPDATE delegations
                 SET state = ?1, owner_generation = ?2, lease_expires_at_ms = NULL,
                     terminal_json = ?3, updated_at_ms = ?4
                 WHERE delegation_id = ?5 AND state = 'running'
                   AND owner_generation = ?6 AND fencing_token = ?7",
                params![
                    outcome.status_name(),
                    next_generation,
                    serde_json::to_string(&outcome).map_err(storage_error)?,
                    to_i64(completed_at_ms, "completion timestamp")?,
                    delegation_id.as_str(),
                    to_i64(expected_generation.get(), "expected generation")?,
                    to_i64(fencing_token.get(), "fencing token")?,
                ],
            )
            .map_err(storage_error)?;
        if updated != 1 {
            return Err(DelegationStoreError::GenerationConflict {
                delegation_id: delegation_id.clone(),
                expected: expected_generation.get(),
                actual: u64_from_i64(raw.owner_generation, "owner generation")?,
            });
        }
        insert_completion(&transaction, &completion)?;
        transaction.commit().map_err(storage_error)?;
        Ok(completion)
    }

    fn reconcile_expired(
        &mut self,
        now_ms: u64,
    ) -> Result<Vec<DelegationCompletion>, DelegationStoreError> {
        let now = to_i64(now_ms, "reconciliation timestamp")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let mut statement = transaction
            .prepare(
                "SELECT delegation_id, completion_event_id, parent_session_id,
                        child_session_id, goal, context, state, owner_generation,
                        worker_id, fencing_token, lease_expires_at_ms, terminal_json,
                        created_at_ms, updated_at_ms
                 FROM delegations
                 WHERE state = 'running' AND lease_expires_at_ms <= ?1
                 ORDER BY lease_expires_at_ms ASC, delegation_id ASC",
            )
            .map_err(storage_error)?;
        let rows = statement.query_map(params![now], raw_from_row).map_err(storage_error)?;
        let expired = rows.map(|row| row.map_err(storage_error)).collect::<Result<Vec<_>, _>>()?;
        drop(statement);

        let mut completions = Vec::with_capacity(expired.len());
        for raw in expired {
            ensure_monotonic_timestamp(&raw, now_ms, "reconciliation timestamp")?;
            let deadline = raw.lease_expires_at_ms.ok_or_else(|| {
                DelegationStoreError::Invalid(
                    "running delegation has no durable lease deadline".into(),
                )
            })?;
            let outcome = DelegationTerminal::OutcomeUnknown {
                reason: format!(
                    "worker lease expired at {deadline}; no terminal outcome was durably recorded"
                ),
            };
            let completion = completion_from_raw(&raw, outcome.clone(), now_ms)?;
            let next_generation = increment(raw.owner_generation, "owner generation")?;
            let updated = transaction
                .execute(
                    "UPDATE delegations
                     SET state = 'outcome_unknown', owner_generation = ?1,
                         lease_expires_at_ms = NULL, terminal_json = ?2, updated_at_ms = ?3
                     WHERE delegation_id = ?4 AND state = 'running'
                       AND owner_generation = ?5 AND fencing_token = ?6
                       AND lease_expires_at_ms <= ?3",
                    params![
                        next_generation,
                        serde_json::to_string(&outcome).map_err(storage_error)?,
                        now,
                        raw.delegation_id,
                        raw.owner_generation,
                        raw.fencing_token,
                    ],
                )
                .map_err(storage_error)?;
            if updated != 1 {
                return Err(DelegationStoreError::GenerationConflict {
                    delegation_id: completion.delegation_id.clone(),
                    expected: u64_from_i64(raw.owner_generation, "owner generation")?,
                    actual: u64_from_i64(raw.owner_generation, "owner generation")?,
                });
            }
            insert_completion(&transaction, &completion)?;
            completions.push(completion);
        }
        transaction.commit().map_err(storage_error)?;
        Ok(completions)
    }

    fn available_completions(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<DelegationCompletion>, DelegationStoreError> {
        if limit == 0 {
            return Err(DelegationStoreError::Invalid(
                "completion limit must be greater than zero".into(),
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT payload_json FROM delegation_completions
                 WHERE delivery_state = 'pending'
                   AND (delivery_claim_id IS NULL OR delivery_claim_expires_at_ms <= ?1)
                 ORDER BY created_at_ms ASC, event_id ASC LIMIT ?2",
            )
            .map_err(storage_error)?;
        let rows = statement
            .query_map(
                params![
                    to_i64(now_ms, "completion availability timestamp")?,
                    usize_to_i64(limit, "completion limit")?,
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(storage_error)?;
        rows.map(|row| {
            let encoded = row.map_err(storage_error)?;
            serde_json::from_str(&encoded).map_err(|error| {
                DelegationStoreError::Invalid(format!(
                    "stored delegation completion is invalid: {error}"
                ))
            })
        })
        .collect()
    }

    fn claim_completion(
        &mut self,
        event_id: &CompletionEventId,
        claim_id: DeliveryClaimId,
        now_ms: u64,
        claim_expires_at_ms: u64,
    ) -> Result<Option<DelegationCompletion>, DelegationStoreError> {
        validate_claim(now_ms, claim_expires_at_ms)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let updated = transaction
            .execute(
                "UPDATE delegation_completions
                 SET delivery_claim_id = ?1, delivery_claim_expires_at_ms = ?2,
                     delivery_attempts = delivery_attempts + 1
                 WHERE event_id = ?3 AND delivery_state = 'pending'
                   AND (delivery_claim_id IS NULL OR delivery_claim_expires_at_ms <= ?4)",
                params![
                    claim_id.as_str(),
                    to_i64(claim_expires_at_ms, "delivery claim deadline")?,
                    event_id.as_str(),
                    to_i64(now_ms, "delivery claim timestamp")?,
                ],
            )
            .map_err(storage_error)?;
        if updated != 1 {
            transaction.commit().map_err(storage_error)?;
            return Ok(None);
        }
        let encoded = transaction
            .query_row(
                "SELECT payload_json FROM delegation_completions WHERE event_id = ?1",
                params![event_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .map_err(storage_error)?;
        let completion = serde_json::from_str(&encoded).map_err(|error| {
            DelegationStoreError::Invalid(format!(
                "stored delegation completion is invalid: {error}"
            ))
        })?;
        transaction.commit().map_err(storage_error)?;
        Ok(Some(completion))
    }

    fn acknowledge_completion(
        &mut self,
        event_id: &CompletionEventId,
        claim_id: &DeliveryClaimId,
        delivered_at_ms: u64,
    ) -> Result<bool, DelegationStoreError> {
        let delivered_at = to_i64(delivered_at_ms, "delivery timestamp")?;
        let updated = self
            .connection
            .execute(
                "UPDATE delegation_completions
                 SET delivery_state = 'delivered', delivered_at_ms = ?1,
                     delivery_claim_id = NULL, delivery_claim_expires_at_ms = NULL
                 WHERE event_id = ?2 AND delivery_state = 'pending'
                   AND delivery_claim_id = ?3 AND created_at_ms <= ?1",
                params![delivered_at, event_id.as_str(), claim_id.as_str()],
            )
            .map_err(storage_error)?;
        Ok(updated == 1)
    }

    fn release_completion(
        &mut self,
        event_id: &CompletionEventId,
        claim_id: &DeliveryClaimId,
    ) -> Result<bool, DelegationStoreError> {
        let updated = self
            .connection
            .execute(
                "UPDATE delegation_completions
                 SET delivery_claim_id = NULL, delivery_claim_expires_at_ms = NULL
                 WHERE event_id = ?1 AND delivery_state = 'pending'
                   AND delivery_claim_id = ?2",
                params![event_id.as_str(), claim_id.as_str()],
            )
            .map_err(storage_error)?;
        Ok(updated == 1)
    }
}

fn load_raw(
    connection: &Connection,
    delegation_id: &DelegationId,
) -> Result<RawDelegation, DelegationStoreError> {
    connection
        .query_row(
            "SELECT delegation_id, completion_event_id, parent_session_id,
                    child_session_id, goal, context, state, owner_generation,
                    worker_id, fencing_token, lease_expires_at_ms, terminal_json,
                    created_at_ms, updated_at_ms
             FROM delegations WHERE delegation_id = ?1",
            params![delegation_id.as_str()],
            raw_from_row,
        )
        .optional()
        .map_err(storage_error)?
        .ok_or_else(|| DelegationStoreError::NotFound(delegation_id.clone()))
}

fn raw_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawDelegation> {
    Ok(RawDelegation {
        delegation_id: row.get(0)?,
        completion_event_id: row.get(1)?,
        parent_session_id: row.get(2)?,
        child_session_id: row.get(3)?,
        goal: row.get(4)?,
        context: row.get(5)?,
        state: row.get(6)?,
        owner_generation: row.get(7)?,
        worker_id: row.get(8)?,
        fencing_token: row.get(9)?,
        lease_expires_at_ms: row.get(10)?,
        terminal_json: row.get(11)?,
        created_at_ms: row.get(12)?,
        updated_at_ms: row.get(13)?,
    })
}

fn snapshot_from_raw(raw: RawDelegation) -> Result<DelegationSnapshot, DelegationStoreError> {
    let spec = spec_from_raw(&raw)?;
    let state = match raw.state.as_str() {
        "pending" => DelegationState::Pending,
        "running" => DelegationState::Running {
            worker_id: DelegationWorkerId::new(raw.worker_id.ok_or_else(|| {
                DelegationStoreError::Invalid("running delegation has no worker id".into())
            })?)
            .map_err(|error| DelegationStoreError::Invalid(error.to_string()))?,
            fencing_token: fencing_token(raw.fencing_token)?,
            lease_expires_at_ms: u64_from_i64(
                raw.lease_expires_at_ms.ok_or_else(|| {
                    DelegationStoreError::Invalid("running delegation has no lease deadline".into())
                })?,
                "lease deadline",
            )?,
        },
        "completed" | "failed" | "cancelled" | "outcome_unknown" => {
            let encoded = raw.terminal_json.as_deref().ok_or_else(|| {
                DelegationStoreError::Invalid("terminal delegation has no outcome".into())
            })?;
            let outcome = serde_json::from_str::<DelegationTerminal>(encoded).map_err(|error| {
                DelegationStoreError::Invalid(format!(
                    "stored delegation terminal is invalid: {error}"
                ))
            })?;
            if outcome.status_name() != raw.state {
                return Err(DelegationStoreError::Invalid(format!(
                    "delegation state {} disagrees with terminal {}",
                    raw.state,
                    outcome.status_name()
                )));
            }
            DelegationState::Terminal {
                outcome,
                completed_at_ms: u64_from_i64(raw.updated_at_ms, "completion timestamp")?,
            }
        }
        other => {
            return Err(DelegationStoreError::Invalid(format!(
                "unknown delegation state {other:?}"
            )));
        }
    };
    Ok(DelegationSnapshot {
        spec,
        owner_generation: generation(raw.owner_generation)?,
        state,
        created_at_ms: u64_from_i64(raw.created_at_ms, "creation timestamp")?,
        updated_at_ms: u64_from_i64(raw.updated_at_ms, "update timestamp")?,
    })
}

fn completion_from_raw(
    raw: &RawDelegation,
    outcome: DelegationTerminal,
    completed_at_ms: u64,
) -> Result<DelegationCompletion, DelegationStoreError> {
    let spec = spec_from_raw(raw)?;
    Ok(DelegationCompletion {
        event_id: spec.completion_event_id.clone(),
        delegation_id: spec.delegation_id.clone(),
        spec,
        outcome,
        completed_at_ms,
    })
}

fn spec_from_raw(raw: &RawDelegation) -> Result<DelegationSpec, DelegationStoreError> {
    let spec = DelegationSpec {
        delegation_id: DelegationId::new(raw.delegation_id.clone())
            .map_err(|error| DelegationStoreError::Invalid(error.to_string()))?,
        completion_event_id: CompletionEventId::new(raw.completion_event_id.clone())
            .map_err(|error| DelegationStoreError::Invalid(error.to_string()))?,
        parent_session_id: SessionId::new(raw.parent_session_id.clone())
            .map_err(|error| DelegationStoreError::Invalid(error.to_string()))?,
        child_session_id: SessionId::new(raw.child_session_id.clone())
            .map_err(|error| DelegationStoreError::Invalid(error.to_string()))?,
        goal: raw.goal.clone(),
        context: raw.context.clone(),
    };
    validate_spec(&spec)?;
    Ok(spec)
}

fn insert_completion(
    connection: &Connection,
    completion: &DelegationCompletion,
) -> Result<(), DelegationStoreError> {
    connection
        .execute(
            "INSERT INTO delegation_completions (
                event_id, delegation_id, payload_json, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                completion.event_id.as_str(),
                completion.delegation_id.as_str(),
                serde_json::to_string(completion).map_err(storage_error)?,
                to_i64(completion.completed_at_ms, "completion timestamp")?,
            ],
        )
        .map_err(storage_error)?;
    Ok(())
}

fn ensure_session_exists(
    connection: &Connection,
    session_id: &SessionId,
    relation: &str,
) -> Result<(), DelegationStoreError> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM sessions WHERE session_id = ?1",
            params![session_id.as_str()],
            |row| row.get::<_, u8>(0),
        )
        .optional()
        .map_err(storage_error)?
        .is_some();
    if !exists {
        return Err(DelegationStoreError::Invalid(format!(
            "{relation} session does not exist: {session_id}"
        )));
    }
    Ok(())
}

fn ensure_generation(
    raw: &RawDelegation,
    expected: OwnerGeneration,
) -> Result<(), DelegationStoreError> {
    let actual = u64_from_i64(raw.owner_generation, "owner generation")?;
    if actual != expected.get() {
        return Err(DelegationStoreError::GenerationConflict {
            delegation_id: DelegationId::new(raw.delegation_id.clone())
                .map_err(|error| DelegationStoreError::Invalid(error.to_string()))?,
            expected: expected.get(),
            actual,
        });
    }
    Ok(())
}

fn ensure_running_fence(
    raw: &RawDelegation,
    expected: FencingToken,
) -> Result<(), DelegationStoreError> {
    let actual = (raw.fencing_token > 0)
        .then(|| u64_from_i64(raw.fencing_token, "fencing token"))
        .transpose()?;
    if raw.state != "running" || actual != Some(expected.get()) {
        return Err(DelegationStoreError::FencingConflict {
            delegation_id: DelegationId::new(raw.delegation_id.clone())
                .map_err(|error| DelegationStoreError::Invalid(error.to_string()))?,
            expected: expected.get(),
            actual,
        });
    }
    Ok(())
}

fn ensure_monotonic_timestamp(
    raw: &RawDelegation,
    timestamp_ms: u64,
    name: &str,
) -> Result<(), DelegationStoreError> {
    let previous = u64_from_i64(raw.updated_at_ms, "previous update timestamp")?;
    if timestamp_ms < previous {
        return Err(DelegationStoreError::Invalid(format!(
            "{name} {timestamp_ms} precedes the previous update at {previous}"
        )));
    }
    Ok(())
}

fn validate_spec(spec: &DelegationSpec) -> Result<(), DelegationStoreError> {
    if spec.parent_session_id == spec.child_session_id {
        return Err(DelegationStoreError::Invalid(
            "delegation parent and child sessions must be distinct".into(),
        ));
    }
    if spec.goal.is_empty() || spec.goal.trim() != spec.goal {
        return Err(DelegationStoreError::Invalid(
            "delegation goal must be non-empty and have no surrounding whitespace".into(),
        ));
    }
    Ok(())
}

fn validate_terminal(outcome: &DelegationTerminal) -> Result<(), DelegationStoreError> {
    let detail = match outcome {
        DelegationTerminal::Completed { .. } => return Ok(()),
        DelegationTerminal::Failed { error } => error,
        DelegationTerminal::Cancelled { reason }
        | DelegationTerminal::OutcomeUnknown { reason } => reason,
    };
    if detail.trim().is_empty() {
        return Err(DelegationStoreError::Invalid(
            "non-completed delegation terminal detail must be non-empty".into(),
        ));
    }
    Ok(())
}

fn validate_lease(now_ms: u64, deadline_ms: u64) -> Result<(), DelegationStoreError> {
    if deadline_ms <= now_ms {
        return Err(DelegationStoreError::Invalid(
            "delegation lease deadline must be later than the mutation timestamp".into(),
        ));
    }
    Ok(())
}

fn validate_claim(now_ms: u64, deadline_ms: u64) -> Result<(), DelegationStoreError> {
    if deadline_ms <= now_ms {
        return Err(DelegationStoreError::Invalid(
            "delivery claim deadline must be later than the claim timestamp".into(),
        ));
    }
    Ok(())
}

fn storage_error(error: impl std::fmt::Display) -> DelegationStoreError {
    DelegationStoreError::Storage(error.to_string())
}

fn to_i64(value: u64, name: &str) -> Result<i64, DelegationStoreError> {
    i64::try_from(value)
        .map_err(|_| DelegationStoreError::Invalid(format!("{name} exceeds SQLite integer range")))
}

fn usize_to_i64(value: usize, name: &str) -> Result<i64, DelegationStoreError> {
    i64::try_from(value)
        .map_err(|_| DelegationStoreError::Invalid(format!("{name} exceeds SQLite integer range")))
}

fn u64_from_i64(value: i64, name: &str) -> Result<u64, DelegationStoreError> {
    u64::try_from(value).map_err(|_| DelegationStoreError::Invalid(format!("{name} is negative")))
}

fn increment(value: i64, name: &str) -> Result<i64, DelegationStoreError> {
    value.checked_add(1).ok_or_else(|| DelegationStoreError::Invalid(format!("{name} overflowed")))
}

fn generation(value: i64) -> Result<OwnerGeneration, DelegationStoreError> {
    OwnerGeneration::new(u64_from_i64(value, "owner generation")?)
        .map_err(|error| DelegationStoreError::Invalid(error.to_string()))
}

fn fencing_token(value: i64) -> Result<FencingToken, DelegationStoreError> {
    FencingToken::new(u64_from_i64(value, "fencing token")?)
        .map_err(|error| DelegationStoreError::Invalid(error.to_string()))
}

struct RawDelegation {
    delegation_id: String,
    completion_event_id: String,
    parent_session_id: String,
    child_session_id: String,
    goal: String,
    context: Option<String>,
    state: String,
    owner_generation: i64,
    worker_id: Option<String>,
    fencing_token: i64,
    lease_expires_at_ms: Option<i64>,
    terminal_json: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use domain::{
        CompletionEventId, DelegationId, DelegationSpec, DelegationState, DelegationTerminal,
        DelegationWorkerId, DeliveryClaimId, EngineId, FencingToken, LineageId, ManifestDigest,
        PromptManifest, SessionId,
    };
    use ports::{DelegationStore, DelegationStoreError, SessionStore};
    use protocol::{SessionConfig, TransportKind};
    use rusqlite::Connection;
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{SqliteDelegationStore, SqliteSessionStore};

    #[test]
    fn worker_and_delivery_mutations_are_generation_and_claim_fenced()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let mut store = SqliteDelegationStore::open(&fixture.database)?;
        let created = store.create(fixture.spec.clone(), 100)?;
        assert_eq!(created.owner_generation.get(), 1);
        assert_eq!(created.state, DelegationState::Pending);
        assert_eq!(store.pending(10)?, vec![created.clone()]);

        let claimed = store.claim(
            &fixture.spec.delegation_id,
            created.owner_generation,
            DelegationWorkerId::new("worker-one")?,
            110,
            210,
        )?;
        let fence = match &claimed.state {
            DelegationState::Running { fencing_token, lease_expires_at_ms, .. } => {
                assert_eq!(*lease_expires_at_ms, 210);
                *fencing_token
            }
            other => return Err(format!("expected running state, got {other:?}").into()),
        };
        assert_eq!(fence.get(), 1);
        assert_eq!(claimed.owner_generation.get(), 2);
        assert!(matches!(
            store.claim(
                &fixture.spec.delegation_id,
                created.owner_generation,
                DelegationWorkerId::new("worker-two")?,
                120,
                220,
            ),
            Err(DelegationStoreError::GenerationConflict { expected: 1, actual: 2, .. })
        ));

        let heartbeat = store.heartbeat(
            &fixture.spec.delegation_id,
            claimed.owner_generation,
            fence,
            150,
            300,
        )?;
        assert_eq!(heartbeat.owner_generation.get(), 3);
        assert!(matches!(
            store.heartbeat(
                &fixture.spec.delegation_id,
                heartbeat.owner_generation,
                FencingToken::new(2)?,
                160,
                310,
            ),
            Err(DelegationStoreError::FencingConflict { expected: 2, actual: Some(1), .. })
        ));

        let outcome = DelegationTerminal::Completed { summary: "child summary".into() };
        let completion = store.finish(
            &fixture.spec.delegation_id,
            heartbeat.owner_generation,
            fence,
            outcome.clone(),
            180,
        )?;
        assert_eq!(completion.event_id, fixture.spec.completion_event_id);
        assert_eq!(completion.outcome, outcome);
        assert!(store.load(&fixture.spec.delegation_id)?.state.is_terminal());
        assert!(matches!(
            store.finish(
                &fixture.spec.delegation_id,
                heartbeat.owner_generation,
                fence,
                DelegationTerminal::Failed { error: "late".into() },
                181,
            ),
            Err(DelegationStoreError::GenerationConflict { .. })
        ));

        assert_eq!(store.available_completions(180, 10)?, vec![completion.clone()]);
        let first_claim = DeliveryClaimId::new("delivery-one")?;
        assert_eq!(
            store.claim_completion(&completion.event_id, first_claim.clone(), 180, 280,)?,
            Some(completion.clone())
        );
        assert!(store.available_completions(200, 10)?.is_empty());
        assert!(
            store
                .claim_completion(
                    &completion.event_id,
                    DeliveryClaimId::new("delivery-two")?,
                    200,
                    300,
                )?
                .is_none()
        );
        assert!(
            !store.release_completion(
                &completion.event_id,
                &DeliveryClaimId::new("not-the-owner")?
            )?
        );
        assert!(store.release_completion(&completion.event_id, &first_claim)?);

        let second_claim = DeliveryClaimId::new("delivery-two")?;
        assert!(
            store
                .claim_completion(&completion.event_id, second_claim.clone(), 220, 320,)?
                .is_some()
        );
        assert!(!store.acknowledge_completion(
            &completion.event_id,
            &DeliveryClaimId::new("not-the-owner")?,
            230,
        )?);
        assert!(store.acknowledge_completion(&completion.event_id, &second_claim, 230)?);
        assert!(store.available_completions(500, 10)?.is_empty());

        drop(store);
        let mut reopened = SqliteDelegationStore::open(&fixture.database)?;
        assert!(reopened.load(&fixture.spec.delegation_id)?.state.is_terminal());
        assert!(reopened.available_completions(500, 10)?.is_empty());
        Ok(())
    }

    #[test]
    fn expired_worker_becomes_unknown_without_replaying_pending_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let mut store = SqliteDelegationStore::open(&fixture.database)?;
        let pending = store.create(fixture.spec.clone(), 100)?;
        let claimed = store.claim(
            &fixture.spec.delegation_id,
            pending.owner_generation,
            DelegationWorkerId::new("worker-one")?,
            110,
            150,
        )?;
        let fence = match claimed.state {
            DelegationState::Running { fencing_token, .. } => fencing_token,
            other => return Err(format!("expected running state, got {other:?}").into()),
        };

        let second = fixture.second_spec()?;
        store.create(second.clone(), 120)?;
        assert!(store.reconcile_expired(149)?.is_empty());
        let reconciled = store.reconcile_expired(150)?;
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].delegation_id, fixture.spec.delegation_id);
        assert!(matches!(&reconciled[0].outcome, DelegationTerminal::OutcomeUnknown { .. }));
        let snapshot = store.load(&fixture.spec.delegation_id)?;
        assert_eq!(snapshot.owner_generation.get(), 3);
        assert!(snapshot.state.is_terminal());
        assert_eq!(store.pending(10)?.len(), 1);
        assert_eq!(store.pending(10)?[0].spec, second);

        assert!(matches!(
            store.finish(
                &fixture.spec.delegation_id,
                snapshot.owner_generation,
                fence,
                DelegationTerminal::Completed { summary: "too late".into() },
                151,
            ),
            Err(DelegationStoreError::FencingConflict { expected: 1, actual: Some(1), .. })
        ));
        assert_eq!(store.available_completions(150, 10)?, reconciled);
        Ok(())
    }

    #[test]
    fn create_requires_distinct_existing_parent_and_child_sessions()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = Fixture::new()?;
        let mut store = SqliteDelegationStore::open(&fixture.database)?;
        let mut same = fixture.spec.clone();
        same.child_session_id = same.parent_session_id.clone();
        assert!(matches!(store.create(same, 100), Err(DelegationStoreError::Invalid(_))));

        let mut missing = fixture.spec.clone();
        missing.delegation_id = DelegationId::new("delegation-missing")?;
        missing.completion_event_id = CompletionEventId::new("completion-missing")?;
        missing.child_session_id = SessionId::new("missing-child")?;
        assert!(matches!(store.create(missing, 100), Err(DelegationStoreError::Invalid(_))));
        Ok(())
    }

    #[test]
    fn version_two_state_database_migrates_without_rebuilding_sessions()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let database = directory.path().join("state.db");
        let mut sessions = SqliteSessionStore::open(&database)?;
        sessions.create(session_config("existing", directory.path())?)?;
        drop(sessions);

        let connection = Connection::open(&database)?;
        connection.execute_batch(
            "DROP TABLE delegation_completions;
             DROP TABLE delegations;
             PRAGMA user_version = 2;",
        )?;
        drop(connection);

        let _delegations = SqliteDelegationStore::open(&database)?;
        let connection = Connection::open(&database)?;
        let version =
            connection.query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))?;
        let table_count = connection.query_row(
            "SELECT count(*) FROM sqlite_master
             WHERE type = 'table' AND name IN ('delegations', 'delegation_completions')",
            [],
            |row| row.get::<_, u32>(0),
        )?;
        assert_eq!(version, 3);
        assert_eq!(table_count, 2);

        let mut sessions = SqliteSessionStore::open(&database)?;
        assert_eq!(sessions.load(&SessionId::new("existing")?)?.config.model, "test-model");
        Ok(())
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        database: std::path::PathBuf,
        spec: DelegationSpec,
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            let directory = tempdir()?;
            let database = directory.path().join("state.db");
            let mut sessions = SqliteSessionStore::open(&database)?;
            sessions.create(session_config("parent", directory.path())?)?;
            sessions.create(session_config("child", directory.path())?)?;
            sessions.create(session_config("child-two", directory.path())?)?;
            Ok(Self {
                _directory: directory,
                database,
                spec: DelegationSpec {
                    delegation_id: DelegationId::new("delegation-one")?,
                    completion_event_id: CompletionEventId::new("completion-one")?,
                    parent_session_id: SessionId::new("parent")?,
                    child_session_id: SessionId::new("child")?,
                    goal: "Inspect the workspace".into(),
                    context: Some("Focus on README.md".into()),
                },
            })
        }

        fn second_spec(&self) -> Result<DelegationSpec, Box<dyn std::error::Error>> {
            Ok(DelegationSpec {
                delegation_id: DelegationId::new("delegation-two")?,
                completion_event_id: CompletionEventId::new("completion-two")?,
                parent_session_id: self.spec.parent_session_id.clone(),
                child_session_id: SessionId::new("child-two")?,
                goal: "Inspect another file".into(),
                context: None,
            })
        }
    }

    fn session_config(
        id: &str,
        root: &std::path::Path,
    ) -> Result<SessionConfig, Box<dyn std::error::Error>> {
        let tools: Vec<Value> = vec![json!({
            "type": "function",
            "function": {"name": "read_file", "parameters": {"type": "object"}}
        })];
        let system_prompt = format!("Frozen prompt for {id}.");
        Ok(SessionConfig {
            session_id: SessionId::new(id)?,
            lineage_id: LineageId::new(id)?,
            prompt_manifest: PromptManifest::new(
                1,
                EngineId::new(format!("rust-v1:test:{id}"))?,
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
