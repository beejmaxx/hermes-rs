//! Long-lived execution of durable, fenced leaf delegations.

use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use domain::{
    DelegationAuthority, DelegationState, DelegationTerminal, DelegationWorkerId, FencingToken,
    OwnerGeneration,
};
use ports::{DelegationStore, SessionStore};
use protocol::{ContractOutcome, DelegationSnapshot};
use tokio::task::{JoinHandle, JoinSet};

use super::chat::{LiveSettings, completed_response, execute_turn};
use crate::adapters::{SqliteDelegationStore, SqliteSessionStore};

const SUPERVISOR_INTERVAL: Duration = Duration::from_millis(50);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const LEASE_DURATION_MS: u64 = 90_000;
const PENDING_SCAN_LIMIT: usize = 32;
const CHILD_PROMPT: &str =
    "Complete the assigned task and return a concise result for the parent agent.";

/// Process-owned durable delegation supervisor, cancelled with its gateway.
pub(super) struct BackgroundSupervisor {
    task: JoinHandle<()>,
}

impl BackgroundSupervisor {
    /// Spawn the supervisor loop for one exclusively owned state database.
    pub(super) fn spawn(state: PathBuf) -> Self {
        Self { task: tokio::spawn(async move { supervise(state).await }) }
    }
}

impl Drop for BackgroundSupervisor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn supervise(state: PathBuf) {
    let worker_id = match fresh_worker_id() {
        Ok(worker_id) => worker_id,
        Err(_) => return,
    };
    let mut jobs = JoinSet::new();
    loop {
        while jobs.try_join_next().is_some() {}
        let now_ms = match unix_time_ms() {
            Ok(now_ms) => now_ms,
            Err(_) => return,
        };
        let pending = match SqliteDelegationStore::open(&state).and_then(|mut store| {
            store.reconcile_expired(now_ms).and_then(|_| store.pending(PENDING_SCAN_LIMIT))
        }) {
            Ok(pending) => pending,
            Err(_) => {
                tokio::time::sleep(SUPERVISOR_INTERVAL).await;
                continue;
            }
        };
        for delegation in pending {
            let claimed = SqliteDelegationStore::open(&state).and_then(|mut store| {
                store.claim(
                    &delegation.spec.delegation_id,
                    delegation.owner_generation,
                    worker_id.clone(),
                    now_ms,
                    lease_deadline(now_ms),
                )
            });
            if let Ok(claimed) = claimed {
                let worker_state = state.clone();
                jobs.spawn(async move {
                    let _ = execute_claim(worker_state, claimed).await;
                });
            }
        }
        tokio::time::sleep(SUPERVISOR_INTERVAL).await;
    }
}

async fn execute_claim(state: PathBuf, claim: DelegationSnapshot) -> anyhow::Result<()> {
    let (fencing_token, mut delegation_generation) = running_authority(&claim)?;
    let mut sessions = SqliteSessionStore::open(&state)?;
    let child = sessions.load(&claim.spec.child_session_id)?;
    if !child.conversation.is_empty() {
        anyhow::bail!("pending delegation child session was not empty");
    }
    let child_generation = child.owner_generation;
    let settings = LiveSettings::from_snapshot(&child)?;
    let scope = format!(
        "delegation:{}:generation:{}",
        claim.spec.delegation_id,
        delegation_generation.get()
    );
    let child_session_id = claim.spec.child_session_id.clone();
    let mut execution = Box::pin(execute_turn(
        &settings,
        child.conversation,
        CHILD_PROMPT,
        &scope,
        &state,
        Some(&child_session_id),
    ));
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.tick().await;
    let outcome = loop {
        tokio::select! {
            outcome = &mut execution => break outcome,
            _ = heartbeat.tick() => {
                let now_ms = unix_time_ms()?;
                let snapshot = SqliteDelegationStore::open(&state)?.heartbeat(
                    &claim.spec.delegation_id,
                    delegation_generation,
                    fencing_token,
                    now_ms,
                    lease_deadline(now_ms),
                )?;
                delegation_generation = snapshot.owner_generation;
            }
        }
    };
    finish_execution(
        &state,
        &claim,
        delegation_generation,
        fencing_token,
        child_generation,
        outcome,
    )
}

fn finish_execution(
    state: &std::path::Path,
    claim: &DelegationSnapshot,
    delegation_generation: OwnerGeneration,
    fencing_token: FencingToken,
    child_generation: OwnerGeneration,
    outcome: anyhow::Result<ContractOutcome>,
) -> anyhow::Result<()> {
    let completed_at_ms = unix_time_ms()?;
    let mut store = SqliteDelegationStore::open(state)?;
    let completed = outcome.and_then(|outcome| {
        let summary = completed_response(&outcome)?.to_owned();
        Ok((outcome, summary))
    });
    match completed {
        Ok((outcome, summary)) => {
            store.complete_child(
                &claim.spec.delegation_id,
                DelegationAuthority { owner_generation: delegation_generation, fencing_token },
                child_generation,
                &outcome.semantic_conversation,
                summary,
                completed_at_ms,
            )?;
        }
        Err(error) => {
            store.finish(
                &claim.spec.delegation_id,
                delegation_generation,
                fencing_token,
                DelegationTerminal::Failed { error: normalized_error(&error) },
                completed_at_ms,
            )?;
        }
    }
    Ok(())
}

fn running_authority(
    claim: &DelegationSnapshot,
) -> anyhow::Result<(FencingToken, OwnerGeneration)> {
    match claim.state {
        DelegationState::Running { fencing_token, .. } => {
            Ok((fencing_token, claim.owner_generation))
        }
        ref state => anyhow::bail!("claimed delegation is not running: {state:?}"),
    }
}

fn fresh_worker_id() -> anyhow::Result<DelegationWorkerId> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    DelegationWorkerId::new(format!("worker-{}-{now:x}", std::process::id())).map_err(Into::into)
}

fn unix_time_ms() -> anyhow::Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("Unix timestamp exceeds u64 milliseconds")
}

fn lease_deadline(now_ms: u64) -> u64 {
    now_ms.saturating_add(LEASE_DURATION_MS)
}

fn normalized_error(error: &anyhow::Error) -> String {
    let message = error.to_string();
    let message = message.trim();
    if message.is_empty() { "background delegation failed".into() } else { message.into() }
}
