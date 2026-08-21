//! Long-lived execution of durable, fenced leaf delegations.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use domain::{
    DelegationAuthority, DelegationState, DelegationTerminal, DelegationWorkerId, FencingToken,
    OwnerGeneration,
};
use ports::{DelegationStore, SessionStore};
use protocol::{ContractOutcome, DelegationSnapshot};
use tokio::{
    sync::oneshot,
    task::{JoinHandle, JoinSet},
};

use super::chat::{LiveSettings, completed_response, execute_turn};
use crate::adapters::{SqliteDelegationStore, SqliteSessionStore};

const SUPERVISOR_INTERVAL: Duration = Duration::from_millis(50);
const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const LEASE_DURATION_MS: u64 = 90_000;
const PENDING_SCAN_LIMIT: usize = 32;
const CHILD_PROMPT: &str =
    "Complete the assigned task and return a concise result for the parent agent.";

/// Process-owned durable delegation supervisor, cancelled with its gateway.
pub(super) struct BackgroundSupervisor {
    task: JoinHandle<()>,
    control: BackgroundControl,
}

impl BackgroundSupervisor {
    /// Spawn the supervisor loop for one exclusively owned state database.
    pub(super) fn spawn(state: PathBuf) -> Self {
        let control = BackgroundControl::default();
        let task_control = control.clone();
        Self { task: tokio::spawn(async move { supervise(state, task_control).await }), control }
    }

    /// Obtain the live signal path paired with durable cancellation writes.
    pub(super) fn control(&self) -> BackgroundControl {
        self.control.clone()
    }
}

impl Drop for BackgroundSupervisor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Best-effort low-latency signal path; durable state remains authoritative.
#[derive(Clone, Default)]
pub(super) struct BackgroundControl {
    cancellations: Arc<Mutex<HashMap<String, oneshot::Sender<String>>>>,
}

impl BackgroundControl {
    fn register(&self, delegation_id: &str) -> anyhow::Result<oneshot::Receiver<String>> {
        let (sender, receiver) = oneshot::channel();
        self.cancellations
            .lock()
            .map_err(|_| anyhow::anyhow!("background-control lock poisoned"))?
            .insert(delegation_id.into(), sender);
        Ok(receiver)
    }

    /// Signal a worker only after its cancellation intent has committed.
    pub(super) fn signal(&self, delegation_id: &str, reason: String) -> bool {
        let sender =
            self.cancellations.lock().ok().and_then(|mut controls| controls.remove(delegation_id));
        sender.is_some_and(|sender| sender.send(reason).is_ok())
    }

    fn remove(&self, delegation_id: &str) {
        if let Ok(mut controls) = self.cancellations.lock() {
            controls.remove(delegation_id);
        }
    }
}

async fn supervise(state: PathBuf, control: BackgroundControl) {
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
                let delegation_id = claimed.spec.delegation_id.as_str().to_owned();
                let cancel_receiver = match control.register(&delegation_id) {
                    Ok(receiver) => receiver,
                    Err(error) => {
                        if let Ok((fencing_token, owner_generation)) = running_authority(&claimed) {
                            let _ = SqliteDelegationStore::open(&state).and_then(|mut store| {
                                store
                                    .finish(
                                        &claimed.spec.delegation_id,
                                        owner_generation,
                                        fencing_token,
                                        DelegationTerminal::Failed {
                                            error: normalized_error(&error),
                                        },
                                        now_ms,
                                    )
                                    .map(|_| ())
                            });
                        }
                        continue;
                    }
                };
                let worker_control = control.clone();
                jobs.spawn(async move {
                    let _ = execute_claim(worker_state, claimed, cancel_receiver).await;
                    worker_control.remove(&delegation_id);
                });
            }
        }
        tokio::time::sleep(SUPERVISOR_INTERVAL).await;
    }
}

async fn execute_claim(
    state: PathBuf,
    claim: DelegationSnapshot,
    mut cancel_receiver: oneshot::Receiver<String>,
) -> anyhow::Result<()> {
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
    let mut cancellation_poll = tokio::time::interval(CANCELLATION_POLL_INTERVAL);
    let exit = loop {
        tokio::select! {
            outcome = &mut execution => break ChildExit::Outcome(outcome),
            cancellation = &mut cancel_receiver => {
                if let Ok(reason) = cancellation {
                    break ChildExit::Cancelled(reason);
                }
            }
            _ = cancellation_poll.tick() => {
                let snapshot = SqliteDelegationStore::open(&state)?
                    .load(&claim.spec.delegation_id)?;
                if let Some(reason) = cancellation_reason(&snapshot) {
                    break ChildExit::Cancelled(reason.into());
                }
            }
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
                if let Some(reason) = cancellation_reason(&snapshot) {
                    break ChildExit::Cancelled(reason.into());
                }
            }
        }
    };
    finish_execution(&state, &claim, delegation_generation, fencing_token, child_generation, exit)
}

enum ChildExit {
    Outcome(anyhow::Result<ContractOutcome>),
    Cancelled(String),
}

fn finish_execution(
    state: &std::path::Path,
    claim: &DelegationSnapshot,
    delegation_generation: OwnerGeneration,
    fencing_token: FencingToken,
    child_generation: OwnerGeneration,
    exit: ChildExit,
) -> anyhow::Result<()> {
    let completed_at_ms = unix_time_ms()?;
    let mut store = SqliteDelegationStore::open(state)?;
    let outcome = match exit {
        ChildExit::Cancelled(reason) => {
            store.finish(
                &claim.spec.delegation_id,
                delegation_generation,
                fencing_token,
                DelegationTerminal::Cancelled { reason },
                completed_at_ms,
            )?;
            return Ok(());
        }
        ChildExit::Outcome(outcome) => outcome,
    };
    if let Some(reason) = cancellation_reason(&store.load(&claim.spec.delegation_id)?) {
        store.finish(
            &claim.spec.delegation_id,
            delegation_generation,
            fencing_token,
            DelegationTerminal::Cancelled { reason: reason.into() },
            completed_at_ms,
        )?;
        return Ok(());
    }
    let completed = outcome.and_then(|outcome| {
        let summary = completed_response(&outcome)?.to_owned();
        Ok((outcome, summary))
    });
    let result = match completed {
        Ok((outcome, summary)) => store
            .complete_child(
                &claim.spec.delegation_id,
                DelegationAuthority { owner_generation: delegation_generation, fencing_token },
                child_generation,
                &outcome.semantic_conversation,
                summary,
                completed_at_ms,
            )
            .map(|_| ()),
        Err(error) => store
            .finish(
                &claim.spec.delegation_id,
                delegation_generation,
                fencing_token,
                DelegationTerminal::Failed { error: normalized_error(&error) },
                completed_at_ms,
            )
            .map(|_| ()),
    };
    if let Err(error) = result {
        let snapshot = store.load(&claim.spec.delegation_id)?;
        if let Some(reason) = cancellation_reason(&snapshot) {
            let (current_fence, current_generation) = running_authority(&snapshot)?;
            store.finish(
                &claim.spec.delegation_id,
                current_generation,
                current_fence,
                DelegationTerminal::Cancelled { reason: reason.into() },
                unix_time_ms()?,
            )?;
        } else {
            return Err(error.into());
        }
    }
    Ok(())
}

fn cancellation_reason(snapshot: &DelegationSnapshot) -> Option<&str> {
    match &snapshot.state {
        DelegationState::Running { cancellation: Some(cancellation), .. } => {
            Some(cancellation.reason.as_str())
        }
        _ => None,
    }
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
