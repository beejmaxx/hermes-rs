//! Kernel-owned provider and tool ports consumed by the offline runtime.

use std::pin::Pin;

use domain::{
    CompletionEventId, DelegationAuthority, DelegationId, DelegationSpec, DelegationTerminal,
    DelegationWorkerId, DeliveryClaimId, FencingToken, ForegroundTurnId, ForegroundTurnSpec,
    ForegroundTurnTerminal, OwnerGeneration, PlannedToolCall, SemanticMessage, SessionId, ToolCall,
    ToolTerminal,
};
use futures_core::Stream;
use futures_util::future::BoxFuture;
use protocol::{
    ChatCompletionsRequest, DelegationCompletion, DelegationSnapshot, ForegroundTurnSnapshot,
    PendingEffect, ProviderEvent, SessionConfig, SessionSnapshot, SessionSummary,
};
use thiserror::Error;

/// Provider attempt failure policy selected before the attempt begins.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AttemptErrorPolicy {
    /// A provider error terminates the turn.
    #[default]
    Stop,
    /// A provider error before visible output advances to the next attempt.
    FallbackBeforeVisibleOutput,
}

/// One identified provider stream returned for a submitted request.
pub struct ProviderAttempt {
    /// Stable attempt identity.
    pub attempt_id: String,
    /// Preselected error handling policy.
    pub error_policy: AttemptErrorPolicy,
    /// Normalized provider events in transport arrival order.
    pub events: Pin<Box<dyn Stream<Item = Result<ProviderEvent, ProviderError>> + Send>>,
}

/// A provider adapter failed before or while producing normalized events.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("provider error: {message}")]
pub struct ProviderError {
    message: String,
}

impl ProviderError {
    /// Construct a provider error with a stable diagnostic message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

/// A model-provider transport capable of starting normalized streams.
pub trait Provider: Send {
    /// Submit one provider request and return its identified event stream.
    fn stream<'a>(
        &'a mut self,
        request: ChatCompletionsRequest,
    ) -> BoxFuture<'a, Result<ProviderAttempt, ProviderError>>;

    /// Remaining scripted attempts, when the implementation is deterministic.
    fn remaining_attempts(&self) -> Option<usize> {
        None
    }
}

/// Tool planning or execution failed before a typed terminal outcome existed.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("tool broker error: {message}")]
pub struct ToolBrokerError {
    message: String,
}

impl ToolBrokerError {
    /// Construct a broker error with a stable diagnostic message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

/// Policy-aware tool planner and executor.
pub trait ToolBroker: Send {
    /// Attach effect, approval, and execution-key policy to model calls.
    fn plan(&mut self, calls: &[ToolCall]) -> Result<Vec<PlannedToolCall>, ToolBrokerError>;

    /// Execute a batch and return terminals in actual completion order.
    fn execute<'a>(
        &'a mut self,
        calls: &'a [PlannedToolCall],
    ) -> BoxFuture<'a, Result<Vec<ToolTerminal>, ToolBrokerError>>;
}

/// A durable session operation failed without exposing backend-specific errors.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SessionStoreError {
    /// A create operation targeted an existing session.
    #[error("session already exists: {0}")]
    AlreadyExists(SessionId),
    /// A requested session did not exist.
    #[error("session not found: {0}")]
    NotFound(SessionId),
    /// Another writer advanced the authority generation first.
    #[error("session {session_id} write conflict: expected generation {expected}, actual {actual}")]
    Conflict {
        /// Conflicting session.
        session_id: SessionId,
        /// Generation supplied by the caller.
        expected: u64,
        /// Current durable generation.
        actual: u64,
    },
    /// Input or stored session state violated an invariant.
    #[error("invalid session state: {0}")]
    Invalid(String),
    /// The storage backend failed.
    #[error("session storage failed: {0}")]
    Storage(String),
}

/// Single-writer durable session repository owned by the kernel boundary.
pub trait SessionStore: Send {
    /// Create an empty session at owner generation one.
    fn create(&mut self, config: SessionConfig) -> Result<SessionSnapshot, SessionStoreError>;

    /// Load one complete session and validate its conversation.
    fn load(&mut self, session_id: &SessionId) -> Result<SessionSnapshot, SessionStoreError>;

    /// Append one complete turn under an optimistic generation guard.
    fn append(
        &mut self,
        session_id: &SessionId,
        expected_generation: OwnerGeneration,
        messages: &[SemanticMessage],
    ) -> Result<SessionSnapshot, SessionStoreError>;

    /// List compact session records, newest first.
    fn list(&mut self) -> Result<Vec<SessionSummary>, SessionStoreError>;
}

/// A durable foreground-turn mutation failed.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ForegroundTurnStoreError {
    /// A start operation reused an existing attempt identity.
    #[error("foreground turn already exists: {0}")]
    AlreadyExists(ForegroundTurnId),
    /// Another running foreground attempt already owns the session.
    #[error("session already has a running foreground turn: {0}")]
    SessionBusy(SessionId),
    /// The referenced session does not exist.
    #[error("session not found: {0}")]
    SessionNotFound(SessionId),
    /// The referenced attempt does not exist.
    #[error("foreground turn not found: {0}")]
    NotFound(ForegroundTurnId),
    /// Another writer advanced the session authority generation first.
    #[error("session {session_id} write conflict: expected generation {expected}, actual {actual}")]
    GenerationConflict {
        /// Conflicting session.
        session_id: SessionId,
        /// Generation supplied by the caller.
        expected: u64,
        /// Current durable generation.
        actual: u64,
    },
    /// The attempt was not running when a terminal mutation arrived.
    #[error("foreground turn {turn_id} cannot transition from state {state}")]
    NotRunning {
        /// Conflicting attempt.
        turn_id: ForegroundTurnId,
        /// Current durable state name.
        state: String,
    },
    /// Input or stored foreground-turn state violated an invariant.
    #[error("invalid foreground turn state: {0}")]
    Invalid(String),
    /// The foreground-turn repository failed.
    #[error("foreground turn storage failed: {0}")]
    Storage(String),
}

/// Durable foreground ownership, terminalization, and crash reconciliation.
pub trait ForegroundTurnStore: Send {
    /// Claim a session generation before any provider or tool work begins.
    fn start(
        &mut self,
        spec: ForegroundTurnSpec,
        provider_prompt: &str,
        expected_generation: OwnerGeneration,
        started_at_ms: u64,
    ) -> Result<ForegroundTurnSnapshot, ForegroundTurnStoreError>;

    /// Claim a session generation and atomically acknowledge completion claims
    /// captured in the exact provider prompt.
    fn start_with_deliveries(
        &mut self,
        spec: ForegroundTurnSpec,
        provider_prompt: &str,
        expected_generation: OwnerGeneration,
        delivery_claims: &[(CompletionEventId, DeliveryClaimId)],
        started_at_ms: u64,
    ) -> Result<ForegroundTurnSnapshot, ForegroundTurnStoreError>;

    /// Atomically append one complete semantic turn and terminalize its claim.
    fn complete(
        &mut self,
        turn_id: &ForegroundTurnId,
        expected_generation: OwnerGeneration,
        messages: &[SemanticMessage],
        completed_at_ms: u64,
    ) -> Result<SessionSnapshot, ForegroundTurnStoreError>;

    /// Terminalize an uncommitted attempt without changing session history.
    fn terminate(
        &mut self,
        turn_id: &ForegroundTurnId,
        expected_generation: OwnerGeneration,
        outcome: ForegroundTurnTerminal,
        completed_at_ms: u64,
    ) -> Result<ForegroundTurnSnapshot, ForegroundTurnStoreError>;

    /// Load the most recently accepted attempt for a session, when one exists.
    fn latest(
        &mut self,
        session_id: &SessionId,
    ) -> Result<Option<ForegroundTurnSnapshot>, ForegroundTurnStoreError>;

    /// Conservatively terminalize every claim abandoned by a previous host.
    fn reconcile_running(
        &mut self,
        reason: &str,
        completed_at_ms: u64,
    ) -> Result<Vec<ForegroundTurnSnapshot>, ForegroundTurnStoreError>;
}

/// A durable background-delegation operation failed.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DelegationStoreError {
    /// A create operation reused a durable delegation identity.
    #[error("delegation already exists: {0}")]
    AlreadyExists(DelegationId),
    /// A requested delegation did not exist.
    #[error("delegation not found: {0}")]
    NotFound(DelegationId),
    /// The run was not in the state required by the requested transition.
    #[error("delegation {delegation_id} cannot transition from state {state}")]
    NotClaimable {
        /// Conflicting delegation.
        delegation_id: DelegationId,
        /// Persisted lifecycle state.
        state: String,
    },
    /// Another authoritative writer advanced the run generation first.
    #[error(
        "delegation {delegation_id} write conflict: expected generation {expected}, actual {actual}"
    )]
    GenerationConflict {
        /// Conflicting delegation.
        delegation_id: DelegationId,
        /// Generation supplied by the caller.
        expected: u64,
        /// Current durable generation.
        actual: u64,
    },
    /// A stale worker attempted to mutate a newer or terminal run.
    #[error(
        "delegation {delegation_id} fencing conflict: expected token {expected}, actual {actual:?}"
    )]
    FencingConflict {
        /// Conflicting delegation.
        delegation_id: DelegationId,
        /// Token supplied by the worker.
        expected: u64,
        /// Current token, absent before the first claim.
        actual: Option<u64>,
    },
    /// Input or stored delegation state violated an invariant.
    #[error("invalid delegation state: {0}")]
    Invalid(String),
    /// The delegation repository failed.
    #[error("delegation storage failed: {0}")]
    Storage(String),
}

/// Durable lease, fencing, terminal, and completion-outbox repository.
pub trait DelegationStore: Send {
    /// Durably accept one background unit before any worker can run it.
    fn create(
        &mut self,
        spec: DelegationSpec,
        now_ms: u64,
    ) -> Result<DelegationSnapshot, DelegationStoreError>;

    /// Atomically create the immutable child lineage and accept its background unit.
    fn create_with_child(
        &mut self,
        child_config: SessionConfig,
        spec: DelegationSpec,
        now_ms: u64,
    ) -> Result<DelegationSnapshot, DelegationStoreError>;

    /// Load one complete durable run.
    fn load(
        &mut self,
        delegation_id: &DelegationId,
    ) -> Result<DelegationSnapshot, DelegationStoreError>;

    /// List one parent's runs in deterministic newest-first order.
    fn list_for_parent(
        &mut self,
        parent_session_id: &SessionId,
        limit: usize,
    ) -> Result<Vec<DelegationSnapshot>, DelegationStoreError>;

    /// List unclaimed work in deterministic creation order.
    fn pending(&mut self, limit: usize) -> Result<Vec<DelegationSnapshot>, DelegationStoreError>;

    /// Claim pending work and mint its first fencing token.
    fn claim(
        &mut self,
        delegation_id: &DelegationId,
        expected_generation: OwnerGeneration,
        worker_id: DelegationWorkerId,
        now_ms: u64,
        lease_expires_at_ms: u64,
    ) -> Result<DelegationSnapshot, DelegationStoreError>;

    /// Extend the lease held by the current worker generation.
    fn heartbeat(
        &mut self,
        delegation_id: &DelegationId,
        expected_generation: OwnerGeneration,
        fencing_token: FencingToken,
        now_ms: u64,
        lease_expires_at_ms: u64,
    ) -> Result<DelegationSnapshot, DelegationStoreError>;

    /// Persist cancellation before any live worker is signalled.
    ///
    /// Pending work becomes terminal immediately. Running work retains its
    /// generation and fencing authority, but may thereafter commit only the
    /// matching cancelled terminal.
    fn cancel(
        &mut self,
        delegation_id: &DelegationId,
        expected_generation: OwnerGeneration,
        reason: &str,
        requested_at_ms: u64,
    ) -> Result<DelegationSnapshot, DelegationStoreError>;

    /// Atomically record a terminal child outcome and its one completion event.
    fn finish(
        &mut self,
        delegation_id: &DelegationId,
        expected_generation: OwnerGeneration,
        fencing_token: FencingToken,
        outcome: DelegationTerminal,
        completed_at_ms: u64,
    ) -> Result<DelegationCompletion, DelegationStoreError>;

    /// Atomically append a successful child turn, terminalize its fenced run,
    /// and enqueue the one completion event.
    fn complete_child(
        &mut self,
        delegation_id: &DelegationId,
        authority: DelegationAuthority,
        child_generation: OwnerGeneration,
        child_messages: &[SemanticMessage],
        summary: String,
        completed_at_ms: u64,
    ) -> Result<DelegationCompletion, DelegationStoreError>;

    /// Mark every expired running owner as outcome-unknown and enqueue completions.
    fn reconcile_expired(
        &mut self,
        now_ms: u64,
    ) -> Result<Vec<DelegationCompletion>, DelegationStoreError>;

    /// Conservatively terminalize every running owner abandoned by a replaced host.
    fn reconcile_running(
        &mut self,
        reason: &str,
        now_ms: u64,
    ) -> Result<Vec<DelegationCompletion>, DelegationStoreError>;

    /// List completion events whose delivery claim is absent or expired.
    fn available_completions(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<DelegationCompletion>, DelegationStoreError>;

    /// List deliverable completions routed to one exact parent session.
    fn available_completions_for(
        &mut self,
        parent_session_id: &SessionId,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<DelegationCompletion>, DelegationStoreError>;

    /// Claim one pending completion across competing delivery consumers.
    fn claim_completion(
        &mut self,
        event_id: &CompletionEventId,
        claim_id: DeliveryClaimId,
        now_ms: u64,
        claim_expires_at_ms: u64,
    ) -> Result<Option<DelegationCompletion>, DelegationStoreError>;

    /// Acknowledge a completion accepted at a legal new-turn boundary.
    fn acknowledge_completion(
        &mut self,
        event_id: &CompletionEventId,
        claim_id: &DeliveryClaimId,
        delivered_at_ms: u64,
    ) -> Result<bool, DelegationStoreError>;

    /// Release a transiently failed delivery claim for a later consumer.
    fn release_completion(
        &mut self,
        event_id: &CompletionEventId,
        claim_id: &DeliveryClaimId,
    ) -> Result<bool, DelegationStoreError>;
}

/// Durable effect-ledger operation failed.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EffectLedgerError {
    /// An execution key already has a durable plan or terminal.
    #[error("effect execution key is already recorded: {0}")]
    AlreadyRecorded(String),
    /// A terminal was supplied without a matching durable plan.
    #[error("effect execution key has no durable plan: {0}")]
    MissingPlan(String),
    /// A terminal disagreed with its frozen durable plan.
    #[error("effect terminal does not match its durable plan: {0}")]
    PlanMismatch(String),
    /// A recorded invocation already has a terminal disposition.
    #[error("effect execution key already has a terminal disposition: {0}")]
    AlreadyTerminal(String),
    /// Input or stored ledger state violated an invariant.
    #[error("invalid effect ledger state: {0}")]
    Invalid(String),
    /// The ledger backend failed.
    #[error("effect ledger storage failed: {0}")]
    Storage(String),
}

/// Durable write-ahead ledger for every dispatched tool effect.
pub trait EffectLedger: Send {
    /// Atomically record complete plans before any invocation is dispatched.
    fn record_plans(
        &mut self,
        execution_scope: &str,
        plans: &[PlannedToolCall],
    ) -> Result<(), EffectLedgerError>;

    /// Atomically attach exactly one terminal disposition to every completed plan.
    fn record_terminals(&mut self, terminals: &[ToolTerminal]) -> Result<(), EffectLedgerError>;

    /// List plans left without a terminal disposition after interruption or crash.
    fn pending(&mut self) -> Result<Vec<PendingEffect>, EffectLedgerError>;
}
