//! Kernel-owned provider and tool ports consumed by the offline runtime.

use std::pin::Pin;

use domain::{PlannedToolCall, ToolCall, ToolTerminal};
use futures_core::Stream;
use futures_util::future::BoxFuture;
use protocol::{ChatCompletionsRequest, ProviderEvent};
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
