//! Supervised Codex app-server process and its typed stdio protocol subset.

mod authority;
mod protocol;

use std::{
    collections::VecDeque,
    ffi::OsString,
    io,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

pub use authority::{CodexAuthorityError, CodexAuthorityManifest, CodexAuthorityPolicy};
pub use protocol::{
    CodexAgentMessageDelta, CodexAppServerEvent, CodexApprovalPolicy, CodexConfigReadParams,
    CodexConfigReadResponse, CodexDynamicToolCallOutputContentItem, CodexDynamicToolCallParams,
    CodexDynamicToolCallResponse, CodexDynamicToolFunctionSpec, CodexDynamicToolSpec,
    CodexInitializeParams, CodexInitializeResponse, CodexNotification, CodexRequestId,
    CodexSandboxMode, CodexServerRequest, CodexThread, CodexThreadOpenResponse,
    CodexThreadResumeParams, CodexThreadStartParams, CodexTurn, CodexTurnCompleted,
    CodexTurnInterruptParams, CodexTurnStartParams, CodexTurnStartResponse, CodexTurnStarted,
    CodexTurnStatus,
};
use protocol::{
    InboundMessage, OutboundErrorResponse, OutboundNotification, OutboundRequest, OutboundResponse,
    OutboundRpcError, decode_event,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    process::{Child, ChildStdin, ChildStdout, Command},
    task::JoinHandle,
    time::timeout,
};

const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const STDERR_TAIL_BYTES: usize = 16 * 1024;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Exact executable and arguments used to launch `codex app-server --stdio`.
#[derive(Clone, Debug)]
pub struct CodexAppServerCommand {
    executable: PathBuf,
    arguments: Vec<OsString>,
    current_dir: Option<PathBuf>,
}

impl CodexAppServerCommand {
    /// Select the authenticated Codex executable to supervise.
    #[must_use]
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self { executable: executable.into(), arguments: Vec::new(), current_dir: None }
    }

    /// Append one literal app-server argument without shell parsing.
    #[must_use]
    pub fn arg(mut self, argument: impl Into<OsString>) -> Self {
        self.arguments.push(argument.into());
        self
    }

    /// Set the child process working directory.
    #[must_use]
    pub fn current_dir(mut self, directory: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(directory.into());
        self
    }

    /// Configured Codex executable path.
    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    /// Literal arguments appended after `app-server --stdio`.
    #[must_use]
    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
}

/// Failure while supervising or speaking to a Codex app-server worker.
#[derive(Debug, Error)]
pub enum CodexAppServerError {
    /// The configured executable could not be started.
    #[error("could not spawn Codex app-server {executable}: {source}")]
    Spawn {
        /// Executable that was selected.
        executable: PathBuf,
        /// Operating-system spawn failure.
        #[source]
        source: io::Error,
    },
    /// The child did not expose one of its required stdio pipes.
    #[error("Codex app-server did not expose its {0} pipe")]
    MissingPipe(&'static str),
    /// Reading or writing the app-server transport failed.
    #[error("Codex app-server transport failed: {0}")]
    Io(#[from] io::Error),
    /// An outbound typed protocol message could not be encoded.
    #[error("could not encode Codex app-server message: {0}")]
    Encode(#[source] serde_json::Error),
    /// An inbound line was not a valid protocol message.
    #[error("Codex app-server emitted a malformed protocol frame: {0}")]
    MalformedFrame(#[source] serde_json::Error),
    /// A frame exceeded the bounded stdio transport limit.
    #[error("Codex app-server frame exceeded {limit} bytes")]
    FrameTooLarge {
        /// Configured maximum frame size.
        limit: usize,
    },
    /// A decoded frame violated the expected request/response lifecycle.
    #[error("Codex app-server protocol violation: {0}")]
    Protocol(String),
    /// The worker rejected a named request.
    #[error("Codex app-server rejected {method} with RPC error {code}: {message}")]
    Remote {
        /// Method submitted by the client.
        method: String,
        /// App-server error code.
        code: i64,
        /// App-server error message.
        message: String,
        /// Opaque error details retained at the adapter boundary.
        data: Value,
    },
    /// The child exited before or during an operation.
    #[error("Codex app-server exited with status {code:?}")]
    ProcessExited {
        /// Platform process exit code, when available.
        code: Option<i32>,
        /// Bounded stderr tail retained for operator diagnostics.
        stderr: String,
    },
    /// The worker did not stop after stdin was closed and had to be killed.
    #[error("Codex app-server did not exit within the shutdown timeout")]
    ShutdownTimedOut,
    /// No additional numeric request identity can be allocated.
    #[error("Codex app-server request identity space was exhausted")]
    RequestIdExhausted,
}

/// A single supervised Codex app-server connection over newline-delimited stdio.
pub struct CodexAppServer {
    child: Child,
    stdin: Option<BufWriter<ChildStdin>>,
    stdout: BufReader<ChildStdout>,
    stderr_task: Option<JoinHandle<io::Result<Vec<u8>>>>,
    next_request_id: i64,
    buffered_events: VecDeque<CodexAppServerEvent>,
}

impl CodexAppServer {
    /// Spawn `codex app-server --stdio` with inherited authentication and environment.
    pub fn spawn(specification: &CodexAppServerCommand) -> Result<Self, CodexAppServerError> {
        let mut command = Command::new(&specification.executable);
        command
            .arg("app-server")
            .arg("--stdio")
            .args(&specification.arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(directory) = &specification.current_dir {
            command.current_dir(directory);
        }
        let mut child = command.spawn().map_err(|source| CodexAppServerError::Spawn {
            executable: specification.executable.clone(),
            source,
        })?;
        let stdin = child.stdin.take().ok_or(CodexAppServerError::MissingPipe("stdin"))?;
        let stdout = child.stdout.take().ok_or(CodexAppServerError::MissingPipe("stdout"))?;
        let stderr = child.stderr.take().ok_or(CodexAppServerError::MissingPipe("stderr"))?;
        let stderr_task = tokio::spawn(capture_stderr_tail(stderr));
        Ok(Self {
            child,
            stdin: Some(BufWriter::new(stdin)),
            stdout: BufReader::new(stdout),
            stderr_task: Some(stderr_task),
            next_request_id: 1,
            buffered_events: VecDeque::new(),
        })
    }

    /// Negotiate the protocol and return worker platform metadata.
    pub async fn initialize(
        &mut self,
        params: &CodexInitializeParams,
    ) -> Result<CodexInitializeResponse, CodexAppServerError> {
        self.request("initialize", params).await
    }

    /// Notify the worker that initialization is complete.
    pub async fn initialized(&mut self) -> Result<(), CodexAppServerError> {
        self.write_frame(&OutboundNotification { method: "initialized" }).await
    }

    /// Read effective worker configuration for authority-policy construction.
    pub async fn read_config(
        &mut self,
        params: &CodexConfigReadParams,
    ) -> Result<CodexConfigReadResponse, CodexAppServerError> {
        self.request("config/read", params).await
    }

    /// Create a new worker-owned thread.
    pub async fn start_thread(
        &mut self,
        params: &CodexThreadStartParams,
    ) -> Result<CodexThreadOpenResponse, CodexAppServerError> {
        self.request("thread/start", params).await
    }

    /// Reopen a worker thread identified by a Hermes-owned binding.
    pub async fn resume_thread(
        &mut self,
        params: &CodexThreadResumeParams,
    ) -> Result<CodexThreadOpenResponse, CodexAppServerError> {
        self.request("thread/resume", params).await
    }

    /// Submit one turn to an opened worker thread.
    pub async fn start_turn(
        &mut self,
        params: &CodexTurnStartParams,
    ) -> Result<CodexTurnStartResponse, CodexAppServerError> {
        self.request("turn/start", params).await
    }

    /// Request cooperative interruption of one active worker turn.
    pub async fn interrupt_turn(
        &mut self,
        params: &CodexTurnInterruptParams,
    ) -> Result<(), CodexAppServerError> {
        let _response: EmptyResponse = self.request("turn/interrupt", params).await?;
        Ok(())
    }

    /// Read the next notification or server request in transport arrival order.
    pub async fn next_event(&mut self) -> Result<CodexAppServerEvent, CodexAppServerError> {
        if let Some(event) = self.buffered_events.pop_front() {
            return Ok(event);
        }
        let message = self.read_message().await?;
        decode_event(message).map_err(CodexAppServerError::Protocol)
    }

    /// Answer one server-owned request with a successful opaque result.
    pub async fn respond(
        &mut self,
        id: &CodexRequestId,
        result: &Value,
    ) -> Result<(), CodexAppServerError> {
        self.write_frame(&OutboundResponse { id, result }).await
    }

    /// Return a typed result for one `item/tool/call` request.
    pub async fn respond_dynamic_tool_call(
        &mut self,
        request: &CodexServerRequest,
        response: &CodexDynamicToolCallResponse,
    ) -> Result<(), CodexAppServerError> {
        if request.dynamic_tool_call().is_none() {
            return Err(CodexAppServerError::Protocol(format!(
                "cannot send a dynamic-tool response for request {}",
                request.method()
            )));
        }
        let result = serde_json::to_value(response).map_err(CodexAppServerError::Encode)?;
        self.respond(request.id(), &result).await
    }

    /// Answer one server-owned request with a JSON-RPC error.
    pub async fn respond_error(
        &mut self,
        id: &CodexRequestId,
        code: i64,
        message: &str,
        data: &Value,
    ) -> Result<(), CodexAppServerError> {
        self.write_frame(&OutboundErrorResponse {
            id,
            error: OutboundRpcError { code, message, data },
        })
        .await
    }

    /// Close stdin and require the worker to exit promptly and successfully.
    pub async fn shutdown(mut self) -> Result<(), CodexAppServerError> {
        if let Some(mut stdin) = self.stdin.take() {
            stdin.shutdown().await?;
        }
        let status = match timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(status) => status?,
            Err(_) => {
                self.child.kill().await?;
                let _status = self.child.wait().await?;
                return Err(CodexAppServerError::ShutdownTimedOut);
            }
        };
        let stderr = self.take_stderr().await;
        if status.success() { Ok(()) } else { Err(process_exited(status, stderr)) }
    }

    async fn request<P, R>(&mut self, method: &str, params: &P) -> Result<R, CodexAppServerError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let id = self.allocate_request_id()?;
        self.write_frame(&OutboundRequest { id: id.clone(), method, params }).await?;
        loop {
            match self.read_message().await? {
                InboundMessage::Response { id: response_id, result } if response_id == id => {
                    return serde_json::from_value(result)
                        .map_err(CodexAppServerError::MalformedFrame);
                }
                InboundMessage::Error { id: response_id, error } if response_id == id => {
                    return Err(CodexAppServerError::Remote {
                        method: method.into(),
                        code: error.code,
                        message: error.message,
                        data: error.data,
                    });
                }
                message
                @ (InboundMessage::Request { .. } | InboundMessage::Notification { .. }) => {
                    self.buffered_events
                        .push_back(decode_event(message).map_err(CodexAppServerError::Protocol)?);
                }
                InboundMessage::Response { id: response_id, .. }
                | InboundMessage::Error { id: response_id, .. } => {
                    return Err(CodexAppServerError::Protocol(format!(
                        "received response {response_id:?} while waiting for {id:?}"
                    )));
                }
            }
        }
    }

    fn allocate_request_id(&mut self) -> Result<CodexRequestId, CodexAppServerError> {
        let id = self.next_request_id;
        self.next_request_id = id.checked_add(1).ok_or(CodexAppServerError::RequestIdExhausted)?;
        Ok(CodexRequestId::Integer(id))
    }

    async fn write_frame<T>(&mut self, message: &T) -> Result<(), CodexAppServerError>
    where
        T: Serialize,
    {
        let encoded = serde_json::to_vec(message).map_err(CodexAppServerError::Encode)?;
        if encoded.len() > MAX_FRAME_BYTES {
            return Err(CodexAppServerError::FrameTooLarge { limit: MAX_FRAME_BYTES });
        }
        let stdin = self.stdin.as_mut().ok_or_else(|| {
            CodexAppServerError::Protocol("cannot write after app-server stdin was closed".into())
        })?;
        stdin.write_all(&encoded).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<InboundMessage, CodexAppServerError> {
        let Some(line) = read_bounded_line(&mut self.stdout).await? else {
            let status = self.child.wait().await?;
            let stderr = self.take_stderr().await;
            return Err(process_exited(status, stderr));
        };
        let value: Value =
            serde_json::from_slice(&line).map_err(CodexAppServerError::MalformedFrame)?;
        validate_message_shape(&value).map_err(CodexAppServerError::Protocol)?;
        serde_json::from_value(value).map_err(CodexAppServerError::MalformedFrame)
    }

    async fn take_stderr(&mut self) -> String {
        let Some(task) = self.stderr_task.take() else {
            return String::new();
        };
        match task.await {
            Ok(Ok(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
            Ok(Err(error)) => format!("stderr capture failed: {error}"),
            Err(error) => format!("stderr capture task failed: {error}"),
        }
    }
}

#[derive(serde::Deserialize)]
struct EmptyResponse {}

fn validate_message_shape(value: &Value) -> Result<(), String> {
    let object = value.as_object().ok_or_else(|| "top-level frame must be an object".to_owned())?;
    let has_id = object.contains_key("id");
    let has_method = object.contains_key("method");
    let has_result = object.contains_key("result");
    let has_error = object.contains_key("error");
    let valid = matches!(
        (has_id, has_method, has_result, has_error),
        (true, true, false, false)
            | (false, true, false, false)
            | (true, false, true, false)
            | (true, false, false, true)
    );
    if valid {
        Ok(())
    } else {
        Err("frame must be exactly one request, notification, response, or error".into())
    }
}

async fn read_bounded_line(
    reader: &mut BufReader<ChildStdout>,
) -> Result<Option<Vec<u8>>, CodexAppServerError> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if line.is_empty() { Ok(None) } else { Ok(Some(line)) };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let content = newline.map_or(available, |position| &available[..position]);
        if line.len().saturating_add(content.len()) > MAX_FRAME_BYTES {
            return Err(CodexAppServerError::FrameTooLarge { limit: MAX_FRAME_BYTES });
        }
        line.extend_from_slice(content);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(line));
        }
    }
}

async fn capture_stderr_tail(mut stderr: tokio::process::ChildStderr) -> io::Result<Vec<u8>> {
    let mut tail = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let count = stderr.read(&mut chunk).await?;
        if count == 0 {
            return Ok(tail);
        }
        tail.extend_from_slice(&chunk[..count]);
        if tail.len() > STDERR_TAIL_BYTES {
            let excess = tail.len() - STDERR_TAIL_BYTES;
            tail.drain(..excess);
        }
    }
}

fn process_exited(status: ExitStatus, stderr: String) -> CodexAppServerError {
    CodexAppServerError::ProcessExited { code: status.code(), stderr }
}
