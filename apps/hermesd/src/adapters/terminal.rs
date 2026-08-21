//! Explicitly approved local shell-command adapter.

use std::{ffi::OsStr, path::Path, process::Stdio, time::Duration};

use domain::{PlannedToolCall, ToolEffect, ToolResultStatus, ToolTerminal};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};

use crate::adapters::ApprovalDecision;

const TOOL_NAME: &str = "terminal";
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MAX_COMMAND_CHARS: usize = 16_000;
const MAX_STREAM_BYTES: usize = 100_000;

/// Gateway-only shell tool whose every call requires a live user decision.
pub struct TerminalTool;

impl TerminalTool {
    /// Stable provider-visible tool name.
    pub const NAME: &'static str = TOOL_NAME;

    /// Frozen OpenAI-compatible schema for new gateway sessions.
    #[must_use]
    pub fn schema() -> Value {
        json!({
            "type": "function",
            "function": {
                "name": TOOL_NAME,
                "description": "Run one shell command from the configured workspace root after explicit user approval. The command may read, write, start processes, or access the network. Use at most one terminal call in a response.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "Exact shell command shown to the user for approval.",
                            "minLength": 1,
                            "maxLength": MAX_COMMAND_CHARS
                        },
                        "timeout_ms": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAX_TIMEOUT_MS,
                            "default": DEFAULT_TIMEOUT_MS
                        }
                    },
                    "required": ["command"],
                    "additionalProperties": false
                }
            }
        })
    }

    /// Extract and validate the command used in the approval prompt.
    pub fn approval_command(plan: &PlannedToolCall) -> Result<String, String> {
        decode(plan).map(|arguments| arguments.command)
    }

    /// Execute one resolved plan or return a rejected terminal without dispatch.
    pub async fn execute(
        root: &Path,
        plan: PlannedToolCall,
        decision: ApprovalDecision,
    ) -> ToolTerminal {
        if decision == ApprovalDecision::Deny {
            return terminal(
                plan,
                ToolResultStatus::Rejected,
                "User denied terminal command approval.".into(),
                None,
            );
        }
        let arguments = match decode(&plan) {
            Ok(arguments) => arguments,
            Err(error) => return terminal(plan, ToolResultStatus::Failed, error, None),
        };
        let mut command = shell_command(&arguments.command);
        command
            .current_dir(root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        remove_sensitive_environment(&mut command);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                return terminal(
                    plan,
                    ToolResultStatus::Failed,
                    format!("could not execute terminal command: {error}"),
                    None,
                );
            }
        };
        let mut process_group = ProcessGroupGuard::new(child.id());
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                return terminal(
                    plan,
                    ToolResultStatus::Failed,
                    "terminal command stdout was not captured".into(),
                    None,
                );
            }
        };
        let stderr = match child.stderr.take() {
            Some(stderr) => stderr,
            None => {
                return terminal(
                    plan,
                    ToolResultStatus::Failed,
                    "terminal command stderr was not captured".into(),
                    None,
                );
            }
        };
        let completion = async {
            let (status, stdout, stderr) =
                tokio::join!(child.wait(), read_bounded(stdout), read_bounded(stderr));
            Ok::<_, std::io::Error>((status?, stdout?, stderr?))
        };
        let (exit_status, stdout, stderr) =
            match tokio::time::timeout(Duration::from_millis(arguments.timeout_ms), completion)
                .await
            {
                Ok(Ok(output)) => output,
                Ok(Err(error)) => {
                    return terminal(
                        plan,
                        ToolResultStatus::Failed,
                        format!("could not collect terminal command output: {error}"),
                        None,
                    );
                }
                Err(_) => {
                    process_group.terminate();
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    return terminal(
                        plan,
                        ToolResultStatus::Failed,
                        format!("terminal command timed out after {} ms", arguments.timeout_ms),
                        None,
                    );
                }
            };
        process_group.terminate();
        let status = if exit_status.success() {
            ToolResultStatus::Succeeded
        } else {
            ToolResultStatus::Failed
        };
        let code = exit_status.code().map_or_else(|| "signal".into(), |code| code.to_string());
        let content = render_output(&stdout, &stderr, &code);
        terminal(plan, status, content, Some(format!("exit:{code}")))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalArgs {
    command: String,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

fn decode(plan: &PlannedToolCall) -> Result<TerminalArgs, String> {
    let arguments = serde_json::from_value::<TerminalArgs>(Value::Object(Map::from_iter(
        plan.arguments.0.clone(),
    )))
    .map_err(|error| format!("invalid terminal arguments: {error}"))?;
    if arguments.command.is_empty() || arguments.command.trim() != arguments.command {
        return Err("terminal command must be non-empty with no surrounding whitespace".into());
    }
    if arguments.command.chars().count() > MAX_COMMAND_CHARS {
        return Err(format!("terminal command exceeds {MAX_COMMAND_CHARS} characters"));
    }
    if arguments.timeout_ms == 0 || arguments.timeout_ms > MAX_TIMEOUT_MS {
        return Err(format!("terminal timeout_ms must be between 1 and {MAX_TIMEOUT_MS}"));
    }
    Ok(arguments)
}

#[cfg(unix)]
fn shell_command(command: &str) -> Command {
    let mut process = Command::new("/bin/sh");
    process.arg("-c").arg(command);
    process.process_group(0);
    process
}

#[cfg(windows)]
fn shell_command(command: &str) -> Command {
    let mut process = Command::new("cmd.exe");
    process.args(["/D", "/S", "/C", command]);
    process
}

fn remove_sensitive_environment(command: &mut Command) {
    for (name, _) in std::env::vars_os() {
        if sensitive_environment_name(&name) {
            command.env_remove(name);
        }
    }
}

fn sensitive_environment_name(name: &OsStr) -> bool {
    let name = name.to_string_lossy().to_ascii_uppercase();
    ["API_KEY", "AUTH", "CREDENTIAL", "PASSWORD", "PASSWD", "SECRET", "TOKEN"]
        .iter()
        .any(|marker| name.contains(marker))
}

#[cfg(unix)]
struct ProcessGroupGuard {
    process_group_id: Option<u32>,
}

#[cfg(unix)]
impl ProcessGroupGuard {
    fn new(process_group_id: Option<u32>) -> Self {
        Self { process_group_id }
    }

    fn terminate(&mut self) {
        let Some(process_group_id) = self.process_group_id.take() else {
            return;
        };
        let _ = std::process::Command::new("/bin/kill")
            .args(["-KILL", "--", &format!("-{process_group_id}")])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[cfg(unix)]
impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(windows)]
struct ProcessGroupGuard;

#[cfg(windows)]
impl ProcessGroupGuard {
    fn new(_process_group_id: Option<u32>) -> Self {
        Self
    }

    fn terminate(&mut self) {}
}

struct CapturedStream {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn read_bounded(mut stream: impl AsyncRead + Unpin) -> std::io::Result<CapturedStream> {
    let mut bytes = Vec::with_capacity(MAX_STREAM_BYTES);
    let mut buffer = [0_u8; 8 * 1024];
    let mut truncated = false;
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = MAX_STREAM_BYTES.saturating_sub(bytes.len());
        let retained = remaining.min(read);
        bytes.extend_from_slice(&buffer[..retained]);
        truncated |= retained < read;
    }
    Ok(CapturedStream { bytes, truncated })
}

fn render_output(stdout: &CapturedStream, stderr: &CapturedStream, code: &str) -> String {
    let mut rendered = String::new();
    append_stream(&mut rendered, "stdout", stdout);
    append_stream(&mut rendered, "stderr", stderr);
    if rendered.is_empty() {
        format!("Command exited with status {code} and produced no output.")
    } else {
        rendered
    }
}

fn append_stream(rendered: &mut String, name: &str, stream: &CapturedStream) {
    if stream.bytes.is_empty() && !stream.truncated {
        return;
    }
    if !rendered.is_empty() {
        rendered.push('\n');
    }
    rendered.push_str(name);
    rendered.push_str(":\n");
    rendered.push_str(&String::from_utf8_lossy(&stream.bytes));
    if stream.truncated {
        rendered.push_str("\n[output truncated]");
    }
}

fn terminal(
    plan: PlannedToolCall,
    status: ToolResultStatus,
    content: String,
    receipt: Option<String>,
) -> ToolTerminal {
    ToolTerminal {
        call_id: plan.call_id,
        name: plan.name,
        status,
        content,
        execution_key: plan.execution_key,
        effect: ToolEffect::ProcessControl,
        receipt,
    }
}

const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use domain::{PlannedToolCall, ToolArguments, ToolCallId, ToolEffect, ToolResultStatus};
    use serde_json::json;
    use tempfile::tempdir;

    use super::{ApprovalDecision, TerminalTool};

    fn plan(command: &str) -> Result<PlannedToolCall, Box<dyn std::error::Error>> {
        Ok(PlannedToolCall {
            call_id: ToolCallId::new("call-terminal")?,
            name: TerminalTool::NAME.into(),
            arguments: ToolArguments(BTreeMap::from([("command".into(), json!(command))])),
            execution_key: "terminal:call-terminal".into(),
            effect: ToolEffect::ProcessControl,
            approval: None,
        })
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn approved_command_runs_from_the_frozen_root() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempdir()?;
        let terminal = TerminalTool::execute(
            root.path(),
            plan("printf approved > result.txt")?,
            ApprovalDecision::Allow,
        )
        .await;
        assert_eq!(terminal.status, ToolResultStatus::Succeeded);
        assert_eq!(std::fs::read_to_string(root.path().join("result.txt"))?, "approved");
        Ok(())
    }

    #[tokio::test]
    async fn denied_command_never_dispatches() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempdir()?;
        let terminal = TerminalTool::execute(
            root.path(),
            plan("printf denied > result.txt")?,
            ApprovalDecision::Deny,
        )
        .await;
        assert_eq!(terminal.status, ToolResultStatus::Rejected);
        assert!(!root.path().join("result.txt").exists());
        Ok(())
    }
}
