use std::{ffi::OsString, path::Path, process::Stdio, time::Duration};

use anyhow::Context;
use protocol::{GatewayRequest, JSON_RPC_VERSION};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};

const CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const STDERR_TAIL_BYTES: usize = 16 * 1024;

pub(super) struct GatewayClient {
    child: Child,
    input: Option<BufWriter<ChildStdin>>,
    frames: mpsc::UnboundedReceiver<anyhow::Result<Value>>,
    stdout_task: JoinHandle<()>,
    stderr_task: JoinHandle<anyhow::Result<Vec<u8>>>,
    next_id: u64,
}

impl GatewayClient {
    pub(super) fn spawn(state: &Path, gateway_arguments: Vec<OsString>) -> anyhow::Result<Self> {
        let executable = std::env::current_exe().context("could not resolve hermesd executable")?;
        Self::spawn_executable(&executable, state, gateway_arguments)
    }

    fn spawn_executable(
        executable: &Path,
        state: &Path,
        gateway_arguments: Vec<OsString>,
    ) -> anyhow::Result<Self> {
        if !executable.is_file() {
            anyhow::bail!("hermesd executable does not exist: {}", executable.display());
        }
        let mut command = Command::new(executable);
        command
            .arg("--state")
            .arg(state)
            .arg("gateway")
            .args(gateway_arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().context("could not start hermesd gateway child")?;
        let input = child.stdin.take().context("gateway child omitted stdin")?;
        let output = child.stdout.take().context("gateway child omitted stdout")?;
        let stderr = child.stderr.take().context("gateway child omitted stderr")?;
        let (sender, frames) = mpsc::unbounded_channel();
        let stdout_task = tokio::spawn(async move {
            let mut lines = BufReader::new(output).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        let parsed = serde_json::from_str(&line)
                            .with_context(|| format!("gateway emitted malformed frame: {line}"));
                        let terminal = parsed.is_err();
                        if sender.send(parsed).is_err() || terminal {
                            break;
                        }
                    }
                    Ok(None) => {
                        let _ = sender.send(Err(anyhow::anyhow!("gateway stdout closed")));
                        break;
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error.into()));
                        break;
                    }
                }
            }
        });
        let stderr_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            BufReader::new(stderr).read_to_end(&mut bytes).await?;
            if bytes.len() > STDERR_TAIL_BYTES {
                bytes.drain(..bytes.len() - STDERR_TAIL_BYTES);
            }
            Ok(bytes)
        });
        Ok(Self {
            child,
            input: Some(BufWriter::new(input)),
            frames,
            stdout_task,
            stderr_task,
            next_id: 1,
        })
    }

    pub(super) async fn request(
        &mut self,
        method: impl Into<String>,
        params: Value,
    ) -> anyhow::Result<u64> {
        let id = self.next_id;
        self.next_id =
            self.next_id.checked_add(1).context("gateway request identity space exhausted")?;
        let request = GatewayRequest {
            jsonrpc: JSON_RPC_VERSION.into(),
            id: Value::from(id),
            method: method.into(),
            params,
        };
        let encoded = serde_json::to_vec(&request)?;
        let input = self.input.as_mut().context("gateway stdin is closed")?;
        input.write_all(&encoded).await?;
        input.write_all(b"\n").await?;
        input.flush().await?;
        Ok(id)
    }

    pub(super) async fn next_frame(&mut self) -> anyhow::Result<Value> {
        self.frames.recv().await.context("gateway frame channel closed")?
    }

    pub(super) async fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(mut input) = self.input.take() {
            input.shutdown().await?;
        }
        let status = match timeout(CHILD_SHUTDOWN_TIMEOUT, self.child.wait()).await {
            Ok(status) => status?,
            Err(_) => {
                self.child.kill().await?;
                self.child.wait().await?
            }
        };
        let _ = self.stdout_task.await;
        let stderr = self.stderr_task.await??;
        if !status.success() {
            anyhow::bail!(
                "gateway exited with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use serde_json::json;
    use tempfile::tempdir;

    use super::GatewayClient;

    #[tokio::test]
    async fn child_transport_preserves_literal_argv_and_round_trips_frames() -> anyhow::Result<()> {
        let fixture = tempdir()?;
        let state = fixture.path().join("state with spaces.db");
        let executable = fixture.path().join("fake gateway");
        fs::write(
            &executable,
            format!(
                r#"#!/bin/sh
set -eu
[ "$1" = "--state" ]
[ "$2" = "{}" ]
[ "$3" = "gateway" ]
[ "$4" = "--system" ]
[ "$5" = 'literal;$(not-a-command)' ]
printf '%s\n' '{{"jsonrpc":"2.0","method":"event","params":{{"type":"gateway.ready"}}}}'
IFS= read -r request
case "$request" in *'"method":"session.list"'*) ;; *) exit 91 ;; esac
printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"sessions":[]}}}}'
if IFS= read -r extra; then exit 92; fi
"#,
                state.display()
            ),
        )?;
        let mut permissions = fs::metadata(&executable)?.permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&executable, permissions)?;
        let mut client = GatewayClient::spawn_executable(
            &executable,
            &state,
            vec!["--system".into(), "literal;$(not-a-command)".into()],
        )?;
        assert_eq!(client.next_frame().await?["params"]["type"], "gateway.ready");
        assert_eq!(client.request("session.list", json!({})).await?, 1);
        assert_eq!(client.next_frame().await?["result"]["sessions"], json!([]));
        client.shutdown().await?;
        Ok(())
    }
}
