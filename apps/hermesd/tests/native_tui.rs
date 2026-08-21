//! PTY-level proof for the native Rust client over a real gateway child.

#![cfg(unix)]

use std::{
    fs,
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use hermesd::adapters::SqliteSessionStore;
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use ports::SessionStore;
use tempfile::{TempDir, tempdir};

#[test]
fn native_tui_denies_a_real_gateway_effect_and_resumes_kernel_state()
-> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let workspace = tempdir()?;
    let root = fs::canonicalize(workspace.path())?;
    let database = state_dir.path().join("state.db");
    let forbidden_artifact = workspace.path().join("must-not-exist.txt");
    let (_fixture, codex) = fake_codex(&root, &forbidden_artifact)?;
    let (mut child, master, mut input, output, reader_thread) =
        spawn_tui(&database, &codex, &root)?;
    wait_for_output(&output, "Sessions", Duration::from_secs(5))?;
    input.write_all(b"n")?;
    input.flush()?;
    wait_for_output(&output, "Message", Duration::from_secs(5))?;
    input.write_all(b"Run the requested command\r")?;
    input.flush()?;
    wait_for_output(&output, "Approval", Duration::from_secs(5))?;
    wait_for_output(&output, "must-not-exist.txt", Duration::from_secs(5))?;
    input.write_all(b"n")?;
    input.flush()?;
    wait_for_output(&output, "Denied", Duration::from_secs(5))?;
    wait_for_output(&output, "safely.", Duration::from_secs(5))?;
    input.write_all(&[3])?;
    input.flush()?;
    finish_tui(&mut child, master, input, reader_thread)?;
    assert!(!forbidden_artifact.exists(), "denied command changed the filesystem");
    let sessions = SqliteSessionStore::open(&database)?.list()?;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].message_count, 4);

    let (mut resumed_child, resumed_master, mut resumed_input, resumed_output, resumed_reader) =
        spawn_tui(&database, &codex, &root)?;
    wait_for_output(&resumed_output, "Sessions", Duration::from_secs(5))?;
    resumed_input.write_all(b"\r")?;
    resumed_input.flush()?;
    wait_for_output(&resumed_output, "Denied", Duration::from_secs(5))?;
    wait_for_output(&resumed_output, "safely.", Duration::from_secs(5))?;
    resumed_input.write_all(&[3])?;
    resumed_input.flush()?;
    finish_tui(&mut resumed_child, resumed_master, resumed_input, resumed_reader)?;
    Ok(())
}

type PtyChild = Box<dyn Child + Send + Sync>;
type PtyMaster = Box<dyn MasterPty + Send>;
type PtyWriter = Box<dyn Write + Send>;
type OutputBuffer = Arc<Mutex<String>>;
type SpawnedTui = (PtyChild, PtyMaster, PtyWriter, OutputBuffer, thread::JoinHandle<()>);

fn spawn_tui(
    database: &Path,
    codex: &Path,
    root: &Path,
) -> Result<SpawnedTui, Box<dyn std::error::Error>> {
    let pair = native_pty_system().openpty(PtySize {
        rows: 24,
        cols: 100,
        pixel_width: 0,
        pixel_height: 0,
    })?;
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_hermesd"));
    command.args([
        "--state",
        path_text(database)?,
        "tui",
        "--engine",
        "codex",
        "--codex-command",
        path_text(codex)?,
        "--model",
        "gpt-test",
        "--root",
        path_text(root)?,
    ]);
    let child = pair.slave.spawn_command(command)?;
    drop(pair.slave);
    let output = Arc::new(Mutex::new(String::new()));
    let output_reader = Arc::clone(&output);
    let mut reader = pair.master.try_clone_reader()?;
    let reader_thread = thread::spawn(move || {
        let mut bytes = [0_u8; 4096];
        loop {
            match reader.read(&mut bytes) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    if let Ok(mut output) = output_reader.lock() {
                        output.push_str(&String::from_utf8_lossy(&bytes[..read]));
                    }
                }
            }
        }
    });
    let input = pair.master.take_writer()?;
    Ok((child, pair.master, input, output, reader_thread))
}

fn finish_tui(
    child: &mut PtyChild,
    master: PtyMaster,
    input: PtyWriter,
    reader: thread::JoinHandle<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            return Err("native TUI did not exit after idle Ctrl-C".into());
        }
        thread::sleep(Duration::from_millis(20));
    };
    assert!(status.success(), "native TUI exited with {status:?}");
    drop(input);
    drop(master);
    reader.join().map_err(|_| "PTY reader thread panicked")?;
    Ok(())
}

fn wait_for_output(
    output: &Arc<Mutex<String>>,
    needle: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let found = output.lock().map_err(|_| "PTY output lock poisoned")?.contains(needle);
        if found {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let tail = output.lock().map_err(|_| "PTY output lock poisoned")?;
            let start = tail.len().saturating_sub(2_000);
            return Err(format!(
                "timed out waiting for {needle:?}; output tail: {}",
                &tail[start..]
            )
            .into());
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn fake_codex(
    workspace: &Path,
    forbidden_artifact: &Path,
) -> Result<(TempDir, PathBuf), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let executable = directory.path().join("fake-codex");
    let script = format!(
        r#"#!/bin/sh
set -eu
emit() {{ printf '%s\n' "$1"; }}
read_frame() {{ IFS= read -r line || exit 90; }}
require() {{ case "$line" in *"$1"*) ;; *) echo "missing $1: $line" >&2; exit 91 ;; esac; }}
[ "$1" = "app-server" ]
[ "$2" = "--stdio" ]
read_frame
require '"method":"initialize"'
emit '{{"id":1,"result":{{"userAgent":"fake-codex/native-tui","codexHome":"/tmp/fake-home","platformFamily":"unix","platformOs":"macos"}}}}'
read_frame
require '"method":"initialized"'
read_frame
require '"method":"config/read"'
emit '{{"id":2,"result":{{"config":{{"mcp_servers":{{}}}},"origins":{{}},"layers":null}}}}'
read_frame
require '"method":"thread/start"'
require '"name":"terminal"'
emit '{{"id":3,"result":{{"thread":{{"id":"thread-tui"}},"model":"gpt-test","modelProvider":"openai_http","cwd":"{}","approvalPolicy":"never","sandbox":{{"type":"readOnly"}}}}}}'
read_frame
require '"method":"turn/start"'
emit '{{"id":4,"result":{{"turn":{{"id":"turn-tui","status":"inProgress","items":[]}}}}}}'
emit '{{"id":"tool-tui","method":"item/tool/call","params":{{"threadId":"thread-tui","turnId":"turn-tui","callId":"call-tui","namespace":null,"tool":"terminal","arguments":{{"command":"printf forbidden > {}"}}}}}}'
read_frame
require '"id":"tool-tui"'
require '"success":false'
require 'User denied terminal command approval.'
emit '{{"method":"item/agentMessage/delta","params":{{"threadId":"thread-tui","turnId":"turn-tui","itemId":"message-tui","delta":"Denied safely."}}}}'
emit '{{"method":"turn/completed","params":{{"threadId":"thread-tui","turn":{{"id":"turn-tui","status":"completed","items":[]}}}}}}'
if IFS= read -r extra; then exit 92; fi
"#,
        path_text(workspace)?,
        path_text(forbidden_artifact)?,
    );
    fs::write(&executable, script)?;
    let mut permissions = fs::metadata(&executable)?.permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&executable, permissions)?;
    Ok((directory, executable))
}

fn path_text(path: &Path) -> Result<&str, Box<dyn std::error::Error>> {
    path.to_str().ok_or_else(|| "test path is not valid UTF-8".into())
}
