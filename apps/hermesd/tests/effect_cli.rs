//! CLI visibility for write-ahead effect records left pending by interruption.

use std::{collections::BTreeMap, process::Command};

use domain::{PlannedToolCall, ToolArguments, ToolCallId, ToolEffect};
use hermesd::adapters::SqliteEffectLedger;
use ports::EffectLedger;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn pending_command_reports_a_durable_plan() -> Result<(), Box<dyn std::error::Error>> {
    let state_dir = tempdir()?;
    let database = state_dir.path().join("state.db");
    let plan = PlannedToolCall {
        call_id: ToolCallId::new("call-read")?,
        name: "read_file".into(),
        arguments: ToolArguments(BTreeMap::from([("path".into(), json!("README.md"))])),
        execution_key: "session:demo:generation:1:call-read".into(),
        effect: ToolEffect::ReadOnly,
        approval: None,
    };
    let mut ledger = SqliteEffectLedger::open(&database)?;
    ledger.record_plans("session:demo:generation:1", &[plan])?;
    drop(ledger);

    let output = Command::new(env!("CARGO_BIN_EXE_hermesd"))
        .args([
            "--state",
            database.to_str().ok_or("test path is not valid UTF-8")?,
            "effect",
            "pending",
        ])
        .output()?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "session:demo:generation:1:call-read\tsession:demo:generation:1\tread_file\tread_only\n"
    );
    Ok(())
}
