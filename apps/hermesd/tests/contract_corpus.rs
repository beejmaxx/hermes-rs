//! End-to-end conformance against the pinned Python-oracle agent turns.

use std::path::PathBuf;

use futures_executor::block_on;
use hermesd::contracts::{ContractCorpus, scripted_agent_turn};
use protocol::ContractKind;
use serde_json::Value;

#[derive(Default)]
struct RecordingObserver {
    events: Vec<Value>,
}

impl runtime::RuntimeEventObserver for RecordingObserver {
    fn observe(&mut self, event: &Value) -> Result<(), runtime::RuntimeEventObserverError> {
        self.events.push(event.clone());
        Ok(())
    }
}

fn contract_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts/hermes-v1")
}

#[test]
fn rust_runtime_matches_every_pinned_agent_turn() -> Result<(), Box<dyn std::error::Error>> {
    let corpus = ContractCorpus::load(contract_root())?;
    let fixtures = corpus
        .fixtures()
        .iter()
        .filter(|fixture| fixture.kind == ContractKind::AgentTurn)
        .collect::<Vec<_>>();
    assert!(!fixtures.is_empty(), "the pinned corpus must contain agent-turn behavior");

    for fixture in fixtures {
        let scripted = scripted_agent_turn(fixture)?;
        let mut provider = scripted.provider;
        let mut tools = scripted.tools;
        let mut observer = RecordingObserver::default();
        let actual = block_on(runtime::run_turn_observed(
            scripted.request,
            &mut provider,
            &mut tools,
            &mut observer,
        ))?;
        assert_eq!(
            observer.events, actual.public_events,
            "fixture {} observer diverged",
            fixture.id
        );
        assert_eq!(actual, fixture.expected, "fixture {} diverged", fixture.id);
    }

    Ok(())
}
