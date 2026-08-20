//! End-to-end conformance against the pinned Python-oracle agent turns.

use std::path::PathBuf;

use futures_executor::block_on;
use protocol::ContractKind;
use testkit::{ContractCorpus, scripted_agent_turn};

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
        let actual = block_on(runtime::run_turn(scripted.request, &mut provider, &mut tools))?;
        assert_eq!(actual, fixture.expected, "fixture {} diverged", fixture.id);
    }

    Ok(())
}
