//! Offline developer CLI for the Hermes Rust sketch.

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use futures_executor::block_on;
use protocol::ContractKind;
use testkit::{ContractCorpus, scripted_agent_turn};

#[derive(Debug, Parser)]
#[command(name = "hermes", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect the pinned language-neutral behavior contracts.
    Contract {
        #[command(subcommand)]
        command: ContractCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ContractCommand {
    /// Verify checksums, schemas, and strongly typed golden records.
    Check {
        /// Directory containing SOURCE.json and the fixtures directory.
        bundle: PathBuf,
    },
    /// Execute every agent-turn fixture through the Rust runtime.
    Run {
        /// Directory containing SOURCE.json and the fixtures directory.
        bundle: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Contract { command: ContractCommand::Check { bundle } } => {
            let corpus = ContractCorpus::load(&bundle)
                .with_context(|| format!("contract bundle {} is invalid", bundle.display()))?;
            println!(
                "verified {} fixtures from {} ({})",
                corpus.fixtures().len(),
                corpus.manifest().source_repository,
                corpus.manifest().source_contract_state
            );
        }
        Command::Contract { command: ContractCommand::Run { bundle } } => {
            let corpus = ContractCorpus::load(&bundle)
                .with_context(|| format!("contract bundle {} is invalid", bundle.display()))?;
            let mut passed = 0_usize;
            for fixture in
                corpus.fixtures().iter().filter(|fixture| fixture.kind == ContractKind::AgentTurn)
            {
                let scripted = scripted_agent_turn(fixture)
                    .with_context(|| format!("fixture {} is invalid", fixture.id))?;
                let mut provider = scripted.provider;
                let mut tools = scripted.tools;
                let actual =
                    block_on(runtime::run_turn(scripted.request, &mut provider, &mut tools))
                        .with_context(|| {
                            format!("fixture {} failed in the Rust runtime", fixture.id)
                        })?;
                if actual != fixture.expected {
                    anyhow::bail!(
                        "fixture {} diverged\nexpected:\n{}\nactual:\n{}",
                        fixture.id,
                        serde_json::to_string_pretty(&fixture.expected)?,
                        serde_json::to_string_pretty(&actual)?,
                    );
                }
                passed += 1;
                println!("✓ {}", fixture.id);
            }
            println!("\n{passed} agent-turn contracts passed");
        }
    }
    Ok(())
}
