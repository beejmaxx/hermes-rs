//! Hermes Rust agent runtime executable.

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use hermesd::{
    cli::{ChatArgs, GatewayArgs, list_pending_effects, list_sessions, run_chat, run_gateway},
    contracts::{ContractCorpus, scripted_agent_turn},
};
use protocol::ContractKind;

#[derive(Debug, Parser)]
#[command(name = "hermesd", version, about)]
struct Cli {
    /// Override the SQLite state database used by durable sessions.
    #[arg(long, global = true, value_name = "PATH")]
    state: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a live turn, optionally creating or resuming a durable session.
    Chat(ChatArgs),
    /// Serve the minimal Hermes JSON-RPC gateway over newline-delimited stdio.
    Gateway(GatewayArgs),
    /// Inspect durable sessions.
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },
    /// Inspect durable tool-effect records.
    Effect {
        #[command(subcommand)]
        command: EffectCommand,
    },
    /// Inspect the pinned language-neutral behavior contracts.
    Contract {
        #[command(subcommand)]
        command: ContractCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SessionCommand {
    /// List durable sessions in the selected state database.
    List,
}

#[derive(Debug, Subcommand)]
enum EffectCommand {
    /// List plans that do not yet have a terminal or reconciliation disposition.
    Pending,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Chat(arguments) => run_chat(arguments, cli.state.as_deref()).await,
        Command::Gateway(arguments) => run_gateway(arguments, cli.state.as_deref()).await,
        Command::Session { command: SessionCommand::List } => list_sessions(cli.state.as_deref()),
        Command::Effect { command: EffectCommand::Pending } => {
            list_pending_effects(cli.state.as_deref())
        }
        Command::Contract { command: ContractCommand::Check { bundle } } => {
            check_contracts(&bundle)
        }
        Command::Contract { command: ContractCommand::Run { bundle } } => {
            run_contracts(&bundle).await
        }
    }
}

fn check_contracts(bundle: &PathBuf) -> anyhow::Result<()> {
    let corpus = ContractCorpus::load(bundle)
        .with_context(|| format!("contract bundle {} is invalid", bundle.display()))?;
    println!(
        "verified {} fixtures from {} ({})",
        corpus.fixtures().len(),
        corpus.manifest().source_repository,
        corpus.manifest().source_contract_state
    );
    Ok(())
}

async fn run_contracts(bundle: &PathBuf) -> anyhow::Result<()> {
    let corpus = ContractCorpus::load(bundle)
        .with_context(|| format!("contract bundle {} is invalid", bundle.display()))?;
    let mut passed = 0_usize;
    for fixture in
        corpus.fixtures().iter().filter(|fixture| fixture.kind == ContractKind::AgentTurn)
    {
        let scripted = scripted_agent_turn(fixture)
            .with_context(|| format!("fixture {} is invalid", fixture.id))?;
        let mut provider = scripted.provider;
        let mut tools = scripted.tools;
        let actual = runtime::run_turn(scripted.request, &mut provider, &mut tools)
            .await
            .with_context(|| format!("fixture {} failed in the Rust runtime", fixture.id))?;
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
    Ok(())
}
