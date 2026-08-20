//! Developer CLI for the Hermes Rust kernel.

use std::{path::PathBuf, time::SystemTime};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use local_tools::ReadOnlyLocalTools;
use protocol::{AgentTurnRequest, ContractKind, ProviderMessage, TransportKind};
use providers::OpenAiCompatibleProvider;
use testkit::{ContractCorpus, scripted_agent_turn};

#[derive(Debug, Parser)]
#[command(name = "hermes", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run one live agent turn with root-confined read-only tools.
    Chat(ChatArgs),
    /// Inspect the pinned language-neutral behavior contracts.
    Contract {
        #[command(subcommand)]
        command: ContractCommand,
    },
}

#[derive(Debug, clap::Args)]
struct ChatArgs {
    /// Provider preset controlling the default base URL and credential variable.
    #[arg(long, value_enum, default_value_t = ProviderPreset::OpenAi)]
    provider: ProviderPreset,
    /// Provider model identifier.
    #[arg(long)]
    model: String,
    /// Override the provider API base URL.
    #[arg(long)]
    base_url: Option<String>,
    /// Override the environment variable from which the API key is read.
    #[arg(long)]
    api_key_env: Option<String>,
    /// Filesystem root visible to read_file and search_files.
    #[arg(long, default_value = ".")]
    root: PathBuf,
    /// Override the frozen system prompt for this turn.
    #[arg(long)]
    system: Option<String>,
    /// User prompt. Separate it from options with `--` when it begins with a dash.
    #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
    prompt: Vec<String>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProviderPreset {
    /// OpenAI at api.openai.com, using OPENAI_API_KEY.
    #[value(name = "openai")]
    OpenAi,
    /// OpenRouter at openrouter.ai, using OPENROUTER_API_KEY.
    #[value(name = "openrouter")]
    OpenRouter,
    /// Any OpenAI-compatible endpoint supplied with --base-url.
    Custom,
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
    match Cli::parse().command {
        Command::Chat(arguments) => run_chat(arguments).await,
        Command::Contract { command: ContractCommand::Check { bundle } } => {
            check_contracts(&bundle)
        }
        Command::Contract { command: ContractCommand::Run { bundle } } => {
            run_contracts(&bundle).await
        }
    }
}

async fn run_chat(arguments: ChatArgs) -> anyhow::Result<()> {
    let (base_url, api_key) = provider_settings(
        arguments.provider,
        arguments.base_url.as_deref(),
        arguments.api_key_env.as_deref(),
    )?;
    let root = std::fs::canonicalize(&arguments.root)
        .with_context(|| format!("could not resolve tool root {}", arguments.root.display()))?;
    let scope = live_execution_scope()?;
    let mut provider = OpenAiCompatibleProvider::new(&base_url, api_key)?;
    let mut tools = ReadOnlyLocalTools::new(&root, &scope)?;
    let system_prompt = arguments.system.unwrap_or_else(|| {
        format!(
            "You are Hermes RS, a precise and helpful agent. You may inspect the workspace at {} using read_file and search_files. These tools are read-only. Never claim to have modified files or run commands.",
            root.display()
        )
    });
    let request = AgentTurnRequest {
        execution_scope: scope,
        transport: TransportKind::ChatCompletions,
        model: arguments.model,
        system_prompt: Some(system_prompt),
        conversation: vec![ProviderMessage::User { content: arguments.prompt.join(" ") }],
        tools: ReadOnlyLocalTools::catalog(),
    };
    let outcome = runtime::run_turn(request, &mut provider, &mut tools).await?;
    if let Some(response) = outcome.terminal_outcome.final_response {
        println!("{response}");
        return Ok(());
    }
    anyhow::bail!(
        "agent turn ended with status {:?}: {}",
        outcome.terminal_outcome.status,
        outcome.terminal_outcome.reason.as_deref().unwrap_or("no provider reason")
    )
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

fn provider_settings(
    preset: ProviderPreset,
    base_url_override: Option<&str>,
    api_key_env_override: Option<&str>,
) -> anyhow::Result<(String, Option<String>)> {
    let (default_base_url, default_api_key_env) = match preset {
        ProviderPreset::OpenAi => ("https://api.openai.com/v1", Some("OPENAI_API_KEY")),
        ProviderPreset::OpenRouter => ("https://openrouter.ai/api/v1", Some("OPENROUTER_API_KEY")),
        ProviderPreset::Custom => ("", None),
    };
    let base_url = base_url_override.unwrap_or(default_base_url);
    if base_url.is_empty() {
        anyhow::bail!("--base-url is required with --provider custom");
    }
    let api_key_env = api_key_env_override.or(default_api_key_env);
    let api_key = api_key_env
        .map(|name| {
            std::env::var(name)
                .with_context(|| {
                    format!("provider credential environment variable {name} is not set")
                })
                .and_then(|value| {
                    if value.is_empty() {
                        anyhow::bail!("provider credential environment variable {name} is empty");
                    }
                    Ok(value)
                })
        })
        .transpose()?;
    Ok((base_url.into(), api_key))
}

fn live_execution_scope() -> anyhow::Result<String> {
    let nanos = SystemTime::UNIX_EPOCH
        .elapsed()
        .context("system clock is earlier than the Unix epoch")?
        .as_nanos();
    Ok(format!("cli-{}-{nanos}", std::process::id()))
}
