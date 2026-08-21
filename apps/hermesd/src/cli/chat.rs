//! Live stateless and durable chat command paths.

use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::Context;
use clap::ValueEnum;
use domain::{EngineId, LineageId, ManifestDigest, PromptManifest, SessionId};
use ports::{SessionStore, SessionStoreError};
use protocol::{
    AgentTurnRequest, ContractOutcome, ProviderMessage, SessionConfig, SessionSnapshot,
    TerminalStatus, TransportKind,
};
use runtime::JournaledToolBroker;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::adapters::{
    AgentTools, AgentToolsConfig, OpenAiCompatibleProvider, ReadOnlyLocalTools, SqliteEffectLedger,
    SqliteSessionStore,
};
use crate::cli::state::state_path;

/// Provider and immutable runtime settings shared by live hosts.
#[derive(Clone, Debug, clap::Args)]
pub(super) struct RuntimeArgs {
    /// Provider preset. Defaults to openai for a new or ephemeral session.
    #[arg(long, value_enum)]
    provider: Option<ProviderPreset>,
    /// Provider model identifier. Required for a new or ephemeral session.
    #[arg(long)]
    model: Option<String>,
    /// Override the provider API base URL for a new session.
    #[arg(long)]
    base_url: Option<String>,
    /// Override the environment variable from which the API key is read.
    #[arg(long)]
    api_key_env: Option<String>,
    /// Filesystem root visible to read_file and search_files. Defaults to `.`.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Override the frozen system prompt for a new session.
    #[arg(long)]
    system: Option<String>,
}

/// Arguments for one live chat turn.
#[derive(Debug, clap::Args)]
pub struct ChatArgs {
    /// Durable session to create or resume. Omit for an ephemeral turn.
    #[arg(long)]
    session: Option<String>,
    /// Provider and immutable runtime settings.
    #[command(flatten)]
    runtime: RuntimeArgs,
    /// User prompt. Separate it from options with `--` when it begins with a dash.
    #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
    prompt: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
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

#[derive(Clone)]
pub(super) struct LiveSettings {
    provider: ProviderPreset,
    base_url: String,
    api_key_env: Option<String>,
    model: String,
    root: PathBuf,
    system_prompt: String,
    tools: Vec<Value>,
}

/// Execute one ephemeral or durable live agent turn.
pub async fn run_chat(arguments: ChatArgs, state_override: Option<&Path>) -> anyhow::Result<()> {
    let prompt = arguments.prompt.join(" ");
    if prompt.trim().is_empty() {
        anyhow::bail!("prompt must be non-empty");
    }
    if let Some(session) = arguments.session.clone() {
        run_durable_chat(arguments, &session, &prompt, state_override).await
    } else {
        run_ephemeral_chat(&arguments, &prompt, state_override).await
    }
}

/// Print durable session summaries from the selected state database.
pub fn list_sessions(state_override: Option<&Path>) -> anyhow::Result<()> {
    let state = state_path(state_override)?;
    let mut store = SqliteSessionStore::open(&state)
        .with_context(|| format!("could not open session state {}", state.display()))?;
    let sessions = store.list()?;
    if sessions.is_empty() {
        println!("No durable sessions.");
        return Ok(());
    }
    for session in sessions {
        println!(
            "{}\t{}\t{}\tgeneration={}\tmessages={}",
            session.session_id,
            session.provider_adapter,
            session.model,
            session.owner_generation.get(),
            session.message_count
        );
    }
    Ok(())
}

async fn run_ephemeral_chat(
    arguments: &ChatArgs,
    prompt: &str,
    state_override: Option<&Path>,
) -> anyhow::Result<()> {
    let settings = LiveSettings::for_new(&arguments.runtime)?;
    let scope = live_execution_scope()?;
    let state = state_path(state_override)?;
    let outcome = execute_turn(&settings, Vec::new(), prompt, &scope, &state, None).await?;
    println!("{}", completed_response(&outcome)?);
    Ok(())
}

async fn run_durable_chat(
    arguments: ChatArgs,
    session_name: &str,
    prompt: &str,
    state_override: Option<&Path>,
) -> anyhow::Result<()> {
    let session_id = SessionId::new(session_name)?;
    let state = state_path(state_override)?;
    let mut store = SqliteSessionStore::open(&state)
        .with_context(|| format!("could not open session state {}", state.display()))?;
    let (snapshot, settings) = match store.load(&session_id) {
        Ok(snapshot) => {
            let settings = LiveSettings::for_resume(&arguments.runtime, &snapshot)?;
            (snapshot, settings)
        }
        Err(SessionStoreError::NotFound(_)) => {
            let settings = LiveSettings::for_new(&arguments.runtime)?;
            let config = settings.session_config(session_id)?;
            let snapshot = store.create(config)?;
            (snapshot, settings)
        }
        Err(error) => return Err(error.into()),
    };
    let scope = format!(
        "session:{}:generation:{}",
        snapshot.config.session_id,
        snapshot.owner_generation.get()
    );
    let session_id = snapshot.config.session_id.clone();
    let generation = snapshot.owner_generation;
    let previous_len = snapshot.conversation.len();
    let outcome =
        execute_turn(&settings, snapshot.conversation, prompt, &scope, &state, Some(&session_id))
            .await?;
    let response = completed_response(&outcome)?.to_owned();
    let appended = outcome.semantic_conversation.get(previous_len..).ok_or_else(|| {
        anyhow::anyhow!("runtime returned a conversation shorter than its durable prefix")
    })?;
    store.append(&session_id, generation, appended)?;
    println!("{response}");
    Ok(())
}

pub(super) async fn execute_turn(
    settings: &LiveSettings,
    semantic_history: Vec<domain::SemanticMessage>,
    prompt: &str,
    scope: &str,
    state: &Path,
    session_id: Option<&SessionId>,
) -> anyhow::Result<ContractOutcome> {
    execute_turn_inner(settings, semantic_history, prompt, scope, state, session_id, None).await
}

pub(super) async fn execute_turn_observed(
    settings: &LiveSettings,
    semantic_history: Vec<domain::SemanticMessage>,
    prompt: &str,
    scope: &str,
    state: &Path,
    session_id: Option<&SessionId>,
    observer: &mut dyn runtime::RuntimeEventObserver,
) -> anyhow::Result<ContractOutcome> {
    execute_turn_inner(settings, semantic_history, prompt, scope, state, session_id, Some(observer))
        .await
}

async fn execute_turn_inner(
    settings: &LiveSettings,
    semantic_history: Vec<domain::SemanticMessage>,
    prompt: &str,
    scope: &str,
    state: &Path,
    session_id: Option<&SessionId>,
    observer: Option<&mut dyn runtime::RuntimeEventObserver>,
) -> anyhow::Result<ContractOutcome> {
    let api_key = read_api_key(settings.api_key_env.as_deref())?;
    let mut provider = OpenAiCompatibleProvider::new(&settings.base_url, api_key.clone())?;
    let mut tools_config = AgentToolsConfig::new(
        settings.root.clone(),
        state.to_path_buf(),
        settings.base_url.clone(),
        api_key,
        settings.model.clone(),
        AgentTools::catalog_enables_delegation(&settings.tools),
    )?;
    if AgentTools::catalog_uses_background_delivery(&settings.tools) {
        let parent = session_id
            .context("durable background delegation requires an owning parent session")?;
        tools_config = tools_config.with_background_parent(parent.clone());
    }
    let tools = AgentTools::new(tools_config, scope)?;
    let ledger = SqliteEffectLedger::open(state)
        .with_context(|| format!("could not open effect ledger {}", state.display()))?;
    let mut tools = JournaledToolBroker::new(tools, ledger, scope)?;
    let mut conversation = runtime::project_conversation(&semantic_history)?;
    conversation.push(ProviderMessage::User { content: prompt.into() });
    let request = AgentTurnRequest {
        execution_scope: scope.into(),
        transport: TransportKind::ChatCompletions,
        model: settings.model.clone(),
        system_prompt: Some(settings.system_prompt.clone()),
        conversation,
        tools: settings.tools.clone(),
    };
    match observer {
        Some(observer) => runtime::run_turn_observed(request, &mut provider, &mut tools, observer)
            .await
            .map_err(Into::into),
        None => runtime::run_turn(request, &mut provider, &mut tools).await.map_err(Into::into),
    }
}

pub(super) fn completed_response(outcome: &ContractOutcome) -> anyhow::Result<&str> {
    if outcome.terminal_outcome.status != TerminalStatus::Completed {
        anyhow::bail!(
            "agent turn ended with status {:?}: {}",
            outcome.terminal_outcome.status,
            outcome.terminal_outcome.reason.as_deref().unwrap_or("no provider reason")
        );
    }
    outcome
        .terminal_outcome
        .final_response
        .as_deref()
        .context("completed agent turn had no final response")
}

impl LiveSettings {
    pub(super) fn for_new(arguments: &RuntimeArgs) -> anyhow::Result<Self> {
        Self::for_new_with_tools(arguments, AgentTools::catalog())
    }

    pub(super) fn for_gateway(arguments: &RuntimeArgs) -> anyhow::Result<Self> {
        Self::for_new_with_tools(arguments, AgentTools::background_catalog())
    }

    fn for_new_with_tools(arguments: &RuntimeArgs, tools: Vec<Value>) -> anyhow::Result<Self> {
        let provider = arguments.provider.unwrap_or(ProviderPreset::OpenAi);
        let model = arguments
            .model
            .as_deref()
            .context("--model is required for a new or ephemeral session")?;
        if model.is_empty() || model.trim() != model {
            anyhow::bail!("model must be non-empty and have no surrounding whitespace");
        }
        let root_input = arguments.root.as_deref().unwrap_or_else(|| Path::new("."));
        let root = std::fs::canonicalize(root_input)
            .with_context(|| format!("could not resolve tool root {}", root_input.display()))?;
        let (default_base_url, default_api_key_env) = provider.defaults();
        let base_url = arguments.base_url.as_deref().unwrap_or(default_base_url);
        if base_url.is_empty() {
            anyhow::bail!("--base-url is required with --provider custom");
        }
        OpenAiCompatibleProvider::validate_base_url(base_url)?;
        let _validated_tools = ReadOnlyLocalTools::new(&root, "session-config-validation")?;
        let api_key_env =
            arguments.api_key_env.clone().or_else(|| default_api_key_env.map(str::to_owned));
        let system_prompt = arguments.system.clone().unwrap_or_else(|| {
            format!(
                "You are Hermes RS, a precise and helpful agent. You may inspect the workspace at {} using read_file and search_files. These tools are read-only. You may delegate focused independent subtasks to isolated leaf agents. Never claim to have modified files or run commands.",
                root.display()
            )
        });
        Ok(Self {
            provider,
            base_url: base_url.into(),
            api_key_env,
            model: model.into(),
            root,
            system_prompt,
            tools,
        })
    }

    fn for_resume(arguments: &RuntimeArgs, snapshot: &SessionSnapshot) -> anyhow::Result<Self> {
        let config = &snapshot.config;
        let provider = ProviderPreset::from_adapter(&config.provider_adapter)?;
        if arguments.provider.is_some_and(|value| value != provider) {
            anyhow::bail!("--provider cannot change for an existing session");
        }
        reject_changed("--model", arguments.model.as_deref(), Some(&config.model))?;
        reject_changed("--base-url", arguments.base_url.as_deref(), Some(&config.base_url))?;
        reject_changed(
            "--api-key-env",
            arguments.api_key_env.as_deref(),
            config.api_key_env.as_deref(),
        )?;
        reject_changed("--system", arguments.system.as_deref(), Some(&config.system_prompt))?;
        let root = PathBuf::from(&config.tool_root);
        if let Some(supplied) = &arguments.root {
            let supplied = std::fs::canonicalize(supplied)
                .with_context(|| format!("could not resolve tool root {}", supplied.display()))?;
            if supplied != root {
                anyhow::bail!("--root cannot change for an existing session");
            }
        }
        Ok(Self {
            provider,
            base_url: config.base_url.clone(),
            api_key_env: config.api_key_env.clone(),
            model: config.model.clone(),
            root,
            system_prompt: config.system_prompt.clone(),
            tools: config.tools.clone(),
        })
    }

    pub(super) fn from_snapshot(snapshot: &SessionSnapshot) -> anyhow::Result<Self> {
        let config = &snapshot.config;
        Ok(Self {
            provider: ProviderPreset::from_adapter(&config.provider_adapter)?,
            base_url: config.base_url.clone(),
            api_key_env: config.api_key_env.clone(),
            model: config.model.clone(),
            root: PathBuf::from(&config.tool_root),
            system_prompt: config.system_prompt.clone(),
            tools: config.tools.clone(),
        })
    }

    pub(super) fn session_config(&self, session_id: SessionId) -> anyhow::Result<SessionConfig> {
        let tools_bytes = serde_json::to_vec(&self.tools)?;
        let engine = EngineId::new(format!(
            "rust-v1:chat-completions:{}:{}",
            self.provider.as_str(),
            self.model
        ))?;
        let root = self.root.to_str().context("tool root is not valid UTF-8")?.to_owned();
        Ok(SessionConfig {
            lineage_id: LineageId::new(session_id.as_str())?,
            session_id,
            prompt_manifest: PromptManifest::new(
                1,
                engine,
                ManifestDigest::new(digest(self.system_prompt.as_bytes()))?,
                ManifestDigest::new(digest(&tools_bytes))?,
            )?,
            transport: TransportKind::ChatCompletions,
            provider_adapter: self.provider.as_str().into(),
            base_url: self.base_url.clone(),
            api_key_env: self.api_key_env.clone(),
            model: self.model.clone(),
            tool_root: root,
            system_prompt: self.system_prompt.clone(),
            tools: self.tools.clone(),
        })
    }

    pub(super) fn model(&self) -> &str {
        &self.model
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) fn api_key_env(&self) -> Option<&str> {
        self.api_key_env.as_deref()
    }
}

impl ProviderPreset {
    const fn defaults(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::OpenAi => ("https://api.openai.com/v1", Some("OPENAI_API_KEY")),
            Self::OpenRouter => ("https://openrouter.ai/api/v1", Some("OPENROUTER_API_KEY")),
            Self::Custom => ("", None),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::OpenRouter => "openrouter",
            Self::Custom => "custom",
        }
    }

    fn from_adapter(value: &str) -> anyhow::Result<Self> {
        match value {
            "openai" => Ok(Self::OpenAi),
            "openrouter" => Ok(Self::OpenRouter),
            "custom" => Ok(Self::Custom),
            other => anyhow::bail!("session uses unsupported provider adapter {other:?}"),
        }
    }
}

fn reject_changed(name: &str, supplied: Option<&str>, frozen: Option<&str>) -> anyhow::Result<()> {
    if let Some(supplied) = supplied
        && Some(supplied) != frozen
    {
        anyhow::bail!("{name} cannot change for an existing session");
    }
    Ok(())
}

fn read_api_key(variable: Option<&str>) -> anyhow::Result<Option<String>> {
    variable
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
        .transpose()
}

fn live_execution_scope() -> anyhow::Result<String> {
    let nanos = SystemTime::UNIX_EPOCH
        .elapsed()
        .context("system clock is earlier than the Unix epoch")?
        .as_nanos();
    Ok(format!("cli-{}-{nanos}", std::process::id()))
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
