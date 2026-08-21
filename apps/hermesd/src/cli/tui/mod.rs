mod client;
mod model;
mod render;

use std::{io, path::Path};

use anyhow::Context;
use client::GatewayClient;
use crossterm::{
    event::{
        DisableBracketedPaste, EnableBracketedPaste, Event, EventStream, KeyCode, KeyEvent,
        KeyEventKind, KeyModifiers,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures_util::StreamExt;
use model::{AppModel, ModelAction, PendingRequest, Screen};
use ratatui::{Terminal, backend::CrosstermBackend};
use serde_json::{Value, json};

use super::{chat::RuntimeArgs, state::state_path};

/// Arguments for the native Rust terminal client and its supervised gateway.
#[derive(Debug, clap::Args)]
pub struct TuiArgs {
    /// Provider and immutable runtime settings forwarded literally to the gateway child.
    #[command(flatten)]
    runtime: RuntimeArgs,
}

/// Run the engine-neutral native terminal client.
pub async fn run_tui(arguments: TuiArgs, state_override: Option<&Path>) -> anyhow::Result<()> {
    let state = state_path(state_override)?;
    let mut client = GatewayClient::spawn(&state, arguments.runtime.gateway_argv())?;
    let _restore = TerminalRestore;
    enable_raw_mode().context("could not enable terminal raw mode")?;
    execute!(io::stdout(), EnterAlternateScreen, EnableBracketedPaste)
        .context("could not enter alternate screen")?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let run_result = run_loop(&mut terminal, &mut client).await;
    let shutdown_result = client.shutdown().await;
    run_result.and(shutdown_result)
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    client: &mut GatewayClient,
) -> anyhow::Result<()> {
    let mut model = AppModel::default();
    track_request(&mut model, client, PendingRequest::List, "session.list", json!({})).await?;
    let mut terminal_events = EventStream::new();
    loop {
        terminal.draw(|frame| render::render(frame, &model))?;
        tokio::select! {
            frame = client.next_frame() => {
                let actions = model.apply_frame(frame?)?;
                for action in actions {
                    apply_model_action(&mut model, client, action).await?;
                }
            }
            event = terminal_events.next() => {
                let event = event.context("terminal event stream closed")??;
                if handle_terminal_event(&mut model, client, event).await? {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn handle_terminal_event(
    model: &mut AppModel,
    client: &mut GatewayClient,
    event: Event,
) -> anyhow::Result<bool> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => handle_key(model, client, key).await,
        Event::Paste(text) if model.screen == Screen::Chat && !model.busy => {
            model.draft.push_str(&text.replace(['\r', '\n'], " "));
            Ok(false)
        }
        _ => Ok(false),
    }
}

async fn handle_key(
    model: &mut AppModel,
    client: &mut GatewayClient,
    key: KeyEvent,
) -> anyhow::Result<bool> {
    let ctrl_c = key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL);
    match model.screen {
        Screen::Loading => Ok(ctrl_c),
        Screen::Sessions => handle_session_key(model, client, key, ctrl_c).await,
        Screen::Chat => handle_chat_key(model, client, key, ctrl_c).await,
    }
}

async fn handle_session_key(
    model: &mut AppModel,
    client: &mut GatewayClient,
    key: KeyEvent,
    ctrl_c: bool,
) -> anyhow::Result<bool> {
    match key.code {
        KeyCode::Char('q') if key.modifiers.is_empty() => return Ok(true),
        _ if ctrl_c => return Ok(true),
        KeyCode::Up => {
            model.selected_session = model.selected_session.saturating_sub(1);
        }
        KeyCode::Down => {
            model.selected_session = model
                .selected_session
                .saturating_add(1)
                .min(model.sessions.len().saturating_sub(1));
        }
        KeyCode::Enter if !model.sessions.is_empty() => {
            let session_id = model.sessions[model.selected_session].id.clone();
            resume(model, client, session_id).await?;
        }
        KeyCode::Char('n') if key.modifiers.is_empty() => {
            model.screen = Screen::Loading;
            model.status = "creating session".into();
            track_request(model, client, PendingRequest::Create, "session.create", json!({}))
                .await?;
        }
        _ => {}
    }
    Ok(false)
}

async fn handle_chat_key(
    model: &mut AppModel,
    client: &mut GatewayClient,
    key: KeyEvent,
    ctrl_c: bool,
) -> anyhow::Result<bool> {
    if model.approval.is_some() {
        return handle_approval_key(model, client, key, ctrl_c).await;
    }
    if ctrl_c {
        if !model.draft.is_empty() {
            model.draft.clear();
            return Ok(false);
        }
        if model.busy {
            interrupt(model, client).await?;
            return Ok(false);
        }
        return Ok(true);
    }
    match key.code {
        KeyCode::Esc if model.busy => interrupt(model, client).await?,
        KeyCode::F(2) if !model.busy => {
            model.screen = Screen::Loading;
            model.status = "loading sessions".into();
            track_request(model, client, PendingRequest::List, "session.list", json!({})).await?;
        }
        KeyCode::PageUp => model.scroll = model.scroll.saturating_add(8),
        KeyCode::PageDown => model.scroll = model.scroll.saturating_sub(8),
        KeyCode::Backspace if !model.busy => {
            model.draft.pop();
        }
        KeyCode::Enter if !model.busy && !model.draft.trim().is_empty() => {
            let session_id = active_session(model)?.to_owned();
            let text = std::mem::take(&mut model.draft);
            let params = json!({"session_id": session_id, "text": text});
            model.begin_submit(text);
            track_request(model, client, PendingRequest::Submit, "prompt.submit", params).await?;
        }
        KeyCode::Char(character)
            if !model.busy && !key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            model.draft.push(character);
        }
        _ => {}
    }
    Ok(false)
}

async fn handle_approval_key(
    model: &mut AppModel,
    client: &mut GatewayClient,
    key: KeyEvent,
    ctrl_c: bool,
) -> anyhow::Result<bool> {
    let approval = model.approval.as_mut().context("approval disappeared")?;
    match key.code {
        KeyCode::Left | KeyCode::Up => approval.selected = 0,
        KeyCode::Right | KeyCode::Down => approval.selected = 1,
        KeyCode::Char('y') if key.modifiers.is_empty() => {
            respond_approval(model, client, "once").await?
        }
        KeyCode::Char('n') | KeyCode::Esc if key.modifiers.is_empty() => {
            respond_approval(model, client, "deny").await?
        }
        KeyCode::Enter => {
            let choice = if approval.selected == 0 { "once" } else { "deny" };
            respond_approval(model, client, choice).await?;
        }
        _ if ctrl_c => interrupt(model, client).await?,
        _ => {}
    }
    Ok(false)
}

async fn respond_approval(
    model: &mut AppModel,
    client: &mut GatewayClient,
    choice: &str,
) -> anyhow::Result<()> {
    let session_id = active_session(model)?.to_owned();
    track_request(
        model,
        client,
        PendingRequest::Approval,
        "approval.respond",
        json!({"session_id": session_id, "choice": choice}),
    )
    .await
}

async fn interrupt(model: &mut AppModel, client: &mut GatewayClient) -> anyhow::Result<()> {
    if model.interrupting {
        return Ok(());
    }
    let session_id = active_session(model)?.to_owned();
    model.interrupting = true;
    model.status = "interrupting".into();
    track_request(
        model,
        client,
        PendingRequest::Interrupt,
        "session.interrupt",
        json!({"session_id": session_id}),
    )
    .await
}

async fn apply_model_action(
    model: &mut AppModel,
    client: &mut GatewayClient,
    action: ModelAction,
) -> anyhow::Result<()> {
    match action {
        ModelAction::Resume(session_id) => resume(model, client, session_id).await,
        ModelAction::ReloadActive => {
            let session_id = active_session(model)?.to_owned();
            resume(model, client, session_id).await
        }
    }
}

async fn resume(
    model: &mut AppModel,
    client: &mut GatewayClient,
    session_id: String,
) -> anyhow::Result<()> {
    model.screen = Screen::Loading;
    model.status = "loading canonical session".into();
    track_request(
        model,
        client,
        PendingRequest::Resume,
        "session.resume",
        json!({"session_id": session_id}),
    )
    .await
}

async fn track_request(
    model: &mut AppModel,
    client: &mut GatewayClient,
    pending: PendingRequest,
    method: &str,
    params: Value,
) -> anyhow::Result<()> {
    let id = client.request(method, params).await?;
    model.track(id, pending);
    Ok(())
}

fn active_session(model: &AppModel) -> anyhow::Result<&str> {
    model.active_session.as_deref().context("no active session")
}

struct TerminalRestore;

impl Drop for TerminalRestore {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), DisableBracketedPaste, LeaveAlternateScreen);
    }
}
