use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};

use super::model::{AppModel, Screen};

const ACCENT: Color = Color::Cyan;

pub(super) fn render(frame: &mut Frame<'_>, model: &AppModel) {
    match model.screen {
        Screen::Loading => render_loading(frame, model),
        Screen::Sessions => render_sessions(frame, model),
        Screen::Chat => render_chat(frame, model),
    }
}

fn render_loading(frame: &mut Frame<'_>, model: &AppModel) {
    let paragraph = Paragraph::new(model.status.as_str())
        .block(Block::default().borders(Borders::ALL).title(" Hermes RS "))
        .alignment(ratatui::layout::Alignment::Center);
    frame.render_widget(paragraph, centered(frame.area(), 54, 5));
}

fn render_sessions(frame: &mut Frame<'_>, model: &AppModel) {
    let area = centered(frame.area(), 70, 18);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(4), Constraint::Length(2)])
        .split(area);
    let items = if model.sessions.is_empty() {
        vec![ListItem::new("No durable sessions yet")]
    } else {
        model
            .sessions
            .iter()
            .enumerate()
            .map(|(index, session)| {
                let marker = if index == model.selected_session { "›" } else { " " };
                let style = if index == model.selected_session {
                    Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                ListItem::new(format!(
                    "{marker} {}  ({} messages)",
                    session.id, session.message_count
                ))
                .style(style)
            })
            .collect()
    };
    frame.render_widget(
        List::new(items).block(Block::default().borders(Borders::ALL).title(" Sessions ")),
        chunks[0],
    );
    frame.render_widget(
        Paragraph::new("↑/↓ select  Enter resume  n new  q quit")
            .style(Style::default().fg(Color::DarkGray)),
        chunks[1],
    );
}

fn render_chat(frame: &mut Frame<'_>, model: &AppModel) {
    let input_height = if model.approval.is_some() { 8 } else { 3 };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(4), Constraint::Length(input_height), Constraint::Length(1)])
        .split(frame.area());
    render_conversation(frame, model, chunks[0]);
    if let Some(approval) = &model.approval {
        let choices = ["Allow once", "Deny"];
        let choice_spans = choices
            .iter()
            .enumerate()
            .map(|(index, choice)| {
                if index == approval.selected {
                    Span::styled(
                        format!("  [{choice}]  "),
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    )
                } else {
                    Span::raw(format!("  {choice}  "))
                }
            })
            .collect::<Vec<_>>();
        let approval_text = vec![
            Line::from(Span::styled(
                approval.description.clone(),
                Style::default().fg(Color::Yellow),
            )),
            Line::from(approval.command.clone()),
            Line::from(""),
            Line::from(choice_spans),
        ];
        frame.render_widget(
            Paragraph::new(approval_text)
                .block(Block::default().borders(Borders::ALL).title(" Approval required "))
                .wrap(Wrap { trim: false }),
            chunks[1],
        );
    } else {
        let available = usize::from(chunks[1].width.saturating_sub(3));
        let prompt = if model.busy {
            " agent is working".into()
        } else {
            input_tail(&model.draft, available)
        };
        frame.render_widget(
            Paragraph::new(format!(">{prompt}"))
                .block(Block::default().borders(Borders::ALL).title(" Message ")),
            chunks[1],
        );
        if !model.busy {
            let cursor = u16::try_from(prompt.chars().count()).unwrap_or(u16::MAX);
            frame.set_cursor_position((
                chunks[1].x.saturating_add(2).saturating_add(cursor),
                chunks[1].y + 1,
            ));
        }
    }
    let session = model.active_session.as_deref().unwrap_or("no session");
    let error = model.error.as_deref().map(|error| format!(" | {error}")).unwrap_or_default();
    let keys =
        if model.busy { "Esc/Ctrl-C interrupt" } else { "Enter send | F2 sessions | Ctrl-C quit" };
    let footer =
        format!(" {session} | {} {} | {} | {keys}{error}", model.engine, model.model, model.status);
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(if model.error.is_some() {
            Color::Red
        } else {
            Color::DarkGray
        })),
        chunks[2],
    );
}

fn input_tail(value: &str, capacity: usize) -> String {
    let characters = value.chars().collect::<Vec<_>>();
    characters[characters.len().saturating_sub(capacity)..].iter().collect()
}

fn render_conversation(frame: &mut Frame<'_>, model: &AppModel, area: Rect) {
    let mut lines = Vec::new();
    for message in &model.messages {
        let (label, color) = match message.role.as_str() {
            "user" => ("You", Color::Green),
            "assistant" => ("Agent", ACCENT),
            "tool" => ("Tool", Color::Magenta),
            _ => (message.role.as_str(), Color::DarkGray),
        };
        lines.push(Line::from(Span::styled(
            format!("{label}:"),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        )));
        lines.extend(message.text.lines().map(|line| Line::from(format!("  {line}"))));
        lines.push(Line::from(""));
    }
    for tool in &model.tools {
        lines.push(Line::from(vec![
            Span::styled("• ", Style::default().fg(Color::Magenta)),
            Span::styled(tool.name.clone(), Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(" ({})", tool.status)),
        ]));
    }
    if !model.streaming.is_empty() {
        lines.push(Line::from(Span::styled(
            "Agent:",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        )));
        lines.extend(model.streaming.lines().map(|line| Line::from(format!("  {line}"))));
    }
    let viewport = usize::from(area.height.saturating_sub(2));
    let max_top = lines.len().saturating_sub(viewport);
    let top = max_top.saturating_sub(usize::from(model.scroll));
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(" Conversation "))
            .wrap(Wrap { trim: false })
            .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0)),
        area,
    );
}

fn centered(area: Rect, width_percent: u16, height: u16) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(height.min(area.height)),
            Constraint::Fill(1),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage((100 - width_percent) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

#[cfg(test)]
mod tests {
    use ratatui::{Terminal, backend::TestBackend};

    use super::render;
    use crate::cli::tui::model::{AppModel, Screen, TranscriptMessage};

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn chat_projection_exposes_effects_and_interrupt_affordance() -> anyhow::Result<()> {
        let mut model = AppModel::default();
        model.screen = Screen::Chat;
        model.active_session = Some("session-a".into());
        model.engine = "codex".into();
        model.model = "gpt-test".into();
        model.busy = true;
        model.messages.push(TranscriptMessage { role: "user".into(), text: "hello".into() });
        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|frame| render(frame, &model))?;
        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("You:"));
        assert!(rendered.contains("Esc/Ctrl-C interrupt"));
        Ok(())
    }
}
