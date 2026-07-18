//! helios-cmdb TUI — ratatui-based topology browser.
//!
//! Layout:
//!   ┌─ Entities ──────┬─ Detail ──────────────────────────┐
//!   │ • fleet.host/.. │ attrs:  { ... }                   │
//!   │ • fleet.agent/. │ facts:  load_1 = 0.42             │
//!   │ • infra.pod/... │ rels:   runs_on → fleet.host/xyz  │
//!   │ ...             │                                    │
//!   ├─────────────────┴──────────────────────────────────┤
//!   │ j/k move · /search · t traverse · c cypher · ? help │
//!   └────────────────────────────────────────────────────┘

use anyhow::Result;
use cmdb_core::entity::Entity;
use cmdb_core::store::QueryFilter;
use cmdb_core::Store;
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Terminal;
use std::io::Stdout;
use std::sync::Arc;

pub async fn run(store: Arc<dyn Store>, namespace: String) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = ratatui::backend::CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(store, namespace);
    app.refresh().await;

    let result = run_loop(&mut terminal, &mut app).await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

#[derive(Default)]
struct App {
    store: Option<Arc<dyn Store>>,
    namespace: String,
    entities: Vec<Entity>,
    selected: ListState,
    filter_type: Option<String>,
    search_query: String,
    detail_lines: Vec<String>,
    cypher_input: String,
    cypher_output: Vec<Vec<String>>,
    mode: Mode,
    message: String,
    show_help: bool,
}

#[derive(Default, PartialEq, Clone, Copy)]
enum Mode {
    #[default]
    Browse,
    Search,
    TypeFilter,
    Cypher,
}

impl App {
    fn new(store: Arc<dyn Store>, namespace: String) -> Self {
        let mut s = Self {
            store: Some(store),
            namespace,
            ..Default::default()
        };
        s.selected.select(Some(0));
        s
    }

    async fn refresh(&mut self) {
        if let Some(store) = &self.store {
            let mut filter =
                QueryFilter::new().in_namespace(&self.namespace).with_limit(200);
            if let Some(t) = &self.filter_type {
                filter = filter.of_type(t);
            }
            match store.query_entities(filter).await {
                Ok(es) => self.entities = es,
                Err(e) => {
                    self.entities.clear();
                    self.message = format!("load error: {e}");
                }
            }
            self.update_detail().await;
        }
    }

    async fn update_detail(&mut self) {
        let Some(idx) = self.selected.selected() else {
            self.detail_lines.clear();
            return;
        };
        let Some(entity) = self.entities.get(idx).cloned() else {
            self.detail_lines.clear();
            return;
        };
        let mut lines = vec![
            format!("id:          {}", entity.id),
            format!("type:        {}", entity.entity_type),
            format!("name:        {}", entity.name),
            format!("namespace:   {}", entity.namespace),
            format!("version:     {}", entity.version),
            format!("tags:        {}", entity.tags.iter().cloned().collect::<Vec<_>>().join(", ")),
            String::new(),
            "attrs:".into(),
        ];
        if let serde_json::Value::Object(m) = &entity.attrs {
            for (k, v) in m {
                lines.push(format!("  {} = {}", k, v));
            }
        } else {
            lines.push(format!("  {}", entity.attrs));
        }

        if let Some(store) = &self.store {
            lines.push(String::new());
            lines.push("facts:".into());
            match store.effective_facts(entity.id, Default::default()).await {
                Ok(facts) => {
                    if facts.is_empty() {
                        lines.push("  (none)".into());
                    }
                    for f in facts.iter().take(10) {
                        lines.push(format!(
                            "  {} = {} (conf {:.2}, src {})",
                            f.key, f.value, f.source.confidence, f.source.identity
                        ));
                    }
                }
                Err(e) => lines.push(format!("  error: {e}")),
            }

            lines.push(String::new());
            lines.push("relations (outgoing):".into());
            use cmdb_core::store::{Direction, TraverseStep};
            let step = TraverseStep {
                relation_type: None,
                direction: Direction::Outgoing,
                max_depth: 1,
            };
            match store.traverse(entity.id, step).await {
                Ok(hits) => {
                    if hits.is_empty() {
                        lines.push("  (none)".into());
                    }
                    for h in hits.iter() {
                        lines.push(format!(
                            "  --{:>10}--> {}/{}",
                            h.via_relation_type.as_deref().unwrap_or("?"),
                            h.entity.entity_type,
                            h.entity.name
                        ));
                    }
                }
                Err(e) => lines.push(format!("  error: {e}")),
            }
        }

        self.detail_lines = lines;
    }

    fn move_selection(&mut self, delta: i32) {
        let len = self.entities.len();
        if len == 0 {
            return;
        }
        let cur = self.selected.selected().unwrap_or(0) as i32;
        let next = ((cur + delta).rem_euclid(len as i32)) as usize;
        self.selected.select(Some(next));
    }
}

async fn run_loop(
    terminal: &mut Terminal<ratatui::backend::CrosstermBackend<Stdout>>,
    app: &mut App,
) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f, app))?;

        if !event::poll(std::time::Duration::from_millis(100))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if handle_key(app, key).await {
            return Ok(());
        }
    }
}

async fn handle_key(app: &mut App, key: KeyEvent) -> bool {
    match app.mode {
        Mode::Browse => handle_browse_key(app, key).await,
        Mode::Search => handle_search_key(app, key),
        Mode::TypeFilter => handle_filter_key(app, key),
        Mode::Cypher => handle_cypher_key(app, key).await,
    }
}

async fn handle_browse_key(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Char('q') => return true,
        KeyCode::Char('j') | KeyCode::Down => app.move_selection(1),
        KeyCode::Char('k') | KeyCode::Up => app.move_selection(-1),
        KeyCode::Char('G') => app.selected.select(Some(app.entities.len().saturating_sub(1))),
        KeyCode::Char('g') => app.selected.select(Some(0)),
        KeyCode::Char('r') => {
            app.message = "refreshing...".into();
            app.refresh().await;
            app.message.clear();
        }
        KeyCode::Char('/') => {
            app.mode = Mode::Search;
            app.search_query.clear();
        }
        KeyCode::Char('t') | KeyCode::Enter => {
            // Traverse from selected: filter list to neighbors.
            if let Some(idx) = app.selected.selected() {
                if let Some(store) = &app.store {
                    if let Some(entity) = app.entities.get(idx).cloned() {
                        use cmdb_core::store::{Direction, TraverseStep};
                        let step = TraverseStep {
                            relation_type: None,
                            direction: Direction::Both,
                            max_depth: 2,
                        };
                        match store.traverse(entity.id, step).await {
                            Ok(hits) => {
                                let mut nearby: Vec<Entity> = hits.into_iter().map(|h| h.entity).collect();
                                nearby.insert(0, entity);
                                app.entities = nearby;
                                app.selected.select(Some(0));
                                app.message = "showing 2-hop neighborhood; press 'r' to reset".into();
                                app.update_detail().await;
                            }
                            Err(e) => app.message = format!("traverse error: {e}"),
                        }
                    }
                }
            }
        }
        KeyCode::Char('f') => {
            app.mode = Mode::TypeFilter;
            app.search_query = app.filter_type.clone().unwrap_or_default();
        }
        KeyCode::Char('c') => {
            app.mode = Mode::Cypher;
            app.cypher_input.clear();
            app.cypher_output.clear();
        }
        KeyCode::Char('?') => app.show_help = !app.show_help,
        _ => {}
    }
    false
}

fn handle_search_key(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Esc => {
            app.mode = Mode::Browse;
        }
        KeyCode::Enter => {
            // Apply substring filter locally to the loaded list.
            let q = app.search_query.to_lowercase();
            if !q.is_empty() {
                app.entities.retain(|e| {
                    e.name.to_lowercase().contains(&q)
                        || e.entity_type.to_lowercase().contains(&q)
                });
                app.selected.select(Some(0));
                app.message = format!("filtered to {} matches; press 'r' to reset", app.entities.len());
            }
            app.mode = Mode::Browse;
        }
        KeyCode::Backspace => {
            app.search_query.pop();
        }
        KeyCode::Char(c) => {
            if (c as u32) >= 32 {
                app.search_query.push(c);
            }
        }
        _ => {}
    }
    false
}

fn handle_filter_key(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Esc => {
            app.mode = Mode::Browse;
        }
        KeyCode::Enter => {
            app.filter_type = if app.search_query.is_empty() {
                None
            } else {
                Some(app.search_query.clone())
            };
            app.mode = Mode::Browse;
            app.message = format!("type filter set to: {:?}", app.filter_type);
        }
        KeyCode::Backspace => {
            app.search_query.pop();
        }
        KeyCode::Char(c) => {
            if (c as u32) >= 32 {
                app.search_query.push(c);
            }
        }
        _ => {}
    }
    false
}

async fn handle_cypher_key(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Esc => {
            app.mode = Mode::Browse;
        }
        KeyCode::Enter => {
            if let Some(store) = &app.store {
                match store.cypher(&app.cypher_input).await {
                    Ok(rows) => {
                        app.cypher_output = rows;
                        app.message = format!("cypher returned {} rows", app.cypher_output.len());
                    }
                    Err(e) => app.message = format!("cypher error: {e}"),
                }
            }
            app.cypher_input.clear();
        }
        KeyCode::Backspace => {
            app.cypher_input.pop();
        }
        KeyCode::Char(c) => {
            if (c as u32) >= 32 {
                app.cypher_input.push(c);
            }
        }
        _ => {}
    }
    false
}

fn ui(f: &mut ratatui::Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(3), Constraint::Length(1)])
        .split(f.area());

    let main = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(chunks[0]);

    // Entity list
    let items: Vec<ListItem> = app
        .entities
        .iter()
        .map(|e| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<14}", e.entity_type),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw(" "),
                Span::styled(e.name.clone(), Style::default().fg(Color::Yellow)),
            ]))
        })
        .collect();

    let title = match &app.filter_type {
        Some(t) => format!(" Entities (type={t}, {}) ", app.entities.len()),
        None => format!(" Entities ({}) ", app.entities.len()),
    };

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD).bg(Color::DarkGray))
        .highlight_symbol("▶ ");
    f.render_stateful_widget(list, main[0], &mut app.selected);

    // Detail / Cypher output
    let right_content = if app.mode == Mode::Cypher {
        let mut lines: Vec<String> = vec![format!("cypher> {}", app.cypher_input)];
        lines.push(String::new());
        for row in app.cypher_output.iter().take(50) {
            lines.push(row.join("  |  "));
        }
        lines
    } else {
        app.detail_lines.clone()
    };
    let detail_title = if app.mode == Mode::Cypher {
        " Cypher Output ".to_string()
    } else {
        " Detail ".to_string()
    };
    let detail_text: Vec<Line> = right_content
        .iter()
        .map(|s| Line::from(s.clone()))
        .collect();
    let detail = Paragraph::new(detail_text)
        .block(Block::default().borders(Borders::ALL).title(detail_title))
        .wrap(Wrap { trim: false });
    f.render_widget(detail, main[1]);

    // Bottom: input or status
    let status_text = match app.mode {
        Mode::Browse => {
            let hint = if app.show_help {
                "q quit · j/k move · g/G top/bottom · r refresh · / search · f type-filter · t/Enter traverse · c cypher · ? help"
            } else {
                "press ? for help"
            };
            if app.message.is_empty() {
                hint.to_string()
            } else {
                format!("{}  |  {}", app.message, hint)
            }
        }
        Mode::Search => format!("/{}", app.search_query),
        Mode::TypeFilter => format!("type filter: {}", app.search_query),
        Mode::Cypher => format!("cypher> {}  (Enter to run, Esc to cancel)", app.cypher_input),
    };
    let prompt_style = match app.mode {
        Mode::Browse => Style::default().fg(Color::DarkGray),
        _ => Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
    };
    let status = Paragraph::new(status_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Status ")
                .border_style(prompt_style),
        )
        .style(prompt_style);
    f.render_widget(status, chunks[1]);

    // Help line
    let help_line = if app.show_help {
        "j/k move · /search · f filter by type · t traverse · c cypher · r refresh · q quit · ? hide help"
    } else {
        ""
    };
    let help = Paragraph::new(help_line).style(Style::default().fg(Color::DarkGray));
    f.render_widget(help, chunks[2]);
}

#[allow(dead_code)]
fn _suppress_mod() {
    let _ = KeyModifiers::CONTROL;
}
