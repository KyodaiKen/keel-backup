//! keel TUI — capabilities, plan, profile, live log.
//!
//! Keys:
//!   1-4 / ← →   tabs          i            toggle hidden capabilities
//!   ↑ ↓         select/scroll Enter        start run (log tab)
//!   y / n       confirm / cancel  r        reload profile
//!   q or Esc    quit (press twice while a run is going)

use std::collections::BTreeMap;
use std::io::{self, Stdout};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Tabs};
use ratatui::{Frame, Terminal};

use crate::detect::{self, Capability};
use crate::engine::human;
use crate::plan::{self, EventArc, Plan, RunEvent};
use crate::profile;

const TABS: [&str; 4] = [" capabilities ", " plan ", " profile ", " log "];

#[derive(Debug, Clone)]
struct State {
    tab: usize,
    // capabilities
    caps: Vec<Capability>,
    cap_sel: usize,
    cap_show_hidden: bool,
    cap_scroll: usize,
    // profile + plan
    profile: Option<(PathBuf, profile::Profile)>,
    profile_yaml: String,
    profile_err: Option<String>,
    plan: Option<Plan>,
    plan_err: Option<String>,
    profile_scroll: usize,
    // run
    events: Vec<Line<'static>>,
    bars: BTreeMap<usize, (u64, u64)>,
    running: bool,
    run_done: Option<bool>,
    confirm_start: bool,
    quit_confirm: bool,
    log_scroll: usize,
    msg: String,
}

impl State {
    fn new(profile_ref: Option<String>) -> Self {
        let mut st = State {
            tab: 0,
            caps: detect::registry(),
            cap_sel: 0,
            cap_show_hidden: false,
            cap_scroll: 0,
            profile: None,
            profile_yaml: String::new(),
            profile_err: None,
            plan: None,
            plan_err: None,
            profile_scroll: 0,
            events: Vec::new(),
            bars: BTreeMap::new(),
            running: false,
            run_done: None,
            confirm_start: false,
            quit_confirm: false,
            log_scroll: 0,
            msg: String::new(),
        };
        st.reload(profile_ref);
        st
    }

    /// (Re)load the profile + rebuild the plan.
    fn reload(&mut self, profile_ref: Option<String>) {
        self.profile = None;
        self.plan = None;
        self.profile_err = None;
        self.plan_err = None;
        self.profile_yaml = String::new();
        match profile::load(profile_ref.as_deref()) {
            Ok((pp, prof)) => {
                self.profile_yaml =
                    serde_yaml::to_string(&prof).unwrap_or_else(|_| "<profile>".into());
                match plan::build_plan(&pp, &prof) {
                    Ok(p) => self.plan = Some(p),
                    Err(e) => self.plan_err = Some(format!("{e:#}")),
                }
                self.profile = Some((pp, prof));
            }
            Err(e) => {
                self.profile_err = Some(format!(
                    "{e:#}\n\nHint: `keel suggest > ~/.config/keel/profile.yaml`"
                ));
            }
        }
        self.caps = detect::registry();
        if self.cap_sel >= self.visible_cap_idx().len() {
            self.cap_sel = 0;
        }
    }

    /// Indices (into `caps`) that are currently visible in the dropdown.
    fn visible_cap_idx(&self) -> Vec<usize> {
        (0..self.caps.len())
            .filter(|&i| self.cap_show_hidden || self.caps[i].applicable)
            .collect()
    }
}

fn apply_event(st: &mut State, e: &RunEvent) {
    match e {
        RunEvent::Note(s) => st
            .events
            .push(Line::styled(format!("   · {s}"), Style::default().fg(Color::DarkGray))),
        RunEvent::Destination(s) => st
            .events
            .push(Line::styled(format!("   ▸ {s}"), Style::default().fg(Color::Cyan))),
        RunEvent::DayDir(s) => st
            .events
            .push(Line::styled(
                format!("   ▸ day dir: {s}"),
                Style::default().fg(Color::Cyan),
            )),
        RunEvent::SourceStart { idx, label, engine, out_rel } => {
            st.bars.insert(*idx, (0, 0));
            st.events.push(Line::styled(
                format!("   ▶ {label}  →  {out_rel}   [{engine}]"),
                Style::default().fg(Color::Yellow),
            ));
        }
        RunEvent::Progress { idx, raw, written } => {
            if let Some(v) = st.bars.get_mut(idx) {
                *v = (*raw, *written);
            }
        }
        RunEvent::SourceOk { idx, raw, size } => {
            st.bars.remove(idx);
            st.events.push(Line::styled(
                format!("   ✔ done: {} raw → {} compressed", human(*raw), human(*size)),
                Style::default().fg(Color::Green),
            ));
        }
        RunEvent::SourceFail { idx, err } => {
            st.bars.remove(idx);
            st.events.push(Line::styled(format!("   ✘ failed: {err}"), Style::default().fg(Color::Red)));
        }
        RunEvent::Manifest(s) => st
            .events
            .push(Line::styled(format!("   ▸ manifest: {s}"), Style::default().fg(Color::Cyan))),
        RunEvent::Retained(s) => st
            .events
            .push(Line::styled(format!("   ▸ {s}"), Style::default().fg(Color::Cyan))),
        RunEvent::Done => {
            st.events
                .push(Line::styled("   ✔ run complete", Style::default().fg(Color::Green)));
            st.run_done = Some(true);
        }
    }
}

fn spawn_run(shared: Arc<Mutex<State>>) -> Result<()> {
    let (_pp, prof) = {
        let st = shared.lock().expect("state lock");
        st.profile.clone().ok_or_else(|| anyhow!("no profile loaded — see the profile tab"))?
    };
    let p = {
        let st = shared.lock().expect("state lock");
        st.plan.clone().ok_or_else(|| anyhow!("no plan (source check failed) — see the plan tab"))?
    };
    let s2 = shared.clone();
    let ev: EventArc = Arc::new(Mutex::new(move |e: &RunEvent| {
        if let Ok(mut st) = s2.lock() {
            apply_event(&mut st, e);
        }
    }));
    let s3 = shared.clone();
    std::thread::spawn(move || {
        let _ = plan::run(&p, &prof, &ev, false);
        if let Ok(mut st) = s3.lock() {
            st.running = false;
            if st.run_done.is_none() {
                st.run_done = Some(false);
            }
            st.msg = if st.run_done == Some(true) {
                "run finished ✔ — see log".into()
            } else {
                "run finished with errors ✘ — see log".into()
            };
        }
    });
    {
        let mut st = shared.lock().expect("state lock");
        st.running = true;
        st.confirm_start = false;
        st.msg = "run started — streaming…".into();
    }
    Ok(())
}

fn render(f: &mut Frame, st: &State) {
    let tabs = Tabs::new(TABS).select(st.tab).style(
        Style::default()
            .fg(Color::DarkGray)
            .bg(Color::Reset),
    );
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(1),
        ])
        .split(f.area());

    f.render_widget(tabs, chunks[0]);

    match st.tab {
        0 => render_caps(f, st, chunks[1]),
        1 => render_plan(f, st, chunks[1]),
        2 => render_profile(f, st, chunks[1]),
        _ => render_log(f, st, chunks[1]),
    }

    let profile_s = match &st.profile {
        Some((pp, p)) => format!("{} ({})", p.name, pp.display()),
        None => "— no profile —".to_string(),
    };
    let run_s = if st.running {
        " RUNNING… "
    } else if let Some(d) = st.run_done {
        if d { " DONE " } else { " FAILED " }
    } else {
        ""
    };
    let status = format!(
        " keel {}  ·  profile: {profile_s}{run_s}  ·  keys: 1-4 tabs · i info · r reload · q quit ",
        env!("CARGO_PKG_VERSION")
    );
    let status_line = Paragraph::new(Line::from(vec![Span::styled(
        status,
        Style::default().fg(Color::White).bg(Color::Black).add_modifier(Modifier::BOLD),
    )]))
    .style(Style::default().bg(Color::DarkGray).fg(Color::White));
    f.render_widget(status_line, chunks[2]);
}

fn render_caps(f: &mut Frame, st: &State, area: ratatui::layout::Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);

    let vis = st.visible_cap_idx();
    let mut lines: Vec<Line> = Vec::new();
    for (row, idx) in vis.iter().enumerate() {
        let c = &st.caps[*idx];
        let mark = if c.applicable { "✓" } else { "✗" };
        let hidden = if c.applicable { "" } else { "   (hidden)" };
        let text = format!("{mark}  {}{}", c.title, hidden);
        let style = if row == st.cap_sel {
            Style::default().bg(Color::Blue).fg(Color::White)
        } else if c.applicable {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        lines.push(Line::styled(text, style));
    }
    let title = format!(
        "capabilities — {} visible {} · [i] {}",
        vis.len(),
        if st.cap_show_hidden { "(incl. hidden)" } else { "(applicable only)" },
        if st.cap_show_hidden { "hide hidden" } else { "show hidden + why" },
    );
    let inner_h = (cols[0].height as usize).saturating_sub(4);
    let offset = st
        .cap_scroll
        .min(vis.len().saturating_sub(1))
        .min(inner_h.saturating_sub(1));
    f.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(ratatui::widgets::Borders::ALL).title(title))
            .scroll((offset as u16, 0)),
        cols[0],
    );

    // Right: details + "why" panel for the selected capability.
    let sel_idx = *vis.get(st.cap_sel).unwrap_or_else(|| vis.first().unwrap_or(&0));
    let c = &st.caps[sel_idx];
    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(c.id.to_string(), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
    ]));
    lines.push(Line::from(c.title.clone()));
    lines.push(Line::from(Span::raw(" ")));
    let (verdict, vcol) = if c.applicable {
        ("APPLICABLE", Color::Green)
    } else {
        ("HIDDEN", Color::Red)
    };
    lines.push(Line::styled(format!("status : {verdict}"), Style::default().fg(vcol)));
    lines.push(Line::styled(
        format!("engine : {}", if c.implemented { "implemented (Phase 0)" } else { "planned (Phase 1+)" }),
        Style::default().fg(Color::Yellow),
    ));
    lines.push(Line::from(Span::raw(" ")));
    lines.push(Line::styled("why:".to_string(), Style::default().add_modifier(Modifier::BOLD)));
    for w in wrap(&c.reason, 30) {
        lines.push(Line::from(format!("  {w}")));
    }
    lines.push(Line::from(Span::raw(" ")));
    lines.push(Line::styled("evidence:".to_string(), Style::default().add_modifier(Modifier::BOLD)));
    for e in &c.evidence {
        for w in wrap(e, 30) {
            lines.push(Line::styled(format!("  · {w}"), Style::default().fg(Color::DarkGray)));
        }
    }
    f.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(ratatui::widgets::Borders::ALL).title("details — why applicable / hidden"))
            .wrap(ratatui::widgets::Wrap { trim: false }),
        cols[1],
    );
}

fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for w in text.split_whitespace() {
        if cur.is_empty() {
            cur = w.to_string();
        } else if cur.len() + 1 + w.len() <= width {
            cur.push(' ');
            cur.push_str(w);
        } else {
            out.push(cur);
            cur = w.to_string();
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn render_plan(f: &mut Frame, st: &State, area: ratatui::layout::Rect) {
    let title = "backup plan";
    match &st.plan {
        None => {
            let msg = st.plan_err.clone().unwrap_or_else(|| "no plan".into());
            f.render_widget(
                Paragraph::new(format!("   {msg}\n\n   Fix the profile, then press [r] to reload."))
                    .block(Block::default().borders(ratatui::widgets::Borders::ALL).title(title)),
                area,
            );
        }
        Some(p) => {
            let mut lines = Vec::new();
            lines.push(Line::styled(format!("   profile   : {}", p.profile_path.display()), Style::default().fg(Color::DarkGray)));
            lines.push(Line::styled(format!("   name      : {}", p.profile_name), Style::default().fg(Color::DarkGray)));
            lines.push(Line::styled(format!("   destination: {}", p.destination), Style::default().fg(Color::Cyan)));
            lines.push(Line::styled(format!("   day dir   : {}", p.day_dir.display()), Style::default().fg(Color::Cyan)));
            lines.push(Line::from(Span::raw("   ")));
            for it in &p.items {
                lines.push(Line::from(vec![
                    Span::styled(format!("   {} → ", it.category), Style::default().fg(Color::Yellow)),
                    Span::styled(format!("{}", it.source.display()), Style::default().add_modifier(Modifier::BOLD)),
                    Span::styled(format!("  →  {}/{}.zst   [{}]", it.category, it.name, it.engine), Style::default().fg(Color::Green)),
                ]));
            }
            lines.push(Line::from(Span::raw("   ")));
            lines.push(Line::styled(
                "   start: switch to the [log] tab and press Enter.",
                Style::default().fg(Color::DarkGray),
            ));
            f.render_widget(
                Paragraph::new(lines)
                    .block(Block::default().borders(ratatui::widgets::Borders::ALL).title(title))
                    .wrap(ratatui::widgets::Wrap { trim: false }),
                area,
            );
        }
    }
}

fn render_profile(f: &mut Frame, st: &State, area: ratatui::layout::Rect) {
    let title = "profile (yaml)";
    if st.profile.is_none() {
        let msg = st.profile_err.clone().unwrap_or_else(|| "no profile".into());
        f.render_widget(
            Paragraph::new(format!("   {msg}"))
                .block(Block::default().borders(ratatui::widgets::Borders::ALL).title(title)),
            area,
        );
        return;
    }
    let text = if st.profile_yaml.is_empty() {
        String::new()
    } else {
        st.profile_yaml.clone()
    };
    let lines: Vec<Line> = text.lines().map(|l| Line::from(l.to_string())).collect();
    let inner_h = (area.height as usize).saturating_sub(4);
    let offset = st
        .profile_scroll
        .min(lines.len().saturating_sub(1))
        .min(inner_h.saturating_sub(1));
    f.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(ratatui::widgets::Borders::ALL).title(title))
            .scroll((offset as u16, 0)),
        area,
    );
}

fn render_log(f: &mut Frame, st: &State, area: ratatui::layout::Rect) {
    let mut lines: Vec<Line> = Vec::new();
    if st.events.is_empty() && !st.running {
        lines.push(Line::from(Span::raw("   no run yet.")));
        lines.push(Line::styled(
            "   review [capabilities] and [plan], then press Enter here to start.",
            Style::default().fg(Color::Yellow),
        ));
    }
    lines.extend(st.events.iter().cloned());
    // live bars for sources still streaming
    for (idx, (raw, written)) in &st.bars {
        lines.push(Line::styled(
            format!(
                "   … source #{idx}: {} raw → {} compressed (streaming…)",
                human(*raw),
                human(*written)
            ),
            Style::default().fg(Color::Green),
        ));
    }
    if st.confirm_start {
        lines.push(Line::from(Span::raw("   ")));
        lines.push(Line::styled(
            "   START BACKUP?  [y] yes   [n] cancel",
            Style::default().add_modifier(Modifier::BOLD).fg(Color::Yellow),
        ));
    }
    if st.running {
        lines.push(Line::styled(
            "   running… (press q twice to abort)",
            Style::default().fg(Color::DarkGray),
        ));
    }
    if !st.msg.is_empty() {
        lines.push(Line::styled(format!("   {msg}", msg = st.msg), Style::default().add_modifier(Modifier::BOLD)));
    }

    let inner_h = (area.height as usize).saturating_sub(4);
    let n = lines.len();
    let offset = if st.log_scroll == usize::MAX {
        n.saturating_sub(1).min(inner_h.saturating_sub(1))
    } else {
        st.log_scroll.min(n.saturating_sub(1)).min(inner_h.saturating_sub(1))
    };
    f.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(ratatui::widgets::Borders::ALL).title("log — live run output"))
            .scroll((offset as u16, 0)),
        area,
    );
}

pub fn run_app(profile_ref: Option<String>) -> Result<()> {
    let shared = Arc::new(Mutex::new(State::new(profile_ref.clone())));

    enable_raw_mode().context("enabling raw mode (is this a terminal?)")?;
    let mut stdout: Stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut term = Terminal::new(backend)?;

    let result = run_loop(&mut term, &shared, profile_ref);

    let _ = term.show_cursor();
    execute!(term.backend_mut(), LeaveAlternateScreen)?;
    disable_raw_mode()?;
    result
}

fn run_loop(
    term: &mut Terminal<CrosstermBackend<Stdout>>,
    shared: &Arc<Mutex<State>>,
    profile_ref: Option<String>,
) -> Result<()> {
    loop {
        term.draw(|f| render(f, &shared.lock().expect("state lock")))?;

        if !event::poll(Duration::from_millis(60))? {
            continue;
        }
        let ev = match event::read()? {
            Event::Key(k) if k.kind == KeyEventKind::Press => k,
            _ => continue,
        };

        let mut st = shared.lock().expect("state lock");

        // quit handling (double-press guard while running)
        if ev.code == KeyCode::Char('q') || ev.code == KeyCode::Esc {
            if st.running && !st.quit_confirm {
                st.quit_confirm = true;
                st.msg = "abort the running backup? press q again to confirm, any key to cancel".into();
                continue;
            }
            if !st.running || st.quit_confirm {
                if st.running {
                    // Thread keeps running in the background; we just leave the UI.
                }
                drop(st);
                return Ok(());
            }
            st.quit_confirm = false;
            continue;
        }
        st.quit_confirm = false;

        // confirmation layer
        if st.confirm_start {
            match ev.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    let shared_c = shared.clone();
                    drop(st);
                    spawn_run(shared_c)?;
                    continue;
                }
                KeyCode::Char('n') | KeyCode::Esc => {
                    st.confirm_start = false;
                    st.msg = "start cancelled".into();
                    continue;
                }
                _ => continue,
            }
        }

        match ev.code {
            KeyCode::Char('1') => st.tab = 0,
            KeyCode::Char('2') => st.tab = 1,
            KeyCode::Char('3') => st.tab = 2,
            KeyCode::Char('4') => st.tab = 3,
            KeyCode::Left => st.tab = (st.tab + 3) % 4,
            KeyCode::Right => st.tab = (st.tab + 1) % 4,
            KeyCode::Tab => st.tab = (st.tab + 1) % 4,
            KeyCode::BackTab => st.tab = (st.tab + 3) % 4,

            KeyCode::Char('i') | KeyCode::Char('I') => {
                st.cap_show_hidden = !st.cap_show_hidden;
                st.cap_sel = 0;
                st.cap_scroll = 0;
            }
            KeyCode::Char('r') | KeyCode::Char('R') => {
                if !st.running {
                    st.reload(profile_ref.clone());
                    st.msg = "profile reloaded".into();
                }
            }
            KeyCode::Up => {
                if st.tab == 0 {
                    if st.cap_sel > 0 {
                        st.cap_sel -= 1;
                    }
                    if st.cap_sel < st.cap_scroll {
                        st.cap_scroll = st.cap_sel;
                    }
                } else if st.tab == 2 {
                    st.profile_scroll = st.profile_scroll.saturating_sub(1);
                } else if st.tab == 3 {
                    if st.log_scroll == usize::MAX {
                        // was following the bottom → jump there, then step up one
                        st.log_scroll = st.events.len().saturating_sub(1);
                    } else {
                        st.log_scroll = st.log_scroll.saturating_sub(1);
                    }
                }
            }
            KeyCode::Down => {
                if st.tab == 0 {
                    let vis = st.visible_cap_idx();
                    if st.cap_sel + 1 < vis.len() {
                        st.cap_sel += 1;
                    }
                    let inner_h = 30; // good enough heuristic; clamped at render
                    if st.cap_sel >= st.cap_scroll + inner_h {
                        st.cap_scroll = st.cap_sel;
                    }
                } else if st.tab == 2 {
                    st.profile_scroll += 1;
                } else if st.tab == 3 {
                    st.log_scroll = usize::MAX; // follow to bottom
                }
            }
            KeyCode::Enter => {
                if st.tab == 3 && !st.running {
                    st.confirm_start = true;
                }
            }
            KeyCode::Char('c') if ev.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                if st.running && !st.quit_confirm {
                    st.quit_confirm = true;
                    st.msg = "abort the running backup? press Ctrl+C again to confirm".into();
                    continue;
                }
                drop(st);
                return Ok(());
            }
            _ => {}
        }
    }
}
