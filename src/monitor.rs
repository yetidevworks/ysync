//! Snapshot dashboard with on-demand conflict review.
#[path = "monitor_conflicts.rs"]
mod conflict_panel;
use crate::{
    config,
    daemon::{self, Event, Status},
};
use anyhow::{Result, bail};
use crossterm::event::{self, Event as Input, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, BorderType, Cell, Clear, Paragraph, Row, Sparkline, Table, TableState, Wrap},
};
use std::{
    collections::VecDeque,
    io::{self, IsTerminal},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const BG: Color = Color::Rgb(18, 22, 31);
const PANEL: Color = Color::Rgb(24, 30, 41);
const TEXT: Color = Color::Rgb(216, 225, 239);
const MUTED: Color = Color::Rgb(140, 156, 179);
const CYAN: Color = Color::Rgb(89, 210, 223);
const GREEN: Color = Color::Rgb(133, 218, 165);
const AMBER: Color = Color::Rgb(245, 193, 104);
const RED: Color = Color::Rgb(246, 133, 147);
const HISTORY: usize = 60;

#[derive(Clone, Copy, Default, PartialEq)]
enum Filter {
    #[default]
    Activity,
    All,
    Index,
    Issues,
}
impl Filter {
    fn next(self) -> Self {
        match self {
            Self::Activity => Self::All,
            Self::All => Self::Index,
            Self::Index => Self::Issues,
            Self::Issues => Self::Activity,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Activity => "Activity",
            Self::All => "All",
            Self::Index => "Index",
            Self::Issues => "Issues",
        }
    }
    fn accepts(self, e: &Event) -> bool {
        match self {
            Self::All => true,
            Self::Index => e.kind == "received",
            Self::Issues => issue(e),
            Self::Activity => e.kind != "received",
        }
    }
}
fn issue(e: &Event) -> bool {
    [
        "error", "conflict", "fail", "pending", "reject", "overflow", "approval", "warning",
    ]
    .iter()
    .any(|s| e.kind.contains(s))
}
fn clean(s: &str) -> String {
    s.chars()
        .filter(|c| {
            !c.is_control() && !matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
        .collect()
}
fn bytes(n: f64) -> String {
    let n = if n.is_finite() { n.max(0.0) } else { 0.0 };
    for (scale, unit) in [(1e12, "TB"), (1e9, "GB"), (1e6, "MB"), (1e3, "kB")] {
        if n >= scale {
            return format!("{:.2} {unit}", n / scale);
        }
    }
    format!("{n:.0} B")
}
fn count(n: u64) -> String {
    let s = n.to_string();
    s.chars()
        .enumerate()
        .fold(String::new(), |mut out, (i, c)| {
            if i > 0 && (s.len() - i).is_multiple_of(3) {
                out.push(',');
            }
            out.push(c);
            out
        })
}
fn age(n: u64) -> String {
    if n < 60 {
        format!("{n}s")
    } else if n < 3600 {
        format!("{}m {}s", n / 60, n % 60)
    } else {
        format!("{}h {}m", n / 3600, n % 3600 / 60)
    }
}
fn panel(title: impl Into<Line<'static>>, active: bool) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .title(
            title
                .into()
                .style(Style::new().fg(if active { CYAN } else { MUTED })),
        )
        .border_style(Style::new().fg(if active { CYAN } else { Color::Rgb(55, 69, 89) }))
        .style(Style::new().bg(PANEL).fg(TEXT))
}

#[derive(Default)]
struct Dashboard {
    conflicts: conflict_panel::Panel,
    status: Option<Status>,
    config: Option<config::Config>,
    error: Option<String>,
    selected: Option<String>,
    folder_table: TableState,
    activity_table: TableState,
    activity_focus: bool,
    filter: Filter,
    query: String,
    searching: bool,
    help: bool,
    expanded: bool,
    detail_scroll: u16,
    paused: bool,
    observed_at: u64,
    send: VecDeque<u64>,
    receive: VecDeque<u64>,
}
impl Dashboard {
    fn refresh(&mut self, home: &Path) {
        if self.paused {
            return;
        }
        self.observed_at = daemon::now();
        match (daemon::read_status(home), config::load(home)) {
            (Ok(status), Ok(cfg)) => {
                self.config = Some(cfg);
                self.ingest(status);
                self.error = None;
            }
            (status, cfg) => {
                self.error = Some(clean(&format!(
                    "{}",
                    status.err().or_else(|| cfg.err()).unwrap()
                )));
            }
        }
    }
    fn ingest(&mut self, status: Status) {
        let restart = self
            .status
            .as_ref()
            .is_none_or(|s| (s.pid, s.started) != (status.pid, status.started));
        if restart {
            self.send.clear();
            self.receive.clear();
            self.activity_table = TableState::default();
        }
        let fresh = self
            .status
            .as_ref()
            .is_none_or(|s| s.updated != status.updated)
            || restart;
        if fresh || status.pid == 0 || self.observed_at.saturating_sub(status.updated) >= 5 {
            let live = status.pid != 0 && self.observed_at.saturating_sub(status.updated) < 5;
            for (history, rate) in [
                (&mut self.send, status.send_bytes_per_sec),
                (&mut self.receive, status.receive_bytes_per_sec),
            ] {
                if history.len() == HISTORY {
                    history.pop_front();
                }
                history.push_back(if live && rate.is_finite() {
                    rate.max(0.0) as u64
                } else {
                    0
                });
            }
        }
        if self
            .selected
            .as_ref()
            .is_none_or(|id| !status.folders.contains_key(id))
        {
            self.selected = status.folders.keys().next().cloned();
        }
        self.folder_table.select(
            self.selected
                .as_ref()
                .and_then(|id| status.folders.keys().position(|key| key == id)),
        );
        self.status = Some(status);
    }
    fn live(&self) -> bool {
        self.error.is_none()
            && self
                .status
                .as_ref()
                .is_some_and(|s| s.pid != 0 && self.observed_at.saturating_sub(s.updated) < 5)
    }
    fn events(&self) -> Vec<&Event> {
        self.status
            .as_ref()
            .map(|s| {
                s.events
                    .iter()
                    .rev()
                    .filter(|e| {
                        self.filter.accepts(e)
                            && (self.query.is_empty()
                                || format!(
                                    "{} {} {}",
                                    e.kind,
                                    e.folder.as_deref().unwrap_or(""),
                                    e.detail
                                )
                                .to_lowercase()
                                .contains(&self.query.to_lowercase()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    fn navigate(&mut self, down: bool, step: usize) {
        if self.activity_focus {
            let len = self.events().len();
            let old = self.activity_table.selected().unwrap_or(0);
            self.activity_table.select(if len == 0 {
                None
            } else {
                Some(if down {
                    old.saturating_add(step).min(len - 1)
                } else {
                    old.saturating_sub(step)
                })
            });
        } else if let Some(s) = &self.status {
            let keys: Vec<_> = s.folders.keys().cloned().collect();
            if !keys.is_empty() {
                let old = self
                    .folder_table
                    .selected()
                    .unwrap_or(0)
                    .min(keys.len() - 1);
                let next = if down {
                    old.saturating_add(step).min(keys.len() - 1)
                } else {
                    old.saturating_sub(step)
                };
                self.selected = Some(keys[next].clone());
                self.folder_table.select(Some(next));
            }
        }
    }
    fn key(&mut self, key: KeyEvent) -> bool {
        if key.kind == KeyEventKind::Release {
            return false;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return true;
        }
        if self.conflicts.active {
            return self.conflicts.key(key);
        }
        if self.searching {
            match key.code {
                KeyCode::Esc | KeyCode::Enter => self.searching = false,
                KeyCode::Backspace => {
                    self.query.pop();
                }
                KeyCode::Char(c) if !c.is_control() && self.query.len() < 200 => self.query.push(c),
                _ => {}
            }
            self.activity_table = TableState::default();
            return false;
        }
        if self.expanded {
            match key.code {
                KeyCode::Char('q') => return true,
                KeyCode::Esc | KeyCode::Enter | KeyCode::Char('d') => self.expanded = false,
                KeyCode::Down | KeyCode::Char('j') => {
                    self.detail_scroll = self.detail_scroll.saturating_add(1)
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.detail_scroll = self.detail_scroll.saturating_sub(1)
                }
                _ => {}
            }
            return false;
        }
        if self.help {
            self.help = false;
            return key.code == KeyCode::Char('q');
        }
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return true,
            KeyCode::Char('?') => self.help = true,
            KeyCode::Char('c') => self.conflicts.open(self.selected.clone()),
            KeyCode::Char('d') | KeyCode::Enter => {
                self.expanded = true;
                self.detail_scroll = 0;
            }
            KeyCode::Tab | KeyCode::BackTab => self.activity_focus = !self.activity_focus,
            KeyCode::Down | KeyCode::Char('j') => self.navigate(true, 1),
            KeyCode::Up | KeyCode::Char('k') => self.navigate(false, 1),
            KeyCode::PageDown => self.navigate(true, 10),
            KeyCode::PageUp => self.navigate(false, 10),
            KeyCode::Char('f') => {
                self.filter = self.filter.next();
                self.activity_table = TableState::default();
            }
            KeyCode::Char('/') => {
                self.searching = true;
                self.activity_focus = true;
            }
            KeyCode::Char('x') => {
                self.query.clear();
                self.activity_table = TableState::default();
            }
            KeyCode::Char(' ') => self.paused = !self.paused,
            _ => {}
        }
        false
    }
    fn draw(&mut self, frame: &mut Frame) {
        let area = frame.area();
        frame.render_widget(Block::new().style(Style::new().bg(BG).fg(TEXT)), area);
        if area.width < 60 || area.height < 20 {
            frame.render_widget(Paragraph::new("YSYNC\n\nEnlarge the terminal to at least 60 × 20.\nq / Ctrl-C to leave; synchronization continues.").block(panel(" Monitor ", false)).wrap(Wrap { trim: true }), area);
            return;
        }
        let sections = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(6),
            Constraint::Length(if area.height >= 34 { 12 } else { 6 }),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);
        self.header(frame, sections[0]);
        if self.conflicts.active {
            self.conflicts.draw(
                frame,
                Rect::new(
                    area.x,
                    area.y + 3,
                    area.width,
                    area.height.saturating_sub(3),
                ),
            );
            return;
        }
        self.traffic(frame, sections[1]);
        let middle = if area.width >= 100 {
            Layout::horizontal([Constraint::Percentage(46), Constraint::Percentage(54)])
                .split(sections[2])
        } else {
            Layout::horizontal([Constraint::Percentage(100), Constraint::Length(0)])
                .split(sections[2])
        };
        self.folders(frame, middle[0]);
        if middle[1].width > 0 {
            self.details(frame, middle[1]);
        }
        self.activity(frame, sections[3]);
        let footer = if self.searching {
            format!(
                " /{}▏   Enter apply · Esc close · x clears outside search",
                clean(&self.query)
            )
        } else {
            " q quit  ? help  c conflicts  Tab focus  ↑↓ select  / search  f filter  d details"
                .into()
        };
        frame.render_widget(Paragraph::new(footer).fg(MUTED), sections[4]);
        if self.expanded {
            let popup = Rect::new(
                area.x + 1,
                area.y + 3,
                area.width.saturating_sub(2),
                area.height.saturating_sub(4),
            );
            frame.render_widget(Clear, popup);
            self.details(frame, popup);
        }
        if self.help {
            let width = area.width.min(78);
            let height = area.height.min(24);
            let popup = Rect::new(
                (area.width - width) / 2,
                (area.height - height) / 2,
                width,
                height,
            );
            frame.render_widget(Clear, popup);
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from("Monitoring never stops the service.".bold()),
                    Line::from("c            Review conflicts; resolutions require confirmation"),
                    Line::from(""),
                    Line::from("Tab          Switch folders / activity"),
                    Line::from("↑ ↓ / j k    Select folder or event; full detail appears below"),
                    Line::from("PgUp PgDn    Move ten rows"),
                    Line::from("d / Enter    Folder/device details; arrows scroll, Esc closes"),
                    Line::from("f            Activity → All → Index → Issues"),
                    Line::from("/            Search recent events (folder, kind, path)"),
                    Line::from("x            Clear search"),
                    Line::from("Space        Freeze/resume the display only"),
                    Line::from("q / Ctrl-C   Exit monitor"),
                    Line::from(""),
                    Line::from("Index = a remote record reconciled, not a file overwritten."),
                    Line::from("Payload = file bytes, including preserved conflict versions."),
                    Line::from("Graphs show up to 60 observed snapshots, with independent scales."),
                    Line::from("Watching describes the local scanner, not remote completion."),
                    Line::from("Folder inventory reflects the last completed full scan."),
                    Line::from(""),
                    Line::from("Any key closes help.".fg(CYAN)),
                ])
                .block(panel(" Monitor guide ", true))
                .wrap(Wrap { trim: false }),
                popup,
            );
        }
    }
    fn header(&self, frame: &mut Frame, area: Rect) {
        let status = self.status.as_ref();
        let badge = if self.paused {
            "FROZEN"
        } else if self.live() {
            "LIVE"
        } else {
            "STALE / OFFLINE"
        };
        let color = if self.paused {
            AMBER
        } else if self.live() {
            GREEN
        } else {
            RED
        };
        let name = self
            .config
            .as_ref()
            .map(|c| clean(&c.name).chars().take(24).collect::<String>())
            .unwrap_or_else(|| "Waiting for daemon".into());
        let version = clean(
            status
                .map(|s| s.daemon_version.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("unknown"),
        );
        let age = status
            .map(|s| age(self.observed_at.saturating_sub(s.updated)))
            .unwrap_or_else(|| "—".into());
        let line = Line::from(vec![
            Span::styled(" YSYNC ", Style::new().fg(CYAN).bold()),
            Span::raw(format!(" {name}  ")),
            Span::styled(format!(" {badge} "), Style::new().fg(BG).bg(color).bold()),
            Span::styled(
                format!(
                    "   daemon {version} · monitor {} · snapshot {age}",
                    env!("CARGO_PKG_VERSION")
                ),
                Style::new().fg(MUTED),
            ),
        ]);
        frame.render_widget(
            Paragraph::new(line)
                .block(Block::bordered().border_style(Style::new().fg(Color::Rgb(55, 69, 89)))),
            area,
        );
    }
    fn traffic(&self, frame: &mut Frame, area: Rect) {
        let parts = Layout::horizontal([
            Constraint::Percentage(33),
            Constraint::Percentage(33),
            Constraint::Percentage(34),
        ])
        .split(area);
        let empty = Status::default();
        let s = self.status.as_ref().unwrap_or(&empty);
        for (i, title, color, rate, total, history) in [
            (
                0,
                " ↑ SEND PAYLOAD ",
                CYAN,
                s.send_bytes_per_sec,
                s.sent_bytes,
                &self.send,
            ),
            (
                1,
                " ↓ RECEIVE PAYLOAD ",
                GREEN,
                s.receive_bytes_per_sec,
                s.received_bytes,
                &self.receive,
            ),
        ] {
            let block = panel(title, false);
            let inner = block.inner(parts[i]);
            frame.render_widget(block, parts[i]);
            let rows = Layout::vertical([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .split(inner);
            frame.render_widget(
                Paragraph::new(format!(
                    " {}/s",
                    bytes(if self.live() { rate } else { 0.0 })
                ))
                .fg(color)
                .bold(),
                rows[0],
            );
            frame.render_widget(
                Paragraph::new(format!(" {} this run", bytes(total as f64))).fg(MUTED),
                rows[1],
            );
            // Keep the newest sample visible when the terminal is narrower than history.
            let visible = usize::from(rows[2].width).min(history.len());
            let mut data = vec![0; usize::from(rows[2].width).saturating_sub(visible)];
            data.extend(history.iter().skip(history.len() - visible).copied());
            frame.render_widget(
                Sparkline::default()
                    .data(&data)
                    .style(Style::new().fg(color))
                    .max(data.iter().copied().max().unwrap_or(1).max(1)),
                rows[2],
            );
        }
        let conflicts = s.folders.values().map(|f| f.pending_conflicts).sum::<u64>();
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(format!(" {} index records", count(s.received_entries))).fg(TEXT),
                Line::from(format!(" {} pending conflicts", count(conflicts)))
                    .fg(if conflicts > 0 { AMBER } else { GREEN }),
                Line::from(format!(
                    " {} delta reused",
                    bytes(s.delta_reused_bytes as f64)
                ))
                .fg(MUTED),
                Line::from(format!(" {} resume reused", bytes(s.resumed_bytes as f64))).fg(MUTED),
            ])
            .block(panel(" RECONCILIATION ", false)),
            parts[2],
        );
    }
    fn folders(&mut self, frame: &mut Frame, area: Rect) {
        let rows = self
            .status
            .as_ref()
            .map(|s| {
                s.folders
                    .iter()
                    .map(|(id, f)| {
                        let color = if f.error.is_some() || f.watch_error.is_some() {
                            RED
                        } else if f.pending_conflicts > 0 || f.scan_waiting_for_cooling {
                            AMBER
                        } else {
                            GREEN
                        };
                        Row::new(vec![
                            Cell::from(clean(id)),
                            Cell::from(f.mode.as_str()).fg(MUTED),
                            Cell::from(clean(&f.phase)).fg(color),
                            Cell::from(count(f.files)),
                            Cell::from(if f.pending_conflicts > 0 {
                                count(f.pending_conflicts)
                            } else {
                                "—".into()
                            })
                            .fg(color),
                        ])
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let table = Table::new(
            rows,
            [
                Constraint::Percentage(27),
                Constraint::Length(12),
                Constraint::Percentage(23),
                Constraint::Percentage(20),
                Constraint::Min(9),
            ],
        )
        .header(
            Row::new(["Folder", "Direction", "Scanner", "Last files", "Conflicts"])
                .fg(MUTED)
                .bottom_margin(1),
        )
        .block(panel(" FOLDERS · last full scan ", !self.activity_focus))
        .column_spacing(1)
        .row_highlight_style(
            Style::new()
                .bg(Color::Rgb(39, 59, 77))
                .fg(TEXT)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("› ");
        frame.render_stateful_widget(table, area, &mut self.folder_table);
    }
    fn details(&self, frame: &mut Frame, area: Rect) {
        let mut lines = vec![];
        if let Some(s) = &self.status {
            if let Some((id, f)) = self
                .selected
                .as_ref()
                .and_then(|id| s.folders.get(id).map(|f| (id, f)))
            {
                lines.push(
                    Line::from(format!(
                        "{} · {} · {} · {} files at last scan",
                        clean(id),
                        f.mode.as_str(),
                        bytes(f.bytes as f64),
                        count(f.files)
                    ))
                    .fg(CYAN),
                );
                if let Some(cfg) = &self.config {
                    for peer in cfg.peers.iter().filter(|p| p.folders.contains(id)) {
                        let sample = s.delivery.get(&peer.id).and_then(|folders| folders.get(id));
                        let (label, color) = delivery_label(
                            sample,
                            f,
                            peer.approved,
                            self.live() && s.connected_peers.contains(&peer.id),
                            self.observed_at,
                        );
                        lines.push(
                            Line::from(format!("To {} · {label}", clean(&peer.name))).fg(color),
                        );
                        if self.expanded {
                            if let Some(sample) = sample {
                                lines.push(Line::from(format!("Queue sampled {} ago · {}/{} lanes observed · local revision {}",
                                    age(self.observed_at.saturating_sub(sample.sampled_at)),
                                    sample.acknowledged.iter().flatten().count(), sample.acknowledged.len(), sample.local_head)).fg(MUTED));
                                if let Some(error) = &sample.error {
                                    lines.push(Line::from(clean(error)).fg(RED));
                                }
                            }
                            lines.push(Line::from("Queued bytes are full file sizes, before delta/resume savings.").fg(MUTED));
                            lines.push(Line::from("Delivery confirms indexed changes were reconciled; remote edits/conflicts may remain.").fg(MUTED));
                        }
                    }
                }
                lines.push(Line::from(format!(
                    "{} watches · {} queued · {} uncovered · {} overflows",
                    count(f.native_watches as u64),
                    f.queued_paths,
                    f.unwatched_subtrees,
                    f.watch_overflows
                )));
                lines.push(
                    Line::from(format!(
                        "Scans {} full / {} scoped · {} entries checked",
                        f.full_scans,
                        f.scoped_scans,
                        count(f.checked_entries)
                    ))
                    .fg(MUTED),
                );
                lines.push(
                    Line::from(format!(
                        "Hashed {} files ({}) · {} watch events",
                        count(f.hashed_files),
                        bytes(f.hashed_bytes as f64),
                        count(f.watch_events)
                    ))
                    .fg(MUTED),
                );
                if let Some(e) = f.error.as_ref().or(f.watch_error.as_ref()) {
                    lines.push(Line::from(clean(e)).fg(RED));
                } else if f.pending_conflicts > 0 {
                    lines.push(
                        Line::from(format!(
                            "{} conflicts preserved · ysync conflict list",
                            f.pending_conflicts
                        ))
                        .fg(AMBER),
                    );
                } else {
                    lines.push(Line::from("No pending conflicts in this folder").fg(GREEN));
                }
            }
            if let Some(cfg) = &self.config {
                let thermal = if s.thermal.cooling {
                    "COOLING".into()
                } else if s.thermal.max_temp_c.is_none() {
                    "thermal control disabled".into()
                } else {
                    s.thermal
                        .temperature_c
                        .map(|t| format!("{t:.1}°C"))
                        .unwrap_or_else(|| "temperature unavailable".into())
                };
                lines.push(
                    Line::from(format!("{} scan workers · {thermal}", cfg.scan_workers)).fg(MUTED),
                );
                if self.expanded {
                    lines.push(
                        Line::from(format!(
                            "Daemon PID {} · uptime {} · {}",
                            s.pid,
                            age(self.observed_at.saturating_sub(s.started)),
                            clean(&s.listen)
                        ))
                        .fg(MUTED),
                    );
                    lines.push(Line::from(format!("Device {}", clean(&s.device))).fg(MUTED));
                    if let Some(folder) = cfg
                        .folders
                        .iter()
                        .find(|f| Some(&f.id) == self.selected.as_ref())
                    {
                        lines.push(Line::from(format!(
                            "Path {}",
                            clean(&folder.path.display().to_string())
                        )));
                    }
                    if let Some(f) = self.selected.as_ref().and_then(|id| s.folders.get(id)) {
                        lines.push(
                            Line::from(format!(
                                "Watcher {} · {} events coalesced / {} ignored",
                                clean(&f.watcher),
                                count(f.coalesced_events),
                                count(f.ignored_events)
                            ))
                            .fg(MUTED),
                        );
                        lines.push(
                            Line::from(format!(
                                "Current/latest scan: {} directories · {} entries",
                                count(f.scan_directories),
                                count(f.scanned)
                            ))
                            .fg(MUTED),
                        );
                        if f.scan_waiting_for_cooling {
                            lines.push(
                                Line::from("Scan paused for cooling; progress retained").fg(AMBER),
                            );
                        }
                        if let Some(e) = &f.watch_error {
                            lines.push(Line::from(clean(e)).fg(RED));
                        }
                    }
                    if let Some(e) = &s.thermal.error {
                        lines.push(Line::from(clean(e)).fg(RED));
                    }
                    if let Some(limit) = s.watch_limits.max_user_watches {
                        lines.push(
                            Line::from(format!("Linux shared-user watch limit: {}", count(limit)))
                                .fg(MUTED),
                        );
                    }
                    lines.push(Line::from(""));
                }
                for p in &cfg.peers {
                    let connected = self.live() && s.connected_peers.contains(&p.id);
                    let (state, color) = if !p.approved {
                        ("APPROVAL NEEDED", AMBER)
                    } else if connected {
                        ("CONNECTED", GREEN)
                    } else {
                        ("DISCONNECTED", MUTED)
                    };
                    lines.push(Line::from(vec![
                        Span::styled(format!("{state}  "), Style::new().fg(color)),
                        Span::raw(clean(&p.name)),
                    ]));
                    if self.expanded {
                        lines.push(
                            Line::from(format!(
                                "{} · folders: {}",
                                clean(&p.id),
                                clean(&p.folders.join(", "))
                            ))
                            .fg(MUTED),
                        );
                        if !p.approved {
                            lines.push(Line::from("Review with ysync peer pending; approve with ysync peer approve").fg(AMBER));
                        }
                    }
                }
                if cfg.peers.is_empty() {
                    lines.push(Line::from("No devices configured · ysync peer add").fg(AMBER));
                }
            }
        }
        frame.render_widget(
            Paragraph::new(lines)
                .block(panel(
                    if self.expanded {
                        " FOLDER / DEVICES · ↑↓ scroll · Esc closes "
                    } else {
                        " FOLDER / DEVICES · d expands "
                    },
                    self.expanded,
                ))
                .scroll((if self.expanded { self.detail_scroll } else { 0 }, 0))
                .wrap(Wrap { trim: true }),
            area,
        );
    }
    fn activity(&mut self, frame: &mut Frame, area: Rect) {
        let title = format!(
            " {} · f cycles · / searches{} ",
            self.filter.name(),
            if self.query.is_empty() {
                String::new()
            } else {
                format!(" · {}", clean(&self.query))
            }
        );
        if let Some(e) = &self.error {
            frame.render_widget(Paragraph::new(format!("Snapshot unavailable: {e}\nRetrying every second. Start the service with brew services start ysync.")).fg(AMBER).block(panel(title, self.activity_focus)).wrap(Wrap { trim: true }), area);
            return;
        }
        let events = self.events();
        let selected = self
            .activity_table
            .selected()
            .map(|i| i.min(events.len().saturating_sub(1)));
        let detail = selected.and_then(|i| events.get(i)).map(|e| {
            format!(
                "{} / {}",
                clean(e.folder.as_deref().unwrap_or("daemon")),
                clean(&e.detail)
            )
        });
        let rows: Vec<_> = events
            .iter()
            .map(|e| {
                let (kind, color) = if e.kind == "received" {
                    ("index".into(), MUTED)
                } else if issue(e) {
                    (clean(&e.kind), AMBER)
                } else {
                    (clean(&e.kind), CYAN)
                };
                Row::new(vec![
                    Cell::from(age(self.observed_at.saturating_sub(e.at))).fg(MUTED),
                    Cell::from(kind).fg(color),
                    Cell::from(clean(e.folder.as_deref().unwrap_or("—"))),
                    Cell::from(clean(&e.detail)),
                ])
            })
            .collect();
        let empty = rows.is_empty();
        self.activity_table
            .select(if empty { None } else { selected });
        let block = panel(title, self.activity_focus);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let pieces = Layout::vertical([
            Constraint::Min(1),
            Constraint::Length(if detail.is_some() && inner.height >= 5 {
                3
            } else {
                1
            }),
        ])
        .split(inner);
        if empty {
            frame.render_widget(Paragraph::new("No matching events in the daemon's recent activity window.\nPress f for index records or x to clear search.").fg(MUTED).wrap(Wrap { trim: true }), pieces[0]);
        } else {
            let table = Table::new(
                rows,
                [
                    Constraint::Length(7),
                    Constraint::Length(11),
                    Constraint::Percentage(20),
                    Constraint::Min(10),
                ],
            )
            .column_spacing(1)
            .row_highlight_style(Style::new().bg(Color::Rgb(39, 59, 77)))
            .highlight_symbol("› ");
            frame.render_stateful_widget(table, pieces[0], &mut self.activity_table);
        }
        frame.render_widget(Paragraph::new(detail.unwrap_or_else(|| "Index records reconcile history; payload includes conflict archives. No overwrite implied.".into())).fg(MUTED).wrap(Wrap { trim: false }), pieces[1]);
    }
}

pub fn run(home: &Path) -> Result<()> {
    if !io::stdout().is_terminal() || !io::stdin().is_terminal() {
        bail!("monitor requires an interactive terminal; use ysync status --json for scripts");
    }
    let stop = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&stop);
    ctrlc::set_handler(move || signal.store(true, Ordering::Relaxed))?;
    // Ratatui installs a panic restoration hook. The guard also restores on errors and signals.
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            ratatui::restore();
        }
    }
    let _restore = Restore;
    let mut terminal = ratatui::try_init()?;
    let mut app = Dashboard::default();
    app.refresh(home);
    let mut refresh = Instant::now();
    let mut dirty = true;
    while !stop.load(Ordering::Relaxed) {
        if refresh.elapsed() >= Duration::from_secs(1) {
            app.refresh(home);
            refresh = Instant::now();
            dirty = true;
        }
        dirty |= app.conflicts.tick(home);
        if dirty {
            terminal.draw(|f| app.draw(f))?;
            dirty = false;
        }
        if event::poll(Duration::from_millis(100))? {
            match event::read()? {
                Input::Key(key) => {
                    if app.key(key) {
                        break;
                    }
                    dirty = true;
                }
                Input::Resize(_, _) => dirty = true,
                _ => {}
            }
        }
    }
    Ok(())
}

fn delivery_label(
    sample: Option<&crate::progress::Delivery>,
    folder: &daemon::FolderStatus,
    approved: bool,
    connected: bool,
    observed_at: u64,
) -> (String, Color) {
    if !approved {
        return ("approval needed".into(), AMBER);
    }
    if !folder.mode.can_send() {
        return ("sending disabled · receive-only".into(), MUTED);
    }
    if !connected {
        return ("offline · delivery unconfirmed".into(), MUTED);
    }
    let Some(sample) = sample else {
        return ("awaiting delivery status".into(), MUTED);
    };
    if let Some(reason) = &sample.send_disabled {
        return (format!("sending disabled · {reason}"), AMBER);
    }
    if sample.error.is_some() {
        return ("queue unavailable · d for details".into(), RED);
    }
    if observed_at.saturating_sub(sample.sampled_at) > 15 {
        return ("queue sample stale".into(), AMBER);
    }
    let Some(pending) = &sample.pending else {
        return ("checking delivery".into(), AMBER);
    };
    if pending.entries > 0 || !pending.complete {
        let prefix = if pending.complete { "" } else { "≥ " };
        return (
            format!(
                "{prefix}{} files · {prefix}{} queued · {prefix}{} metadata",
                count(pending.files),
                bytes(pending.bytes as f64),
                count(pending.entries.saturating_sub(pending.files))
            ),
            CYAN,
        );
    }
    if sample.active_lanes < sample.acknowledged.len() {
        return ("waiting for transfer lanes".into(), AMBER);
    }
    if sample.acknowledged.is_empty()
        || sample
            .acknowledged
            .iter()
            .any(|n| n.is_none_or(|n| n < sample.local_head))
    {
        return ("reconciling index".into(), CYAN);
    }
    if folder.pending_conflicts > 0 {
        return ("index delivered · conflicts remain".into(), AMBER);
    }
    if folder.phase != "watching"
        || folder.queued_paths > 0
        || folder.error.is_some()
        || folder.watch_error.is_some()
        || folder.unwatched_subtrees > 0
        || folder.scan_waiting_for_cooling
    {
        return ("index delivered · scan pending".into(), AMBER);
    }
    ("indexed changes delivered".into(), GREEN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};
    #[test]
    fn disabled_direction_is_not_reported_as_delivered() {
        let mut folder = daemon::FolderStatus {
            mode: config::FolderMode::ReceiveOnly,
            ..Default::default()
        };
        let mut sample = crate::progress::Delivery::default();
        assert!(
            delivery_label(Some(&sample), &folder, true, true, 0)
                .0
                .contains("sending disabled")
        );
        folder.mode = config::FolderMode::SendReceive;
        sample.send_disabled = Some("peer folder is send-only".into());
        assert!(
            delivery_label(Some(&sample), &folder, true, true, 0)
                .0
                .contains("sending disabled")
        );
        assert_ne!(
            delivery_label(Some(&sample), &folder, true, true, 0).1,
            GREEN
        );
    }

    #[test]
    fn delivery_never_confuses_scanning_conflicts_or_disconnection_with_delivery() {
        let mut folder = daemon::FolderStatus {
            phase: "watching".into(),
            ..Default::default()
        };
        let mut sample = crate::progress::Delivery {
            active_lanes: 3,
            sampled_at: 100,
            local_head: 42,
            acknowledged: vec![Some(42); 3],
            pending: Some(crate::progress::Pending {
                complete: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            delivery_label(Some(&sample), &folder, true, true, 100).1,
            GREEN
        );
        assert_ne!(
            delivery_label(Some(&sample), &folder, true, false, 100).1,
            GREEN
        );
        assert_ne!(
            delivery_label(Some(&sample), &folder, true, true, 116).1,
            GREEN
        );
        folder.queued_paths = 1;
        assert!(
            delivery_label(Some(&sample), &folder, true, true, 100)
                .0
                .contains("scan pending")
        );
        folder.queued_paths = 0;
        folder.pending_conflicts = 2;
        assert!(
            delivery_label(Some(&sample), &folder, true, true, 100)
                .0
                .contains("conflicts")
        );
        folder.pending_conflicts = 0;
        sample.active_lanes = 2;
        assert!(
            delivery_label(Some(&sample), &folder, true, true, 100)
                .0
                .contains("waiting")
        );
        sample.active_lanes = 3;
        sample.acknowledged[1] = Some(1);
        assert!(
            delivery_label(Some(&sample), &folder, true, true, 100)
                .0
                .contains("reconciling")
        );
        sample.pending.as_mut().unwrap().complete = false;
        assert!(
            delivery_label(Some(&sample), &folder, true, true, 100)
                .0
                .contains("≥")
        );
    }
    #[test]
    fn selected_folder_delivery_is_visible_in_wide_and_expanded_narrow_layouts() {
        let mut app = sample();
        app.config.as_mut().unwrap().peers.push(config::Peer {
            id: "peer".into(),
            name: "home-omarchy".into(),
            address: None,
            approved: true,
            folders: vec!["trilbymedia".into()],
        });
        let status = app.status.as_mut().unwrap();
        status.connected_peers.push("peer".into());
        status.delivery.entry("peer".into()).or_default().insert(
            "trilbymedia".into(),
            crate::progress::Delivery {
                sampled_at: 100,
                active_lanes: 3,
                acknowledged: vec![Some(1); 3],
                local_head: 4,
                pending: Some(crate::progress::Pending {
                    entries: 3,
                    files: 2,
                    bytes: 2_000_000,
                    complete: true,
                }),
                ..Default::default()
            },
        );
        let wide = render(&mut app, 140, 42);
        assert!(wide.contains("To home-omarchy"));
        assert!(wide.contains("2 files · 2.00 MB queued"));
        app.expanded = true;
        let narrow = render(&mut app, 80, 24);
        assert!(narrow.contains("To home-omarchy"));
        assert!(narrow.contains("2 files · 2.00 MB queued"));
    }
    fn sample() -> Dashboard {
        let mut app = Dashboard {
            observed_at: 100,
            config: Some(config::Config {
                name: "mac".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        app.ingest(Status {
            pid: 42,
            started: 10,
            updated: 100,
            daemon_version: "0.2.5".into(),
            received_entries: 552861,
            received_bytes: 4300230,
            send_bytes_per_sec: 1200000.0,
            folders: [(
                "trilbymedia".into(),
                daemon::FolderStatus {
                    phase: "watching".into(),
                    files: 539359,
                    bytes: 31270000000,
                    pending_conflicts: 632,
                    ..Default::default()
                },
            )]
            .into(),
            events: [
                Event {
                    at: 99,
                    kind: "received".into(),
                    folder: Some("trilbymedia".into()),
                    detail: "folder/unchanged.txt".into(),
                },
                Event {
                    at: 99,
                    kind: "conflict".into(),
                    folder: Some("trilbymedia".into()),
                    detail: "folder/kept.txt".into(),
                },
            ]
            .into(),
            ..Default::default()
        });
        app
    }
    fn render(app: &mut Dashboard, w: u16, h: u16) -> String {
        let mut t = Terminal::new(TestBackend::new(w, h)).unwrap();
        t.draw(|f| app.draw(f)).unwrap();
        t.backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect()
    }
    #[test]
    fn renders_live_stale_and_small_terminals_without_overwrite_claims() {
        let mut app = sample();
        let wide = render(&mut app, 140, 42);
        assert!(wide.contains("LIVE"));
        assert!(wide.contains("552,861 index records"));
        assert!(wide.contains("632 pending conflicts"));
        assert!(!wide.contains("unchanged.txt"));
        app.filter = Filter::Index;
        assert!(render(&mut app, 100, 30).contains("unchanged.txt"));
        app.observed_at = 106;
        let stale = render(&mut app, 140, 42);
        assert!(stale.contains("STALE / OFFLINE"));
        assert!(!stale.contains("1.20 MB/s"));
        for (w, h) in [(1, 1), (59, 19), (60, 20), (80, 24), (100, 34)] {
            render(&mut app, w, h);
        }
        assert!(render(&mut app, 40, 10).contains("Enlarge"));
    }
    #[test]
    fn keyboard_search_navigation_and_freeze_are_read_only() {
        let mut app = sample();
        let key = |c| KeyEvent::new(c, KeyModifiers::NONE);
        app.key(key(KeyCode::Char('f')));
        assert_eq!(app.events().len(), 2);
        app.key(key(KeyCode::Char('/')));
        for c in "unchanged".chars() {
            app.key(key(KeyCode::Char(c)));
        }
        app.key(key(KeyCode::Enter));
        assert_eq!(app.events().len(), 1);
        app.key(key(KeyCode::Down));
        assert_eq!(app.activity_table.selected(), Some(0));
        app.key(key(KeyCode::Char(' ')));
        assert!(app.paused);
        let dir = tempfile::tempdir().unwrap();
        app.refresh(dir.path());
        assert!(app.error.is_none());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        app.key(key(KeyCode::Char(' ')));
        app.refresh(dir.path());
        assert!(app.error.is_some());
        assert!(app.key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)));
    }
    #[test]
    fn samples_are_bounded_restart_resets_and_legacy_status_loads() {
        let mut app = sample();
        let mut s = app.status.clone().unwrap();
        app.ingest(s.clone());
        assert_eq!(app.send.len(), 1);
        for at in 101..200 {
            s.updated = at;
            app.observed_at = at;
            app.ingest(s.clone());
        }
        assert_eq!(app.send.len(), HISTORY);
        s.pid = 43;
        app.ingest(s.clone());
        assert_eq!(app.send.len(), 1);
        let mut json = serde_json::to_value(s).unwrap();
        json.as_object_mut().unwrap().remove("daemon_version");
        assert!(
            serde_json::from_value::<Status>(json)
                .unwrap()
                .daemon_version
                .is_empty()
        );
        assert_eq!(clean("a\n\u{1b}\u{202e}b"), "ab");
    }
    #[test]
    fn preview_live_snapshot() {
        // Optional local visual QA artifact; normal test runs do not read live state.
        let Ok(path) = std::env::var("YSYNC_TUI_PREVIEW") else {
            return;
        };
        let mut app = Dashboard::default();
        app.refresh(&config::default_home());
        app.filter = Filter::All;
        let mut terminal = Terminal::new(TestBackend::new(140, 42)).unwrap();
        terminal.draw(|f| app.draw(f)).unwrap();
        let buffer = terminal.backend().buffer();
        let cells: Vec<_> = buffer.content.iter().map(|c| serde_json::json!({"s":c.symbol(),"fg":format!("{:?}",c.fg),"bg":format!("{:?}",c.bg)})).collect();
        std::fs::write(path, serde_json::to_vec(&cells).unwrap()).unwrap();
    }
}
