//! On-demand conflict browser. Database/preview work stays off the terminal event loop.
use super::{AMBER, CYAN, GREEN, MUTED, TEXT, clean, panel};
use crate::{
    conflicts::{self, Conflict, Page, Review},
    model::Entry,
};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style, Stylize},
    text::Line,
    widgets::{Clear, Paragraph, Row, Table, TableState, Wrap},
};
use std::{
    path::Path,
    sync::mpsc::{self, Receiver},
};

enum Request {
    List,
    Review(String, String),
    Resolve(String, String, Entry),
}
enum Response {
    List(Page),
    Review(Box<Review>),
    Resolved,
}

#[derive(Default)]
pub(super) struct Panel {
    pub active: bool,
    folder: Option<String>,
    selected_folder: Option<String>,
    query: String,
    searching: bool,
    search_backup: String,
    notice: Option<String>,
    offset: usize,
    records: Vec<Conflict>,
    more: bool,
    table: TableState,
    review: Option<Review>,
    scroll: u16,
    confirm: bool,
    message: String,
    request: Option<Request>,
    pending: Option<Receiver<Result<Response, String>>>,
    applying: bool,
}
impl Panel {
    pub fn open(&mut self, folder: Option<String>) {
        self.active = true;
        if self.pending.is_some() {
            return;
        }
        self.folder = folder.clone();
        self.selected_folder = folder;
        self.review = None;
        self.confirm = false;
        self.offset = 0;
        self.query.clear();
        self.reload();
    }
    fn reload(&mut self) {
        self.records.clear();
        self.table = TableState::default();
        self.review = None;
        self.confirm = false;
        self.request = Some(Request::List);
    }
    pub fn busy(&self) -> bool {
        self.pending.is_some() || self.request.is_some()
    }
    pub fn tick(&mut self, home: &Path) -> bool {
        let mut dirty = false;
        if let Some(rx) = &self.pending {
            match rx.try_recv() {
                Ok(result) => {
                    self.pending = None;
                    self.applying = false;
                    dirty = true;
                    match result {
                        Ok(Response::List(page)) => {
                            self.records = page.records;
                            self.more = page.more;
                            self.table.select(if self.records.is_empty() {
                                None
                            } else {
                                Some(0)
                            });
                            self.message = self.notice.take().unwrap_or_else(|| {
                                "Enter reviews one record. Nothing is resolved automatically."
                                    .into()
                            });
                        }
                        Ok(Response::Review(review)) => {
                            self.review = Some(*review);
                            self.scroll = 0;
                            self.message =
                                "Reviewed local version is checked again at confirmation.".into();
                        }
                        Ok(Response::Resolved) => {
                            self.reload();
                            self.notice = Some("Kept reviewed local version. Restart the service to propagate the decision. Incoming archive retained.".into());
                        }
                        Err(e) => {
                            self.message = e;
                            self.confirm = false;
                        }
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.pending = None;
                    self.applying = false;
                    self.message = "Review worker stopped; refresh to retry.".into();
                    dirty = true;
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        if self.pending.is_none()
            && let Some(request) = self.request.take()
        {
            self.applying = matches!(request, Request::Resolve(..));
            let home = home.to_owned();
            let folder = self.folder.clone();
            let query = self.query.clone();
            let offset = self.offset;
            let (tx, rx) = mpsc::sync_channel(1);
            self.pending = Some(rx);
            dirty = true;
            std::thread::spawn(move || {
                let result = match request {
                    Request::List => conflicts::page(&home, folder.as_deref(), &query, offset)
                        .map(Response::List),
                    Request::Review(folder, id) => conflicts::review(&home, &folder, &id)
                        .map(|r| Response::Review(Box::new(r))),
                    Request::Resolve(folder, id, current) => {
                        conflicts::keep_local_reviewed(&home, &folder, &id, &current)
                            .map(|()| Response::Resolved)
                    }
                }
                .map_err(|e| format!("{e:#}"));
                let _ = tx.send(result);
            });
        }
        dirty
    }
    /// Return true only for a quit request. No mutation is dispatched until explicit confirmation.
    pub fn key(&mut self, key: KeyEvent) -> bool {
        if key.kind != crossterm::event::KeyEventKind::Press {
            return false;
        }
        if self.busy() {
            if !self.applying && matches!(key.code, KeyCode::Esc | KeyCode::Char('c')) {
                self.active = false;
            }
            return !self.applying && key.code == KeyCode::Char('q');
        }
        if self.searching {
            match key.code {
                KeyCode::Esc => {
                    self.searching = false;
                    self.query = self.search_backup.clone();
                }
                KeyCode::Enter => {
                    self.searching = false;
                    self.offset = 0;
                    self.reload();
                }
                KeyCode::Backspace => {
                    self.query.pop();
                }
                KeyCode::Char(c) if !c.is_control() && self.query.len() < 200 => self.query.push(c),
                _ => {}
            }
            return false;
        }
        if self.confirm {
            match key.code {
                KeyCode::Char('y') => {
                    if let Some(review) = &self.review
                        && let Some(current) = &review.current
                    {
                        self.request = Some(Request::Resolve(
                            review.record.folder.clone(),
                            review.record.id.clone(),
                            current.clone(),
                        ));
                    }
                    self.confirm = false;
                }
                _ => self.confirm = false,
            }
            return false;
        }
        if let Some(review) = &self.review {
            match key.code {
                KeyCode::Esc | KeyCode::Char('c') => {
                    self.review = None;
                    self.scroll = 0;
                }
                KeyCode::Char('q') => return true,
                KeyCode::Down | KeyCode::Char('j') => self.scroll = self.scroll.saturating_add(1),
                KeyCode::Up | KeyCode::Char('k') => self.scroll = self.scroll.saturating_sub(1),
                KeyCode::PageDown => self.scroll = self.scroll.saturating_add(10),
                KeyCode::PageUp => self.scroll = self.scroll.saturating_sub(10),
                KeyCode::Char('r') => {
                    self.request = Some(Request::Review(
                        review.record.folder.clone(),
                        review.record.id.clone(),
                    ))
                }
                KeyCode::Char('l') => {
                    if review.current.is_some() {
                        self.confirm = true;
                    } else {
                        self.message =
                            "No indexed local version available; run a scan and review again."
                                .into();
                    }
                }
                _ => {}
            }
            return false;
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('c') => self.active = false,
            KeyCode::Char('q') => return true,
            KeyCode::Char('/') => {
                self.search_backup = self.query.clone();
                self.searching = true;
            }
            KeyCode::Char('x') => {
                self.query.clear();
                self.offset = 0;
                self.reload();
            }
            KeyCode::Char('r') => self.reload(),
            KeyCode::Char('f') => {
                self.folder = if self.folder.is_some() {
                    None
                } else {
                    self.selected_folder.clone()
                };
                self.offset = 0;
                self.reload();
            }
            KeyCode::Char('n') | KeyCode::PageDown if self.more => {
                self.offset += 50;
                self.reload();
            }
            KeyCode::Char('p') | KeyCode::PageUp if self.offset > 0 => {
                self.offset = self.offset.saturating_sub(50);
                self.reload();
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !self.records.is_empty() {
                    self.table.select(Some(
                        (self.table.selected().unwrap_or(0) + 1).min(self.records.len() - 1),
                    ));
                }
            }
            KeyCode::Up | KeyCode::Char('k') => self
                .table
                .select(self.table.selected().map(|i| i.saturating_sub(1))),
            KeyCode::Enter => {
                if let Some(record) = self.table.selected().and_then(|i| self.records.get(i)) {
                    self.request = Some(Request::Review(record.folder.clone(), record.id.clone()));
                }
            }
            _ => {}
        }
        false
    }
    pub fn draw(&mut self, frame: &mut Frame, area: Rect) {
        frame.render_widget(Clear, area);
        let block = panel(" CONFLICT REVIEW · on demand ", true);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let sections = Layout::vertical([
            Constraint::Length(2),
            Constraint::Min(1),
            Constraint::Length(3),
            Constraint::Length(2),
        ])
        .split(inner);
        if let Some(review) = &self.review {
            frame.render_widget(
                Paragraph::new(format!(
                    "{} / {}",
                    clean(&review.record.folder),
                    clean(&review.record.incoming.path)
                ))
                .fg(CYAN)
                .wrap(Wrap { trim: false }),
                sections[0],
            );
            let columns = if area.width >= 100 {
                Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(sections[1])
            } else {
                Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(sections[1])
            };
            let local = review
                .current
                .as_ref()
                .map(|e| entry_text(e, &review.local_preview))
                .unwrap_or_else(|| "No indexed local version".into());
            let local = format!("Source: working version\n{local}");
            let incoming = format!(
                "Source: preserved incoming\n{}",
                entry_text(&review.record.incoming, &review.incoming_preview)
            );
            for (rect, title, text) in [
                (columns[0], " CURRENT LOCAL · reviewed ", local),
                (columns[1], " PRESERVED INCOMING ", incoming),
            ] {
                frame.render_widget(
                    Paragraph::new(text)
                        .block(panel(title, false))
                        .fg(TEXT)
                        .wrap(Wrap { trim: false })
                        .scroll((self.scroll, 0)),
                    rect,
                );
            }
            let message = format!(
                "{}\nConflict {}\nArchive: {}",
                clean(&self.message),
                clean(&review.record.id),
                clean(
                    review
                        .record
                        .payload
                        .as_deref()
                        .unwrap_or("no file payload for this version")
                )
            );
            frame.render_widget(
                Paragraph::new(message).fg(AMBER).wrap(Wrap { trim: false }),
                sections[2],
            );
            frame.render_widget(Paragraph::new("q quit · Esc list · ↑↓ scroll · r refresh · l keep local\nIncoming selection/manual merge: compare externally, then refresh and keep local.").fg(MUTED),sections[3]);
        } else {
            let scope = self.folder.as_deref().unwrap_or("all folders");
            frame.render_widget(
                Paragraph::new(format!(
                    "{} · rows {}–{}{} · search: {}",
                    clean(scope),
                    if self.records.is_empty() {
                        0
                    } else {
                        self.offset + 1
                    },
                    self.offset + self.records.len(),
                    if self.more { " · more →" } else { "" },
                    clean(&self.query)
                ))
                .fg(CYAN)
                .wrap(Wrap { trim: false }),
                sections[0],
            );
            let rows = self.records.iter().map(|r| {
                Row::new([
                    clean(&r.folder),
                    format!("{:?} / {:?}", r.local.kind, r.incoming.kind),
                    clean(&r.incoming.path),
                ])
            });
            if self.records.is_empty() {
                frame.render_widget(Paragraph::new(if self.busy(){"Loading conflicts…"}else{"No matching conflicts on this page. r refreshes; f changes scope; p goes back."}).fg(MUTED).wrap(Wrap{trim:true}),sections[1]);
            } else {
                frame.render_stateful_widget(
                    Table::new(
                        rows,
                        [
                            Constraint::Percentage(20),
                            Constraint::Length(21),
                            Constraint::Min(10),
                        ],
                    )
                    .header(Row::new(["Folder", "Recorded local / incoming", "Path"]).fg(MUTED))
                    .column_spacing(1)
                    .highlight_symbol("› ")
                    .row_highlight_style(Style::new().bg(Color::Rgb(39, 59, 77))),
                    sections[1],
                    &mut self.table,
                );
            }
            frame.render_widget(
                Paragraph::new(if self.searching {
                    format!("Search /{}▏ · Enter applies", clean(&self.query))
                } else {
                    clean(&self.message)
                })
                .fg(AMBER)
                .wrap(Wrap { trim: false }),
                sections[2],
            );
            frame.render_widget(Paragraph::new("q quit · Esc dashboard · Enter review · ↑↓ select\nn/p page · / search · x clear · f scope · r refresh").fg(MUTED),sections[3]);
        }
        if self.busy() {
            frame.render_widget(Clear, sections[2]);
            frame.render_widget(
                Paragraph::new(if self.applying {
                    "Applying reviewed decision…"
                } else {
                    "Loading…"
                })
                .fg(GREEN),
                sections[2],
            );
        }
        if self.confirm {
            let width = area.width.min(84);
            let height = area.height.min(18);
            let popup = Rect::new(
                area.x + (area.width - width) / 2,
                area.y + (area.height - height) / 2,
                width,
                height,
            );
            frame.render_widget(Clear, popup);
            let review = self.review.as_ref().unwrap();
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from("Keep the reviewed current LOCAL version?".bold()),
                    Line::from(
                        clean(&format!(
                            "{} / {}",
                            review.record.folder, review.record.incoming.path
                        ))
                        .chars()
                        .take(96)
                        .collect::<String>(),
                    ),
                    Line::from(""),
                    Line::from("Resolves this incoming record. Local version propagates."),
                    Line::from("A local Deleted version propagates a deletion."),
                    Line::from("Archive retained. Working contents stay unchanged."),
                    Line::from(""),
                    Line::from("Stop daemon first: brew services stop ysync"),
                    Line::from("Changed versions are rejected; refresh and review again."),
                    Line::from(""),
                    Line::from("y confirms · any other key cancels".fg(AMBER)),
                ])
                .block(panel(" CONFIRM ONE RESOLUTION ", true))
                .wrap(Wrap { trim: false }),
                popup,
            );
        }
    }
}
fn entry_text(e: &Entry, preview: &str) -> String {
    let clocks = e
        .clock
        .iter()
        .map(|(id, n)| format!("{}:{n}", id.chars().take(12).collect::<String>()))
        .collect::<Vec<_>>()
        .join("  ");
    let body = preview
        .lines()
        .enumerate()
        .map(|(i, line)| format!("{:>4}  {}", i + 1, clean(line)))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "Path {}\n{:?} · {} bytes · mode {:03o}\nHash {}\nVersions {}\n\n{}",
        clean(&e.path),
        e.kind,
        e.size,
        e.mode,
        clean(&e.hash),
        clean(&clocks),
        body
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Kind;
    fn entry() -> Entry {
        Entry {
            path: "example.txt".into(),
            kind: Kind::Deleted,
            size: 0,
            hash: String::new(),
            target: None,
            mode: 0,
            clock: Default::default(),
            seq: 0,
            stamp: String::new(),
        }
    }
    #[test]
    fn only_explicit_confirmation_schedules_resolution() {
        let e = entry();
        let mut panel = Panel {
            active: true,
            review: Some(Review {
                record: Conflict {
                    id: "a".repeat(64),
                    folder: "code".into(),
                    local: e.clone(),
                    incoming: e.clone(),
                    payload: None,
                },
                current: Some(e),
                local_preview: String::new(),
                incoming_preview: String::new(),
            }),
            ..Default::default()
        };
        let key = |c| KeyEvent::new(KeyCode::Char(c), crossterm::event::KeyModifiers::NONE);
        panel.key(key('l'));
        assert!(panel.confirm);
        assert!(panel.request.is_none());
        panel.key(key('n'));
        assert!(!panel.confirm);
        assert!(panel.request.is_none());
        panel.key(key('l'));
        panel.key(key('y'));
        assert!(matches!(panel.request, Some(Request::Resolve(..))));
    }
    #[test]
    fn review_renders_narrow_and_wide() {
        use ratatui::{Terminal, backend::TestBackend};
        let mut e = entry();
        e.kind = Kind::File;
        e.path = "config/settings.yaml".into();
        e.mode = 0o644;
        let local_text = "name: example\nworkers: 2\nrescan: 3600\n";
        let incoming_text = "name: example\nworkers: 4\nrescan: 1800\n";
        e.size = local_text.len() as u64;
        e.hash = blake3::hash(local_text.as_bytes()).to_hex().to_string();
        e.clock = [("a".repeat(64), 7)].into();
        let mut incoming = e.clone();
        incoming.hash = blake3::hash(incoming_text.as_bytes()).to_hex().to_string();
        incoming.clock = [("b".repeat(64), 4)].into();
        let mut panel = Panel {
            active: true,
            review: Some(Review {
                record: Conflict {
                    id: "a".repeat(64),
                    folder: "code".into(),
                    local: e.clone(),
                    incoming,
                    payload: Some(format!(".ysync/conflicts/{}", "a".repeat(64))),
                },
                current: Some(e),
                local_preview: "name: example\nworkers: 2\nrescan: 3600\n".into(),
                incoming_preview: "name: example\nworkers: 4\nrescan: 1800\n".into(),
            }),
            ..Default::default()
        };
        for (w, h) in [(60, 17), (80, 21), (140, 39), (140, 42)] {
            let mut t = Terminal::new(TestBackend::new(w, h)).unwrap();
            t.draw(|f| panel.draw(f, f.area())).unwrap();
            let text = t
                .backend()
                .buffer()
                .content
                .iter()
                .map(|c| c.symbol())
                .collect::<String>();
            assert!(text.contains("CURRENT LOCAL"));
            assert!(text.contains("PRESERVED INCOMING"));
            if w == 140
                && h == 42
                && let Ok(path) = std::env::var("YSYNC_CONFLICT_PREVIEW")
            {
                let cells: Vec<_> = t.backend().buffer().content.iter().map(|c| serde_json::json!({"s":c.symbol(),"fg":format!("{:?}",c.fg),"bg":format!("{:?}",c.bg)})).collect();
                std::fs::write(path, serde_json::to_vec(&cells).unwrap()).unwrap();
            }
            panel.confirm = true;
            t.draw(|f| panel.draw(f, f.area())).unwrap();
            let text = t
                .backend()
                .buffer()
                .content
                .iter()
                .map(|c| c.symbol())
                .collect::<String>();
            assert!(
                text.contains("y confirms"),
                "confirmation controls must stay visible at {w}x{h}"
            );
            panel.confirm = false;
        }
    }
}
