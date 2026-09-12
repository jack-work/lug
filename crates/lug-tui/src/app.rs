//! The viewer: terminal lifecycle, the event loop, and the one footer line.

use std::io::{Stdout, stdout};
use std::time::Instant;

use anyhow::{Context, Result};
use lug_proto::{Event as Arrival, Version};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::crossterm::{cursor, execute};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::{Frame, Viewport};
use tokio::sync::{mpsc, watch};

use crate::follow::{Follow, View};
use crate::json;
use crate::tail::Tail;
use crate::tree::Tree;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Log,
    Reducible,
}

enum Body {
    Tail(Tail),
    Tree(Tree),
}

pub struct App {
    name: String,
    version: Version,
    body: Body,
    quit: bool,
}

impl App {
    fn new(name: String, mode: Mode, view: Option<View>) -> Self {
        let (body, version) = match mode {
            Mode::Log => (Body::Tail(Tail::default()), 0),
            Mode::Reducible => {
                let view = view.expect("reducible mode is refused without a view");
                (Body::Tree(Tree::new(&view.value)), view.version)
            }
        };
        Self { name, version, body, quit: false }
    }

    fn on_arrival(&mut self, arrival: Arrival) {
        self.version = arrival.cursor();
        match &mut self.body {
            Body::Tail(tail) => tail.push(arrival),
            Body::Tree(tree) => {
                if matches!(arrival, Arrival::Gap { .. }) {
                    tree.gap();
                }
            }
        }
    }

    fn on_view(&mut self, view: Option<View>) {
        let Some(view) = view else { return };
        self.version = view.version;
        if let Body::Tree(tree) = &mut self.body {
            tree.update(&view.value);
        }
    }

    fn flash_deadline(&self) -> Option<Instant> {
        match &self.body {
            Body::Tree(tree) => tree.flash_deadline(),
            Body::Tail(_) => None,
        }
    }

    fn expire_flash(&mut self) {
        if let Body::Tree(tree) = &mut self.body {
            tree.expire_flash();
        }
    }

    fn on_event(&mut self, event: Event) {
        let Event::Key(key) = event else { return };
        if key.kind != KeyEventKind::Press {
            return;
        }
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => self.quit = true,
            KeyCode::Char('q') | KeyCode::Esc => self.quit = true,
            KeyCode::Up => self.scroll(1),
            KeyCode::Down => self.scroll(-1),
            KeyCode::PageUp => {
                let page = self.page();
                self.scroll(page);
            }
            KeyCode::PageDown => {
                let page = self.page();
                self.scroll(-page);
            }
            KeyCode::Home => match &mut self.body {
                Body::Tail(tail) => tail.to_top(),
                Body::Tree(tree) => tree.to_top(),
            },
            KeyCode::End => match &mut self.body {
                Body::Tail(tail) => tail.to_bottom(),
                Body::Tree(tree) => tree.to_bottom(),
            },
            _ => {}
        }
    }

    fn scroll(&mut self, delta: isize) {
        match &mut self.body {
            Body::Tail(tail) => tail.scroll(delta),
            Body::Tree(tree) => tree.scroll(delta),
        }
    }

    fn page(&self) -> isize {
        match &self.body {
            Body::Tail(tail) => tail.page(),
            Body::Tree(tree) => tree.page(),
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        let [body, footer] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(frame.area());
        match &mut self.body {
            Body::Tail(tail) => tail.render(frame, body),
            Body::Tree(tree) => tree.render(frame, body),
        }
        self.footer(frame, footer);
    }

    fn footer(&self, frame: &mut Frame, area: Rect) {
        let state = match &self.body {
            Body::Tail(tail) if tail.following() => "tail",
            Body::Tail(_) => "tail paused",
            Body::Tree(tree) if tree.resynced() => "view resynced",
            Body::Tree(_) => "view",
        };
        let dim = Style::new().fg(json::GUTTER).add_modifier(Modifier::DIM);
        let left = format!(" {}  v{}  {}", self.name, self.version, state);
        let right = "q quit ";
        let gap = (area.width as usize)
            .saturating_sub(json::display_width(&left) + json::display_width(right));
        let line = Line::from(vec![
            Span::styled(left, dim),
            Span::raw(" ".repeat(gap)),
            Span::styled(right, dim),
        ]);
        frame.render_widget(Paragraph::new(line), area);
    }
}

pub async fn run(name: String, mode: Mode, mut follower: Box<dyn Follow>) -> Result<()> {
    let mut arrivals = follower.events();
    let mut changed = follower.changed();
    let mut app = App::new(name, mode, follower.view());

    let mut terminal = Term::enter()?;
    let mut input = spawn_input();

    while !app.quit {
        terminal.draw(&mut app)?;
        let deadline = app.flash_deadline();
        tokio::select! {
            event = input.recv() => match event {
                Some(event) => app.on_event(event),
                // The reader is gone, so nothing can quit us but a signal.
                None => app.quit = true,
            },
            Some(arrival) = next_arrival(&mut arrivals) => {
                app.on_arrival(arrival);
                drain(&mut arrivals, &mut app);
            }
            () = next_change(&mut changed) => on_change(&mut app, &mut arrivals, follower.view()),
            () = at(deadline) => app.expire_flash(),
        }
    }
    Ok(())
}

async fn next_arrival(arrivals: &mut Option<mpsc::Receiver<Arrival>>) -> Option<Arrival> {
    match arrivals {
        Some(rx) => {
            let arrival = rx.recv().await;
            if arrival.is_none() {
                *arrivals = None;
            }
            arrival
        }
        None => std::future::pending().await,
    }
}

/// The version moved. Whatever is already queued is read first: a gap is sent
/// before the view refetched after it, and taking the view alone would diff it
/// against a state the missing patches never reached, lighting a subtree that
/// nobody watched change.
fn on_change(app: &mut App, arrivals: &mut Option<mpsc::Receiver<Arrival>>, view: Option<View>) {
    drain(arrivals, app);
    app.on_view(view);
}

/// One draw for a burst, not one per record.
fn drain(arrivals: &mut Option<mpsc::Receiver<Arrival>>, app: &mut App) {
    while let Some(arrival) = arrivals.as_mut().and_then(|rx| rx.try_recv().ok()) {
        app.on_arrival(arrival);
    }
}

async fn next_change(changed: &mut watch::Receiver<Version>) {
    // A dead sender must park, not spin the loop at the speed of the CPU.
    if changed.changed().await.is_err() {
        std::future::pending().await
    }
}

async fn at(deadline: Option<Instant>) {
    match deadline {
        Some(instant) => tokio::time::sleep_until(instant.into()).await,
        None => std::future::pending().await,
    }
}

/// Keystrokes arrive on a thread because reading them is a blocking syscall.
fn spawn_input() -> mpsc::Receiver<Event> {
    let (tx, rx) = mpsc::channel(64);
    std::thread::Builder::new()
        .name("lug-tui-input".into())
        .spawn(move || {
            while let Ok(event) = event::read() {
                if tx.blocking_send(event).is_err() {
                    return;
                }
            }
        })
        .expect("spawning the input thread");
    rx
}

struct Term {
    inner: Terminal<CrosstermBackend<Stdout>>,
}

impl Term {
    fn enter() -> Result<Self> {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            // A panic behind the alternate screen is a panic nobody can read.
            let _ = restore();
            previous(info);
        }));

        enable_raw_mode().context("entering raw mode")?;
        execute!(stdout(), EnterAlternateScreen, cursor::Hide)
            .context("entering the alternate screen")?;
        let backend = CrosstermBackend::new(stdout());
        let options = ratatui::TerminalOptions { viewport: Viewport::Fullscreen };
        let inner = Terminal::with_options(backend, options).context("starting ratatui")?;
        Ok(Self { inner })
    }

    fn draw(&mut self, app: &mut App) -> Result<()> {
        self.inner.draw(|frame| app.render(frame))?;
        Ok(())
    }
}

impl Drop for Term {
    fn drop(&mut self) {
        let _ = restore();
    }
}

fn restore() -> std::io::Result<()> {
    execute!(stdout(), LeaveAlternateScreen, cursor::Show)?;
    disable_raw_mode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use serde_json::json;

    fn screen(app: &mut App, width: u16, height: u16) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| app.render(frame)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        (0..height)
            .map(|y| {
                (0..width).map(|x| buffer[(x, y)].symbol().to_owned()).collect::<String>()
            })
            .collect()
    }

    #[test]
    fn the_footer_carries_the_name_version_and_state() {
        let mut app = App::new("orders".into(), Mode::Log, None);
        app.on_arrival(record(9, json!({"a": 1})));
        let lines = screen(&mut app, 40, 4);
        let footer = lines.last().unwrap();
        assert!(footer.contains("orders"), "{footer:?}");
        assert!(footer.contains("v9"), "{footer:?}");
        assert!(footer.contains("tail"), "{footer:?}");
        assert!(footer.contains("q quit"), "{footer:?}");
    }

    #[test]
    fn scrolling_up_marks_the_tail_paused() {
        let mut app = App::new("orders".into(), Mode::Log, None);
        for v in 1..=20 {
            app.on_arrival(record(v, json!({"n": v})));
        }
        screen(&mut app, 40, 4);
        app.on_event(key(KeyCode::Up));
        let footer = screen(&mut app, 40, 4).last().unwrap().clone();
        assert!(footer.contains("tail paused"), "{footer:?}");
        app.on_event(key(KeyCode::End));
        let footer = screen(&mut app, 40, 4).last().unwrap().clone();
        assert!(footer.contains("tail") && !footer.contains("paused"), "{footer:?}");
    }

    #[test]
    fn a_view_update_moves_the_version_indicator() {
        let view = View { version: 3, value: json!({"a": 1}) };
        let mut app = App::new("orders".into(), Mode::Reducible, Some(view));
        app.on_view(Some(View { version: 4, value: json!({"a": 2}) }));
        let lines = screen(&mut app, 40, 4);
        assert!(lines[0].contains("a: 2"), "{lines:?}");
        assert!(lines.last().unwrap().contains("v4"), "{lines:?}");
    }

    #[test]
    fn a_gap_says_so_in_the_footer_and_forgets_the_old_view() {
        let view = View { version: 3, value: json!({"a": 1}) };
        let mut app = App::new("orders".into(), Mode::Reducible, Some(view));
        app.on_arrival(Arrival::Gap { from: 3, to: 91 });
        app.on_view(Some(View { version: 91, value: json!({"a": 2}) }));
        let lines = screen(&mut app, 40, 4);
        assert!(lines[0].contains("a: 2"), "{lines:?}");
        assert!(!lines[0].starts_with('▌'), "a refetched view was lit as a change: {lines:?}");
        let footer = lines.last().expect("a footer").clone();
        assert!(footer.contains("v91"), "{footer:?}");
        assert!(footer.contains("view resynced"), "{footer:?}");
    }

    #[test]
    fn a_gap_queued_behind_a_version_advance_is_read_first() {
        let view = View { version: 3, value: json!({"a": 1, "b": 2}) };
        let mut app = App::new("orders".into(), Mode::Reducible, Some(view));

        // Both are ready at once in the real loop, which is the whole point:
        // the outcome must not depend on which the runtime notices first.
        let (tx, rx) = mpsc::channel(4);
        tx.try_send(Arrival::Gap { from: 3, to: 91 }).expect("queueing the gap");
        let mut arrivals = Some(rx);
        on_change(&mut app, &mut arrivals, Some(View { version: 91, value: json!({"a": 5}) }));

        let lines = screen(&mut app, 40, 4);
        assert!(lines[0].contains("a: 5"), "{lines:?}");
        assert!(
            !lines.iter().any(|l| l.starts_with('▌')),
            "the view refetched after a gap was diffed against a stale one: {lines:?}"
        );
        assert!(lines.last().expect("a footer").contains("view resynced"), "{lines:?}");
    }

    #[test]
    fn quitting_is_q_esc_and_ctrl_c() {
        for code in [KeyCode::Char('q'), KeyCode::Esc] {
            let mut app = App::new("x".into(), Mode::Log, None);
            app.on_event(key(code));
            assert!(app.quit, "{code:?} did not quit");
        }
        let mut app = App::new("x".into(), Mode::Log, None);
        app.on_event(Event::Key(ratatui::crossterm::event::KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        )));
        assert!(app.quit);
    }

    fn record(version: u64, patch: serde_json::Value) -> Arrival {
        Arrival::Record(lug_proto::Record { version, patch })
    }

    fn key(code: KeyCode) -> Event {
        Event::Key(ratatui::crossterm::event::KeyEvent::new(code, KeyModifiers::NONE))
    }
}
