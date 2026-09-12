//! Log mode: a tail. Records arrive newest at the bottom, one block each.

use std::collections::VecDeque;

use lug_proto::Record;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::json;

/// Records kept in scrollback. Older ones fall off the top.
const KEEP: usize = 4096;

/// Dropping one record at a time would re-flow the cache on every arrival.
const TRIM_BATCH: usize = 256;

#[derive(Default)]
pub struct Tail {
    entries: VecDeque<Entry>,
    width: u16,
    /// Lines between the bottom of the content and the bottom of the viewport.
    /// Zero is follow mode.
    offset: usize,
    height: usize,
}

struct Entry {
    record: Record,
    lines: Vec<Line<'static>>,
}

impl Tail {

    pub fn following(&self) -> bool {
        self.offset == 0
    }

    pub fn push(&mut self, record: Record) {
        if self.entries.len() >= KEEP + TRIM_BATCH {
            self.entries.drain(..TRIM_BATCH);
        }
        let lines = render(&record, self.width);
        // Scrolled-back readers keep looking at the same rows while the bottom
        // grows underneath them.
        if self.offset > 0 {
            self.offset += lines.len();
        }
        self.entries.push_back(Entry { record, lines });
    }

    pub fn scroll(&mut self, delta: isize) {
        let max = self.total_lines().saturating_sub(self.height);
        let next = self.offset as isize + delta;
        self.offset = next.clamp(0, max as isize) as usize;
    }

    pub fn page(&self) -> isize {
        self.height.max(1) as isize
    }

    pub fn to_bottom(&mut self) {
        self.offset = 0;
    }

    pub fn to_top(&mut self) {
        self.offset = self.total_lines().saturating_sub(self.height);
    }

    fn total_lines(&self) -> usize {
        self.entries.iter().map(|e| e.lines.len()).sum()
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect) {
        if area.width != self.width {
            self.width = area.width;
            for entry in &mut self.entries {
                entry.lines = render(&entry.record, self.width);
            }
            // Widths change line counts, and an offset in the old units is a
            // lie. Resizing while scrolled back lands you at the bottom.
            self.offset = 0;
        }
        self.height = area.height as usize;

        let total = self.total_lines();
        let end = total.saturating_sub(self.offset);
        let start = end.saturating_sub(self.height);

        let mut lines = Vec::with_capacity(self.height);
        let mut at = 0usize;
        for entry in &self.entries {
            let next = at + entry.lines.len();
            if next > start && at < end {
                let from = start.saturating_sub(at);
                let to = (end - at).min(entry.lines.len());
                lines.extend(entry.lines[from..to].iter().cloned());
            }
            at = next;
        }
        frame.render_widget(Paragraph::new(lines), area);
    }
}

fn render(record: &Record, width: u16) -> Vec<Line<'static>> {
    let gutter = Span::styled(
        format!("{:>7} ", record.version),
        Style::new().fg(json::GUTTER).add_modifier(ratatui::style::Modifier::DIM),
    );
    json::wrap(gutter, &json::spans(&record.patch), width as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use serde_json::json;

    fn record(version: u64) -> Record {
        Record { version, patch: json!({ "n": version }) }
    }

    fn draw(tail: &mut Tail, width: u16, height: u16) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| tail.render(frame, frame.area())).unwrap();
        let buffer = terminal.backend().buffer().clone();
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buffer[(x, y)].symbol().to_owned())
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect()
    }

    #[test]
    fn newest_record_sits_at_the_bottom() {
        let mut tail = Tail::default();
        for v in 1..=3 {
            tail.push(record(v));
        }
        let screen = draw(&mut tail, 40, 3);
        assert!(screen[2].contains("\"n\": 3"), "{screen:?}");
        assert!(screen[0].contains("\"n\": 1"), "{screen:?}");
    }

    #[test]
    fn scrolling_up_holds_position_while_records_arrive() {
        let mut tail = Tail::default();
        for v in 1..=10 {
            tail.push(record(v));
        }
        draw(&mut tail, 40, 4);
        tail.scroll(1);
        let before = draw(&mut tail, 40, 4);
        assert!(!tail.following());
        tail.push(record(11));
        let after = draw(&mut tail, 40, 4);
        assert_eq!(before, after, "arrival moved a paused viewport");
    }

    #[test]
    fn end_resumes_following() {
        let mut tail = Tail::default();
        for v in 1..=10 {
            tail.push(record(v));
        }
        draw(&mut tail, 40, 4);
        tail.scroll(3);
        tail.to_bottom();
        assert!(tail.following());
        let screen = draw(&mut tail, 40, 4);
        assert!(screen[3].contains("\"n\": 10"), "{screen:?}");
    }

    #[test]
    fn scrolling_stops_at_the_ends() {
        let mut tail = Tail::default();
        for v in 1..=5 {
            tail.push(record(v));
        }
        draw(&mut tail, 40, 3);
        tail.scroll(100);
        assert_eq!(tail.offset, 2, "scrolled past the oldest record");
        tail.scroll(-100);
        assert_eq!(tail.offset, 0);
    }

    #[test]
    fn old_records_fall_off_the_top() {
        let mut tail = Tail::default();
        for v in 1..=(KEEP as u64 + TRIM_BATCH as u64 + 1) {
            tail.push(record(v));
        }
        assert!(tail.entries.len() <= KEEP + TRIM_BATCH);
        let screen = draw(&mut tail, 40, 2);
        assert!(screen[1].contains(&format!("\"n\": {}", KEEP + TRIM_BATCH + 1)), "{screen:?}");
    }
}
