//! Reducible mode: the live materialized view as a tree, with the subtree that
//! just changed briefly lit so state can be watched moving.

use std::time::{Duration, Instant};

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use serde_json::Value;

use crate::json;

/// Long enough to catch the eye, short enough that a busy log is not a strobe.
const FLASH: Duration = Duration::from_millis(900);

const FLASH_BG: Color = Color::Indexed(17);
const BAR: &str = "▌ ";
const NO_BAR: &str = "  ";

pub struct Tree {
    previous: Option<Value>,
    lines: Vec<Node>,
    flash_until: Option<Instant>,
    offset: usize,
    height: usize,
}

struct Node {
    depth: usize,
    spans: Vec<Span<'static>>,
    changed: bool,
}

impl Tree {
    pub fn new(value: &Value) -> Self {
        let mut tree = Self {
            previous: None,
            lines: Vec::new(),
            flash_until: None,
            offset: 0,
            height: 0,
        };
        tree.update(value);
        // Nothing has moved yet; the first paint is state, not a change.
        tree.flash_until = None;
        for line in &mut tree.lines {
            line.changed = false;
        }
        tree
    }

    pub fn update(&mut self, value: &Value) {
        self.lines = flatten(value, self.previous.as_ref());
        if self.lines.iter().any(|l| l.changed) {
            self.flash_until = Some(Instant::now() + FLASH);
        }
        self.previous = Some(value.clone());
        let max = self.lines.len().saturating_sub(self.height);
        self.offset = self.offset.min(max);
    }

    /// When the highlight should be repainted away, if it is lit.
    pub fn flash_deadline(&self) -> Option<Instant> {
        self.flash_until
    }

    pub fn expire_flash(&mut self) {
        self.flash_until = None;
    }

    /// Positive scrolls up, towards the root, as in the tail.
    pub fn scroll(&mut self, delta: isize) {
        let max = self.lines.len().saturating_sub(self.height) as isize;
        self.offset = (self.offset as isize - delta).clamp(0, max.max(0)) as usize;
    }

    pub fn page(&self) -> isize {
        self.height.max(1) as isize
    }

    pub fn to_top(&mut self) {
        self.offset = 0;
    }

    pub fn to_bottom(&mut self) {
        self.offset = self.lines.len().saturating_sub(self.height);
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect) {
        self.height = area.height as usize;
        if self.flash_until.is_some_and(|at| at <= Instant::now()) {
            self.flash_until = None;
        }
        let lit = self.flash_until.is_some();

        let width = area.width as usize;
        let visible = self.lines.iter().skip(self.offset).take(self.height);
        let lines: Vec<Line<'static>> = visible
            .map(|node| {
                let on = lit && node.changed;
                let bar = if on { BAR } else { NO_BAR };
                let indent = format!("{bar}{}", " ".repeat(node.depth * 2));
                let mut spans = vec![Span::styled(indent, Style::new().fg(json::PUNCT))];
                spans.extend(node.spans.iter().cloned());
                let spans = json::truncate(spans, width);
                let line = Line::from(spans);
                if on { line.style(Style::new().bg(FLASH_BG)) } else { line }
            })
            .collect();
        frame.render_widget(Paragraph::new(lines), area);
    }
}

fn flatten(value: &Value, old: Option<&Value>) -> Vec<Node> {
    let mut out = Vec::new();
    match value {
        // The root container is the frame of the view, not a row in it.
        Value::Object(fields) => {
            let old = old.and_then(Value::as_object);
            for (key, child) in fields {
                walk(Some(key_span(key)), child, old.and_then(|m| m.get(key)), 0, false, &mut out);
            }
        }
        Value::Array(items) => {
            let old = old.and_then(Value::as_array);
            for (i, child) in items.iter().enumerate() {
                walk(Some(index_span(i)), child, old.and_then(|a| a.get(i)), 0, false, &mut out);
            }
        }
        scalar => walk(None, scalar, old, 0, false, &mut out),
    }
    out
}

/// `forced` carries "an ancestor was replaced", which is what makes a whole new
/// subtree light up instead of only its leaves.
fn walk(
    key: Option<Span<'static>>,
    value: &Value,
    old: Option<&Value>,
    depth: usize,
    forced: bool,
    out: &mut Vec<Node>,
) {
    match value {
        Value::Object(fields) => {
            let old = old.and_then(Value::as_object);
            let changed = forced || old.is_none();
            out.push(Node { depth, spans: header(key, "{", fields.len(), "}"), changed });
            for (k, child) in fields {
                walk(
                    Some(key_span(k)),
                    child,
                    old.and_then(|m| m.get(k)),
                    depth + 1,
                    changed,
                    out,
                );
            }
        }
        Value::Array(items) => {
            let old = old.and_then(Value::as_array);
            let changed = forced || old.is_none();
            out.push(Node { depth, spans: header(key, "[", items.len(), "]"), changed });
            for (i, child) in items.iter().enumerate() {
                walk(
                    Some(index_span(i)),
                    child,
                    old.and_then(|a| a.get(i)),
                    depth + 1,
                    changed,
                    out,
                );
            }
        }
        scalar => {
            let changed = forced || old != Some(scalar);
            let mut spans = Vec::new();
            if let Some(key) = key {
                spans.push(key);
                spans.push(Span::styled(": ", Style::new().fg(json::PUNCT)));
            }
            spans.extend(json::spans(scalar));
            out.push(Node { depth, spans, changed });
        }
    }
}

fn header(key: Option<Span<'static>>, open: &str, len: usize, close: &str) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    if let Some(key) = key {
        spans.push(key);
        spans.push(Span::raw(" "));
    }
    spans.push(Span::styled(
        format!("{open}{len}{close}"),
        Style::new().fg(json::PUNCT).add_modifier(Modifier::DIM),
    ));
    spans
}

fn key_span(key: &str) -> Span<'static> {
    Span::styled(key.to_owned(), Style::new().fg(json::KEY))
}

fn index_span(index: usize) -> Span<'static> {
    Span::styled(format!("{index}"), Style::new().fg(json::PUNCT))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn text(tree: &Tree) -> Vec<(String, bool)> {
        tree.lines
            .iter()
            .map(|n| {
                let body: String = n.spans.iter().map(|s| s.content.as_ref()).collect();
                (format!("{}{}", " ".repeat(n.depth * 2), body), n.changed)
            })
            .collect()
    }

    #[test]
    fn a_view_renders_as_an_indented_tree() {
        let tree = Tree::new(&json!({"orders": {"a1": {"total": 42}}, "count": 1}));
        let lines: Vec<String> = text(&tree).into_iter().map(|(t, _)| t).collect();
        assert_eq!(
            lines,
            vec![
                "count: 1".to_owned(),
                "orders {1}".to_owned(),
                "  a1 {1}".to_owned(),
                "    total: 42".to_owned(),
            ]
        );
    }

    #[test]
    fn only_the_changed_leaf_lights_up() {
        let mut tree = Tree::new(&json!({"a": 1, "b": 2}));
        assert!(text(&tree).iter().all(|(_, changed)| !changed));
        tree.update(&json!({"a": 1, "b": 3}));
        let lit: Vec<String> =
            text(&tree).into_iter().filter(|(_, c)| *c).map(|(t, _)| t).collect();
        assert_eq!(lit, vec!["b: 3".to_owned()]);
        assert!(tree.flash_deadline().is_some());
    }

    #[test]
    fn a_new_subtree_lights_up_whole() {
        let mut tree = Tree::new(&json!({"a": 1}));
        tree.update(&json!({"a": 1, "b": {"c": [1, 2]}}));
        let lit: Vec<String> =
            text(&tree).into_iter().filter(|(_, c)| *c).map(|(t, _)| t).collect();
        assert_eq!(
            lit,
            vec![
                "b {1}".to_owned(),
                "  c [2]".to_owned(),
                "    0: 1".to_owned(),
                "    1: 2".to_owned(),
            ]
        );
    }

    #[test]
    fn an_untouched_container_stays_dark_when_a_sibling_moves() {
        let mut tree = Tree::new(&json!({"keep": {"x": 1}, "move": 1}));
        tree.update(&json!({"keep": {"x": 1}, "move": 2}));
        let lit: Vec<String> =
            text(&tree).into_iter().filter(|(_, c)| *c).map(|(t, _)| t).collect();
        assert_eq!(lit, vec!["move: 2".to_owned()]);
    }

    #[test]
    fn a_scalar_replacing_a_container_lights_up() {
        let mut tree = Tree::new(&json!({"a": {"b": 1}}));
        tree.update(&json!({"a": 7}));
        let lit: Vec<String> =
            text(&tree).into_iter().filter(|(_, c)| *c).map(|(t, _)| t).collect();
        assert_eq!(lit, vec!["a: 7".to_owned()]);
    }

    #[test]
    fn an_unchanged_view_lights_nothing() {
        let mut tree = Tree::new(&json!({"a": 1}));
        tree.update(&json!({"a": 1}));
        assert!(tree.flash_deadline().is_none());
        assert!(text(&tree).iter().all(|(_, c)| !c));
    }
}
