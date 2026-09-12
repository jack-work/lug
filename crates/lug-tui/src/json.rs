//! JSON to colored spans, and hard wrapping of a span run to a width.

use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use serde_json::Value;
use unicode_width::UnicodeWidthChar;

pub const KEY: Color = Color::Cyan;
pub const STRING: Color = Color::Green;
pub const NUMBER: Color = Color::Yellow;
pub const LITERAL: Color = Color::Magenta;
pub const PUNCT: Color = Color::DarkGray;
pub const GUTTER: Color = Color::DarkGray;

/// One compact line of highlighted JSON.
pub fn spans(value: &Value) -> Vec<Span<'static>> {
    let mut out = Vec::new();
    push(value, &mut out);
    out
}

fn punct(text: &'static str) -> Span<'static> {
    Span::styled(text, Style::new().fg(PUNCT))
}

fn push(value: &Value, out: &mut Vec<Span<'static>>) {
    match value {
        Value::Null => out.push(Span::styled("null", Style::new().fg(LITERAL))),
        Value::Bool(b) => {
            out.push(Span::styled(if *b { "true" } else { "false" }, Style::new().fg(LITERAL)));
        }
        Value::Number(n) => out.push(Span::styled(n.to_string(), Style::new().fg(NUMBER))),
        Value::String(s) => out.push(Span::styled(quote(s), Style::new().fg(STRING))),
        Value::Array(items) => {
            out.push(punct("["));
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(punct(", "));
                }
                push(item, out);
            }
            out.push(punct("]"));
        }
        Value::Object(fields) => {
            out.push(punct("{"));
            for (i, (key, item)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(punct(", "));
                }
                out.push(Span::styled(quote(key), Style::new().fg(KEY)));
                out.push(punct(": "));
                push(item, out);
            }
            out.push(punct("}"));
        }
    }
}

/// Escapes exactly as serde would, so what is on screen is what is on the wire.
pub fn quote(text: &str) -> String {
    Value::String(text.to_owned()).to_string()
}

/// Hard wrap a span run into lines of at most `width` columns.
///
/// `gutter` leads the first line and its width is reserved as indent on the
/// rest, so a record stays visually one block. JSON has no good break points,
/// so this breaks mid-token rather than pretending otherwise.
pub fn wrap(gutter: Span<'static>, spans: &[Span<'static>], width: usize) -> Vec<Line<'static>> {
    let indent = display_width(&gutter.content);
    let body = width.saturating_sub(indent).max(1);

    let mut lines = Vec::new();
    let mut current: Vec<Span<'static>> = vec![gutter];
    let mut used = 0usize;

    for span in spans {
        let mut chunk = String::new();
        let mut chunk_width = 0usize;
        for ch in span.content.chars() {
            let w = ch.width().unwrap_or(0);
            if used + chunk_width + w > body {
                if !chunk.is_empty() {
                    current.push(Span::styled(std::mem::take(&mut chunk), span.style));
                }
                lines.push(Line::from(std::mem::take(&mut current)));
                current.push(Span::raw(" ".repeat(indent)));
                used = 0;
                chunk_width = 0;
            }
            chunk.push(ch);
            chunk_width += w;
        }
        if !chunk.is_empty() {
            current.push(Span::styled(chunk, span.style));
            used += chunk_width;
        }
    }
    lines.push(Line::from(current));
    lines
}

pub fn display_width(text: &str) -> usize {
    text.chars().map(|c| c.width().unwrap_or(0)).sum()
}

/// Cut a span run to `width` columns, marking the cut with an ellipsis.
pub fn truncate(spans: Vec<Span<'static>>, width: usize) -> Vec<Span<'static>> {
    let total: usize = spans.iter().map(|s| display_width(&s.content)).sum();
    if total <= width {
        return spans;
    }
    let budget = width.saturating_sub(1);
    let mut out = Vec::new();
    let mut used = 0usize;
    for span in spans {
        if used >= budget {
            break;
        }
        let mut kept = String::new();
        for ch in span.content.chars() {
            let w = ch.width().unwrap_or(0);
            if used + w > budget {
                break;
            }
            kept.push(ch);
            used += w;
        }
        if !kept.is_empty() {
            out.push(Span::styled(kept, span.style));
        }
    }
    out.push(Span::styled("…", Style::new().fg(PUNCT)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn text(lines: &[Line<'_>]) -> Vec<String> {
        lines.iter().map(|l| l.spans.iter().map(|s| s.content.as_ref()).collect()).collect()
    }

    #[test]
    fn compact_json_round_trips_as_text() {
        let value = json!({"a": [1, true, null], "b": "hi"});
        let rendered: String = spans(&value).iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(rendered, r#"{"a": [1, true, null], "b": "hi"}"#);
    }

    #[test]
    fn strings_are_escaped_like_the_wire() {
        let rendered: String =
            spans(&json!({"k": "a\"b\n"})).iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(rendered, r#"{"k": "a\"b\n"}"#);
    }

    #[test]
    fn wrapping_fills_every_line_to_the_width() {
        let value = json!({"key": "0123456789012345678901234567890123456789"});
        let gutter = Span::raw("   7 | ");
        let lines = wrap(gutter, &spans(&value), 20);
        let rendered = text(&lines);
        assert!(rendered.len() > 1, "{rendered:?}");
        for line in &rendered {
            assert!(display_width(line) <= 20, "{line:?}");
        }
        for line in &rendered[1..] {
            assert!(line.starts_with("       "), "continuation lacks indent: {line:?}");
        }
        let joined: String = std::iter::once(rendered[0].trim_start_matches("   7 | "))
            .chain(rendered[1..].iter().map(|l| l.trim_start()))
            .collect();
        assert_eq!(joined, r#"{"key": "0123456789012345678901234567890123456789"}"#);
    }

    #[test]
    fn wrapping_a_short_record_is_one_line() {
        let lines = wrap(Span::raw("1 "), &spans(&json!(3)), 40);
        assert_eq!(text(&lines), vec!["1 3".to_owned()]);
    }

    #[test]
    fn truncation_never_exceeds_the_width() {
        let cut = truncate(spans(&json!({"aaa": "bbbbbbbbbb"})), 8);
        let rendered: String = cut.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(display_width(&rendered), 8);
        assert!(rendered.ends_with('…'));
    }
}
