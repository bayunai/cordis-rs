//! 控制台导出与纯渲染。

use cordis_core::{LogExporter, LogLevel, LogRecord};
use std::{
    collections::BTreeMap,
    io::{self, IsTerminal, Write},
    sync::{Mutex, OnceLock},
    time::Instant,
};
use time::{OffsetDateTime, format_description::OwnedFormatItem};

/// ANSI 颜色策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColorMode {
    #[default]
    Auto,
    Never,
    Always,
}

/// 标签对齐。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LabelAlign {
    #[default]
    Right,
    Left,
}

/// 控制台 exporter 配置。
#[derive(Debug, Clone)]
pub struct ConsoleLoggerConfig {
    pub colors: ColorMode,
    /// 按完整 target 名配置最低输出等级；键 `"default"` 为全局回退。
    /// 若均未命中，再回退到 [`LogRecord::default_level`]。
    pub levels: BTreeMap<String, LogLevel>,
    pub show_time: bool,
    pub show_diff: bool,
    pub max_length: Option<usize>,
    pub label_width: usize,
    pub label_margin: usize,
    pub label_align: LabelAlign,
}

impl Default for ConsoleLoggerConfig {
    fn default() -> Self {
        Self {
            colors: ColorMode::Auto,
            levels: BTreeMap::new(),
            show_time: true,
            show_diff: false,
            max_length: None,
            label_width: 12,
            label_margin: 1,
            label_align: LabelAlign::Right,
        }
    }
}

/// 控制台导出器：写入 stdout，并提供稳定 `render`。
pub struct ConsoleExporter {
    config: ConsoleLoggerConfig,
    last_export: Mutex<Option<Instant>>,
    colors_enabled: bool,
}

impl ConsoleExporter {
    pub fn new(config: ConsoleLoggerConfig) -> Self {
        let colors_enabled = match config.colors {
            ColorMode::Always => true,
            ColorMode::Never => false,
            ColorMode::Auto => io::stdout().is_terminal(),
        };
        Self {
            config,
            last_export: Mutex::new(None),
            colors_enabled,
        }
    }

    /// 纯函数式渲染（测试可固定 diff 为 `None`）。
    pub fn render(&self, record: &LogRecord) -> String {
        self.render_with_diff(record, None)
    }

    pub fn render_with_diff(
        &self,
        record: &LogRecord,
        diff: Option<std::time::Duration>,
    ) -> String {
        if record.level < self.effective_level(record) {
            return String::new();
        }

        let mut prefix = String::new();
        if self.config.show_time {
            prefix.push_str(&format_time(record));
            prefix.push(' ');
        }
        if self.config.show_diff {
            let diff_text = match diff {
                Some(duration) => format_diff(duration),
                None => "+0ms".into(),
            };
            prefix.push_str(&self.paint(diff_text, "\u{001b}[36m"));
            prefix.push(' ');
        }

        let level_tag = level_prefix(record.level);
        prefix.push_str(&self.paint(level_tag.to_string(), level_color(record.level)));
        prefix.push(' ');

        let label = format_label(
            &record.target,
            self.config.label_width,
            self.config.label_align,
        );
        if !label.is_empty() {
            prefix.push_str(&self.paint(label, level_color(record.level)));
            for _ in 0..self.config.label_margin {
                prefix.push(' ');
            }
        }

        let body_column = prefix.chars().count();
        let message = truncate_message(&record.message, self.config.max_length);
        let mut lines = message.lines();
        let mut out = String::new();
        if let Some(first) = lines.next() {
            out.push_str(&prefix);
            out.push_str(&self.paint(first.to_string(), level_color(record.level)));
        }
        let indent = " ".repeat(body_column);
        for line in lines {
            out.push('\n');
            out.push_str(&indent);
            out.push_str(&self.paint(line.to_string(), level_color(record.level)));
        }
        out
    }

    fn effective_level(&self, record: &LogRecord) -> LogLevel {
        self.config
            .levels
            .get(&record.target)
            .or_else(|| self.config.levels.get("default"))
            .copied()
            .unwrap_or(record.default_level)
    }

    fn paint(&self, text: String, ansi: &str) -> String {
        if self.colors_enabled {
            format!("{ansi}{text}\u{001b}[0m")
        } else {
            text
        }
    }
}

impl LogExporter for ConsoleExporter {
    fn export(&self, record: &LogRecord) {
        if record.level < self.effective_level(record) {
            return;
        }
        let diff = if self.config.show_diff {
            let mut last = self.last_export.lock().expect("last export");
            let now = Instant::now();
            let diff = last.map(|previous| now.saturating_duration_since(previous));
            *last = Some(now);
            diff
        } else {
            None
        };
        let line = self.render_with_diff(record, diff);
        if line.is_empty() {
            return;
        }
        let mut stdout = io::stdout().lock();
        let _ = writeln!(stdout, "{line}");
    }
}

fn time_format() -> &'static OwnedFormatItem {
    static FORMAT: OnceLock<OwnedFormatItem> = OnceLock::new();
    FORMAT.get_or_init(|| {
        time::format_description::parse_owned::<2>("[hour]:[minute]:[second]").expect("time format")
    })
}

fn format_time(record: &LogRecord) -> String {
    let Ok(duration) = record.timestamp.duration_since(std::time::UNIX_EPOCH) else {
        return "??:??:??".into();
    };
    let Ok(odt) = OffsetDateTime::from_unix_timestamp(duration.as_secs() as i64) else {
        return "??:??:??".into();
    };
    let offset = time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC);
    let local = odt.to_offset(offset);
    local
        .format(time_format())
        .unwrap_or_else(|_| "??:??:??".into())
}

fn format_diff(duration: std::time::Duration) -> String {
    let ms = duration.as_millis();
    if ms >= 1000 {
        format!("+{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("+{ms}ms")
    }
}

fn format_label(target: &str, width: usize, align: LabelAlign) -> String {
    if width == 0 {
        return String::new();
    }
    let truncated = if target.chars().count() > width {
        let mut out = String::new();
        for (index, ch) in target.chars().enumerate() {
            if index + 1 >= width {
                out.push('…');
                break;
            }
            out.push(ch);
        }
        out
    } else {
        target.to_string()
    };
    match align {
        LabelAlign::Left => format!("{truncated:<width$}"),
        LabelAlign::Right => format!("{truncated:>width$}"),
    }
}

fn truncate_message(message: &str, max_length: Option<usize>) -> String {
    let Some(max) = max_length else {
        return message.to_string();
    };
    if max == 0 {
        return String::new();
    }
    let mut out = String::new();
    for line in message.lines() {
        if !out.is_empty() {
            out.push('\n');
        }
        let count = line.chars().count();
        if count <= max {
            out.push_str(line);
        } else {
            for (index, ch) in line.chars().enumerate() {
                if index + 1 >= max {
                    out.push('…');
                    break;
                }
                out.push(ch);
            }
        }
    }
    out
}

fn level_prefix(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Debug => "[D]",
        LogLevel::Info => "[I]",
        LogLevel::Warn => "[W]",
        LogLevel::Error => "[E]",
    }
}

fn level_color(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Error => "\u{001b}[31m",
        LogLevel::Warn => "\u{001b}[33m",
        LogLevel::Info => "\u{001b}[32m",
        LogLevel::Debug => "\u{001b}[90m",
    }
}
