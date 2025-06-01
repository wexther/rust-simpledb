use rustyline::completion::{Completer, FilenameCompleter, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::{CmdKind, Highlighter, MatchingBracketHighlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{self, MatchingBracketValidator, Validator};
use rustyline::{Cmd, CompletionType, Config, Context, KeyEvent};
use rustyline_derive::{Completer, Helper, Highlighter, Hinter, Validator};
use std::borrow::Cow::{self, Borrowed, Owned};

#[derive(Helper, Completer, Hinter, Validator)]
pub struct SQLHelper {
    #[rustyline(Completer)]
    completer: SQLCompleter,
    #[rustyline(Highlighter)]
    highlighter: MatchingBracketHighlighter,
    #[rustyline(Validator)]
    validator: MatchingBracketValidator,
    #[rustyline(Hinter)]
    hinter: HistoryHinter,
    colored_prompt: String,
}

impl SQLHelper {
    pub fn new() -> Self {
        Self {
            completer: SQLCompleter::new(),
            highlighter: MatchingBracketHighlighter::new(),
            validator: MatchingBracketValidator::new(),
            hinter: HistoryHinter {},
            colored_prompt: "".to_owned(),
        }
    }

    pub fn with_colored_prompt(&mut self, prompt: String) {
        self.colored_prompt = prompt;
    }
}

impl Highlighter for SQLHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Borrowed(&self.colored_prompt)
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned(format!("\x1b[1m{}\x1b[m", hint))
    }

    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize, forced: CmdKind) -> bool {
        self.highlighter.highlight_char(line, pos, forced)
    }
}

pub struct SQLCompleter {
    file_completer: FilenameCompleter,
}

impl SQLCompleter {
    pub fn new() -> Self {
        Self {
            file_completer: FilenameCompleter::new(),
        }
    }

    // SQL 关键字
    const SQL_KEYWORDS: &'static [&'static str] = &[
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
        "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "PRIMARY", "KEY",
        "FOREIGN", "REFERENCES", "CONSTRAINT", "UNIQUE", "NOT", "NULL", "AUTO_INCREMENT",
        "DEFAULT", "CHECK", "AND", "OR", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
        "OFFSET", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AS", "DISTINCT",
        "UNION", "ALL", "EXISTS", "IN", "BETWEEN", "LIKE", "IS", "TRUE", "FALSE",
        "COUNT", "SUM", "AVG", "MIN", "MAX", "CONCAT", "SUBSTRING", "LENGTH",
        "UPPER", "LOWER", "TRIM", "REPLACE", "NOW", "DATE", "TIME", "YEAR", "MONTH", "DAY",
        "INT", "INTEGER", "VARCHAR", "CHAR", "TEXT", "FLOAT", "DOUBLE", "DECIMAL",
        "BOOLEAN", "BOOL", "DATE", "TIME", "DATETIME", "TIMESTAMP",
    ];

    // 元命令
    const META_COMMANDS: &'static [&'static str] = &[
        ".exit", ".quit", ".help", ".tables", ".schema", ".save",
    ];
}

impl Completer for SQLCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let line_up_to_pos = &line[..pos];
        
        // 如果是元命令，提供元命令补全
        if line_up_to_pos.trim().starts_with('.') {
            let start = line_up_to_pos.rfind('.').unwrap_or(0);
            let prefix = &line_up_to_pos[start..];
            
            let matches: Vec<Pair> = Self::META_COMMANDS
                .iter()
                .filter(|&cmd| cmd.starts_with(prefix))
                .map(|&cmd| Pair {
                    display: cmd.to_string(),
                    replacement: cmd.to_string(),
                })
                .collect();
                
            return Ok((start, matches));
        }

        // 文件路径补全（当输入包含 '/' 时）
        if line_up_to_pos.contains('/') || line_up_to_pos.contains('\\') {
            return self.file_completer.complete(line, pos, ctx);
        }

        // SQL 关键字补全
        let word_start = line_up_to_pos
            .rfind(|c: char| c.is_whitespace() || c == '(' || c == ')' || c == ',')
            .map(|i| i + 1)
            .unwrap_or(0);
        
        let prefix = &line_up_to_pos[word_start..].to_uppercase();
        
        let matches: Vec<Pair> = Self::SQL_KEYWORDS
            .iter()
            .filter(|&keyword| keyword.starts_with(prefix))
            .map(|&keyword| Pair {
                display: keyword.to_string(),
                replacement: keyword.to_string(),
            })
            .collect();

        Ok((word_start, matches))
    }
}