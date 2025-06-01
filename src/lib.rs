use clap::{Parser, Subcommand};
use executor::QueryResult;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

pub mod completion;
pub mod error;
pub mod executor;
pub mod planner;
pub mod storage;

use error::{DBError, Result};
use storage::StorageEngine;

/// Simple DB - 一个简单的数据库引擎
#[derive(Parser)]
#[command(name = "simple_db")]
#[command(about = "一个简单的数据库引擎")]
#[command(version = "1.0")]
pub struct DBConfig {
    /// SQL 文件路径
    #[arg(value_name = "FILE")]
    pub sql_file: Option<String>,

    /// 数据库基础目录
    #[arg(short = 'd', long = "data-dir")]
    pub base_dir: Option<String>,

    /// 数据库名称
    #[arg(short = 'n', long = "db-name")]
    pub db_name: Option<String>,

    /// 执行单条 SQL 命令
    #[arg(short = 'e', long = "execute")]
    pub execute: Option<String>,

    /// 进入交互模式
    #[arg(short = 'i', long = "interactive")]
    pub interactive: bool,

    /// 详细输出
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,
}

impl DBConfig {
    pub fn from_args() -> Self {
        Self::parse()
    }

    pub fn get_run_mode(&self) -> RunMode {
        if let Some(sql) = &self.execute {
            RunMode::SingleCommand(sql.clone())
        } else if self.interactive || self.sql_file.is_none() {
            RunMode::Interactive
        } else if let Some(file) = &self.sql_file {
            RunMode::File(file.clone())
        } else {
            RunMode::Interactive
        }
    }
}

#[derive(Debug)]
pub enum RunMode {
    File(String),
    Interactive,
    SingleCommand(String),
}

pub struct SimpleDB {
    storage_engine: StorageEngine,
    config: DBConfig,
}

impl SimpleDB {
    pub fn new() -> Result<Self> {
        Self::with_config(DBConfig::from_args())
    }

    pub fn with_config(config: DBConfig) -> Result<Self> {
        Ok(Self {
            storage_engine: StorageEngine::new(
                config.base_dir.as_deref().map(Path::new),
                config.db_name.as_deref(),
            )?,
            config,
        })
    }

    pub fn from_args() -> Result<Self> {
        let config = DBConfig::from_args();
        Self::with_config(config)
    }

    pub fn execute_sql_file(&mut self, file_path: &str) -> Result<Vec<Result<QueryResult>>> {
        if self.config.verbose {
            println!("正在读取文件: {}", file_path);
        }
        let sql_content = fs::read_to_string(file_path)?;
        self.execute_sql(&sql_content)
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Vec<Result<QueryResult>>> {
        let dialect = MySqlDialect {};
        let ast_statements = SqlParser::parse_sql(&dialect, sql)?;

        let mut executor = executor::Executor::new(&mut self.storage_engine);
        let planner = planner::Planner::new();

        let mut results = Vec::new();

        for stmt in ast_statements {
            if self.config.verbose {
                println!("执行语句: {:?}", stmt);
            }
            let plan = planner.plan(&stmt)?;
            let result = executor.execute(plan);
            results.push(result);
        }

        Ok(results)
    }

    pub fn execute_single_sql(&mut self, sql: &str) -> Result<QueryResult> {
        let results = self.execute_sql(sql)?;
        if let Some(result) = results.into_iter().next() {
            result
        } else {
            Ok(QueryResult::Success)
        }
    }

    pub fn save(&mut self) -> Result<()> {
        if self.config.verbose {
            println!("正在保存数据库...");
        }
        self.storage_engine.save()
    }

    pub fn run(&mut self) -> Result<()> {
        match self.config.get_run_mode() {
            RunMode::File(file_path) => self.run_file_mode(&file_path),
            RunMode::Interactive => self.run_interactive_mode(),
            RunMode::SingleCommand(sql) => self.run_single_command_mode(&sql),
        }
    }

    fn run_file_mode(&mut self, file_path: &str) -> Result<()> {
        if self.config.verbose {
            println!("执行 SQL 文件模式: {}", file_path);
        }

        let results = self.execute_sql_file(file_path)?;

        for result in &results {
            match result {
                Ok(res) => println!("{}", res),
                Err(e) => eprintln!("执行错误: {}", e),
            }
        }

        self.save()?;
        Ok(())
    }

    fn run_single_command_mode(&mut self, sql: &str) -> Result<()> {
        if self.config.verbose {
            println!("执行单条命令模式: {}", sql);
        }

        match self.execute_single_sql(sql) {
            Ok(result) => println!("{}", result),
            Err(e) => eprintln!("执行错误: {}", e),
        }

        self.save()?;
        Ok(())
    }

    fn run_interactive_mode(&mut self) -> Result<()> {
        use crate::completion::SQLHelper;
        use rustyline::error::ReadlineError;
        use rustyline::{ColorMode, Config, Editor};

        // 配置 rustyline
        let config = Config::builder()
            .history_ignore_space(true)
            .completion_type(rustyline::CompletionType::List)
            .edit_mode(rustyline::EditMode::Emacs)
            .color_mode(ColorMode::Enabled)
            .build();

        let mut rl = Editor::with_config(config)?;

        // 设置自定义助手
        let mut helper = SQLHelper::new();
        helper.with_colored_prompt("\x1b[1;32msimple_db>\x1b[0m ".to_owned());
        rl.set_helper(Some(helper));

        // 尝试加载历史记录
        let history_file = "simple_db_history.txt";
        if rl.load_history(history_file).is_err() {
            if self.config.verbose {
                println!("未找到历史记录文件，将创建新文件");
            }
        }

        println!("Simple DB 交互模式 (增强版)");
        println!("功能:");
        println!("  • 使用上下箭头键浏览命令历史");
        println!("  • 使用 Tab 键自动补全 SQL 关键字和元命令");
        println!("  • 支持语法高亮和括号匹配");
        println!("  • Ctrl+C 中断当前输入，Ctrl+D 退出");
        println!("输入 .help 查看帮助信息");
        if self.config.verbose {
            println!("详细模式已启用");
        }
        println!();

        loop {
            let readline = rl.readline("simple_db> ");
            match readline {
                Ok(line) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    // 添加到历史记录
                    rl.add_history_entry(trimmed)?;

                    // 处理元命令
                    if self.handle_meta_command(trimmed)? {
                        break;
                    }

                    // 执行 SQL 命令
                    if !trimmed.starts_with('.') {
                        match self.execute_single_sql(trimmed) {
                            Ok(result) => println!("{}", result),
                            Err(e) => eprintln!("错误: {}", e),
                        }
                        println!();
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("^D");
                    break;
                }
                Err(err) => {
                    eprintln!("读取输入错误: {:?}", err);
                    break;
                }
            }
        }

        // 保存历史记录
        if let Err(e) = rl.save_history(history_file) {
            if self.config.verbose {
                eprintln!("保存历史记录失败: {}", e);
            }
        } else if self.config.verbose {
            println!("历史记录已保存到 {}", history_file);
        }

        println!("正在保存数据库...");
        self.save()?;
        println!("再见!");
        Ok(())
    }

    // 扩展元命令处理，添加更多功能
    fn handle_meta_command(&mut self, command: &str) -> Result<bool> {
        match command {
            ".exit" | ".quit" | "\\q" => {
                return Ok(true);
            }

            ".help" | "\\h" => {
                self.print_interactive_help();
            }

            ".tables" => match self.execute_single_sql("SHOW TABLES") {
                Ok(result) => println!("{}", result),
                Err(e) => eprintln!("获取表列表失败: {}", e),
            },

            ".save" => match self.save() {
                Ok(()) => println!("数据库已保存"),
                Err(e) => eprintln!("保存失败: {}", e),
            },

            ".clear" => {
                // 清屏
                print!("\x1B[2J\x1B[1;1H");
                io::stdout().flush().unwrap();
            }

            ".version" => {
                println!("Simple DB version 1.0");
            }

            ".status" => {
                println!("数据库状态:");
                if let Some(db_name) = &self.config.db_name {
                    println!("  当前数据库: {}", db_name);
                } else {
                    println!("  当前数据库: 默认");
                }
                if let Some(data_dir) = &self.config.base_dir {
                    println!("  数据目录: {}", data_dir);
                } else {
                    println!("  数据目录: 默认");
                }
                println!("  详细模式: {}", self.config.verbose);
            }

            cmd if cmd.starts_with(".schema") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() == 2 {
                    let table_name = parts[1];
                    let sql = format!("DESCRIBE {}", table_name);
                    match self.execute_single_sql(&sql) {
                        Ok(result) => println!("{}", result),
                        Err(e) => eprintln!("获取表结构失败: {}", e),
                    }
                } else {
                    eprintln!("用法: .schema <table_name>");
                }
            }

            cmd if cmd.starts_with(".read") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() == 2 {
                    let file_path = parts[1];
                    match self.execute_sql_file(file_path) {
                        Ok(results) => {
                            for result in &results {
                                match result {
                                    Ok(res) => println!("{}", res),
                                    Err(e) => eprintln!("执行错误: {}", e),
                                }
                            }
                        }
                        Err(e) => eprintln!("读取文件失败: {}", e),
                    }
                } else {
                    eprintln!("用法: .read <file_path>");
                }
            }

            _ => {}
        }

        Ok(false)
    }

    fn print_interactive_help(&self) {
        println!("交互模式命令:");
        println!("  .exit, .quit, \\q              # 退出程序");
        println!("  .help, \\h                     # 显示帮助信息");
        println!("  .tables                       # 显示所有表");
        println!("  .schema <table_name>          # 显示表结构");
        println!("  .save                         # 手动保存数据库");
        println!("  .clear                        # 清屏");
        println!("  .version                      # 显示版本信息");
        println!("  .status                       # 显示数据库状态");
        println!("  .read <file_path>             # 执行SQL文件");
        println!();

        println!("增强功能 (rustyline):");
        println!("  ↑↓ 箭头键                     # 浏览命令历史");
        println!("  Tab 键                        # 自动补全");
        println!("  Ctrl+C                        # 中断当前输入");
        println!("  Ctrl+D                        # 退出程序");
        println!();

        println!("SQL示例:");
        println!("  CREATE TABLE users (id INT, name VARCHAR(50));");
        println!("  INSERT INTO users VALUES (1, 'Alice');");
        println!("  SELECT * FROM users;");
        println!("  DROP TABLE users;");
    }
}

impl Drop for SimpleDB {
    fn drop(&mut self) {
        if let Err(e) = self.save() {
            eprintln!("数据库保存失败: {}", e);
        }
    }
}
