use query::result::QueryResult;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::env;
use std::fs;
use std::path::Path;

pub mod error;
pub mod query;
pub mod storage;

use error::{DBError, Result};
use query::QueryProcessor;
use storage::StorageEngine;

pub struct DBConfig {
    pub sql_file_path: String,
    pub base_dir: Option<String>,
    pub db_name: Option<String>,
}

impl Default for DBConfig {
    fn default() -> Self {
        Self {
            sql_file_path: String::new(),
            base_dir: None,
            db_name: None,
        }
    }
}

impl DBConfig {
    pub fn from_args() -> Result<Self> {
        let args: Vec<String> = env::args().collect();
        if args.len() >= 2 {
            Ok(Self {
                sql_file_path: args[1].clone(),
                ..Self::default()
            })
        } else {
            Err(DBError::Other("用法: simple_db <sql文件>".to_string()))
        }
    }
}

pub struct SimpleDB {
    storage_engine: StorageEngine,
    config: DBConfig,
}

impl SimpleDB {
    pub fn new() -> Result<Self> {
        Self::with_config(DBConfig::default())
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
        let config = DBConfig::from_args()?;
        Self::with_config(config)
    }

    pub fn execute_sql_file(&mut self, file_path: &str) -> Result<Vec<Result<QueryResult>>> {
        let sql_content = fs::read_to_string(file_path)?;
        self.execute_sql(&sql_content)
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Vec<Result<QueryResult>>> {
        let dialect = MySqlDialect {};
        let ast_statements = Parser::parse_sql(&dialect, sql)?;

        let mut query_processor = QueryProcessor::new(&mut self.storage_engine);
        let mut results = Vec::new();

        for stmt in ast_statements {
            results.push(query_processor.execute(stmt));
        }

        Ok(results)
    }

    pub fn save(&mut self) -> Result<()> {
        self.storage_engine.save()
    }

    // 新增：提供主运行函数
    pub fn run(&mut self) -> Result<()> {
        // 执行SQL文件
        let file_path = self.config.sql_file_path.clone();
        let results = self.execute_sql_file(&file_path)?;

        // 打印结果
        for result in &results {
            match result {
                Ok(res) => println!("{}", res),
                Err(e) => eprintln!("执行错误: {}", e),
            }
        }

        self.save()?;

        Ok(())
    }
}

// 可选：实现Drop trait以在离开作用域时自动保存
impl Drop for SimpleDB {
    fn drop(&mut self) {
        if let Err(e) = self.save() {
            eprintln!("数据库保存失败: {}", e);
        }
    }
}
