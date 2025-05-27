use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::env;
use std::fs;

mod error;
mod query;
mod storage;

use error::{DBError, Result};
use query::QueryProcessor;
use storage::engine::StorageEngine;

fn main() {
    if let Err(e) = run() {
        eprintln!("{}", e);
    }
}

fn run() -> Result<()> {
    // 原来main函数中的所有逻辑
    let sql_file_path = parse_args()?;

    // 创建并初始化存储引擎，使用默认参数
    let mut storage_engine = StorageEngine::new(None::<&str>, None)?;

    // 执行SQL文件
    execute_sql_file(&sql_file_path, &mut storage_engine)?;

    // 持久化存储引擎状态
    storage_engine.save()?;

    Ok(())
}

/// 解析命令行参数
fn parse_args() -> Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 {
        Ok(args[1].clone())
    } else {
        Err(DBError::Other("用法: simple_db <sql文件>".to_string()))
    }
}

/// 执行SQL文件
fn execute_sql_file(file_path: &str, storage_engine: &mut StorageEngine) -> Result<()> {
    // 1. 读取SQL文件
    let sql_content = fs::read_to_string(file_path)?;

    // 2. 解析SQL(Parser)
    let dialect = GenericDialect {};
    let ast_statements = Parser::parse_sql(&dialect, &sql_content)?;

    // 3. 创建查询处理器(Query Processing) - 现在传递引用而非所有权
    let mut query_processor = QueryProcessor::new(storage_engine);

    // 4. 执行每条语句并输出结果
    for stmt in ast_statements {
        match query_processor.execute(stmt) {
            Ok(result) => println!("{}", result),
            Err(err) => eprintln!("执行错误: {}", err),
        }
    }

    Ok(())
}
