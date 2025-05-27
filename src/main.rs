use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::env;
use std::fs;

mod error;
mod query;
mod storage;

use error::{DBError, Result};
use query::QueryProcessor;
use storage::StorageEngine;

fn main() {
    if let Err(e) = run() {
        eprintln!("{}", e);
    }
}

fn run() -> Result<()> {
    let sql_file_path = parse_args()?;

    let mut storage_engine = StorageEngine::new(None, None)?;

    execute_sql_file(&sql_file_path, &mut storage_engine)?;

    storage_engine.save()?;

    Ok(())
}

fn parse_args() -> Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() >= 2 {
        Ok(args[1].clone())
    } else {
        // 可扩充其余用法
        Err(DBError::Other("用法: simple_db <sql文件> ..".to_string()))
    }
}

fn execute_sql_file(file_path: &str, storage_engine: &mut StorageEngine) -> Result<()> {
    let sql_content = fs::read_to_string(file_path)?;

    let dialect = GenericDialect {};
    let ast_statements = Parser::parse_sql(&dialect, &sql_content)?;

    // 3. 创建查询处理器(Query Processing)
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
