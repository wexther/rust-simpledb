use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::env;
use std::fs;

// 引入自定义模块
mod error;
mod parser;
mod query;
mod storage;

use error::{DBError, Result};
use query::QueryProcessor;
use storage::engine::StorageEngine;

/// 解析命令行参数，只返回SQL文件路径
fn parse_args() -> Result<String> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        return Err(DBError::Other("用法: simple_db <sql文件>".to_string()));
    }
    
    Ok(args[1].clone())
}

/// 执行SQL文件
fn execute_sql_file(file_path: &str, storage_engine: &mut StorageEngine) -> Result<()> {
    // 1. 读取SQL文件
    let sql_content = fs::read_to_string(file_path)
        .map_err(|e| DBError::IO(format!("无法读取SQL文件: {}", e)))?;
    
    // 2. 解析SQL(Parser)
    let dialect = GenericDialect {};
    let ast_statements = Parser::parse_sql(&dialect, &sql_content)
        .map_err(|e| DBError::Parse(format!("SQL解析错误: {}", e)))?;
    
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

fn main() -> Result<()> {
    // 解析命令行参数
    let sql_file_path = parse_args()?;
    
    // 创建并初始化存储引擎，使用默认参数
    let mut storage_engine = StorageEngine::new(None::<&str>, None)?;
    
    // 执行SQL文件
    execute_sql_file(&sql_file_path, &mut storage_engine)?;
    
    // 持久化存储引擎状态
    storage_engine.save()?;
    
    Ok(())
}
