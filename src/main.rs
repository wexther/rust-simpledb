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

fn main() -> Result<()> {
    // 解析命令行参数
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        return Err(DBError::Other("用法: simple_db <sql文件>".to_string()));
    }
    let file_path = &args[1];

    // 1. 初始化存储引擎(Storage Engine)
    let mut storage_engine = StorageEngine::new();
    
    // 可选：从磁盘加载持久化数据
    storage_engine.load("database.db")?;
    
    // 创建默认数据库（如果不存在）
    const DEFAULT_DB_NAME: &str = "default";
    if !storage_engine.has_database(DEFAULT_DB_NAME) {
        storage_engine.create_database(DEFAULT_DB_NAME.to_string())?;
        println!("已创建默认数据库 '{}'", DEFAULT_DB_NAME);
    }
    
    // 如果没有选中任何数据库，则选择默认数据库
    if storage_engine.current_database().is_err() {
        storage_engine.use_database(DEFAULT_DB_NAME)?;
        println!("已切换到默认数据库 '{}'", DEFAULT_DB_NAME);
    }
    
    // 2. 读取SQL文件
    let sql_content = fs::read_to_string(file_path)
        .map_err(|e| DBError::IO(format!("无法读取SQL文件: {}", e)))?;
    
    // 3. 解析SQL(Parser)
    let dialect = GenericDialect {};
    let ast_statements = Parser::parse_sql(&dialect, &sql_content)
        .map_err(|e| DBError::Parse(format!("SQL解析错误: {}", e)))?;
    
    // 4. 创建查询处理器(Query Processing) - 现在传递引用而非所有权
    let mut query_processor = QueryProcessor::new(&mut storage_engine);
    
    // 5. 执行每条语句并输出结果
    for stmt in ast_statements {
        match query_processor.execute(stmt) {
            Ok(result) => println!("{}", result),
            Err(err) => eprintln!("执行错误: {}", err),
        }
    }
    
    // 可选：持久化存储引擎状态 - 现在可以直接使用storage_engine
    storage_engine.save("database.db")?;
    
    Ok(())
}
