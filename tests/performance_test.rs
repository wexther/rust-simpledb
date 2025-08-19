use simple_db::{SimpleDB, DBConfig};
use std::time::Instant;

/// 从环境变量获取配置值，如果没有则使用默认值
fn get_env_or_default(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// 简化的性能测试
#[test]
fn test_database_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Simple DB 性能测试 ===\n");

    // 从环境变量读取配置
    let insert_count = get_env_or_default("PERF_INSERT_COUNT", 1000);
    let query_count = get_env_or_default("PERF_QUERY_COUNT", 100);
    let update_count = get_env_or_default("PERF_UPDATE_COUNT", 100);
    let delete_count = get_env_or_default("PERF_DELETE_COUNT", 100);

    println!("配置:");
    println!("  插入数据量: {}", insert_count);
    println!("  查询次数: {}", query_count);
    println!("  更新次数: {}", update_count);
    println!("  删除次数: {}", delete_count);
    println!();

    // 创建测试数据库
    let config = DBConfig {
        sql_file: None,
        base_dir: Some("data/performance_test".to_string()),
        db_name: Some("perf_test".to_string()),
        execute: None,
        interactive: false,
        verbose: false,
    };

    let mut db = SimpleDB::with_config(config)?;

    // 准备测试环境
    println!("正在准备测试环境...");
    let _ = db.execute_single_sql("DROP TABLE IF EXISTS perf_table");
    db.execute_single_sql("CREATE TABLE perf_table (id INT, name VARCHAR(50), score INT)")?;

    // 测试1: 批量插入性能
    println!("测试1: 批量插入性能");
    let start = Instant::now();
    
    for i in 1..=insert_count {
        let sql = format!("INSERT INTO perf_table VALUES ({}, 'user{}', {})", i, i, i % 100);
        db.execute_single_sql(&sql)?;
        
        if i % 100 == 0 {
            print!("\r插入进度: {}/{}", i, insert_count);
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    
    let insert_duration = start.elapsed();
    println!("\n插入 {} 条记录耗时: {:.2}秒", insert_count, insert_duration.as_secs_f64());
    println!("插入速度: {:.2} records/sec\n", insert_count as f64 / insert_duration.as_secs_f64());

    // 测试2: 查询性能
    println!("测试2: 查询性能");
    let start = Instant::now();
    
    for i in 1..=query_count {
        let sql = format!("SELECT * FROM perf_table WHERE id = {}", i);
        db.execute_single_sql(&sql)?;
    }
    
    let query_duration = start.elapsed();
    println!("执行 {} 次查询耗时: {:.2}秒", query_count, query_duration.as_secs_f64());
    println!("查询速度: {:.2} queries/sec\n", query_count as f64 / query_duration.as_secs_f64());

    // 测试3: 全表扫描
    println!("测试3: 全表扫描性能");
    let start = Instant::now();
    
    let result = db.execute_single_sql("SELECT * FROM perf_table")?;
    
    let scan_duration = start.elapsed();
    println!("全表扫描耗时: {:.2}秒", scan_duration.as_secs_f64());
    
    // 计算扫描的记录数（如果是ResultSet）
    if let simple_db::executor::QueryResult::ResultSet(rs) = result {
        println!("扫描了 {} 条记录", rs.rows.len());
        println!("扫描速度: {:.2} records/sec\n", rs.rows.len() as f64 / scan_duration.as_secs_f64());
    }

    // 测试4: 更新性能
    println!("测试4: 更新性能");
    let start = Instant::now();
    
    for i in 1..=update_count {
        let sql = format!("UPDATE perf_table SET score = {} WHERE id = {}", i * 2, i);
        db.execute_single_sql(&sql)?;
    }
    
    let update_duration = start.elapsed();
    println!("执行 {} 次更新耗时: {:.2}秒", update_count, update_duration.as_secs_f64());
    println!("更新速度: {:.2} updates/sec\n", update_count as f64 / update_duration.as_secs_f64());

    // 测试5: 删除性能
    println!("测试5: 删除性能");
    let start = Instant::now();
    
    let delete_start = std::cmp::max(1, insert_count - delete_count + 1);
    for i in delete_start..=insert_count {  // 删除最后的记录
        let sql = format!("DELETE FROM perf_table WHERE id = {}", i);
        db.execute_single_sql(&sql)?;
    }
    
    let delete_duration = start.elapsed();
    println!("执行 {} 次删除耗时: {:.2}秒", delete_count, delete_duration.as_secs_f64());
    println!("删除速度: {:.2} deletes/sec\n", delete_count as f64 / delete_duration.as_secs_f64());

    // 综合性能报告
    println!("=== 性能总结 ===");
    println!("操作类型        | 速度 (ops/sec)  | 平均延迟 (ms)");
    println!("----------------|----------------|---------------");
    println!("插入            | {:>10.2}     | {:>10.2}", 
             insert_count as f64 / insert_duration.as_secs_f64(),
             insert_duration.as_secs_f64() * 1000.0 / insert_count as f64);
    println!("查询            | {:>10.2}     | {:>10.2}", 
             query_count as f64 / query_duration.as_secs_f64(),
             query_duration.as_secs_f64() * 1000.0 / query_count as f64);
    println!("更新            | {:>10.2}     | {:>10.2}", 
             update_count as f64 / update_duration.as_secs_f64(),
             update_duration.as_secs_f64() * 1000.0 / update_count as f64);
    println!("删除            | {:>10.2}     | {:>10.2}", 
             delete_count as f64 / delete_duration.as_secs_f64(),
             delete_duration.as_secs_f64() * 1000.0 / delete_count as f64);

    println!("\n测试完成！");
    Ok(())
}
