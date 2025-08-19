use simple_db::{SimpleDB, DBConfig};
use std::time::{Duration, Instant};
use std::fmt;

/// 延迟统计数据
#[derive(Debug, Clone)]
struct LatencyStats {
    min: Duration,
    max: Duration,
    total: Duration,
    count: usize,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            min: Duration::MAX,
            max: Duration::ZERO,
            total: Duration::ZERO,
            count: 0,
        }
    }

    fn add(&mut self, latency: Duration) {
        self.min = self.min.min(latency);
        self.max = self.max.max(latency);
        self.total += latency;
        self.count += 1;
    }

    fn average(&self) -> Duration {
        if self.count == 0 {
            Duration::ZERO
        } else {
            self.total / self.count as u32
        }
    }

    fn ops_per_sec(&self) -> f64 {
        if self.total.as_secs_f64() == 0.0 {
            0.0
        } else {
            self.count as f64 / self.total.as_secs_f64()
        }
    }
}

impl fmt::Display for LatencyStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "操作数: {}, 平均延迟: {:.2}ms, 最小延迟: {:.2}ms, 最大延迟: {:.2}ms, 吞吐量: {:.2} ops/sec",
            self.count,
            self.average().as_secs_f64() * 1000.0,
            self.min.as_secs_f64() * 1000.0,
            self.max.as_secs_f64() * 1000.0,
            self.ops_per_sec()
        )
    }
}

/// 测试配置
struct TestConfig {
    insert_count: usize,
    select_count: usize,
    update_count: usize,
    delete_count: usize,
    enable_detailed_stats: bool,
    enable_full_scan: bool,
}

impl TestConfig {
    fn from_env() -> Self {
        Self {
            insert_count: get_env_or_default("PERF_INSERT_COUNT", 1000),
            select_count: get_env_or_default("PERF_SELECT_COUNT", 100),
            update_count: get_env_or_default("PERF_UPDATE_COUNT", 100),
            delete_count: get_env_or_default("PERF_DELETE_COUNT", 100),
            enable_detailed_stats: std::env::var("PERF_DETAILED_STATS").unwrap_or_default() == "1",
            enable_full_scan: std::env::var("PERF_FULL_SCAN").unwrap_or("1".to_string()) == "1",
        }
    }
}

/// 从环境变量获取配置值，如果没有则使用默认值
fn get_env_or_default(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// 数据库性能测试器
struct DatabaseTester {
    db: SimpleDB,
    config: TestConfig,
}

impl DatabaseTester {
    fn new(config: TestConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let db_config = DBConfig {
            sql_file: None,
            base_dir: Some("data/performance_test".to_string()),
            db_name: Some("perf_test".to_string()),
            execute: None,
            interactive: false,
            verbose: false,
        };

        let mut db = SimpleDB::with_config(db_config)?;
        
        // 准备测试环境
        let _ = db.execute_single_sql("DROP TABLE IF EXISTS perf_table");
        db.execute_single_sql("CREATE TABLE perf_table (id INT, name VARCHAR(50), score INT)")?;

        Ok(Self { db, config })
    }

    fn run_complete_test(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("=== Simple DB 性能与基准测试 ===\n");

        println!("配置:");
        println!("  插入数据量: {}", self.config.insert_count);
        println!("  查询次数: {}", self.config.select_count);
        println!("  更新次数: {}", self.config.update_count);
        println!("  删除次数: {}", self.config.delete_count);
        println!("  详细统计: {}", if self.config.enable_detailed_stats { "启用" } else { "禁用" });
        println!("  全表扫描: {}", if self.config.enable_full_scan { "启用" } else { "禁用" });
        println!();

        // 运行所有测试
        let insert_stats = self.test_inserts()?;
        let select_stats = self.test_selects()?;
        let update_stats = self.test_updates()?;
        
        if self.config.enable_full_scan {
            self.test_full_scan()?;
        }
        
        let delete_stats = self.test_deletes()?;

        // 输出结果
        self.print_results(&insert_stats, &select_stats, &update_stats, &delete_stats);

        Ok(())
    }

    fn test_inserts(&mut self) -> Result<LatencyStats, Box<dyn std::error::Error>> {
        println!("🔄 测试1: 批量插入性能");
        let mut stats = LatencyStats::new();
        let start_time = Instant::now();
        
        for i in 1..=self.config.insert_count {
            let sql = format!("INSERT INTO perf_table VALUES ({}, 'user{}', {})", i, i, i % 100);
            
            let start = Instant::now();
            self.db.execute_single_sql(&sql)?;
            let latency = start.elapsed();
            
            if self.config.enable_detailed_stats {
                stats.add(latency);
            }
            
            if i % 100 == 0 {
                print!("\r插入进度: {}/{}", i, self.config.insert_count);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }
        
        let total_duration = start_time.elapsed();
        println!("\n插入 {} 条记录耗时: {:.2}秒", self.config.insert_count, total_duration.as_secs_f64());
        println!("插入速度: {:.2} records/sec\n", self.config.insert_count as f64 / total_duration.as_secs_f64());

        if !self.config.enable_detailed_stats {
            // 如果没有详细统计，创建一个简单的统计
            stats.count = self.config.insert_count;
            stats.total = total_duration;
            stats.min = total_duration / self.config.insert_count as u32;
            stats.max = stats.min;
        }

        Ok(stats)
    }

    fn test_selects(&mut self) -> Result<LatencyStats, Box<dyn std::error::Error>> {
        println!("🔍 测试2: 查询性能");
        let mut stats = LatencyStats::new();
        let start_time = Instant::now();
        
        for i in 1..=self.config.select_count {
            let sql = format!("SELECT * FROM perf_table WHERE id = {}", i);
            
            let start = Instant::now();
            self.db.execute_single_sql(&sql)?;
            let latency = start.elapsed();
            
            if self.config.enable_detailed_stats {
                stats.add(latency);
            }
        }
        
        let total_duration = start_time.elapsed();
        println!("执行 {} 次查询耗时: {:.2}秒", self.config.select_count, total_duration.as_secs_f64());
        println!("查询速度: {:.2} queries/sec\n", self.config.select_count as f64 / total_duration.as_secs_f64());

        if !self.config.enable_detailed_stats {
            stats.count = self.config.select_count;
            stats.total = total_duration;
            stats.min = total_duration / self.config.select_count as u32;
            stats.max = stats.min;
        }

        Ok(stats)
    }

    fn test_full_scan(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("📊 测试3: 全表扫描性能");
        let start = Instant::now();
        
        let result = self.db.execute_single_sql("SELECT * FROM perf_table")?;
        
        let scan_duration = start.elapsed();
        println!("全表扫描耗时: {:.2}秒", scan_duration.as_secs_f64());
        
        if let simple_db::executor::QueryResult::ResultSet(rs) = result {
            println!("扫描了 {} 条记录", rs.rows.len());
            println!("扫描速度: {:.2} records/sec\n", rs.rows.len() as f64 / scan_duration.as_secs_f64());
        }

        Ok(())
    }

    fn test_updates(&mut self) -> Result<LatencyStats, Box<dyn std::error::Error>> {
        println!("✏️ 测试4: 更新性能");
        let mut stats = LatencyStats::new();
        let start_time = Instant::now();
        
        for i in 1..=self.config.update_count {
            let sql = format!("UPDATE perf_table SET score = {} WHERE id = {}", i * 2, i);
            
            let start = Instant::now();
            self.db.execute_single_sql(&sql)?;
            let latency = start.elapsed();
            
            if self.config.enable_detailed_stats {
                stats.add(latency);
            }
        }
        
        let total_duration = start_time.elapsed();
        println!("执行 {} 次更新耗时: {:.2}秒", self.config.update_count, total_duration.as_secs_f64());
        println!("更新速度: {:.2} updates/sec\n", self.config.update_count as f64 / total_duration.as_secs_f64());

        if !self.config.enable_detailed_stats {
            stats.count = self.config.update_count;
            stats.total = total_duration;
            stats.min = total_duration / self.config.update_count as u32;
            stats.max = stats.min;
        }

        Ok(stats)
    }

    fn test_deletes(&mut self) -> Result<LatencyStats, Box<dyn std::error::Error>> {
        println!("🗑️ 测试5: 删除性能");
        let mut stats = LatencyStats::new();
        let start_time = Instant::now();
        
        let actual_delete_count = std::cmp::min(self.config.delete_count, self.config.insert_count);
        let delete_start = self.config.insert_count - actual_delete_count + 1;
        
        for i in delete_start..=self.config.insert_count {
            let sql = format!("DELETE FROM perf_table WHERE id = {}", i);
            
            let start = Instant::now();
            self.db.execute_single_sql(&sql)?;
            let latency = start.elapsed();
            
            if self.config.enable_detailed_stats {
                stats.add(latency);
            }
        }
        
        let total_duration = start_time.elapsed();
        println!("执行 {} 次删除耗时: {:.2}秒", actual_delete_count, total_duration.as_secs_f64());
        println!("删除速度: {:.2} deletes/sec\n", actual_delete_count as f64 / total_duration.as_secs_f64());

        if !self.config.enable_detailed_stats {
            stats.count = actual_delete_count;
            stats.total = total_duration;
            if actual_delete_count > 0 {
                stats.min = total_duration / actual_delete_count as u32;
                stats.max = stats.min;
            }
        }

        Ok(stats)
    }

    fn print_results(&self, insert_stats: &LatencyStats, select_stats: &LatencyStats, 
                     update_stats: &LatencyStats, delete_stats: &LatencyStats) {
        if self.config.enable_detailed_stats {
            println!("=== 详细基准测试结果 ===\n");
            println!("📝 插入操作统计:");
            println!("  {}", insert_stats);
            println!();
            println!("🔍 查询操作统计:");
            println!("  {}", select_stats);
            println!();
            println!("✏️  更新操作统计:");
            println!("  {}", update_stats);
            println!();
            println!("🗑️  删除操作统计:");
            println!("  {}", delete_stats);
            println!();
        }

        println!("=== 性能总结 ===");
        println!("操作类型        | 速度 (ops/sec)  | 平均延迟 (ms)");
        println!("----------------|----------------|---------------");
        println!("插入            | {:>10.2}     | {:>10.2}", 
                 insert_stats.ops_per_sec(),
                 insert_stats.average().as_secs_f64() * 1000.0);
        println!("查询            | {:>10.2}     | {:>10.2}", 
                 select_stats.ops_per_sec(),
                 select_stats.average().as_secs_f64() * 1000.0);
        println!("更新            | {:>10.2}     | {:>10.2}", 
                 update_stats.ops_per_sec(),
                 update_stats.average().as_secs_f64() * 1000.0);
        println!("删除            | {:>10.2}     | {:>10.2}", 
                 delete_stats.ops_per_sec(),
                 delete_stats.average().as_secs_f64() * 1000.0);

        let total_ops = insert_stats.count + select_stats.count + update_stats.count + delete_stats.count;
        let total_time = insert_stats.total + select_stats.total + update_stats.total + delete_stats.total;
        println!();
        println!("📊 总体统计:");
        println!("  总操作数: {}", total_ops);
        println!("  总耗时: {:.2}秒", total_time.as_secs_f64());
        println!("  总体吞吐量: {:.2} ops/sec", total_ops as f64 / total_time.as_secs_f64());
        println!("\n测试完成！");
    }
}

#[test]
fn test_database_performance() -> Result<(), Box<dyn std::error::Error>> {
    let config = TestConfig::from_env();
    let mut tester = DatabaseTester::new(config)?;
    tester.run_complete_test()?;
    Ok(())
}
