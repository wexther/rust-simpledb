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

    fn avg(&self) -> Duration {
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
            self.avg().as_secs_f64() * 1000.0,
            self.min.as_secs_f64() * 1000.0,
            self.max.as_secs_f64() * 1000.0,
            self.ops_per_sec()
        )
    }
}

/// 基准测试配置
struct BenchmarkConfig {
    /// 插入记录数量
    insert_count: usize,
    /// 查询操作数量
    select_count: usize,
    /// 更新操作数量
    update_count: usize,
    /// 删除操作数量
    delete_count: usize,
    /// 是否使用临时数据库
    use_temp_db: bool,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            insert_count: 100,
            select_count: 50,
            update_count: 20,
            delete_count: 10,
            use_temp_db: true,
        }
    }
}

/// 数据库基准测试器
struct DatabaseBenchmark {
    db: SimpleDB,
    config: BenchmarkConfig,
}

impl DatabaseBenchmark {
    fn new(config: BenchmarkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let db_config = if config.use_temp_db {
            DBConfig {
                sql_file: None,
                base_dir: Some("data/benchmark".to_string()),
                db_name: Some("benchmark_test".to_string()),
                execute: None,
                interactive: false,
                verbose: false,
            }
        } else {
            DBConfig {
                sql_file: None,
                base_dir: None,
                db_name: None,
                execute: None,
                interactive: false,
                verbose: false,
            }
        };

        let db = SimpleDB::with_config(db_config)?;
        
        Ok(Self { db, config })
    }

    /// 运行完整的基准测试
    fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("=== Simple DB 读写延迟基准测试 ===");
        println!("配置:");
        println!("  插入操作: {} 次", self.config.insert_count);
        println!("  查询操作: {} 次", self.config.select_count);
        println!("  更新操作: {} 次", self.config.update_count);
        println!("  删除操作: {} 次", self.config.delete_count);
        println!();

        // 清理并初始化测试环境
        self.setup_test_environment()?;

        // 运行各项测试
        let insert_stats = self.benchmark_inserts()?;
        let select_stats = self.benchmark_selects()?;
        let update_stats = self.benchmark_updates()?;
        let delete_stats = self.benchmark_deletes()?;

        // 打印结果
        self.print_results(&insert_stats, &select_stats, &update_stats, &delete_stats);

        Ok(())
    }

    /// 设置测试环境
    fn setup_test_environment(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("正在设置测试环境...");
        
        // 删除可能存在的测试表
        let _ = self.db.execute_single_sql("DROP TABLE IF EXISTS benchmark_table");
        
        // 创建测试表
        let create_table_sql = "
            CREATE TABLE benchmark_table (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                age INT,
                email VARCHAR(100)
            )
        ";
        
        self.db.execute_single_sql(create_table_sql)?;
        println!("测试表创建完成");
        
        Ok(())
    }

    /// 基准测试插入操作
    fn benchmark_inserts(&mut self) -> Result<LatencyStats, Box<dyn std::error::Error>> {
        println!("开始插入基准测试...");
        let mut stats = LatencyStats::new();
        
        for i in 0..self.config.insert_count {
            let sql = format!(
                "INSERT INTO benchmark_table VALUES ({}, 'user{}', {}, 'user{}@example.com')",
                i + 1,
                i + 1,
                20 + (i % 50),
                i + 1
            );
            
            let start = Instant::now();
            self.db.execute_single_sql(&sql)?;
            let latency = start.elapsed();
            
            stats.add(latency);
            
            if (i + 1) % 100 == 0 {
                print!("\r插入进度: {}/{}", i + 1, self.config.insert_count);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }
        
        println!("\n插入测试完成");
        Ok(stats)
    }

    /// 基准测试查询操作
    fn benchmark_selects(&mut self) -> Result<LatencyStats, Box<dyn std::error::Error>> {
        println!("开始查询基准测试...");
        let mut stats = LatencyStats::new();
        
        for i in 0..self.config.select_count {
            // 随机查询不同类型的操作
            let sql = match i % 3 {
                0 => format!("SELECT * FROM benchmark_table WHERE id = {}", (i % self.config.insert_count) + 1),
                1 => "SELECT * FROM benchmark_table WHERE age > 30".to_string(),
                _ => "SELECT name, email FROM benchmark_table WHERE age < 40".to_string(),
            };
            
            let start = Instant::now();
            self.db.execute_single_sql(&sql)?;
            let latency = start.elapsed();
            
            stats.add(latency);
            
            if (i + 1) % 50 == 0 {
                print!("\r查询进度: {}/{}", i + 1, self.config.select_count);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }
        
        println!("\n查询测试完成");
        Ok(stats)
    }

    /// 基准测试更新操作
    fn benchmark_updates(&mut self) -> Result<LatencyStats, Box<dyn std::error::Error>> {
        println!("开始更新基准测试...");
        let mut stats = LatencyStats::new();
        
        for i in 0..self.config.update_count {
            let id = (i % self.config.insert_count) + 1;
            let sql = format!(
                "UPDATE benchmark_table SET age = {} WHERE id = {}",
                30 + (i % 40),
                id
            );
            
            let start = Instant::now();
            self.db.execute_single_sql(&sql)?;
            let latency = start.elapsed();
            
            stats.add(latency);
            
            if (i + 1) % 20 == 0 {
                print!("\r更新进度: {}/{}", i + 1, self.config.update_count);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }
        
        println!("\n更新测试完成");
        Ok(stats)
    }

    /// 基准测试删除操作
    fn benchmark_deletes(&mut self) -> Result<LatencyStats, Box<dyn std::error::Error>> {
        println!("开始删除基准测试...");
        let mut stats = LatencyStats::new();
        
        for i in 0..self.config.delete_count {
            // 删除最后添加的记录
            let id = self.config.insert_count - i;
            let sql = format!("DELETE FROM benchmark_table WHERE id = {}", id);
            
            let start = Instant::now();
            self.db.execute_single_sql(&sql)?;
            let latency = start.elapsed();
            
            stats.add(latency);
            
            if (i + 1) % 10 == 0 {
                print!("\r删除进度: {}/{}", i + 1, self.config.delete_count);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }
        
        println!("\n删除测试完成");
        Ok(stats)
    }

    /// 打印测试结果
    fn print_results(
        &self,
        insert_stats: &LatencyStats,
        select_stats: &LatencyStats,
        update_stats: &LatencyStats,
        delete_stats: &LatencyStats,
    ) {
        println!("\n=== 基准测试结果 ===");
        println!();
        
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

        // 总体统计
        let total_ops = insert_stats.count + select_stats.count + update_stats.count + delete_stats.count;
        let total_time = insert_stats.total + select_stats.total + update_stats.total + delete_stats.total;
        let overall_ops_per_sec = if total_time.as_secs_f64() > 0.0 {
            total_ops as f64 / total_time.as_secs_f64()
        } else {
            0.0
        };

        println!("📊 总体统计:");
        println!("  总操作数: {}", total_ops);
        println!("  总耗时: {:.2}秒", total_time.as_secs_f64());
        println!("  总体吞吐量: {:.2} ops/sec", overall_ops_per_sec);
        println!();
    }
}

#[test]
fn test_database_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    // 使用默认配置运行基准测试
    let config = BenchmarkConfig::default();
    
    let mut benchmark = DatabaseBenchmark::new(config)?;
    benchmark.run()?;
    
    Ok(())
}
