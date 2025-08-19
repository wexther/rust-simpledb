#!/usr/bin/env cargo script

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::io::Write;

/// 测试用例结构
#[derive(Debug)]
struct TestCase {
    name: String,
    input_file: String,
    expected_output_file: String,
}

/// 颜色输出工具
struct ColorOutput;

impl ColorOutput {
    fn green(text: &str) -> String {
        format!("\x1b[32m{}\x1b[0m", text)
    }
    
    fn red(text: &str) -> String {
        format!("\x1b[31m{}\x1b[0m", text)
    }
    
    fn yellow(text: &str) -> String {
        format!("\x1b[33m{}\x1b[0m", text)
    }
    
    fn blue(text: &str) -> String {
        format!("\x1b[34m{}\x1b[0m", text)
    }
    
    fn cyan(text: &str) -> String {
        format!("\x1b[36m{}\x1b[0m", text)
    }
}

/// 规范化输出文本以便比较
fn normalize_output(output: &str) -> String {
    output
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// 发现所有测试用例
fn discover_test_cases() -> Result<Vec<TestCase>, Box<dyn std::error::Error>> {
    let examples_dir = Path::new("examples");
    let mut test_cases = Vec::new();
    
    for entry in fs::read_dir(examples_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            let dir_name = path.file_name().unwrap().to_string_lossy().to_string();
            
            // 检查是否是数字目录（测试用例）
            if dir_name.chars().all(|c| c.is_ascii_digit()) {
                let input_file = path.join("input.txt");
                let output_file = path.join("output.txt");
                
                if input_file.exists() && output_file.exists() {
                    test_cases.push(TestCase {
                        name: dir_name,
                        input_file: input_file.to_string_lossy().to_string(),
                        expected_output_file: output_file.to_string_lossy().to_string(),
                    });
                }
            }
        }
    }
    
    // 按数字排序
    test_cases.sort_by(|a, b| {
        let a_num: i32 = a.name.parse().unwrap_or(0);
        let b_num: i32 = b.name.parse().unwrap_or(0);
        a_num.cmp(&b_num)
    });
    
    Ok(test_cases)
}

/// 运行单个测试用例
fn run_test_case(test_case: &TestCase) -> Result<bool, Box<dyn std::error::Error>> {
    println!("🧪 运行测试用例: {}", ColorOutput::cyan(&test_case.name));
    
    // 读取输入SQL
    let input_sql = fs::read_to_string(&test_case.input_file)?;
    println!("   📄 输入文件: {}", test_case.input_file);
    
    // 读取期望输出
    let expected_output = fs::read_to_string(&test_case.expected_output_file)?;
    println!("   📄 期望输出文件: {}", test_case.expected_output_file);
    
    // 创建临时数据库目录
    let temp_db_dir = format!("data/test_case_{}", test_case.name);
    
    // 执行 simple_db
    let mut cmd = Command::new("cargo");
    cmd.args(&[
        "run", "--", 
        "--data-dir", &temp_db_dir,
        "--db-name", &format!("test_{}", test_case.name),
        "--execute", &input_sql
    ]);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    
    let output = cmd.output()?;
    let actual_output = String::from_utf8_lossy(&output.stdout);
    let stderr_output = String::from_utf8_lossy(&output.stderr);
    
    // 检查是否有错误
    if !output.status.success() {
        println!("   {} 执行失败", ColorOutput::red("❌"));
        println!("   错误信息: {}", stderr_output);
        return Ok(false);
    }
    
    // 规范化输出进行比较
    let normalized_actual = normalize_output(&actual_output);
    let normalized_expected = normalize_output(&expected_output);
    
    println!("   📤 实际输出:");
    for line in actual_output.lines() {
        println!("      {}", line);
    }
    
    println!("   📥 期望输出:");
    for line in expected_output.lines() {
        println!("      {}", line);
    }
    
    // 比较输出
    if normalized_actual == normalized_expected {
        println!("   {} 测试通过", ColorOutput::green("✅"));
        Ok(true)
    } else {
        println!("   {} 测试失败", ColorOutput::red("❌"));
        println!("   {} 输出不匹配", ColorOutput::yellow("⚠️"));
        println!("   实际输出 (规范化): '{}'", normalized_actual);
        println!("   期望输出 (规范化): '{}'", normalized_expected);
        Ok(false)
    }
}

/// 运行基准测试
fn run_benchmark_tests() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", ColorOutput::blue("🚀 === 运行性能基准测试 ==="));
    
    // 运行 benchmark.rs
    println!("📊 运行基准测试...");
    let output = Command::new("cargo")
        .args(&["run", "--example", "benchmark"])
        .output()?;
    
    if output.status.success() {
        println!("{} 基准测试完成", ColorOutput::green("✅"));
    } else {
        println!("{} 基准测试失败", ColorOutput::red("❌"));
        println!("错误: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    // 运行 performance_test.rs
    println!("📈 运行性能测试...");
    let output = Command::new("cargo")
        .args(&["run", "--example", "performance_test"])
        .output()?;
    
    if output.status.success() {
        println!("{} 性能测试完成", ColorOutput::green("✅"));
    } else {
        println!("{} 性能测试失败", ColorOutput::red("❌"));
        println!("错误: {}", String::from_utf8_lossy(&output.stderr));
    }
    
    Ok(())
}

/// 清理测试数据
fn cleanup_test_data() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧹 清理测试数据...");
    
    // 删除测试数据目录
    if Path::new("data").exists() {
        for entry in fs::read_dir("data")? {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(name) = path.file_name() {
                let name_str = name.to_string_lossy();
                if name_str.starts_with("test_case_") || 
                   name_str == "benchmark" || 
                   name_str == "performance_test" ||
                   name_str == "optimization_test" {
                    if path.is_dir() {
                        fs::remove_dir_all(&path)?;
                        println!("   🗑️  删除: {}", path.display());
                    }
                }
            }
        }
    }
    
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", ColorOutput::blue("🧪 === Simple DB Examples 测试套件 ==="));
    println!();
    
    // 确保在项目根目录
    if !Path::new("Cargo.toml").exists() {
        eprintln!("错误: 请在 simple_db 项目根目录下运行此脚本");
        std::process::exit(1);
    }
    
    // 编译项目
    println!("🔨 编译项目...");
    let output = Command::new("cargo")
        .args(&["build"])
        .output()?;
    
    if !output.status.success() {
        eprintln!("编译失败:");
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        std::process::exit(1);
    }
    println!("{} 项目编译成功", ColorOutput::green("✅"));
    
    // 发现测试用例
    println!("\n🔍 发现测试用例...");
    let test_cases = discover_test_cases()?;
    println!("发现 {} 个测试用例", test_cases.len());
    
    // 运行所有测试用例
    println!("\n{}", ColorOutput::blue("📝 === 运行SQL功能测试 ==="));
    let mut passed = 0;
    let mut failed = 0;
    
    for test_case in &test_cases {
        match run_test_case(test_case) {
            Ok(true) => passed += 1,
            Ok(false) => failed += 1,
            Err(e) => {
                println!("   {} 测试出错: {}", ColorOutput::red("💥"), e);
                failed += 1;
            }
        }
        println!(); // 空行分隔
    }
    
    // 运行性能测试
    run_benchmark_tests()?;
    
    // 输出测试总结
    println!("\n{}", ColorOutput::blue("📊 === 测试总结 ==="));
    println!("总测试用例: {}", test_cases.len());
    println!("{}: {}", ColorOutput::green("通过"), passed);
    println!("{}: {}", ColorOutput::red("失败"), failed);
    
    let success_rate = if test_cases.len() > 0 {
        (passed as f64 / test_cases.len() as f64) * 100.0
    } else {
        0.0
    };
    println!("成功率: {:.1}%", success_rate);
    
    if failed == 0 {
        println!("\n{} 所有测试都通过了！", ColorOutput::green("🎉"));
    } else {
        println!("\n{} 有 {} 个测试失败", ColorOutput::yellow("⚠️"), failed);
    }
    
    // 询问是否清理测试数据
    print!("\n是否清理测试数据? (y/N): ");
    std::io::stdout().flush()?;
    
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    
    if input.trim().to_lowercase() == "y" || input.trim().to_lowercase() == "yes" {
        cleanup_test_data()?;
        println!("{} 测试数据已清理", ColorOutput::green("✅"));
    }
    
    Ok(())
}
