use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

/// 运行单个示例测试
fn run_example_test(test_num: u32) -> Result<(), Box<dyn std::error::Error>> {
    let test_dir = format!("examples/{}", test_num);
    let input_file = format!("{}/input.txt", test_dir);
    let expected_output_file = format!("{}/output.txt", test_dir);
    
    // 检查测试文件是否存在
    if !Path::new(&input_file).exists() || !Path::new(&expected_output_file).exists() {
        return Err(format!("测试文件不存在: {}", test_dir).into());
    }
    
    // 创建临时数据库目录
    let temp_dir = TempDir::new()?;
    let temp_db_path = temp_dir.path().join("test_db");
    
    // 运行 simple_db
    let output = Command::new("cargo")
        .args(["run", "--", "--data-dir", temp_db_path.to_str().unwrap(), "--db-name", &format!("test_{}", test_num), &input_file])
        .current_dir(".")
        .output()?;
    
    let actual_output = String::from_utf8_lossy(&output.stdout);
    let expected_output = fs::read_to_string(&expected_output_file)?;
    
    // 规范化输出以便比较
    let normalized_actual = normalize_output(&actual_output);
    let normalized_expected = normalize_output(&expected_output);
    
    if normalized_actual != normalized_expected {
        return Err(format!(
            "测试 {} 失败:\n期望输出:\n{}\n实际输出:\n{}",
            test_num, expected_output, actual_output
        ).into());
    }
    
    println!("✅ 测试 {} 通过", test_num);
    Ok(())
}

/// 规范化输出用于比较
fn normalize_output(output: &str) -> String {
    output
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn test_example_1() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(1)
}

#[test]
fn test_example_2() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(2)
}

#[test]
fn test_example_3() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(3)
}

#[test]
fn test_example_4() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(4)
}

#[test]
fn test_example_5() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(5)
}

#[test]
fn test_example_6() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(6)
}

#[test]
fn test_example_7() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(7)
}

#[test]
fn test_example_8() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(8)
}

#[test]
fn test_example_10() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(10)
}

#[test]
fn test_example_11() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(11)
}

#[test]
fn test_example_12() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(12)
}

#[test]
fn test_example_13() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(13)
}

#[test]
fn test_example_14() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(14)
}

#[test]
fn test_example_15() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(15)
}

#[test]
fn test_example_18() -> Result<(), Box<dyn std::error::Error>> {
    run_example_test(18)
}
