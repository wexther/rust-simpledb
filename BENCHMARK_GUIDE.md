# Simple DB 读写延迟测试指南

## 概述

本项目提供了两个基准测试工具，用于测试 Simple DB 的读写性能和延迟：

1. **benchmark.rs** - 详细的多维度基准测试
2. **performance_test.rs** - 简化的性能测试

## 使用方法

### 1. 基础基准测试

运行默认基准测试（100插入、50查询、20更新、10删除）：

```bash
cargo run --example benchmark
```

### 2. 自定义规模测试

你可以自定义各种操作的数量：

```bash
# 大规模测试
cargo run --example benchmark -- --insert 2000 --select 1000 --update 500 --delete 200

# 小规模测试
cargo run --example benchmark -- --insert 50 --select 25 --update 10 --delete 5

# 只测试插入性能
cargo run --example benchmark -- --insert 1000 --select 0 --update 0 --delete 0
```

### 3. 使用非临时数据库

默认情况下，基准测试使用临时数据库。如果你想使用默认数据库：

```bash
cargo run --example benchmark -- --no-temp-db
```

### 4. 简化性能测试

运行包含各类操作的综合性能测试：

```bash
cargo run --example performance_test
```

### 5. 查看帮助信息

```bash
cargo run --example benchmark -- --help
```

## 测试结果解读

### 基准测试输出

基准测试会显示以下指标：

- **操作数**: 执行的操作总数
- **平均延迟**: 单次操作的平均耗时（毫秒）
- **最小延迟**: 单次操作的最短耗时（毫秒）
- **最大延迟**: 单次操作的最长耗时（毫秒）
- **吞吐量**: 每秒操作数 (ops/sec)

### 性能参考指标

在一般的开发环境中，Simple DB 的典型性能表现：

| 操作类型 | 平均延迟 | 吞吐量 (ops/sec) |
|----------|----------|------------------|
| 插入     | 1-3ms    | 300-1000         |
| 查询     | 1-3ms    | 300-1200         |
| 更新     | 2-4ms    | 250-500          |
| 删除     | 1-2ms    | 500-1000         |

## 故障排除

### 常见问题

1. **页面大小限制错误**
   - 现象：出现 "页面数据过大" 错误
   - 解决：已通过增加页面大小和改进分页逻辑解决

2. **内存不足**
   - 现象：大规模测试时系统响应慢
   - 解决：减少测试规模或关闭其他应用程序

3. **磁盘空间不足**
   - 现象：测试过程中出现IO错误
   - 解决：清理磁盘空间，删除不需要的数据文件
