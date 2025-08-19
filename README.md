# rust_db

## 基本架构

本数据库系统由三个主要层次组成：

1. **语法解析层**：使用 sqlparser crate 解析SQL语句生成AST
2. **查询处理层**：负责生成查询计划并执行查询操作
3. **存储引擎层**：负责数据的存储、检索和管理

**执行流程**：main函数首先读取SQL文件，使用sqlparser解析SQL生成AST，然后查询处理器接收AST并生成查询计划，执行器执行查询计划并调用存储引擎API，最后返回查询结果。

## 详细架构

```text
src/
├── main.rs                        # 程序入口
├── error.rs                       # 统一错误处理
├── query.rs                       # 查询处理模块入口
├── storage.rs                     # 存储引擎模块入口
│
├── query/                         # 查询处理层
│   ├── planner.rs                 # 查询计划生成
│   ├── executor.rs                # 查询执行器
│   └── result.rs                  # 查询结果处理
│
└── storage/                       # 存储引擎层
    ├── catalog.rs                 # 元数据管理
    ├── database.rs                # 数据库对象管理
    ├── record.rs                  # 记录管理
    ├── table.rs                   # 表管理
    ├── transaction.rs             # 事务管理
    ├── io.rs                      # IO模块入口
    └── io/                        # IO子模块
        ├── buffer_manager.rs      # 缓冲区管理
        ├── disk_manager.rs        # 磁盘管理
        ├── page.rs                # 页面管理
        └── persistence.rs         # 持久化实现
```

## 基础功能

- 记录数据类型：int，varchar；
- 支持单行与多行注释；
- 支持记录的增删改查，即select，insert，update，delete；
- 支持数据表的create，drop；
- 持久化存储引擎
- 执行引擎，可读入SQL执行，返回表结果或报错信息
- 支持cargo test

## 编译构建

使用cargo即可。
使用以下命令构建。

```bash
cargo build --release
```

## 使用方法

### 交互模式

使用以下命令运行交互模式。

```bash
cargo run
```

在交互模式可以直接键入SQL语句，也可以使用以下命令：

```text
交互模式命令:
  .exit, .quit, \q              # 退出程序
  .help, \h                     # 显示帮助信息
  .tables                       # 显示所有表
  .schema <table_name>          # 显示表结构
  .save                         # 手动保存数据库
  .clear                        # 清屏
  .version                      # 显示版本信息
  .status                       # 显示数据库状态
  .read <file_path>             # 执行SQL文件
  .v, .verbose                  # 切换详细模式
  ↑↓ 箭头键                     # 浏览命令历史
  Tab 键                        # 自动补全
  Ctrl+C                        # 中断当前输入
  Ctrl+D                        # 退出程序

SQL示例:
  CREATE TABLE users (id INT, name VARCHAR(50));
  INSERT INTO users VALUES (1, 'Alice');
  SELECT * FROM users;
  DROP TABLE users;
```

### 单文件模式

使用以下命令运行单文件模式，将/path/to/your/sqlfile替换为文件地址：

```bash
cargo run /path/to/your/sqlfile
```

### 测试

本项目提供了完整的测试套件，包括功能测试、性能测试和基准测试。

#### 运行所有测试

```bash
# 运行所有集成测试
cargo test

# 运行测试并显示详细输出（推荐）
cargo test -- --nocapture
```

#### 运行特定测试

```bash
# 运行功能测试（基于examples目录）
cargo test --test examples_test

# 运行性能与基准测试（合并版本）
cargo test --test performance_benchmark

# 运行性能测试并显示详细结果
cargo test --test performance_benchmark -- --nocapture
```

#### 自定义性能测试

性能测试支持环境变量配置，可以自定义测试数据大小和测试模式：

```bash
# 基本配置
PERF_INSERT_COUNT=2000 cargo test --test performance_benchmark -- --nocapture
PERF_SELECT_COUNT=1000 cargo test --test performance_benchmark -- --nocapture
PERF_UPDATE_COUNT=500 cargo test --test performance_benchmark -- --nocapture
PERF_DELETE_COUNT=200 cargo test --test performance_benchmark -- --nocapture

# 启用详细基准统计（包含最小、最大、平均延迟）
PERF_DETAILED_STATS=1 cargo test --test performance_benchmark -- --nocapture

# 禁用全表扫描测试
PERF_FULL_SCAN=0 cargo test --test performance_benchmark -- --nocapture

# 组合使用
PERF_INSERT_COUNT=2000 PERF_SELECT_COUNT=1000 PERF_UPDATE_COUNT=500 PERF_DELETE_COUNT=200 PERF_DETAILED_STATS=1 cargo test --test performance_benchmark -- --nocapture
```

#### 测试说明

- **功能测试** (`examples_test`): 验证数据库基本功能，包括15个测试用例
- **性能与基准测试** (`performance_benchmark`): 统一的性能测试，支持简单模式和详细基准模式
  - 简单模式：快速性能概览，显示总体吞吐量和平均延迟
  - 详细模式：完整的基准测试，包含最小、最大、平均延迟统计

#### 查看测试覆盖的功能

功能测试涵盖以下SQL特性：

- 表的创建和删除
- 数据的插入、查询、更新、删除
- WHERE条件查询
- ORDER BY排序
- 表达式计算
- NULL值处理
- 主键约束

## 输入

允许且仅允许系统输入单个参数，为内含sql语句的txt文件路径。

## 数据结构

1. 数据库链表。该链表包含多个打开的数据库。

2. 数据库结构。一个数据库结构由以下部分组成：一个数据库头结构，一个模板结构，一个记录链表结构。数据库头结构包含以下信息：数据库的名称，数据库的记录数量等。模板结构是一个模板单元的链表，记录链表结构是一个记录的链表。
**注：考虑到rust的所有权系统，这里可以尽量使用Vec实现。对于频繁查找的内容使用HashMap或BTreeMap。**

3. 模板单元结构。该结构包含单元类型，该属性名称，模板单元属性信息。其中模板单元属性信息又包括：是否可设置为空，是否为主键，是否设置索引，是否unique，具体记录内容的最大位数等。

4. 记录结构。该结构包含该记录的唯一索引值和一个记录单元链表。记录单元链表是记录单元结构的链表。

5. 记录单元结构。该结构包含单元类型记录的属性具体的值，记录单元属性信息，其中单元属性信息又包括：是否为空，具体的位数等。

## 存储引擎层

存储引擎层是唯一能调用保存的数据结构的层级，该层主要有六个函数：

create函数用于创建一个数据库结构。该函数接受一个字符串类型和一个数据库模板，根据该模板链表创建一个数据库，返回这个新创建的数据库，如果创建失败返回空。
**注：这里可使用rust的错误处理，使用Result<T, E>表示可能失败的操作，下同。**

drop函数用于删除一个数据库结构。该函数接受一个字符串类型，为要删除的数据库的名称，之后找到对应的数据库进行删除，若成功删除返回1，否则返回0。

firstLog函数用于获取该数据库的第一条记录。该函数接受一个数据库，返回该数据库的第一条记录，若该数据库没有记录则返回空。

getLog函数用于获取该数据库已获得记录的下一条记录。该函数接受一个数据库和一条该数据库中的记录，返回该数据库中该条记录的下一条记录，若没有下一条记录则返回空。
**注：或许可以实现Iterator特征，并使用特征中的next函数实现**

insertLog函数用于创建一条新记录。该函数接受一个数据库和一条记录中各个属性的值，该函数新建一条对应的记录，并将该条记录插入到这个数据库中。

deleteLog函数用于删除一条记录。该函数接受一个数据库和数据库中的一条记录，将这条记录从这个数据库中删除。

## 查询处理层

查询处理层获取要执行的ast树的节点，然后解析该ast树，根据树的不同调用不同的存储引擎层命令获取调用结果，再根据调用结果进行输出。

该层还有一个辅助函数satisfy函数，该函数接受一条记录和一个ast节点（条件语句），判断该语句是否满足该ast节点所对应的条件。

where函数接受一个ast树，返回一个链表，该链表包含满足条件的所有记录。该函数调用firstLog获取数据库的第一条记录，之后循环调用getLog获取下一条记录，对于每条记录调用satisfy函数判断是否满足，若满足则加到链表里。
