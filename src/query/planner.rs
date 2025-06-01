use sqlparser::ast;

mod analyzer;

use crate::error::{DBError, Result};
use crate::storage::table::{ColumnDef, DataType, Record, Table, Value};
use analyzer::{Analyzer, Condition};

/// 表示选择的内容
#[derive(Debug, Clone)]
pub enum SelectColumns {
    /// 通配符 * - 选择所有列
    Wildcard,
    /// 具体的列列表（可以是列名或表达式）
    Columns(Vec<SelectItem>),
}

/// 表示单个选择项（列名或表达式）
#[derive(Debug, Clone)]
pub struct SelectItem {
    /// 表达式（包括简单列名和复杂表达式）
    pub expr: analyzer::Expression,
    /// 别名
    pub alias: Option<String>,
    /// 原始文本
    pub original_text: String,
}

// 新增的排序相关结构
/// 排序方向
#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Asc,  // 升序
    Desc, // 降序
}

/// 排序项
#[derive(Debug, Clone)]
pub struct OrderByItem {
    /// 排序的列名或表达式
    pub column: String,
    /// 排序方向
    pub direction: SortDirection,
}

/// 表示查询计划的枚举
#[derive(Debug)]
pub enum Plan {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    DropTable {
        name: String,
    },
    Select {
        table_name: Option<String>,
        columns: SelectColumns,
        conditions: Option<Condition>,
        order_by: Option<Vec<OrderByItem>>,
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Update {
        table_name: String,
        set_pairs: Vec<(String, Value)>,
        conditions: Option<Condition>,
    },
    Delete {
        table_name: String,
        conditions: Option<Condition>,
    },
    // 以下是数据库管理操作
    CreateDatabase {
        name: String,
    },
    DropDatabase {
        name: String,
    },
    UseDatabase {
        name: String,
    },
    ShowDatabases,
    ShowTables,
}

/// 查询计划生成器 - 负责使用分析后的数据生成查询计划
pub struct Planner {
    analyzer: Analyzer,
}

impl Planner {
    pub fn new() -> Self {
        Self {
            analyzer: Analyzer::new(),
        }
    }

    /// 将AST转换为查询计划
    pub fn plan(&self, stmt: &ast::Statement) -> Result<Plan> {
        match stmt {
            ast::Statement::CreateTable(create_table) => self.plan_create_table(create_table),
            ast::Statement::Drop {
                object_type, names, ..
            } => self.plan_drop_table(object_type, names),
            ast::Statement::Query(query) => self.plan_query(query),
            ast::Statement::Insert(insert) => self.plan_insert(insert),
            ast::Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let table_name = match table {
                    sqlparser::ast::TableWithJoins { relation, .. } => match relation {
                        ast::TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err(DBError::Planner("仅支持简单表引用".to_string())),
                    },
                };

                // 解析SET子句
                let mut set_pairs = Vec::new();
                for assignment in assignments {
                    let column_name = assignment.target.to_string();
                    let value = self.analyzer.analyze_expr_to_value(&assignment.value)?;
                    set_pairs.push((column_name, value));
                }

                // 解析WHERE子句
                let conditions = if let Some(expr) = selection {
                    Some(Analyzer::analyze_condition(expr)?)
                } else {
                    None
                };

                Ok(Plan::Update {
                    table_name,
                    set_pairs,
                    conditions,
                })
            }
            ast::Statement::Delete(delete) => {
                if delete.tables.len() != 1 {
                    return Err(DBError::Parse("仅支持单表删除".to_string()));
                }
                let table_name = delete.tables[0].to_string();
                let selection = &delete.selection;

                // 解析WHERE子句
                let conditions = if let Some(expr) = selection {
                    Some(Analyzer::analyze_condition(expr)?)
                } else {
                    None
                };

                Ok(Plan::Delete {
                    table_name,
                    conditions,
                })
            }
            // 处理数据库管理语句
            ast::Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => Ok(Plan::CreateDatabase {
                name: schema_name.to_string(),
            }),
            ast::Statement::ShowTables { .. } => Ok(Plan::ShowTables),
            ast::Statement::ShowDatabases { .. } => Ok(Plan::ShowDatabases),
            _ => Err(DBError::Parse(format!("不支持的SQL语句类型: {:?}", stmt))),
        }
    }

    fn plan_query(&self, query: &Box<ast::Query>) -> Result<Plan> {
        self.analyzer.analyze_select(query)
    }

    fn plan_create_table(&self, create_table: &ast::CreateTable) -> Result<Plan> {
        Ok(Plan::CreateTable {
            name: create_table.name.to_string(),
            columns: self
                .analyzer
                .analyze_column_definitions(&create_table.columns)?,
        })
    }

    fn plan_drop_table(
        &self,
        object_type: &ast::ObjectType,
        names: &Vec<ast::ObjectName>,
    ) -> Result<Plan> {
        match object_type {
            ast::ObjectType::Table => {
                if let Some(name) = names.first() {
                    Ok(Plan::DropTable {
                        name: name.to_string(),
                    })
                } else {
                    Err(DBError::Parse("DROP TABLE缺少表名".to_string()))
                }
            }
            _ => Err(DBError::Parse(format!(
                "不支持的DROP操作: {:?}",
                object_type
            ))),
        }
    }

    fn plan_insert(&self, insert: &ast::Insert) -> Result<Plan> {
        // 修改这里的模式匹配
        let table_name = match &insert.table {
            // 改为直接获取 ObjectName
            ast::TableObject::TableName(name) => name.to_string(),
            _ => return Err(DBError::Parse("仅支持简单表引用".to_string())),
        };

        // 获取列名（如果 SQL 中指定了列名）
        let columns: Vec<String> = if insert.columns.is_empty() {
            // 如果没有指定列名，需要从表结构中获取所有列名
            // 这里可能需要访问 catalog 来获取表的列定义
            return Err(DBError::Parse("暂不支持不指定列名的插入".to_string()));
        } else {
            insert.columns.iter().map(|col| col.to_string()).collect()
        };

        // 解析行数据
        let mut rows = Vec::new();
        if let Some(ast::SetExpr::Values(values_list)) = &insert.source.as_ref().map(|s| &*s.body) {
            for row in &values_list.rows {
                let mut row_values = Vec::new();
                for expr in row {
                    let value = self.analyzer.analyze_expr_to_value(expr)?;
                    row_values.push(value);
                }

                // 验证值的数量与列数是否匹配
                if row_values.len() != columns.len() {
                    return Err(DBError::Parse(format!(
                        "第 {} 行的值数量({})与列数({})不匹配",
                        rows.len() + 1,
                        row_values.len(),
                        columns.len()
                    )));
                }

                rows.push(row_values);
            }
        } else {
            return Err(DBError::Parse("不支持的INSERT语法".to_string()));
        }

        Ok(Plan::Insert {
            table_name,
            columns,
            rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::table::DataType;

    use super::*;

    #[test]
    fn test_create_table_plan() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "CREATE TABLE users (
    id INT(32) PRIMARY KEY,
    name VARCHAR(100),
    left_num INT(32),
    discription VARCHAR(150),
    price INT NOT NULL NOT NULL,
    time INTEGER
);";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::CreateTable { name, columns } = plan {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 6);

            assert_eq!(columns[0].name, "id");
            assert_eq!(columns[0].data_type, DataType::Int(32));
            assert!(columns[0].is_primary);
            assert!(columns[0].not_null);
            assert!(columns[0].unique);

            assert_eq!(columns[1].name, "name");
            assert_eq!(columns[1].data_type, DataType::Varchar(100));

            assert_eq!(columns[2].name, "left_num");
            assert_eq!(columns[2].data_type, DataType::Int(32));

            assert_eq!(columns[3].name, "discription");
            assert_eq!(columns[3].data_type, DataType::Varchar(150));

            assert_eq!(columns[4].name, "price");
            assert!(matches!(columns[4].data_type, DataType::Int(_)));

            assert_eq!(columns[5].name, "time");
            assert!(matches!(columns[5].data_type, DataType::Int(_)));
        } else {
            panic!("预期生成CreateTable查询计划");
        }
    }

    #[test]
    fn test_drop_table_plan() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "DROP TABLE users;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::DropTable { name } = plan {
            assert_eq!(name, "users");
        } else {
            panic!("预期生成DropTable查询计划");
        }
    }

    #[test]
    fn test_select_expression_plan_1() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT 1 * 2;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            // 验证是无表查询
            assert!(table_name.is_none());

            // 验证表达式列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 1);
                assert!(items[0].alias.is_none());
                assert_eq!(items[0].original_text, "1 * 2");

                // 可以进一步验证表达式结构
                if let analyzer::Expression::Binary { operator, .. } = &items[0].expr {
                    assert_eq!(*operator, analyzer::BinaryOperator::Multiply);
                }
            } else {
                panic!("预期具体列选择");
            }

            assert!(conditions.is_none());
            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_expression_plan_2() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT 1300;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert!(table_name.is_none());

            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].original_text, "1300");

                if let analyzer::Expression::Value(value) = &items[0].expr {
                    assert_eq!(*value, Value::Int(1300));
                }
            }
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_mixed_expression_and_table() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, price * 2, 'constant' FROM products;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            // 有表查询
            assert_eq!(table_name.as_ref().unwrap(), "products");

            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 3);

                // 第一列：简单列名
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 第二列：表达式
                if let analyzer::Expression::Binary { operator, .. } = &items[1].expr {
                    assert_eq!(*operator, analyzer::BinaryOperator::Multiply);
                }

                // 第三列：常量
                if let analyzer::Expression::Value(value) = &items[2].expr {
                    assert_eq!(*value, Value::String("constant".to_string()));
                }
            }
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_with_order_by() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, name FROM users WHERE age > 18 ORDER BY name ASC, id DESC;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 修改：验证具体列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：id
                assert_eq!(items[0].original_text, "id");
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let analyzer::Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            assert!(conditions.is_some());

            // 测试 ORDER BY
            let order_by = order_by.unwrap();
            assert_eq!(order_by.len(), 2);
            assert_eq!(order_by[0].column, "name");
            assert_eq!(order_by[0].direction, SortDirection::Asc);
            assert_eq!(order_by[1].column, "id");
            assert_eq!(order_by[1].direction, SortDirection::Desc);
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_plan() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, name FROM users WHERE left_num > 10;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 修改：验证具体列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：id
                assert_eq!(items[0].original_text, "id");
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let analyzer::Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 补充完整的 conditions 测试
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            // 验证条件的具体内容：left_num > 10
            match condition {
                analyzer::Condition::Expression(expr) => {
                    // 验证表达式是二元操作
                    if let analyzer::Expression::Binary {
                        left,
                        operator,
                        right,
                    } = expr
                    {
                        // 验证左操作数是列名 "left_num"
                        if let analyzer::Expression::Column(column_name) = &*left {
                            assert_eq!(column_name, "left_num");
                        } else {
                            panic!("预期左操作数是列名");
                        }

                        // 验证操作符是 ">"
                        assert_eq!(operator, analyzer::BinaryOperator::GreaterThan);

                        // 验证右操作数是值 10
                        if let analyzer::Expression::Value(value) = &*right {
                            assert_eq!(*value, Value::Int(10));
                        } else {
                            panic!("预期右操作数是整数值 10");
                        }
                    } else {
                        panic!("预期生成二元比较表达式");
                    }
                }
                analyzer::Condition::IsNull(_) => panic!("预期生成表达式条件，而不是 IS NULL"),
                analyzer::Condition::IsNotNull(_) => {
                    panic!("预期生成表达式条件，而不是 IS NOT NULL")
                }
                analyzer::Condition::Constant(_) => panic!("预期生成表达式条件，而不是常量条件"),
            }

            // 验证没有 ORDER BY 子句
            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_with_complex_conditions() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, name FROM users WHERE age > 18 AND name = 'Alice';";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 修改：验证具体列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：id
                assert_eq!(items[0].original_text, "id");
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let analyzer::Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试复杂条件：age > 18 AND name = 'Alice'
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            match condition {
                analyzer::Condition::Expression(expr) => {
                    if let analyzer::Expression::Binary {
                        left,
                        operator,
                        right,
                    } = expr
                    {
                        assert_eq!(operator, analyzer::BinaryOperator::And);

                        // 验证左边条件：age > 18
                        if let analyzer::Expression::Binary {
                            left: age_left,
                            operator: age_op,
                            right: age_right,
                        } = &*left
                        {
                            if let analyzer::Expression::Column(col) = &**age_left {
                                assert_eq!(col, "age");
                            }
                            assert_eq!(*age_op, analyzer::BinaryOperator::GreaterThan);
                            if let analyzer::Expression::Value(val) = &**age_right {
                                assert_eq!(*val, Value::Int(18));
                            }
                        }

                        // 验证右边条件：name = 'Alice'
                        if let analyzer::Expression::Binary {
                            left: name_left,
                            operator: name_op,
                            right: name_right,
                        } = &*right
                        {
                            if let analyzer::Expression::Column(col) = &**name_left {
                                assert_eq!(col, "name");
                            }
                            assert_eq!(*name_op, analyzer::BinaryOperator::Equal);
                            if let analyzer::Expression::Value(val) = &**name_right {
                                assert_eq!(*val, Value::String("Alice".to_string()));
                            }
                        }
                    } else {
                        panic!("预期生成二元逻辑表达式");
                    }
                }
                _ => panic!("预期生成表达式条件"),
            }

            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_with_is_null_condition() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, name FROM users WHERE email IS NULL;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 修改：验证具体列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：id
                assert_eq!(items[0].original_text, "id");
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let analyzer::Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试 IS NULL 条件
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            match condition {
                analyzer::Condition::IsNull(expr) => {
                    if let analyzer::Expression::Column(column_name) = expr {
                        assert_eq!(column_name, "email");
                    } else {
                        panic!("预期 IS NULL 应用于列名");
                    }
                }
                _ => panic!("预期生成 IS NULL 条件"),
            }

            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_with_is_not_null_condition() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, name FROM users WHERE email IS NOT NULL;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 修改：验证具体列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：id
                assert_eq!(items[0].original_text, "id");
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let analyzer::Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试 IS NOT NULL 条件
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            match condition {
                analyzer::Condition::IsNotNull(expr) => {
                    if let analyzer::Expression::Column(column_name) = expr {
                        assert_eq!(column_name, "email");
                    } else {
                        panic!("预期 IS NOT NULL 应用于列名");
                    }
                }
                _ => panic!("预期生成 IS NOT NULL 条件"),
            }

            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_with_constant_condition() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, name FROM users WHERE true;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 修改：验证具体列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：id
                assert_eq!(items[0].original_text, "id");
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let analyzer::Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试常量条件
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            match condition {
                analyzer::Condition::Constant(val) => {
                    assert_eq!(val, true);
                }
                _ => panic!("预期生成常量条件"),
            }

            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_without_conditions() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, name FROM users;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 修改：验证具体列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：id
                assert_eq!(items[0].original_text, "id");
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let analyzer::Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试没有 WHERE 条件的情况
            assert!(conditions.is_none());
            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_wildcard() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT * FROM users;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 验证是通配符
            if let SelectColumns::Wildcard = columns {
                // 正确
            } else {
                panic!("预期通配符选择");
            }

            assert!(conditions.is_none());
            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_specific_columns() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id, name * 2 AS double_name FROM users;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "users"); // 修改：使用 Option<String>

            // 验证是具体列
            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：id（无别名）
                assert!(items[0].alias.is_none());
                assert_eq!(items[0].original_text, "id");
                if let analyzer::Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name * 2（有别名）
                assert_eq!(items[1].alias.as_ref().unwrap(), "double_name");
                assert!(
                    items[1].original_text.contains("name") && items[1].original_text.contains("2")
                );
            } else {
                panic!("预期具体列选择");
            }

            assert!(conditions.is_none());
            assert!(order_by.is_none());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_wildcard_with_other_columns_should_fail() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT *, id FROM users;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();

        // 这应该返回错误
        let result = planner.plan(&ast[0]);
        assert!(result.is_err());

        if let Err(DBError::Parse(msg)) = result {
            assert!(msg.contains("通配符"));
        } else {
            panic!("预期解析错误");
        }
    }

    #[test]
    fn test_select_expression_column_names() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "SELECT id *price* 2, name AS user_name FROM books_test12;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Select {
            table_name,
            columns,
            conditions,
            order_by,
        } = plan
        {
            assert_eq!(table_name.as_ref().unwrap(), "books_test12"); // 修改：使用 Option<String>

            if let SelectColumns::Columns(items) = columns {
                assert_eq!(items.len(), 2);

                // 验证第一列：表达式无别名，使用原始文本作为列名
                assert!(items[0].alias.is_none());
                let original_text = &items[0].original_text;
                assert_eq!( original_text, "id *price* 2" );

                // 验证第二列：有别名
                assert_eq!(items[1].alias.as_ref().unwrap(), "user_name");
                assert_eq!(items[1].original_text, "name");
            } else {
                panic!("预期具体列选择");
            }
        } else {
            panic!("预期生成Select查询计划");
        }
    }
}
