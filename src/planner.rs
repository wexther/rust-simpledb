use crate::error::{DBError, Result};
use crate::storage::table::{ColumnDef, DataType, Record, Table, Value};
use sqlparser::ast;

/// 表达式枚举（从 analyzer.rs 移过来）
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Column(String),
    Value(Value),
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
    Unary {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
}

/// 二元操作符（从 analyzer.rs 移过来）
#[derive(Clone, Debug, PartialEq)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    And,
    Or,
}

/// 一元操作符（从 analyzer.rs 移过来）
#[derive(Clone, Debug, PartialEq)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

/// 条件枚举（从 analyzer.rs 移过来）
#[derive(Clone, Debug, PartialEq)]
pub enum Condition {
    Expression(Expression),
    IsNull(Expression),
    IsNotNull(Expression),
    Constant(bool),
}

/// 选择列枚举
#[derive(Debug, Clone)]
pub enum SelectColumns {
    /// 通配符 * - 选择所有列
    Wildcard,
    /// 具体的列列表
    Columns(Vec<SelectItem>),
}

/// 选择项结构
#[derive(Debug, Clone)]
pub struct SelectItem {
    pub expr: Expression,
    pub alias: Option<String>,
    //这里可能可以删去
    pub original_text: String,
}

/// 排序方向
#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// 排序项
#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub column: String,
    pub direction: SortDirection,
}

/// 查询计划枚举
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
        /// 空时表示插入所有列， 非空时表示指定列
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

/// 统一的查询计划生成器
pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    /// 主要的计划生成方法
    pub fn plan(&self, stmt: &ast::Statement) -> Result<Plan> {
        match stmt {
            ast::Statement::CreateTable(create_table) => Ok(Plan::CreateTable {
                name: create_table.name.to_string(),
                columns: self.analyze_column_definitions(&create_table.columns)?,
            }),

            ast::Statement::Drop {
                object_type, names, ..
            } => match object_type {
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
            },

            ast::Statement::Query(query) => self.analyze_select(query),
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
                let mut set_pairs = Vec::new();

                for assignment in assignments {
                    let column_name = assignment.target.to_string();
                    let value = self.analyze_expr_to_value(&assignment.value)?;
                    set_pairs.push((column_name, value));
                }

                let conditions = if let Some(expr) = selection {
                    Some(self.analyze_condition(expr)?)
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
                //have bug “仅支持单表删除”
                if delete.tables.len() > 1 {
                    return Err(DBError::Parse("仅支持单表删除".to_string()));
                }
                //have bug delete.tables为空
                //let table_name = delete.tables[0].to_string();
                // 兼容不同SQL解析器的Delete结构
                let table_name: String = if !delete.tables.is_empty() {
                    delete.tables[0].to_string()
                } else if let from = &delete.from {
                    let from_str = from.to_string();
                    //此时from的格式为“FROM table_name”，需要从中截取出table_name
                    let parts: Vec<&str> = from_str.trim().split_whitespace().collect();
                    if parts.len() == 2 && parts[0].eq_ignore_ascii_case("from") {
                        parts[1].to_string()
                    } else {
                        from_str
                    }
                } else {
                    return Err(DBError::Parse("DELETE 语句缺少表名".to_string()));
                };

                // 输出表的名字
                /*
                return Err(DBError::Parse(
                    format!("DELETE 语句的表名: {}", table_name),
                ));
                */

                let conditions = if let Some(expr) = &delete.selection {
                    Some(self.analyze_condition(expr)?)
                } else {
                    None
                };

                Ok(Plan::Delete {
                    table_name,
                    conditions,
                })
            }

            ast::Statement::CreateSchema { schema_name, .. } => Ok(Plan::CreateDatabase {
                name: schema_name.to_string(),
            }),

            ast::Statement::ShowTables { .. } => Ok(Plan::ShowTables),
            ast::Statement::ShowDatabases { .. } => Ok(Plan::ShowDatabases),

            _ => Err(DBError::Parse(format!("不支持的SQL语句类型: {:?}", stmt))),
        }
    }

    /// 分析 SELECT 查询
    pub fn analyze_select(&self, query: &ast::Query) -> Result<Plan> {
        let body = match &*query.body {
            ast::SetExpr::Select(select) => &**select,
            _ => return Err(DBError::Planner("仅支持SELECT查询".to_string())),
        };

        if body.from.is_empty() {
            // 无表查询
            let columns = self.analyze_select_columns(&body.projection)?;
            Ok(Plan::Select {
                table_name: None,
                columns,
                conditions: None,
                order_by: None,
            })
        } else {
            // 有表查询
            let table_name = self.extract_table_name(&body.from)?;
            let columns = self.analyze_select_columns(&body.projection)?;

            let conditions = if let Some(selection) = &body.selection {
                Some(self.analyze_condition(selection)?)
            } else {
                None
            };

            let order_by = if let Some(ref order_by_clause) = query.order_by {
                match &order_by_clause.kind {
                    ast::OrderByKind::Expressions(exprs) => Some(self.analyze_order_by(exprs)?),
                    ast::OrderByKind::All(_) => {
                        return Err(DBError::Planner("暂不支持 ORDER BY ALL 语法".to_string()));
                    }
                }
            } else {
                None
            };

            Ok(Plan::Select {
                table_name: Some(table_name),
                columns,
                conditions,
                order_by,
            })
        }
    }

    /// 分析选择列
    fn analyze_select_columns(&self, projection: &[ast::SelectItem]) -> Result<SelectColumns> {
        let has_wildcard = projection.iter().any(|item| {
            matches!(
                item,
                ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(_, _)
            )
        });

        if has_wildcard {
            if projection.len() > 1 {
                return Err(DBError::Parse("通配符 * 不能与其他列同时使用".to_string()));
            }
            return Ok(SelectColumns::Wildcard);
        }

        let mut columns = Vec::new();
        for item in projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    let expression = self.convert_expr(expr)?;
                    let original_text = format!("{}", expr);

                    columns.push(SelectItem {
                        expr: expression,
                        alias: None,
                        original_text,
                    });
                }

                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let expression = self.convert_expr(expr)?;
                    let original_text = format!("{}", expr);

                    columns.push(SelectItem {
                        expr: expression,
                        alias: Some(alias.to_string()),
                        original_text,
                    });
                }

                ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(_, _) => {
                    unreachable!("通配符应该在前面已经处理");
                }
            }
        }

        Ok(SelectColumns::Columns(columns))
    }

    /// 转换表达式
    pub fn convert_expr(&self, expr: &ast::Expr) -> Result<Expression> {
        match expr {
            ast::Expr::Identifier(ident) => Ok(Expression::Column(ident.value.clone())),

            ast::Expr::Value(value_with_span) => {
                let value = self.convert_ast_value(&value_with_span.value)?;
                Ok(Expression::Value(value))
            }

            ast::Expr::BinaryOp { left, op, right } => {
                let left_expr = Box::new(self.convert_expr(left)?);
                let right_expr = Box::new(self.convert_expr(right)?);
                let operator = self.convert_binary_operator(op)?;

                Ok(Expression::Binary {
                    left: left_expr,
                    operator,
                    right: right_expr,
                })
            }

            ast::Expr::UnaryOp { op, expr } => {
                let operand = Box::new(self.convert_expr(expr)?);
                let operator = self.convert_unary_operator(op)?;
                Ok(Expression::Unary { operator, operand })
            }

            _ => Err(DBError::Planner(format!("不支持的表达式: {:?}", expr))),
        }
    }

    /// 分析条件
    pub fn analyze_condition(&self, expr: &ast::Expr) -> Result<Condition> {
        match expr {
            ast::Expr::IsNull(inner_expr) => {
                let expr = self.convert_expr(inner_expr)?;
                Ok(Condition::IsNull(expr))
            }

            ast::Expr::IsNotNull(inner_expr) => {
                let expr = self.convert_expr(inner_expr)?;
                Ok(Condition::IsNotNull(expr))
            }

            ast::Expr::Value(value) => {
                if let ast::Value::Boolean(b) = &value.value {
                    Ok(Condition::Constant(*b))
                } else {
                    let expr = self.convert_expr(expr)?;
                    Ok(Condition::Expression(expr))
                }
            }

            _ => {
                let expr = self.convert_expr(expr)?;
                Ok(Condition::Expression(expr))
            }
        }
    }

    // ====== 辅助方法 ======

    fn convert_binary_operator(&self, op: &ast::BinaryOperator) -> Result<BinaryOperator> {
        match op {
            ast::BinaryOperator::Plus => Ok(BinaryOperator::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOperator::Subtract),
            ast::BinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
            ast::BinaryOperator::Divide => Ok(BinaryOperator::Divide),
            ast::BinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
            ast::BinaryOperator::Eq => Ok(BinaryOperator::Equal),
            ast::BinaryOperator::NotEq => Ok(BinaryOperator::NotEqual),
            ast::BinaryOperator::Lt => Ok(BinaryOperator::LessThan),
            ast::BinaryOperator::LtEq => Ok(BinaryOperator::LessThanOrEqual),
            ast::BinaryOperator::Gt => Ok(BinaryOperator::GreaterThan),
            ast::BinaryOperator::GtEq => Ok(BinaryOperator::GreaterThanOrEqual),
            ast::BinaryOperator::And => Ok(BinaryOperator::And),
            ast::BinaryOperator::Or => Ok(BinaryOperator::Or),
            _ => Err(DBError::Planner(format!("不支持的二元操作符: {:?}", op))),
        }
    }

    fn convert_unary_operator(&self, op: &ast::UnaryOperator) -> Result<UnaryOperator> {
        match op {
            ast::UnaryOperator::Not => Ok(UnaryOperator::Not),
            ast::UnaryOperator::Minus => Ok(UnaryOperator::Minus),
            ast::UnaryOperator::Plus => Ok(UnaryOperator::Plus),
            _ => Err(DBError::Planner(format!("不支持的一元操作符: {:?}", op))),
        }
    }

    fn convert_ast_value(&self, value: &ast::Value) -> Result<Value> {
        match value {
            ast::Value::Number(n, _) => {
                if n.contains('.') {
                    Ok(Value::Float(n.parse().map_err(|e| {
                        DBError::Planner(format!("无法解析浮点数: {}", e))
                    })?))
                } else {
                    let parsed_int: i64 = n
                        .parse()
                        .map_err(|e| DBError::Planner(format!("无法解析整数: {}", e)))?;

                    if parsed_int > i32::MAX as i64 || parsed_int < i32::MIN as i64 {
                        return Err(DBError::Planner("整数超出i32范围".to_string()));
                    }

                    Ok(Value::Int(parsed_int as i32))
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                Ok(Value::String(s.clone()))
            }
            ast::Value::Boolean(b) => Ok(Value::Boolean(*b)),
            ast::Value::Null => Ok(Value::Null),
            _ => Err(DBError::Planner(format!("不支持的值类型: {:?}", value))),
        }
    }

    pub fn analyze_expr_to_value(&self, expr: &ast::Expr) -> Result<Value> {
        // 这个方法可以简化为直接转换表达式然后求值
        match expr {
            ast::Expr::Value(value) => self.convert_ast_value(&value.value),
            ast::Expr::BinaryOp { left, op, right } => {
                let left_value = self.analyze_expr_to_value(left)?;
                let right_value = self.analyze_expr_to_value(right)?;

                // 这里可以直接进行计算，或者构建表达式然后求值
                match op {
                    ast::BinaryOperator::Plus => left_value.add(&right_value),
                    ast::BinaryOperator::Minus => left_value.subtract(&right_value),
                    ast::BinaryOperator::Multiply => left_value.multiply(&right_value),
                    ast::BinaryOperator::Divide => left_value.divide(&right_value),
                    ast::BinaryOperator::Modulo => left_value.modulo(&right_value),
                    _ => Err(DBError::Planner(format!("不支持的二元操作符: {:?}", op))),
                }
            }
            _ => Err(DBError::Planner(format!("不支持的表达式: {:?}", expr))),
        }
    }

    fn plan_insert(&self, insert: &ast::Insert) -> Result<Plan> {
        let table_name = match &insert.table {
            ast::TableObject::TableName(name) => name.to_string(),
            _ => return Err(DBError::Parse("仅支持简单表引用".to_string())),
        };

        // 获取列名（如果 SQL 中指定了列名）
        let columns: Vec<String> = if insert.columns.is_empty() {
            Vec::new()
        } else {
            insert.columns.iter().map(|col| col.to_string()).collect()
        };

        // 解析行数据
        let mut rows = Vec::new();
        if let Some(ast::SetExpr::Values(values_list)) = &insert.source.as_ref().map(|s| &*s.body) {
            for row in &values_list.rows {
                let mut row_values = Vec::new();
                for expr in row {
                    let value = self.analyze_expr_to_value(expr)?;
                    row_values.push(value);
                }

                // 验证值的数量与列数是否匹配
                if !columns.is_empty() {
                    if row_values.len() != columns.len() {
                        return Err(DBError::Parse(format!(
                            "数量({})与指定列数({})不匹配",
                            row_values.len(),
                            columns.len()
                        )));
                    }
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

    /// 解析列定义
    pub fn analyze_column_definitions(&self, cols: &[ast::ColumnDef]) -> Result<Vec<ColumnDef>> {
        let mut columns = Vec::with_capacity(cols.len());

        for col in cols {
            let name = col.name.to_string();

            let data_type = match col.data_type {
                ast::DataType::Int(size) | ast::DataType::Integer(size) => {
                    DataType::Int(size.unwrap_or(64))
                }
                ast::DataType::Varchar(lenth) => match lenth {
                    Some(ast::CharacterLength::IntegerLength { length, .. }) => {
                        DataType::Varchar(length)
                    }
                    None | Some(ast::CharacterLength::Max) => DataType::Varchar(u64::MAX),
                },
                _ => return Err(DBError::Planner(format!("不支持的列类型: {:?}", col))),
            };

            let mut not_null = false;
            let mut unique = false;
            let mut my_is_primaty = false;

            for constraint in &col.options {
                match constraint.option {
                    ast::ColumnOption::NotNull => {
                        not_null = true;
                    }
                    ast::ColumnOption::Unique { is_primary, .. } => {
                        unique = true;
                        my_is_primaty = is_primary;
                        not_null = is_primary;
                    }
                    _ => {
                        return Err(DBError::Planner(format!(
                            "不支持的列选项: {:?}",
                            constraint
                        )));
                    }
                }
            }
            columns.push(ColumnDef {
                name,
                data_type,
                not_null,
                unique,
                is_primary: my_is_primaty,
            });
        }

        Ok(columns)
    }

    fn extract_table_name(&self, from: &[ast::TableWithJoins]) -> Result<String> {
        if from.len() != 1 {
            return Err(DBError::Planner("仅支持单表查询".to_string()));
        }

        match &from[0].relation {
            ast::TableFactor::Table { name, .. } => Ok(name.to_string()),
            _ => Err(DBError::Planner("仅支持简单表引用".to_string())),
        }
    }
    /// 解析 ORDER BY 子句
    fn analyze_order_by(&self, order_by: &[ast::OrderByExpr]) -> Result<Vec<OrderByItem>> {
        let mut items = Vec::new();

        for order_expr in order_by {
            let column = match &order_expr.expr {
                ast::Expr::Identifier(ident) => ident.value.clone(),
                ast::Expr::CompoundIdentifier(parts) => {
                    if parts.len() == 1 {
                        parts[0].value.clone()
                    } else {
                        return Err(DBError::Planner("ORDER BY 暂不支持复合标识符".to_string()));
                    }
                }
                _ => {
                    return Err(DBError::Planner(
                        "ORDER BY 暂不支持表达式，仅支持列名".to_string(),
                    ));
                }
            };

            // 在 sqlparser 0.56.0 中，使用 options.asc
            let direction = match order_expr.options.asc {
                Some(true) | None => SortDirection::Asc, // 默认为升序
                Some(false) => SortDirection::Desc,
            };

            items.push(OrderByItem { column, direction });
        }

        Ok(items)
    }
}

// ====== 为 Expression 和 Condition 实现 evaluate 方法 ======

impl Expression {
    /// 评估表达式的值
    pub fn evaluate(&self, record: &Record, columns: &[ColumnDef]) -> Result<Value> {
        match self {
            Expression::Column(column_name) => {
                let column_idx = columns
                    .iter()
                    .position(|col| &col.name == column_name)
                    .ok_or_else(|| DBError::Planner(format!("列 '{}' 不存在", column_name)))?;

                Ok(record.values()[column_idx].clone())
            }

            Expression::Value(value) => Ok(value.clone()),

            Expression::Binary {
                left,
                operator,
                right,
            } => {
                let left_val = left.evaluate(record, columns)?;
                let right_val = right.evaluate(record, columns)?;

                match operator {
                    // 算术操作
                    BinaryOperator::Add => left_val.add(&right_val),
                    BinaryOperator::Subtract => left_val.subtract(&right_val),
                    BinaryOperator::Multiply => left_val.multiply(&right_val),
                    BinaryOperator::Divide => left_val.divide(&right_val),
                    BinaryOperator::Modulo => left_val.modulo(&right_val),

                    // 比较操作（返回布尔值）
                    BinaryOperator::Equal => Ok(Value::Boolean(left_val.eq(&right_val)?)),
                    BinaryOperator::NotEqual => Ok(Value::Boolean(left_val.ne(&right_val)?)),
                    BinaryOperator::LessThan => Ok(Value::Boolean(left_val.lt(&right_val)?)),
                    BinaryOperator::LessThanOrEqual => Ok(Value::Boolean(left_val.le(&right_val)?)),
                    BinaryOperator::GreaterThan => Ok(Value::Boolean(left_val.gt(&right_val)?)),
                    BinaryOperator::GreaterThanOrEqual => {
                        Ok(Value::Boolean(left_val.ge(&right_val)?))
                    }

                    // 逻辑操作
                    BinaryOperator::And => match (left_val, right_val) {
                        (Value::Boolean(l), Value::Boolean(r)) => Ok(Value::Boolean(l && r)),
                        _ => Err(DBError::Execution("AND 操作需要布尔值".to_string())),
                    },
                    BinaryOperator::Or => match (left_val, right_val) {
                        (Value::Boolean(l), Value::Boolean(r)) => Ok(Value::Boolean(l || r)),
                        _ => Err(DBError::Execution("OR 操作需要布尔值".to_string())),
                    },
                }
            }

            Expression::Unary { operator, operand } => {
                let val = operand.evaluate(record, columns)?;

                match operator {
                    UnaryOperator::Not => {
                        if let Value::Boolean(b) = val {
                            Ok(Value::Boolean(!b))
                        } else {
                            Err(DBError::Execution("NOT 操作需要布尔值".to_string()))
                        }
                    }
                    UnaryOperator::Minus => val.negate(),
                    UnaryOperator::Plus => Ok(val), // 正号不改变值
                }
            }
        }
    }
}

impl Condition {
    /// 创建一个"总是真"的条件
    pub fn always_true() -> Self {
        Condition::Constant(true)
    }

    /// 创建一个"总是假"的条件
    pub fn always_false() -> Self {
        Condition::Constant(false)
    }

    pub fn evaluate(&self, record: &Record, columns: &[ColumnDef]) -> Result<bool> {
        match self {
            Condition::Expression(expr) => {
                let result = expr.evaluate(record, columns)?;
                match result {
                    Value::Boolean(b) => Ok(b),
                    _ => Err(DBError::Execution("条件表达式必须返回布尔值".to_string())),
                }
            }
            // ... 其他分支的实现
            _ => todo!("完整实现"),
        }
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
                if let Expression::Binary { operator, .. } = &items[0].expr {
                    assert_eq!(*operator, BinaryOperator::Multiply);
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

                if let Expression::Value(value) = &items[0].expr {
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
                if let Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 第二列：表达式
                if let Expression::Binary { operator, .. } = &items[1].expr {
                    assert_eq!(*operator, BinaryOperator::Multiply);
                }

                // 第三列：常量
                if let Expression::Value(value) = &items[2].expr {
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
                if let Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let Expression::Column(col) = &items[1].expr {
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
                if let Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let Expression::Column(col) = &items[1].expr {
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
                Condition::Expression(expr) => {
                    // 验证表达式是二元操作
                    if let Expression::Binary {
                        left,
                        operator,
                        right,
                    } = expr
                    {
                        // 验证左操作数是列名 "left_num"
                        if let Expression::Column(column_name) = &*left {
                            assert_eq!(column_name, "left_num");
                        } else {
                            panic!("预期左操作数是列名");
                        }

                        // 验证操作符是 ">"
                        assert_eq!(operator, BinaryOperator::GreaterThan);

                        // 验证右操作数是值 10
                        if let Expression::Value(value) = &*right {
                            assert_eq!(*value, Value::Int(10));
                        } else {
                            panic!("预期右操作数是整数值 10");
                        }
                    } else {
                        panic!("预期生成二元比较表达式");
                    }
                }
                Condition::IsNull(_) => panic!("预期生成表达式条件，而不是 IS NULL"),
                Condition::IsNotNull(_) => {
                    panic!("预期生成表达式条件，而不是 IS NOT NULL")
                }
                Condition::Constant(_) => panic!("预期生成表达式条件，而不是常量条件"),
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
                if let Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试复杂条件：age > 18 AND name = 'Alice'
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            match condition {
                Condition::Expression(expr) => {
                    if let Expression::Binary {
                        left,
                        operator,
                        right,
                    } = expr
                    {
                        assert_eq!(operator, BinaryOperator::And);

                        // 验证左边条件：age > 18
                        if let Expression::Binary {
                            left: age_left,
                            operator: age_op,
                            right: age_right,
                        } = &*left
                        {
                            if let Expression::Column(col) = &**age_left {
                                assert_eq!(col, "age");
                            }
                            assert_eq!(*age_op, BinaryOperator::GreaterThan);
                            if let Expression::Value(val) = &**age_right {
                                assert_eq!(*val, Value::Int(18));
                            }
                        }

                        // 验证右边条件：name = 'Alice'
                        if let Expression::Binary {
                            left: name_left,
                            operator: name_op,
                            right: name_right,
                        } = &*right
                        {
                            if let Expression::Column(col) = &**name_left {
                                assert_eq!(col, "name");
                            }
                            assert_eq!(*name_op, BinaryOperator::Equal);
                            if let Expression::Value(val) = &**name_right {
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
                if let Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试 IS NULL 条件
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            match condition {
                Condition::IsNull(expr) => {
                    if let Expression::Column(column_name) = expr {
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
                if let Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试 IS NOT NULL 条件
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            match condition {
                Condition::IsNotNull(expr) => {
                    if let Expression::Column(column_name) = expr {
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
                if let Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let Expression::Column(col) = &items[1].expr {
                    assert_eq!(col, "name");
                }
            } else {
                panic!("预期具体列选择");
            }

            // 测试常量条件
            assert!(conditions.is_some());
            let condition = conditions.unwrap();

            match condition {
                Condition::Constant(val) => {
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
                if let Expression::Column(col) = &items[0].expr {
                    assert_eq!(col, "id");
                }

                // 验证第二列：name
                assert_eq!(items[1].original_text, "name");
                if let Expression::Column(col) = &items[1].expr {
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
                if let Expression::Column(col) = &items[0].expr {
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
        let sql = "SELECT id * price * 2, name AS user_name FROM books_test12;";
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
                assert_eq!(original_text, "id * price * 2");

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

    #[test]
    fn test_insert_with_columns() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Insert {
            table_name,
            columns,
            rows,
        } = plan
        {
            assert_eq!(table_name, "users");
            assert_eq!(columns, vec!["id", "name"]);
            assert_eq!(rows.len(), 2);

            // 第一行
            assert_eq!(rows[0].len(), 2);
            assert_eq!(rows[0][0], Value::Int(1));
            assert_eq!(rows[0][1], Value::String("Alice".to_string()));

            // 第二行
            assert_eq!(rows[1].len(), 2);
            assert_eq!(rows[1][0], Value::Int(2));
            assert_eq!(rows[1][1], Value::String("Bob".to_string()));
        } else {
            panic!("预期生成Insert查询计划");
        }
    }

    #[test]
    fn test_insert_without_columns() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30);";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let Plan::Insert {
            table_name,
            columns,
            rows,
        } = plan
        {
            assert_eq!(table_name, "users");
            assert!(columns.is_empty()); // 无列名
            assert_eq!(rows.len(), 2);

            // 第一行
            assert_eq!(rows[0].len(), 3);
            assert_eq!(rows[0][0], Value::Int(1));
            assert_eq!(rows[0][1], Value::String("Alice".to_string()));
            assert_eq!(rows[0][2], Value::Int(25));

            // 第二行
            assert_eq!(rows[1].len(), 3);
            assert_eq!(rows[1][0], Value::Int(2));
            assert_eq!(rows[1][1], Value::String("Bob".to_string()));
            assert_eq!(rows[1][2], Value::Int(30));
        } else {
            panic!("预期生成Insert查询计划");
        }
    }

    #[test]
    fn test_insert_column_value_mismatch() {
        let dialect = sqlparser::dialect::MySqlDialect {};
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice', 25);"; // 3个值但只有2列
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = Planner::new();
        let result = planner.plan(&ast[0]);

        assert!(result.is_err());
    }
}
