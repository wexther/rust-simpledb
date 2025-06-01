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
                if delete.tables.len() != 1 {
                    return Err(DBError::Parse("仅支持单表删除".to_string()));
                }

                let table_name = delete.tables[0].to_string();
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
                    let value = self.analyze_expr_to_value(expr)?;
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
        // 从 analyzer.rs 移过来的实现
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
