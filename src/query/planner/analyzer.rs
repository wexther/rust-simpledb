use crate::error::{DBError, Result};
use crate::query::planner::QueryPlan;
use crate::storage::table::{ColumnDef, DataType, Record, Table, Value};
use sqlparser::ast;
use std::fmt;

use super::OrderByItem;

/// 表示表达式的枚举
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    /// 列引用
    Column(String),
    /// 常量值
    Value(Value),
    /// 二元操作
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
    /// 一元操作
    Unary {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
}

/// 二元操作符
#[derive(Clone, Debug, PartialEq)]
pub enum BinaryOperator {
    // 算术操作符
    Add,      // +
    Subtract, // -
    Multiply, // *
    Divide,   // /
    Modulo,   // %

    // 比较操作符
    Equal,              // =
    NotEqual,           // <> or !=
    LessThan,           // <
    LessThanOrEqual,    // <=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=

    // 逻辑操作符
    And, // AND
    Or,  // OR
}

/// 一元操作符
#[derive(Clone, Debug, PartialEq)]
pub enum UnaryOperator {
    Not,   // NOT
    Minus, // -
    Plus,  // +
}

/// 表示查询条件的结构
#[derive(Clone, Debug, PartialEq)]
pub enum Condition {
    /// 表达式条件（表达式的结果必须是布尔值）
    Expression(Expression),

    /// IS NULL 条件
    IsNull(Expression),

    /// IS NOT NULL 条件
    IsNotNull(Expression),

    /// 常量条件（true/false）
    Constant(bool),
}

/// 比较操作符
#[derive(Clone, Debug, PartialEq)]
pub enum CompareOperator {
    Eq,    // 等于
    NotEq, // 不等于
    Lt,    // 小于
    LtEq,  // 小于等于
    Gt,    // 大于
    GtEq,  // 大于等于
    In,    // 在集合中
}

/// 逻辑操作符
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
}

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

    /// 评估条件是否满足
    pub fn evaluate(&self, record: &Record, columns: &[ColumnDef]) -> Result<bool> {
        match self {
            Condition::Expression(expr) => {
                let result = expr.evaluate(record, columns)?;
                match result {
                    Value::Boolean(b) => Ok(b),
                    _ => Err(DBError::Execution("条件表达式必须返回布尔值".to_string())),
                }
            }

            Condition::IsNull(expr) => {
                let val = expr.evaluate(record, columns)?;
                Ok(val.is_null())
            }

            Condition::IsNotNull(expr) => {
                let val = expr.evaluate(record, columns)?;
                Ok(!val.is_null())
            }

            Condition::Constant(val) => Ok(*val),
        }
    }
}

/// 查询分析器 - 负责解析SQL AST并转换为内部结构
pub struct QueryAnalyzer;

impl QueryAnalyzer {
    /// 从SQL AST表达式转换为内部表达式
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

    /// 从SQL AST条件转换为内部条件
    pub fn analyze_condition(expr: &ast::Expr) -> Result<Condition> {
        let analyzer = QueryAnalyzer::new();

        match expr {
            // IS NULL
            ast::Expr::IsNull(inner_expr) => {
                let expr = analyzer.convert_expr(inner_expr)?;
                Ok(Condition::IsNull(expr))
            }

            // IS NOT NULL
            ast::Expr::IsNotNull(inner_expr) => {
                let expr = analyzer.convert_expr(inner_expr)?;
                Ok(Condition::IsNotNull(expr))
            }

            // 布尔常量
            ast::Expr::Value(value) => {
                if let ast::Value::Boolean(b) = &value.value {
                    Ok(Condition::Constant(*b))
                } else {
                    // 其他值转换为表达式
                    let expr = analyzer.convert_expr(expr)?;
                    Ok(Condition::Expression(expr))
                }
            }

            // 其他表达式（包括比较和逻辑操作）
            _ => {
                let expr = analyzer.convert_expr(expr)?;
                Ok(Condition::Expression(expr))
            }
        }
    }

    /// 转换二元操作符
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

    /// 转换一元操作符
    fn convert_unary_operator(&self, op: &ast::UnaryOperator) -> Result<UnaryOperator> {
        match op {
            ast::UnaryOperator::Not => Ok(UnaryOperator::Not),
            ast::UnaryOperator::Minus => Ok(UnaryOperator::Minus),
            ast::UnaryOperator::Plus => Ok(UnaryOperator::Plus),
            _ => Err(DBError::Planner(format!("不支持的一元操作符: {:?}", op))),
        }
    }

    /// 转换 AST 值到内部值
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

    pub fn new() -> Self {
        Self
    }

    /// 计算表达式的值，需要表信息
    pub fn evaluate_expr(
        expr: &ast::Expr,
        record: &Record,
        columns: &[ColumnDef],
    ) -> Result<Value> {
        match expr {
            ast::Expr::Identifier(ident) => {
                // 从记录中获取列值
                let column_name = ident.value.clone();
                let column_idx = columns
                    .iter()
                    .position(|col| col.name == column_name)
                    .ok_or_else(|| DBError::Planner(format!("列 '{}' 不存在", column_name)))?;

                Ok(record.values()[column_idx].clone())
            }

            ast::Expr::Value(value_with_span) => {
                // 转换SQL值到我们的Value类型
                match &value_with_span.value {
                    ast::Value::Number(n, _) => {
                        if n.contains('.') {
                            Ok(Value::Float(n.parse().map_err(|e| {
                                DBError::Planner(format!("无法解析浮点数: {}", e))
                            })?))
                        } else {
                            let parsed_int: i32 = n
                                .parse()
                                .map_err(|e| DBError::Planner(format!("无法解析整数: {}", e)))?;

                            Ok(Value::Int(parsed_int))
                        }
                    }
                    ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                        Ok(Value::String(s.clone()))
                    }
                    ast::Value::Boolean(b) => Ok(Value::Boolean(*b)),
                    ast::Value::Null => Ok(Value::Null),
                    _ => Err(DBError::Planner(format!(
                        "不支持的常量值: {:?}",
                        value_with_span
                    ))),
                }
            }

            // 其他表达式类型...
            _ => Err(DBError::Planner(format!("不支持的表达式: {:?}", expr))),
        }
    }

    /// 将SQL表达式转换为值
    pub fn analyze_expr_to_value(&self, expr: &ast::Expr) -> Result<Value> {
        println!("Analyzing expression: \n{:#?}", expr);
        match expr {
            ast::Expr::Value(value) => match &value.value {
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
            },
            ast::Expr::BinaryOp { left, op, right } => {
                let left_value = self.analyze_expr_to_value(left)?;
                let right_value = self.analyze_expr_to_value(right)?;

                match op {
                    ast::BinaryOperator::Plus => {
                        if let (Value::Int(l), Value::Int(r)) = (left_value, right_value) {
                            Ok(Value::Int(l + r))
                        } else {
                            Err(DBError::Planner("加法操作仅支持整数".to_string()))
                        }
                    }
                    ast::BinaryOperator::Minus => {
                        if let (Value::Int(l), Value::Int(r)) = (left_value, right_value) {
                            Ok(Value::Int(l - r))
                        } else {
                            Err(DBError::Planner("减法操作仅支持整数".to_string()))
                        }
                    }
                    ast::BinaryOperator::Multiply => {
                        if let (Value::Int(l), Value::Int(r)) = (left_value, right_value) {
                            Ok(Value::Int(l * r))
                        } else {
                            Err(DBError::Planner("乘法操作仅支持整数".to_string()))
                        }
                    }
                    ast::BinaryOperator::Divide => {
                        if let (Value::Int(l), Value::Int(r)) = (left_value, right_value) {
                            if r == 0 {
                                return Err(DBError::Planner("除数不能为零".to_string()));
                            }
                            Ok(Value::Int(l / r))
                        } else {
                            Err(DBError::Planner("除法操作仅支持整数".to_string()))
                        }
                    }
                    ast::BinaryOperator::Modulo => {
                        if let (Value::Int(l), Value::Int(r)) = (left_value, right_value) {
                            if r == 0 {
                                return Err(DBError::Planner("模数不能为零".to_string()));
                            }
                            Ok(Value::Int(l % r))
                        } else {
                            Err(DBError::Planner("模运算仅支持整数".to_string()))
                        }
                    }
                    _ => Err(DBError::Planner(format!("不支持的二元操作符: {:?}", op))),
                }
            }
            _ => Err(DBError::Planner(format!("不支持的表达式: {:?}", expr))),
        }
    }

    pub fn analyze_expr_to_string(&self, expr: &ast::Expr) -> Result<String> {
        match expr {
            ast::Expr::Identifier(ident) => Ok(ident.value.clone()),
            ast::Expr::Value(value) => match &value.value {
                ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                    Ok(s.clone())
                }
                ast::Value::Number(n, _) => Ok(n.clone()),
                _ => Err(DBError::Planner(format!("不支持的值类型: {:?}", value))),
            },
            ast::Expr::BinaryOp { left, op, right } => {
                let left_str = self.analyze_expr_to_string(left)?;
                let right_str = self.analyze_expr_to_string(right)?;
                let op_str = match op {
                    ast::BinaryOperator::Plus => "+",
                    ast::BinaryOperator::Minus => "-",
                    ast::BinaryOperator::Multiply => "*",
                    ast::BinaryOperator::Divide => "/",
                    ast::BinaryOperator::Modulo => "%",
                    _ => return Err(DBError::Planner(format!("不支持的操作符: {:?}", op))),
                };
                Ok(format!("{} {} {}", left_str, op_str, right_str))
            }
            _ => Err(DBError::Planner(format!("不支持的表达式: {:?}", expr))),
        }
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

    /// 解析INSERT语句
    pub fn analyze_insert(
        &self,
        insert: &ast::Insert,
    ) -> Result<(String, Vec<String>, Vec<Vec<(String, Value)>>)> {
        // todo!() 实现保持不变
        todo!();
    }

    pub fn analyze_select(&self, query: &ast::Query) -> Result<QueryPlan> {
        let body = match &*query.body {
            ast::SetExpr::Select(select) => &**select,
            _ => return Err(DBError::Planner("仅支持SELECT查询".to_string())),
        };

        if body.from.is_empty() {
            // 无表表达式查询处理（保持不变）
            let mut expressions = Vec::new();
            for item in &body.projection {
                match item {
                    ast::SelectItem::UnnamedExpr(expr) => {
                        let name = format!("{}", expr);
                        let value = self.analyze_expr_to_value(expr)?;
                        expressions.push((name, value));
                    }
                    ast::SelectItem::ExprWithAlias { expr, alias } => {
                        let name = alias.to_string();
                        let value = self.analyze_expr_to_value(expr)?;
                        expressions.push((name, value));
                    }
                    _ => return Err(DBError::Planner("不支持的SELECT项类型".to_string())),
                }
            }
            return Ok(QueryPlan::ExpressionSelect { expressions });
        } else {
            // 有表的查询
            let table_name = self.extract_table_name(&body.from)?;

            // 解析选择列
            let columns = self.analyze_select_columns(&body.projection)?;

            // 解析WHERE条件
            let conditions = if let Some(selection) = &body.selection {
                Some(Self::analyze_condition(selection)?)
            } else {
                None
            };

            // 解析ORDER BY子句 - 修复的代码（针对 sqlparser 0.56.0）
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

            Ok(QueryPlan::Select {
                table_name,
                columns,
                conditions,
                order_by,
            })
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
                Some(true) | None => super::SortDirection::Asc, // 默认为升序
                Some(false) => super::SortDirection::Desc,
            };

            items.push(super::OrderByItem { column, direction });
        }

        Ok(items)
    }

    /// 解析选择列
    fn analyze_select_columns(&self, projection: &[ast::SelectItem]) -> Result<Vec<String>> {
        let mut columns = Vec::new();

        for item in projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    if let ast::Expr::Identifier(ident) = expr {
                        columns.push(ident.value.clone());
                    } else {
                        return Err(DBError::Planner(
                            "复杂表达式暂时不支持，请使用列名".to_string(),
                        ));
                    }
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    if let ast::Expr::Identifier(ident) = expr {
                        columns.push(ident.value.clone());
                    } else {
                        return Err(DBError::Planner(
                            "复杂表达式暂时不支持，请使用列名".to_string(),
                        ));
                    }
                }
                ast::SelectItem::Wildcard(_) => {
                    columns.push("*".to_string());
                }
                ast::SelectItem::QualifiedWildcard(_, _) => {
                    columns.push("*".to_string());
                }
            }
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
}
