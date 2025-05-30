use crate::error::{DBError, Result};
use crate::query::planner::QueryPlan;
use crate::storage::table::{ColumnDef, DataType, Record, Table, Value};
use sqlparser::ast;
use std::fmt;

/// 表示查询条件的结构
#[derive(Clone, Debug)]
pub enum Condition {
    // 比较操作
    Compare {
        left: Box<ast::Expr>,
        op: CompareOperator,
        right: Box<ast::Expr>,
    },
    // 逻辑操作
    Logical {
        left: Box<Condition>,
        op: LogicalOperator,
        right: Box<Condition>,
    },
    // 单操作数条件
    Unary {
        op: UnaryOperator,
        expr: Box<ast::Expr>,
    },
    // 常量条件（true/false）
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

/// 单操作数操作符
#[derive(Clone, Debug, PartialEq)]
pub enum UnaryOperator {
    Not,     // NOT
    IsNull,  // IS NULL
    NotNull, // IS NOT NULL
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
            Condition::Compare { left, op, right } => {
                let left_val = QueryAnalyzer::evaluate_expr(left, record, columns)?;
                let right_val = QueryAnalyzer::evaluate_expr(right, record, columns)?;

                match op {
                    CompareOperator::Eq => left_val.eq(&right_val),
                    CompareOperator::NotEq => left_val.ne(&right_val),
                    CompareOperator::Lt => left_val.lt(&right_val),
                    CompareOperator::LtEq => left_val.le(&right_val),
                    CompareOperator::Gt => left_val.gt(&right_val),
                    CompareOperator::GtEq => left_val.ge(&right_val),
                    CompareOperator::In => Err(DBError::Planner("IN操作符暂不支持".to_string())),
                }
            }

            Condition::Logical { left, op, right } => {
                match op {
                    LogicalOperator::And => {
                        let left_res = left.evaluate(record, columns)?;
                        if !left_res {
                            return Ok(false); // 短路计算
                        }
                        right.evaluate(record, columns)
                    }
                    LogicalOperator::Or => {
                        let left_res = left.evaluate(record, columns)?;
                        if left_res {
                            return Ok(true); // 短路计算
                        }
                        right.evaluate(record, columns)
                    }
                }
            }

            Condition::Unary { op, expr } => match op {
                UnaryOperator::IsNull => {
                    let val = QueryAnalyzer::evaluate_expr(expr, record, columns)?;
                    Ok(val.is_null())
                }
                UnaryOperator::NotNull => {
                    let val = QueryAnalyzer::evaluate_expr(expr, record, columns)?;
                    Ok(!val.is_null())
                }
                UnaryOperator::Not => {
                    let sub_cond = QueryAnalyzer::analyze_condition(expr)?;
                    let res = sub_cond.evaluate(record, columns)?;
                    Ok(!res)
                }
            },

            Condition::Constant(val) => Ok(*val),
        }
    }
}

// 为Condition实现Display特性，方便调试
impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Condition::Compare { left, op, right } => {
                write!(f, "{:?} {:?} {:?}", left, op, right)
            }
            Condition::Logical { left, op, right } => {
                write!(f, "({}) {:?} ({})", left, op, right)
            }
            Condition::Unary { op, expr } => {
                write!(f, "{:?} {:?}", op, expr)
            }
            Condition::Constant(val) => {
                write!(f, "{}", val)
            }
        }
    }
}

/// 查询分析器 - 负责解析SQL AST并转换为内部结构
pub struct QueryAnalyzer;

impl QueryAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// 从SQL AST表达式转换为条件
    pub fn analyze_condition(expr: &ast::Expr) -> Result<Condition> {
        match expr {
            ast::Expr::BinaryOp { left, op, right } => {
                match op {
                    // 比较操作符
                    ast::BinaryOperator::Eq => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::Eq,
                        right: Box::new(*right.clone()),
                    }),
                    ast::BinaryOperator::NotEq => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::NotEq,
                        right: Box::new(*right.clone()),
                    }),
                    ast::BinaryOperator::Lt => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::Lt,
                        right: Box::new(*right.clone()),
                    }),
                    ast::BinaryOperator::LtEq => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::LtEq,
                        right: Box::new(*right.clone()),
                    }),
                    ast::BinaryOperator::Gt => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::Gt,
                        right: Box::new(*right.clone()),
                    }),
                    ast::BinaryOperator::GtEq => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::GtEq,
                        right: Box::new(*right.clone()),
                    }),

                    // 逻辑操作符
                    ast::BinaryOperator::And => Ok(Condition::Logical {
                        left: Box::new(Self::analyze_condition(left)?),
                        op: LogicalOperator::And,
                        right: Box::new(Self::analyze_condition(right)?),
                    }),
                    ast::BinaryOperator::Or => Ok(Condition::Logical {
                        left: Box::new(Self::analyze_condition(left)?),
                        op: LogicalOperator::Or,
                        right: Box::new(Self::analyze_condition(right)?),
                    }),

                    _ => Err(DBError::Planner(format!("不支持的二元操作符: {:?}", op))),
                }
            }

            // 处理IS NULL和IS NOT NULL
            ast::Expr::IsNull(expr) => Ok(Condition::Unary {
                op: UnaryOperator::IsNull,
                expr: Box::new(*expr.clone()),
            }),
            ast::Expr::IsNotNull(expr) => Ok(Condition::Unary {
                op: UnaryOperator::NotNull,
                expr: Box::new(*expr.clone()),
            }),

            // 处理NOT条件
            ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Not,
                expr,
            } => Ok(Condition::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(*expr.clone()),
            }),

            // 常量boolean条件
            ast::Expr::Value(value) => {
                if let ast::Value::Boolean(b) = &value.value {
                    Ok(Condition::Constant(*b))
                } else {
                    Err(DBError::Planner(format!("不支持的常量值: {:?}", value)))
                }
            }

            // 其他情况，比如单个标识符，可能需要特殊处理
            _ => Err(DBError::Planner(format!("不支持的条件表达式: {:?}", expr))),
        }
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

    /// 解析SELECT查询
    pub fn analyze_select(&self, query: &ast::Query) -> Result<QueryPlan> {
        println!("Analyzing query: \n{:#?}", query);
        let body = match &*query.body {
            ast::SetExpr::Select(select) => &**select,
            _ => return Err(DBError::Planner("仅支持SELECT查询".to_string())),
        };

        println!("Query body: \n{:#?}", body);

        if body.from.is_empty() {
            // 无表表达式查询，即计算表达式
            let mut expressions = Vec::new();

            for item in &body.projection {
                match item {
                    ast::SelectItem::UnnamedExpr(expr) => {
                        expressions.push((
                            self.analyze_expr_to_string(expr)?,
                            self.analyze_expr_to_value(expr)?,
                        ));
                    }
                    _ => return Err(DBError::Planner("不支持的SELECT项类型".to_string())),
                }
            }

            return Ok(QueryPlan::ExpressionSelect { expressions });
        } else {
            return Err(DBError::Planner(
                "仅支持无表表达式查询，暂不支持FROM子句".to_string(),
            ));
        }
    }

    /// 解析INSERT语句
    pub fn analyze_insert(
        &self,
        insert: &ast::Insert,
    ) -> Result<(String, Vec<String>, Vec<Vec<(String, Value)>>)> {
        // todo!() 实现保持不变
        todo!();
    }
}
