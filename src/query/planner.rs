use crate::error::{DBError, Result};
use crate::storage::record::Record;
use crate::storage::table::{ColumnDef, Table, Value};
use sqlparser::ast::{BinaryOperator, Expr, ObjectType, Statement, Value as SqlValue};
use std::fmt;

/// 表示查询计划的枚举
pub enum QueryPlan {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    DropTable {
        name: String,
    },
    Select {
        table_name: String,
        columns: Vec<String>,
        conditions: Option<Condition>,
    },
    Insert {
        table_name: String,
        values: Vec<Vec<Value>>,
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
    // 数据库操作计划
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

/// 表示查询条件的结构
#[derive(Clone, Debug)]
pub enum Condition {
    // 比较操作
    Compare {
        left: Box<Expr>,
        op: CompareOperator,
        right: Box<Expr>,
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
        expr: Box<Expr>,
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

    /// 从SQL AST表达式转换为条件
    pub fn from_expr(expr: &Expr) -> Result<Self> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    // 比较操作符
                    BinaryOperator::Eq => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::Eq,
                        right: Box::new(*right.clone()),
                    }),
                    BinaryOperator::NotEq => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::NotEq,
                        right: Box::new(*right.clone()),
                    }),
                    BinaryOperator::Lt => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::Lt,
                        right: Box::new(*right.clone()),
                    }),
                    BinaryOperator::LtEq => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::LtEq,
                        right: Box::new(*right.clone()),
                    }),
                    BinaryOperator::Gt => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::Gt,
                        right: Box::new(*right.clone()),
                    }),
                    BinaryOperator::GtEq => Ok(Condition::Compare {
                        left: Box::new(*left.clone()),
                        op: CompareOperator::GtEq,
                        right: Box::new(*right.clone()),
                    }),

                    // 逻辑操作符
                    BinaryOperator::And => Ok(Condition::Logical {
                        left: Box::new(Condition::from_expr(left)?),
                        op: LogicalOperator::And,
                        right: Box::new(Condition::from_expr(right)?),
                    }),
                    BinaryOperator::Or => Ok(Condition::Logical {
                        left: Box::new(Condition::from_expr(left)?),
                        op: LogicalOperator::Or,
                        right: Box::new(Condition::from_expr(right)?),
                    }),

                    _ => Err(DBError::Parse(format!("不支持的二元操作符: {:?}", op))),
                }
            }

            // 处理IS NULL和IS NOT NULL
            Expr::IsNull(expr) => Ok(Condition::Unary {
                op: UnaryOperator::IsNull,
                expr: Box::new(*expr.clone()),
            }),
            Expr::IsNotNull(expr) => Ok(Condition::Unary {
                op: UnaryOperator::NotNull,
                expr: Box::new(*expr.clone()),
            }),

            // 处理NOT条件
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Not,
                expr,
            } => Ok(Condition::Unary {
                op: UnaryOperator::Not,
                expr: Box::new(*expr.clone()),
            }),

            // 常量boolean条件
            Expr::Value(value) => {
                if let SqlValue::Boolean(b) = &value.value {
                    Ok(Condition::Constant(*b))
                } else {
                    Err(DBError::Parse(format!("不支持的常量值: {:?}", value)))
                }
            }

            // 其他情况，比如单个标识符，可能需要特殊处理
            _ => Err(DBError::Parse(format!("不支持的条件表达式: {:?}", expr))),
        }
    }

    /// 计算表达式的值
    fn evaluate_expr(expr: &Expr, record: &Record, columns: &[ColumnDef]) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                // 从记录中获取列值
                let column_name = ident.value.clone();
                let column_idx = columns
                    .iter()
                    .position(|col| col.name == column_name)
                    .ok_or_else(|| DBError::Schema(format!("列 '{}' 不存在", column_name)))?;

                Ok(record.values()[column_idx].clone())
            }

            Expr::Value(value_with_span) => {
                // 转换SQL值到我们的Value类型
                match &value_with_span.value {
                    SqlValue::Number(n, _) => {
                        if n.contains('.') {
                            Ok(Value::Float(n.parse().map_err(|e| {
                                DBError::Parse(format!("无法解析浮点数: {}", e))
                            })?))
                        } else {
                            let parsed_int: i64 = n
                                .parse()
                                .map_err(|e| DBError::Parse(format!("无法解析整数: {}", e)))?;

                            // 检查i32范围
                            if parsed_int > i32::MAX as i64 || parsed_int < i32::MIN as i64 {
                                return Err(DBError::Parse("整数超出i32范围".to_string()));
                            }

                            Ok(Value::Int(parsed_int as i32))
                        }
                    }
                    SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                        Ok(Value::String(s.clone()))
                    }
                    SqlValue::Boolean(b) => Ok(Value::Boolean(*b)),
                    SqlValue::Null => Ok(Value::Null),
                    _ => Err(DBError::Parse(format!(
                        "不支持的常量值: {:?}",
                        value_with_span
                    ))),
                }
            }

            // 其他表达式类型...
            _ => Err(DBError::Parse(format!("不支持的表达式: {:?}", expr))),
        }
    }

    /// 评估条件是否满足
    pub fn evaluate(&self, record: &Record, columns: &[ColumnDef]) -> Result<bool> {
        match self {
            Condition::Compare { left, op, right } => {
                let left_val = Self::evaluate_expr(left, record, columns)?;
                let right_val = Self::evaluate_expr(right, record, columns)?;

                match op {
                    CompareOperator::Eq => left_val.eq(&right_val),
                    CompareOperator::NotEq => left_val.ne(&right_val),
                    CompareOperator::Lt => left_val.lt(&right_val),
                    CompareOperator::LtEq => left_val.le(&right_val),
                    CompareOperator::Gt => left_val.gt(&right_val),
                    CompareOperator::GtEq => left_val.ge(&right_val),
                    CompareOperator::In => Err(DBError::Parse("IN操作符暂不支持".to_string())),
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
                    let val = Self::evaluate_expr(expr, record, columns)?;
                    Ok(val.is_null())
                }
                UnaryOperator::NotNull => {
                    let val = Self::evaluate_expr(expr, record, columns)?;
                    Ok(!val.is_null())
                }
                UnaryOperator::Not => {
                    let sub_cond = Condition::from_expr(expr)?;
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

/// 查询计划生成器 - 负责将AST转换为查询计划
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn new() -> Self {
        Self
    }

    /// 将AST转换为查询计划
    pub fn plan(&self, stmt: &Statement) -> Result<QueryPlan> {
        match stmt {
            Statement::CreateTable(sqlparser::ast::CreateTable { name, columns, .. }) => {
                // 解析CREATE TABLE语句
                let table_name = name.to_string();
                let column_defs = self.parse_column_definitions(columns)?;

                Ok(QueryPlan::CreateTable {
                    name: table_name,
                    columns: column_defs,
                })
            }
            Statement::Query(query) => {
                // 解析SELECT查询
                // ...
                let _ = query;
                Ok(QueryPlan::Select {
                    table_name: "example".to_string(),
                    columns: vec!["*".to_string()],
                    conditions: None,
                })
            }
            // 数据库操作解析
            Statement::CreateSchema { schema_name, .. } => Ok(QueryPlan::CreateDatabase {
                name: schema_name.to_string(),
            }),
            Statement::Drop {
                object_type, names, ..
            } => {
                todo!();
            }
            // USE 语句可能需要自定义解析，因为sqlparser可能不直接支持
            // ...
            _ => Err(DBError::Parse(format!("不支持的SQL语句: {:?}", stmt))),
        }
    }

    fn parse_column_definitions(
        &self,
        cols: &[sqlparser::ast::ColumnDef],
    ) -> Result<Vec<ColumnDef>> {
        // 解析列定义...
        let _ = cols;
        Ok(vec![])
    }
}
