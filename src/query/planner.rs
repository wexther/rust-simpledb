use sqlparser::ast;

mod analyzer;

use crate::error::{DBError, Result};
use crate::storage::record::Record;
use crate::storage::table::{ColumnDef, DataType, Table, Value};
use analyzer::{Condition, QueryAnalyzer};

/// 表示查询计划的枚举
#[derive(Debug)]
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
        values: Vec<Vec<(String, Value)>>,
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
pub struct QueryPlanner {
    analyzer: QueryAnalyzer,
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self {
            analyzer: QueryAnalyzer::new(),
        }
    }

    /// 将AST转换为查询计划
    pub fn plan(&self, stmt: &ast::Statement) -> Result<QueryPlan> {
        match stmt {
            ast::Statement::CreateTable(create_table) => {
                // 解析CREATE TABLE语句
                let table_name = create_table.name.to_string();
                let column_defs = self
                    .analyzer
                    .analyze_column_definitions(&create_table.columns)?;

                Ok(QueryPlan::CreateTable {
                    name: table_name,
                    columns: column_defs,
                })
            }
            ast::Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => match object_type {
                ast::ObjectType::Table => {
                    if let Some(name) = names.first() {
                        Ok(QueryPlan::DropTable {
                            name: name.to_string(),
                        })
                    } else {
                        Err(DBError::Parse("DROP TABLE缺少表名".to_string()))
                    }
                }
                ast::ObjectType::Schema => {
                    if let Some(name) = names.first() {
                        Ok(QueryPlan::DropDatabase {
                            name: name.to_string(),
                        })
                    } else {
                        Err(DBError::Parse("DROP DATABASE缺少数据库名".to_string()))
                    }
                }
                _ => Err(DBError::Parse(format!(
                    "不支持的DROP操作: {:?}",
                    object_type
                ))),
            },
            ast::Statement::Query(query) => {
                // 使用analyzer解析SELECT查询
                let (table_name, columns, conditions) = self.analyzer.analyze_select(query)?;

                Ok(QueryPlan::Select {
                    table_name,
                    columns,
                    conditions,
                })
            }
            ast::Statement::Insert(insert) => {
                // todo!() 使用analyzer解析INSERT语句
                todo!();
            }
            ast::Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let table_name = match table {
                    sqlparser::ast::TableWithJoins { relation, .. } => match relation {
                        ast::TableFactor::Table { name, .. } => name.to_string(),
                        _ => return Err(DBError::Parse("仅支持简单表引用".to_string())),
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
                    Some(QueryAnalyzer::analyze_condition(expr)?)
                } else {
                    None
                };

                Ok(QueryPlan::Update {
                    table_name,
                    set_pairs,
                    conditions,
                })
            }
            ast::Statement::Delete(delete) => {
                if (delete.tables.len() != 1) {
                    return Err(DBError::Parse("仅支持单表删除".to_string()));
                }
                let table_name = delete.tables[0].to_string();
                let selection = &delete.selection;

                // 解析WHERE子句
                let conditions = if let Some(expr) = selection {
                    Some(QueryAnalyzer::analyze_condition(expr)?)
                } else {
                    None
                };

                Ok(QueryPlan::Delete {
                    table_name,
                    conditions,
                })
            }
            // 处理数据库管理语句
            ast::Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => Ok(QueryPlan::CreateDatabase {
                name: schema_name.to_string(),
            }),
            ast::Statement::ShowTables { .. } => Ok(QueryPlan::ShowTables),
            ast::Statement::ShowDatabases { .. } => Ok(QueryPlan::ShowDatabases),
            _ => Err(DBError::Parse(format!("不支持的SQL语句类型: {:?}", stmt))),
        }
    }
}

#[test]
fn test_create_table_plan() {
    let dialect = sqlparser::dialect::GenericDialect {};
    let sql = "CREATE TABLE users (id INT, name VARCHAR(100))";
    let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();

    // 验证查询计划是否正确
    let planner = QueryPlanner::new();
    let plan = planner.plan(&ast[0]).unwrap();

    if let QueryPlan::CreateTable { name, columns } = plan {
        assert_eq!(name, "users");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, DataType::Int(100));
        assert_eq!(columns[1].name, "name");
        assert!(matches!(columns[1].data_type, DataType::Varchar(100)));
    } else {
        panic!("预期生成CreateTable查询计划");
    }
}
