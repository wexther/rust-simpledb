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
    ExpressionSelect {
        expressions: Vec<(String, Value)>,
    },
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
                if delete.tables.len() != 1 {
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

    fn plan_query(&self, query: &Box<ast::Query>) -> Result<QueryPlan> {
        self.analyzer.analyze_select(query)
    }

    fn plan_create_table(&self, create_table: &ast::CreateTable) -> Result<QueryPlan> {
        Ok(QueryPlan::CreateTable {
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
    ) -> Result<QueryPlan> {
        match object_type {
            ast::ObjectType::Table => {
                if let Some(name) = names.first() {
                    Ok(QueryPlan::DropTable {
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

    fn plan_insert(&self, insert: &ast::Insert) -> Result<QueryPlan> {
        todo!();
        // let table_name = match &insert.table {
        //     ast::TableWithJoins { relation, .. } => match relation {
        //         ast::TableFactor::Table { name, .. } => name.to_string(),
        //         _ => return Err(DBError::Parse("仅支持简单表引用".to_string())),
        //     },
        // };

        // let mut values = Vec::new();
        // for row in &insert.rows {
        //     let mut row_values = Vec::new();
        //     for (i, value) in row.iter().enumerate() {
        //         let column_name = if let Some(col) = &insert.columns.get(i) {
        //             col.to_string()
        //         } else {
        //             return Err(DBError::Parse("插入值与列不匹配".to_string()));
        //         };
        //         let val = self.analyzer.analyze_expr_to_value(value)?;
        //         row_values.push((column_name, val));
        //     }
        //     values.push(row_values);
        // }

        // Ok(QueryPlan::Insert {
        //     table_name,
        //     values,
        // })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::table::DataType;

    use super::*;

    #[test]
    fn test_create_table_plan() {
        let dialect = sqlparser::dialect::GenericDialect {};
        let sql = "CREATE TABLE users (
    id INT(32) PRIMARY KEY,
    name VARCHAR(100),
    left_num INT(32),
    discription VARCHAR(150),
    price INT NOT NULL NOT NULL,
    time INTEGER
);";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let QueryPlan::CreateTable { name, columns } = plan {
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
        let dialect = sqlparser::dialect::GenericDialect {};
        let sql = "DROP TABLE users;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let QueryPlan::DropTable { name } = plan {
            assert_eq!(name, "users");
        } else {
            panic!("预期生成DropTable查询计划");
        }
    }

    #[test]
    fn test_select_plan() {
        let dialect = sqlparser::dialect::GenericDialect {};
        let sql = "SELECT id, name FROM users WHERE left_num > 10;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let QueryPlan::Select {
            table_name,
            columns,
            conditions,
        } = plan
        {
            assert_eq!(table_name, "users");
            assert_eq!(columns, vec!["id", "name"]);
            assert!(conditions.is_some());
        } else {
            panic!("预期生成Select查询计划");
        }
    }

    #[test]
    fn test_select_expression_plan_1() {
        let dialect = sqlparser::dialect::GenericDialect {};
        let sql = "SELECT 1 * 2;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast[0]).map_err(| e | {
            DBError::Planner(format!("查询计划生成失败: {}", e));
        }).unwrap();


        if let QueryPlan::ExpressionSelect { expressions } = plan {
            assert_eq!(expressions.len(), 1);
            assert_eq!(expressions[0].0, "1 * 2");
            assert_eq!(expressions[0].1, crate::storage::table::Value::Int(2));
        } else {
            panic!("预期生成ExpressionSelect查询计划");
        }
    }

    #[test]
    fn test_select_expression_plan_2() {
        let dialect = sqlparser::dialect::GenericDialect {};
        let sql = "SELECT 1300;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let QueryPlan::ExpressionSelect { expressions } = plan {
            assert_eq!(expressions.len(), 1);
            println!("{:#?}", expressions[0]);
            assert_eq!(expressions[0].1, crate::storage::table::Value::Int(1300));
            assert_eq!(expressions[0].0, "1300");
        } else {
            panic!("预期生成ExpressionSelect查询计划");
        }
    }

    #[test]
    fn test_select_expression_plan_3() {
        let dialect = sqlparser::dialect::GenericDialect {};
        let sql = "SELECT 13.12;";
        let ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast[0]).unwrap();

        if let QueryPlan::ExpressionSelect { expressions } = plan {
            assert_eq!(expressions.len(), 1);
            println!("{:#?}", expressions[0]);
            assert_eq!(expressions[0].1, crate::storage::table::Value::Float(13.12));
            assert_eq!(expressions[0].0, "13.12");
        } else {
            panic!("预期生成ExpressionSelect查询计划");
        }
    }
}
