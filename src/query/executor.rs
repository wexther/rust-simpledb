use crate::error::{DBError, Result};
use crate::query::planner::QueryPlan;
use crate::query::result::{QueryResult, ResultSet};
use crate::storage::StorageEngine;
use crate::storage::table::Value;

/// 统一SQL执行器，处理所有类型的SQL操作
pub struct Executor<'a> {
    storage: &'a mut StorageEngine,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a mut StorageEngine) -> Self {
        Self { storage }
    }

    pub fn execute(&mut self, plan: QueryPlan) -> Result<QueryResult> {
        match &plan {
            QueryPlan::CreateTable { name, columns } => {
                                match self.storage.create_table(name.clone(), columns.to_vec()) {
                                    Ok(_) => Ok(QueryResult::Success),
                                    Err(e) => Err(DBError::Schema(e.to_string())),
                                }
                            }
            QueryPlan::DropTable { name } => match self.storage.drop_table(&name) {
                                Ok(_) => Ok(QueryResult::Success),
                                Err(e) => Err(DBError::Schema(e.to_string())),
                            },
            QueryPlan::Insert { table_name, values } => {
                                // 对于values中的每一个record
                                for record in values {
                                    // 获取table的列定义
                                    let columnDefs = self.storage.get_table_columns(table_name)?;
                                    // 检查record的长度是否与列定义匹配
                                    if record.len() != columnDefs.len() {
                                        return Err(DBError::Schema(format!(
                                            "插入记录的列数与表 '{}' 的列定义不匹配",
                                            table_name
                                        )));
                                    }
                                    // 根据columnDefs中的名称，将record构造成合理的vec<Value>
                                    // 新建一个Vec<Value>来存储转换后的值
                                    let mut values: Vec<Value> = Vec::with_capacity(record.len());
                                    // 遍历columnDefs
                                    for (i, column_def) in columnDefs.iter().enumerate() {
                                        // 获取这个column_def的名称
                                        let column_name = &column_def.name;
                                        // 获取record中对应的值
                                        let value = record.get(i).ok_or_else(|| {
                                            DBError::Schema(format!(
                                                "记录中缺少列 '{}' 的值",
                                                column_name
                                            ))
                                        })?;
                                        let value_content = value.1.clone();
                                        // 将值转换为Value类型
                                        let new_value = match value_content {
                                            Value::Int(v) => Value::Int(v),
                                            Value::Float(v) => Value::Float(v),
                                            Value::String(v) => Value::String(v.clone()),
                                            Value::Boolean(v) => Value::Boolean(v),
                                            _ => return Err(DBError::Schema("不支持的值类型".to_string())),
                                        };
                                        // 在values中插入这个value
                                        values.push(new_value);
                                    }
                                    // 插入到表中
                                    self.storage.insert_record(table_name, values)?;
                                }
                                Ok(QueryResult::Success)
                            }
            QueryPlan::Update {
                                table_name,
                                set_pairs,
                                conditions,
                            } => {
                                todo!() // 更新操作的实现
                            }
            QueryPlan::Delete {
                                table_name,
                                conditions,
                            } => {
                                todo!() // 删除操作的实现
                            }
            QueryPlan::Select {
                                table_name, 
                                columns, 
                                conditions
                            } => {
                                // 获取表的列定义
                                let table_columns = self.storage.get_table_columns(table_name)?;
                                // 获取所有记录
                                let records = self.storage.get_all_records(table_name)?;

                                // 新建结果集
                                let mut result_set = ResultSet {
                                    columns: table_columns.iter().map(|c| c.name.clone()).collect(),
                                    rows: Vec::new(),
                                };

                                // 遍历所有记录
                                for record in records {
                                    // 检查条件是否满足
                                    // todo
                                }

                                Ok(QueryResult::ResultSet(result_set))            
                            }
            QueryPlan::CreateDatabase { name } => {
                        match self.storage.create_database(name.clone()) {
                            Ok(_) => Ok(QueryResult::Success),
                            Err(e) => Err(DBError::Schema(e.to_string())),
                        }
                    }
            QueryPlan::DropDatabase { name } => {
                        match self.storage.drop_database(name) {
                            Ok(_) => Ok(QueryResult::Success),
                            Err(e) => Err(DBError::Schema(e.to_string())),
                        }
                    }
            QueryPlan::UseDatabase { name } => {
                        match self.storage.use_database(name) {
                            Ok(_) => Ok(QueryResult::Success),
                            Err(e) => Err(DBError::Schema(e.to_string())),
                        }
                    }
            QueryPlan::ShowDatabases => todo!(),
            QueryPlan::ShowTables => todo!(),
            QueryPlan::ExpressionSelect { expressions } => todo!(),
        }
    }
}
