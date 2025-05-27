use crate::error::Result;
use crate::query::planner::QueryPlan;
use crate::query::result::{QueryResult, ResultSet};
use crate::storage::engine::StorageEngine;
use crate::storage::table::Value;

/// 执行器特性，定义共同的执行接口
pub trait Executor {
    fn execute(&mut self, plan: QueryPlan) -> Result<QueryResult>;
}

/// DDL执行器(Data Definition Language)
pub struct DDLExecutor<'a> {
    storage: &'a mut StorageEngine,
}

impl<'a> DDLExecutor<'a> {
    pub fn new(storage: &'a mut StorageEngine) -> Self {
        Self { storage }
    }
}

impl<'a> Executor for DDLExecutor<'a> {
    fn execute(&mut self, plan: QueryPlan) -> Result<QueryResult> {
        match plan {
            QueryPlan::CreateTable { name, columns } => {
                match self.storage.create_table(name.clone(), columns) {
                    Ok(_) => Ok(QueryResult::Success(format!("表 '{}' 创建成功", name))),
                    Err(e) => Ok(QueryResult::Error(e.to_string())),
                }
            }
            QueryPlan::DropTable { name } => match self.storage.drop_table(&name) {
                Ok(_) => Ok(QueryResult::Success(format!("表 '{}' 删除成功", name))),
                Err(e) => Ok(QueryResult::Error(e.to_string())),
            },
            _ => Ok(QueryResult::Error("不支持的DDL操作".to_string())),
        }
    }
}

/// DML执行器(Data Manipulation Language)
pub struct DMLExecutor<'a> {
    storage: &'a mut StorageEngine,
}

impl<'a> DMLExecutor<'a> {
    pub fn new(storage: &'a mut StorageEngine) -> Self {
        Self { storage }
    }
}

impl<'a> Executor for DMLExecutor<'a> {
    fn execute(&mut self, plan: QueryPlan) -> Result<QueryResult> {
        match plan {
            QueryPlan::Insert { table_name, values } => {
                // 尝试获取当前数据库
                if let Ok(current_database) = self.storage.current_database_mut() {
                    // 遍历要插入的每一行数据
                    for record in values {
                        // 使用database的代理方法插入记录，不需要直接处理buffer_manager
                        if let Err(e) = current_database.insert_record(&table_name, record) {
                            return Ok(QueryResult::Error(format!(
                                "插入数据到表 '{}' 失败: {}",
                                table_name, e
                            )));
                        }
                    }
                    return Ok(QueryResult::Success(format!(
                        "表 '{}' 插入成功",
                        table_name
                    )));
                }
                return Ok(QueryResult::Error("当前没有选择数据库".to_string()));
            }
            QueryPlan::Update {
                table_name,
                set_pairs,
                conditions,
            } => {
                // 实现更新操作
                // new code
                // 尝试获取当前数据库
                if let Ok(current_database) = self.storage.current_database_mut() {
                    // 尝试获取对应的表
                    if let Ok(table) = current_database.get_table(&table_name) {
                        // 通过condition查询到所有满足要求的数据
                        // 对于每条数据更新每一列要更新的值
                    }
                }
                // new code end
                return Ok(QueryResult::Error("更新失败".to_string()));
            }
            QueryPlan::Delete {
                table_name,
                conditions,
            } => {
                // 实现删除操作
                // new code
                // 尝试获取当前数据库
                if let Ok(current_database) = self.storage.current_database_mut() {
                    // 尝试获取对应的表
                    if let Ok(table) = current_database.get_table(&table_name) {
                        // 通过condition查询到所有满足要求的数据
                        // 删除每条数据
                    }
                }
                // new code end
                Ok(QueryResult::Success("删除成功".to_string()))
            }
            _ => Ok(QueryResult::Error("不支持的DML操作".to_string())),
        }
    }
}

/// 查询执行器
pub struct QueryExecutor<'a> {
    storage: &'a mut StorageEngine,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(storage: &'a mut StorageEngine) -> Self {
        Self { storage }
    }
}

impl<'a> Executor for QueryExecutor<'a> {
    fn execute(&mut self, plan: QueryPlan) -> Result<QueryResult> {
        match plan {
            QueryPlan::Select {table_name, columns, conditions} => {
                // 1. 首先获取表的列定义（不需要持有表的引用）
                let table_columns = match self.storage.get_table_columns(&table_name) {
                    Ok(cols) => cols,
                    Err(e) => return Ok(QueryResult::Error(e.to_string())),
                };
                
                // 2. 然后获取当前数据库的可变引用
                if let Ok(current_database) = self.storage.current_database_mut() {
                    // 3. 获取所有记录
                    let all_records = match current_database.get_all_records(&table_name) {
                        Ok(records) => records,
                        Err(e) => return Ok(QueryResult::Error(e.to_string())),
                    };
                    
                    // 4. 根据条件筛选记录
                    let mut matched_records = Vec::new();
                    for record in all_records {
                        if let Some(cond) = &conditions {
                            // 使用修改后的 evaluate 方法，传递列定义而不是表
                            match cond.evaluate(&record, &table_columns) {
                                Ok(true) => matched_records.push(record),
                                Ok(false) => {}, // 不匹配，跳过
                                Err(e) => return Ok(QueryResult::Error(format!("条件评估错误: {}", e))),
                            }
                        } else {
                            // 如果没有条件，所有记录都满足
                            matched_records.push(record);
                        }
                    }
                    
                    // 5. 构造结果集
                    let rows: Vec<Vec<String>> = matched_records.iter().map(|record| {
                        // 将记录转换为字符串向量
                        record.values().iter().map(|v| {
                            // 将值转换为字符串表示
                            match v {
                                Value::Int(i) => i.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::String(s) => s.clone(),
                                Value::Boolean(b) => b.to_string(),
                                Value::Null => "NULL".to_string(),
                            }
                        }).collect()
                    }).collect();
                    
                    let result = ResultSet {
                        columns,
                        rows,
                    };
                    return Ok(QueryResult::ResultSet(result));
                }
                
                Ok(QueryResult::Error("未选择数据库".to_string()))
            },
            _ => Ok(QueryResult::Error("不支持的查询操作".to_string())),
        }
    }
}
