use crate::error::Result;
use crate::query::planner::QueryPlan;
use crate::query::result::{QueryResult, ResultSet};
use crate::storage::engine::StorageEngine;

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
            QueryPlan::Select {table_name,columns,conditions,} => {
                // 1. 根据 table_name 获取 table 的所有记录
                if let Ok(current_database) = self.storage.current_database_mut() {
                    if let Ok(table) = current_database.get_table(&table_name) {
                        let mut all_records = Vec::new();
                        // 假设 Table 有一个 get_all_records 方法来获取所有记录
                        if let Ok(records) = current_database.get_all_records(&table_name) {
                            all_records = records;
                        }

                        // 2. 根据 conditions 中的 evaluate 方法判断每一条记录是否满足，把所有满足的记录放到一个 vec 中
                        let mut matched_records = Vec::new();
                        for record in all_records {
                            if let Some(cond) = &conditions {
                                if let Ok(_) = cond.evaluate(&record, table) {
                                    matched_records.push(record);
                                }
                            } else {
                                // 如果没有条件，所有记录都满足
                                matched_records.push(record);
                            }
                        }

                        // 3. 构造 result（其中 columns 为传入的 columns，rows 为刚刚构造的记录集合）并返回
                        let rows: Vec<Vec<String>> = Vec::new(); /////////// to do

                        let result = ResultSet {
                            columns,
                            rows,
                        };
                        return Ok(QueryResult::ResultSet(result));
                    }
                }
                Ok(QueryResult::Error(format!("表 '{}' 不存在或未选择数据库", table_name)))
            }
            _ => Ok(QueryResult::Error("不支持的查询操作".to_string())),
        }
    }
}
