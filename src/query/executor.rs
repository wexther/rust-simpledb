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
                // 尝试获取当前数据库
                if let Ok(current_database) = self.storage.current_database_mut() {
                    // 遍历要插入的每一行数据
                    for record in values {
                        // 使用database的代理方法插入记录，不需要直接处理buffer_manager
                        todo!();
                    }
                    return Ok(QueryResult::Success);
                }
                return Err(DBError::Schema("当前没有选择数据库".to_string()));
            }
            QueryPlan::Update {
                table_name,
                set_pairs,
                conditions,
            } => {
                // 实现更新操作
                // new code
                // 尝试获取当前数据库
                // 1. 首先获取表的列定义（不需要持有表的引用）
                let table_columns = match self.storage.get_table_columns(&table_name) {
                    Ok(cols) => cols,
                    Err(e) => return Err(DBError::Schema(e.to_string())),
                };
                // 2. 然后获取当前数据库的可变引用
                if let Ok(current_database) = self.storage.current_database_mut() {
                    // 3. 获取所有记录
                    let all_records = match current_database.get_all_records(&table_name) {
                        Ok(records) => records,
                        Err(e) => return Err(DBError::Schema(e.to_string())),
                    };

                    // 4. 根据条件筛选记录
                    let mut matched_records = Vec::new();
                    for record in all_records {
                        if let Some(cond) = &conditions {
                            // 使用修改后的 evaluate 方法，传递列定义而不是表
                            match cond.evaluate(&record, &table_columns) {
                                Ok(true) => matched_records.push(record),
                                Ok(false) => {} // 不匹配，跳过
                                Err(e) => {
                                    return Err(DBError::Schema(format!("条件评估错误: {}", e)));
                                }
                            }
                        } else {
                            // 如果没有条件，所有记录都满足
                            matched_records.push(record);
                        }
                    }

                    // 5. 更新匹配的记录
                    for record in matched_records {
                        if let Err(e) = current_database.update_record(
                            &table_name,
                            record.id().unwrap(),
                            &set_pairs,
                        ) {
                            return Err(DBError::Schema(format!("删除记录失败: {}", e)));
                        }
                    }
                    return Ok(QueryResult::Success);
                }
                // new code end
                return Err(DBError::Schema("更新失败".to_string()));
            }
            QueryPlan::Delete {
                table_name,
                conditions,
            } => {
                // 实现删除操作
                // new code
                // 尝试获取当前数据库
                // 1. 首先获取表的列定义（不需要持有表的引用）
                let table_columns = match self.storage.get_table_columns(&table_name) {
                    Ok(cols) => cols,
                    Err(e) => return Err(DBError::Schema(e.to_string())),
                };
                // 2. 然后获取当前数据库的可变引用
                if let Ok(current_database) = self.storage.current_database_mut() {
                    // 3. 获取所有记录
                    let all_records = match current_database.get_all_records(&table_name) {
                        Ok(records) => records,
                        Err(e) => return Err(DBError::Schema(e.to_string())),
                    };

                    // 4. 根据条件筛选记录
                    let mut matched_records = Vec::new();
                    for record in all_records {
                        if let Some(cond) = &conditions {
                            // 使用修改后的 evaluate 方法，传递列定义而不是表
                            match cond.evaluate(&record, &table_columns) {
                                Ok(true) => matched_records.push(record),
                                Ok(false) => {} // 不匹配，跳过
                                Err(e) => {
                                    return Err(DBError::Schema(format!("条件评估错误: {}", e)));
                                }
                            }
                        } else {
                            // 如果没有条件，所有记录都满足
                            matched_records.push(record);
                        }
                    }

                    // 5. 删除匹配的记录
                    for record in matched_records {
                        if let Err(e) =
                            current_database.delete_record(&table_name, record.id().unwrap())
                        {
                            return Err(DBError::Schema(format!("删除记录失败: {}", e)));
                        }
                    }
                    return Ok(QueryResult::Success);
                }
                // new code end
                Err(DBError::Schema("删除失败".to_string()))
            },
            QueryPlan::Select { .. } => {
                todo!();
            },
            _=>{
                todo!();
            }
        }
    }
}
