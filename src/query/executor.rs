use crate::error::{DBError, Result};
use crate::query::planner::QueryPlan;
use crate::query::result::{QueryResult, ResultSet};
use crate::storage::StorageEngine;
use crate::storage::table::{ColumnDef, DataType, Value};

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
            QueryPlan::Insert {
                table_name,
                columns,
                rows,
            } => {
                // 获取表的列定义
                let table_columns = self.storage.get_table_columns(table_name)?;

                // 验证指定的列名是否都存在于表中
                for column_name in columns {
                    if !table_columns.iter().any(|col| &col.name == column_name) {
                        return Err(DBError::Schema(format!(
                            "表 '{}' 中不存在列 '{}'",
                            table_name, column_name
                        )));
                    }
                }

                // 处理每一行数据
                for (row_index, row_values) in rows.iter().enumerate() {
                    // 检查每行的值数量是否与指定的列数匹配
                    if row_values.len() != columns.len() {
                        return Err(DBError::Schema(format!(
                            "第 {} 行的值数量({})与指定的列数({})不匹配",
                            row_index + 1,
                            row_values.len(),
                            columns.len()
                        )));
                    }

                    // 创建一个按表结构顺序排列的值数组
                    let mut ordered_values = Vec::with_capacity(table_columns.len());

                    // 按照表的列定义顺序填充值
                    for table_column in &table_columns {
                        if let Some(pos) = columns.iter().position(|col| col == &table_column.name)
                        {
                            // 找到了对应的列，使用提供的值
                            let value = &row_values[pos];

                            // 可以添加类型验证
                            self.validate_value_type(value, &table_column.data_type)?;

                            ordered_values.push(value.clone());
                        } else {
                            // 没有为这个列提供值，使用默认值
                            let default_value = self.get_default_value(&table_column)?;
                            ordered_values.push(default_value);
                        }
                    }

                    // 插入记录到存储引擎
                    self.storage.insert_record(table_name, ordered_values)?;
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
                conditions,
                order_by,
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
            QueryPlan::DropDatabase { name } => match self.storage.drop_database(name) {
                Ok(_) => Ok(QueryResult::Success),
                Err(e) => Err(DBError::Schema(e.to_string())),
            },
            QueryPlan::UseDatabase { name } => match self.storage.use_database(name) {
                Ok(_) => Ok(QueryResult::Success),
                Err(e) => Err(DBError::Schema(e.to_string())),
            },
            QueryPlan::ShowDatabases => todo!(),
            QueryPlan::ShowTables => todo!(),
            QueryPlan::ExpressionSelect { expressions } => todo!(),
        }
    }

    fn get_default_value(&self, column_def: &ColumnDef) -> Result<Value> {
        if column_def.not_null {
            return Err(DBError::Schema(format!(
                "列 '{}' 不允许为空且没有提供值",
                column_def.name
            )));
        }

        // 返回 NULL 值作为默认值
        Ok(Value::Null)
    }

    /// 验证值类型是否与列定义匹配
    fn validate_value_type(&self, value: &Value, data_type: &DataType) -> Result<()> {
        match (value, data_type) {
            (Value::Int(_), DataType::Int(_)) => Ok(()),
            (Value::String(s), DataType::Varchar(max_len)) => {
                if s.len() > *max_len as usize {
                    Err(DBError::Schema(format!(
                        "字符串长度({})超过了VARCHAR({})的限制",
                        s.len(),
                        max_len
                    )))
                } else {
                    Ok(())
                }
            }
            (Value::Null, _) => {
                // NULL 值总是被接受，具体的 NOT NULL 约束在 get_default_value 中处理
                Ok(())
            }
            _ => Err(DBError::Schema(format!(
                "值类型 {:?} 与列类型 {:?} 不匹配",
                value, data_type
            ))),
        }
    }
}
