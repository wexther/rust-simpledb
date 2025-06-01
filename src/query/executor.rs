use crate::error::{DBError, Result};
use crate::query::planner::Plan;
use crate::query::result::{QueryResult, ResultSet};
use crate::storage::StorageEngine;
use crate::storage::table::{ColumnDef, DataType, Record, Value};

use super::planner::SelectColumns;

/// 统一SQL执行器，处理所有类型的SQL操作
pub struct Executor<'a> {
    storage: &'a mut StorageEngine,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a mut StorageEngine) -> Self {
        Self { storage }
    }

    pub fn execute(&mut self, plan: Plan) -> Result<QueryResult> {
        match &plan {
            Plan::CreateTable { name, columns } => {
                match self.storage.create_table(name.clone(), columns.to_vec()) {
                    Ok(_) => Ok(QueryResult::Success),
                    Err(e) => Err(DBError::Schema(e.to_string())),
                }
            }
            Plan::DropTable { name } => match self.storage.drop_table(&name) {
                Ok(_) => Ok(QueryResult::Success),
                Err(e) => Err(DBError::Schema(e.to_string())),
            },
            Plan::Insert {
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
            Plan::Update {
                table_name,
                set_pairs,
                conditions,
            } => {
                todo!() // 更新操作的实现
            }
            Plan::Delete {
                table_name,
                conditions,
            } => {
                todo!() // 删除操作的实现
            }
            Plan::Select {
                table_name,
                columns,
                conditions,
                order_by,
            } => {
                // 处理无表查询（如 SELECT 1+1）
                if table_name.is_none() {
                    return self.execute_expression_select(columns);
                }

                let table_name = table_name.as_ref().unwrap();

                // 获取表的列定义
                let table_columns = self.storage.get_table_columns(table_name)?;

                // 获取所有记录
                let mut records = self.storage.get_all_records(table_name)?;

                // 应用WHERE条件过滤
                if let Some(condition) = conditions {
                    records = records
                        .into_iter()
                        .filter(|record| {
                            condition.evaluate(record, &table_columns).unwrap_or(false)
                        })
                        .collect();
                }

                // 应用ORDER BY排序
                if let Some(order_items) = order_by {
                    self.sort_records(&mut records, order_items, &table_columns)?;
                }

                // 处理选择列（投影）
                let result_rows = self.project_columns(&records, columns, &table_columns)?;

                // 生成结果列名
                let result_columns = self.generate_result_columns(columns, &table_columns)?;

                // 创建结果集
                let result_set = ResultSet {
                    columns: result_columns,
                    rows: result_rows,
                };

                Ok(QueryResult::ResultSet(result_set))
            }
            Plan::CreateDatabase { name } => match self.storage.create_database(name.clone()) {
                Ok(_) => Ok(QueryResult::Success),
                Err(e) => Err(DBError::Schema(e.to_string())),
            },
            Plan::DropDatabase { name } => match self.storage.drop_database(name) {
                Ok(_) => Ok(QueryResult::Success),
                Err(e) => Err(DBError::Schema(e.to_string())),
            },
            Plan::UseDatabase { name } => match self.storage.use_database(name) {
                Ok(_) => Ok(QueryResult::Success),
                Err(e) => Err(DBError::Schema(e.to_string())),
            },
            Plan::ShowDatabases => todo!(),
            Plan::ShowTables => todo!(),
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

    /// 投影列（正确处理通配符）
    fn project_columns(
        &self,
        records: &[Record],
        select_columns: &SelectColumns,
        table_columns: &[ColumnDef],
    ) -> Result<Vec<Vec<Value>>> {
        let mut result_rows = Vec::new();

        for record in records {
            let mut row = Vec::new();

            match select_columns {
                SelectColumns::Wildcard => {
                    // 通配符，添加所有列
                    for value in record.values() {
                        row.push(value.clone());
                    }
                }
                SelectColumns::Columns(items) => {
                    // 处理具体的列
                    for item in items {
                        let value = item.expr.evaluate(record, table_columns)?;
                        row.push(value);
                    }
                }
            }

            result_rows.push(row);
        }

        Ok(result_rows)
    }

    /// 生成结果列名（正确处理通配符）
    fn generate_result_columns(
        &self,
        select_columns: &SelectColumns,
        table_columns: &[ColumnDef],
    ) -> Result<Vec<String>> {
        match select_columns {
            SelectColumns::Wildcard => {
                // 通配符，返回所有表列名
                Ok(table_columns.iter().map(|col| col.name.clone()).collect())
            }
            SelectColumns::Columns(items) => {
                // 处理具体的列
                let mut result_columns = Vec::new();

                for item in items {
                    if let Some(alias) = &item.alias {
                        // 有别名，使用别名
                        result_columns.push(alias.clone());
                    } else {
                        // 没有别名，使用原始文本
                        result_columns.push(item.original_text.clone());
                    }
                }

                Ok(result_columns)
            }
        }
    }

    /// 处理无表查询（如 SELECT 1+1, 'hello'）
    fn execute_expression_select(&self, columns: &SelectColumns) -> Result<QueryResult> {
        match columns {
            SelectColumns::Wildcard => {
                return Err(DBError::Execution("无表查询不支持通配符 *".to_string()));
            }
            SelectColumns::Columns(items) => {
                // 创建一个空记录用于表达式求值
                let empty_record = Record::new(Vec::new());
                let empty_columns = Vec::new();

                let mut result_row = Vec::new();
                let mut result_columns = Vec::new();

                // 对每个表达式进行求值
                for item in items {
                    let value = item.expr.evaluate(&empty_record, &empty_columns)?;
                    result_row.push(value);

                    // 生成列名
                    if let Some(alias) = &item.alias {
                        result_columns.push(alias.clone());
                    } else {
                        result_columns.push(item.original_text.clone());
                    }
                }

                let result_set = ResultSet {
                    columns: result_columns,
                    rows: vec![result_row], // 无表查询只返回一行
                };

                Ok(QueryResult::ResultSet(result_set))
            }
        }
    }

    /// 对记录进行排序
    fn sort_records(
        &self,
        records: &mut Vec<Record>,
        order_items: &[super::planner::OrderByItem],
        table_columns: &[ColumnDef],
    ) -> Result<()> {
        use std::cmp::Ordering;

        records.sort_by(|a, b| {
            for order_item in order_items {
                // 找到排序列的索引
                let column_idx = table_columns
                    .iter()
                    .position(|col| col.name == order_item.column)
                    .ok_or_else(|| {
                        DBError::Execution(format!("排序列 '{}' 不存在", order_item.column))
                    });

                let column_idx = match column_idx {
                    Ok(idx) => idx,
                    Err(_) => continue, // 跳过不存在的列
                };

                let val_a = &a.values()[column_idx];
                let val_b = &b.values()[column_idx];

                let cmp_result = self.compare_values(val_a, val_b);

                let final_result = match order_item.direction {
                    super::planner::SortDirection::Asc => cmp_result,
                    super::planner::SortDirection::Desc => cmp_result.reverse(),
                };

                if final_result != Ordering::Equal {
                    return final_result;
                }
            }

            Ordering::Equal
        });

        Ok(())
    }

    /// 比较两个值
    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            // NULL 值处理：NULL < 任何非 NULL 值
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,

            // 整数比较
            (Value::Int(a), Value::Int(b)) => a.cmp(b),

            // 浮点数比较
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),

            // 混合数值比较
            (Value::Int(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Int(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }

            // 字符串比较
            (Value::String(a), Value::String(b)) => a.cmp(b),

            // 布尔值比较
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),

            // 不同类型之间的比较（可以根据需要调整规则）
            _ => Ordering::Equal,
        }
    }
}
