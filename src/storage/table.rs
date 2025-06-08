use super::io::buffer_manager::BufferManager;
use super::io::page::PageId;
use crate::error::{DBError, Result};

pub mod record;
pub mod value;

// 重新导出 record 模块的公共类型
pub use record::{Record, RecordId};
pub use value::{ColumnDef, DataType, Value};

/// 表结构
#[derive(Debug)]
pub struct Table {
    /// 表名
    name: String,
    /// 列定义
    columns: Vec<ColumnDef>,
    /// 表的数据页面ID列表
    page_ids: Vec<PageId>,
    /// 主键索引
    primary_key_index: Option<usize>,
}

impl Table {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        // 找出主键列索引
        let primary_key_index = columns.iter().position(|col| col.is_primary);

        Self {
            name,
            columns,
            page_ids: Vec::new(),
            primary_key_index,
        }
    }

    pub fn get_primary_key_index(&self) -> Option<usize> {
        self.primary_key_index
    }

    /// 获取表名
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 获取列定义
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// 插入记录
    pub fn insert_record(
        &mut self,
        buffer_manager: &mut BufferManager,
        values: Vec<Value>,
    ) -> Result<RecordId> {
        // 验证值的数量与列数是否匹配
        if values.len() != self.columns.len() {
            return Err(DBError::Schema(format!(
                "值的数量({})与列数({})不匹配",
                values.len(),
                self.columns.len()
            )));
        }

        // 验证 NULL 约束
        for (value, column) in values.iter().zip(&self.columns) {
            if value == &Value::Null && column.not_null {
                return Err(DBError::Schema(format!(
                    "Field '{}' doesn't have a default value",
                    column.name
                )));
            }
        }

        // 验证 UNIQUE 约束
        for (i, (value, column)) in values.iter().zip(&self.columns).enumerate() {
            if column.unique && value != &Value::Null {
                // 检查所有现有记录是否有重复值
                for &page_id in &self.page_ids {
                    let page = buffer_manager.get_page(page_id)?;

                    // 遍历页面中的所有记录
                    for (_, record) in page.iter_records() {
                        let record_values = record.values();
                        if i < record_values.len() && &record_values[i] == value {
                            return Err(DBError::Schema(format!(
                                "Duplicate entry '{}' for key 'PRIMARY'”。",
                                value
                            )));
                        }
                    }
                }
            }
        }

        // 尝试在现有页面中插入
        for &page_id in &self.page_ids {
            let page = buffer_manager.get_page_mut(page_id)?;

            // 尝试插入记录 - 直接返回 RecordId
            match page.insert_record(values.clone()) {
                Ok(record_id) => return Ok(record_id),
                Err(_) => continue, // 这个页面满了，尝试下一个
            }
        }

        // 所有现有页面都已满，创建新页面
        let new_page_id = buffer_manager.create_page()?;
        self.page_ids.push(new_page_id);

        // 在新页面中插入记录
        let page = buffer_manager.get_page_mut(new_page_id)?;
        page.insert_record(values)
    }

    /// 删除记录
    pub fn delete_record(
        &mut self,
        buffer_manager: &mut BufferManager,
        id: RecordId,
    ) -> Result<()> {
        if !self.page_ids.contains(&id.page_id) {
            return Err(DBError::NotFound(format!(
                "页面 {} 不属于表 {}",
                id.page_id, self.name
            )));
        }

        let page = buffer_manager.get_page_mut(id.page_id)?;
        page.delete_record(id) // 直接传递 RecordId
    }

    /// 获取记录
    pub fn get_record(&self, buffer_manager: &mut BufferManager, id: RecordId) -> Result<Record> {
        if !self.page_ids.contains(&id.page_id) {
            return Err(DBError::NotFound(format!(
                "页面 {} 不属于表 {}",
                id.page_id, self.name
            )));
        }

        let page = buffer_manager.get_page(id.page_id)?;
        page.get_record(id) // 直接传递 RecordId
    }

    /// 修改记录
    pub fn update_record(
        &mut self,
        buffer_manager: &mut BufferManager,
        id: RecordId,
        set_pairs: &Vec<(String, Value)>,
    ) -> Result<()> {
        if !self.page_ids.contains(&id.page_id) {
            return Err(DBError::NotFound(format!(
                "页面 {} 不属于表 {}",
                id.page_id, self.name
            )));
        }

        let page = buffer_manager.get_page_mut(id.page_id)?;

        // 获取原记录
        let original_record = page.get_record(id)?;
        let mut new_values: Vec<Value> = original_record.values().to_vec();

        // 按照 set_pairs 更新记录值
        for (col_name, new_value) in set_pairs {
            if let Some(col_index) = self.columns.iter().position(|col| &col.name == col_name) {
                // ... 类型验证逻辑 ...
                new_values[col_index] = new_value.clone();
            } else {
                return Err(DBError::Schema(format!(
                    "表 '{}' 中不存在列 '{}'",
                    self.name, col_name
                )));
            }
        }

        // 替换记录
        page.replace_record(id, new_values)?;
        Ok(())
    }

    /// 获取表中所有记录
    pub fn get_all_records(&self, buffer_manager: &mut BufferManager) -> Result<Vec<Record>> {
        let mut records = Vec::new();

        for &page_id in &self.page_ids {
            let page = buffer_manager.get_page(page_id)?;

            // 直接使用迭代器获取所有记录
            for (_, record) in page.iter_records() {
                records.push(record);
            }
        }

        Ok(records)
    }

    /// 从磁盘加载表数据
    pub fn load(
        &mut self,
        buffer_manager: &mut BufferManager,
        page_ids: Vec<PageId>,
    ) -> Result<()> {
        let _ = buffer_manager; // 可能需要在加载时使用 BufferManager
        self.page_ids = page_ids;
        Ok(())
    }

    /// 获取表的页面ID列表
    pub fn page_ids(&self) -> &[PageId] {
        &self.page_ids
    }
}
