use super::io::page::PageId;
use super::table::ColumnDef;
use crate::error::{DBError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 目录 - 存储数据库模式信息（表结构、列定义等元数据）
#[derive(Serialize, Deserialize)]
pub struct Catalog {
    /// 表元数据，存储表名与其对应的列定义
    tables: HashMap<String, TableMetadata>,
}

/// 表的元数据信息
#[derive(Serialize, Deserialize)]
struct TableMetadata {
    /// 列定义
    columns: Vec<ColumnDef>,
    /// 表的数据页ID列表
    page_ids: Vec<PageId>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// 添加表元数据
    pub fn add_table_metadata(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(DBError::Schema(format!("表 '{}' 元数据已存在", name)));
        }

        let metadata = TableMetadata {
            columns,
            page_ids: Vec::new(), // 新表没有数据页
        };

        self.tables.insert(name, metadata);
        Ok(())
    }

    /// 删除表元数据
    pub fn remove_table_metadata(&mut self, name: &str) -> Result<()> {
        if !self.tables.contains_key(name) {
            return Err(DBError::NotFound(format!("表 '{}' 元数据不存在", name)));
        }

        self.tables.remove(name);
        Ok(())
    }

    /// 获取所有表名
    pub fn get_table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// 获取表的列定义
    pub fn get_table_columns(&self, table_name: &str) -> Result<Vec<ColumnDef>> {
        self.tables
            .get(table_name)
            .map(|metadata| metadata.columns.clone())
            .ok_or_else(|| DBError::NotFound(format!("表 '{}' 元数据不存在", table_name)))
    }

    /// 获取表的数据页ID列表
    pub fn get_table_page_ids(&self, table_name: &str) -> Result<Vec<PageId>> {
        self.tables
            .get(table_name)
            .map(|metadata| metadata.page_ids.clone())
            .ok_or_else(|| DBError::NotFound(format!("表 '{}' 元数据不存在", table_name)))
    }

    /// 更新表的数据页ID列表
    pub fn update_table_page_ids(&mut self, table_name: &str, page_ids: Vec<PageId>) -> Result<()> {
        match self.tables.get_mut(table_name) {
            Some(metadata) => {
                metadata.page_ids = page_ids;
                Ok(())
            }
            None => Err(DBError::NotFound(format!(
                "表 '{}' 元数据不存在",
                table_name
            ))),
        }
    }

    /// 添加表的数据页ID
    pub fn add_table_page_id(&mut self, table_name: &str, page_id: PageId) -> Result<()> {
        match self.tables.get_mut(table_name) {
            Some(metadata) => {
                metadata.page_ids.push(page_id);
                Ok(())
            }
            None => Err(DBError::NotFound(format!(
                "表 '{}' 元数据不存在",
                table_name
            ))),
        }
    }

    /// 检查表是否存在
    pub fn has_table(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    /// 获取表元数据的数量
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}
