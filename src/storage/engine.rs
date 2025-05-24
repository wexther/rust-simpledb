use std::collections::HashMap;
use crate::error::{DBError, Result};
use super::table::Table;
use super::catalog::Catalog;
use super::transaction::Transaction;

/// 存储引擎 - 负责数据存储和访问
pub struct StorageEngine {
    catalog: Catalog,
    tables: HashMap<String, Table>,
    // 可以添加缓存、事务管理等组件
}

impl StorageEngine {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
            tables: HashMap::new(),
        }
    }
    
    /// 创建表
    pub fn create_table(&mut self, name: String, columns: Vec<super::table::ColumnDef>) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(DBError::Schema(format!("Table '{}' already exists", name)));
        }
        
        let table = Table::new(name.clone(), columns.clone());
        self.tables.insert(name.clone(), table);
        self.catalog.add_table_metadata(name, columns)?;
        
        Ok(())
    }
    
    /// 删除表
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(DBError::NotFound(format!("Table '{}' not found", name)));
        }
        self.catalog.remove_table_metadata(name)?;
        
        Ok(())
    }
    
    /// 开始新事务
    pub fn begin_transaction(&self) -> Transaction {
        Transaction::new()
    }
    
    /// 获取表引用
    pub fn get_table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| DBError::NotFound(format!("Table '{}' not found", name)))
    }
    
    /// 获取可变表引用
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| DBError::NotFound(format!("Table '{}' not found", name)))
    }
    
    /// 加载数据库
    pub fn load(&mut self, path: &str) -> Result<()> {
        // 从磁盘加载数据...
        Ok(())
    }
    
    /// 保存数据库
    pub fn save(&self, path: &str) -> Result<()> {
        // 将数据保存到磁盘...
        Ok(())
    }
}