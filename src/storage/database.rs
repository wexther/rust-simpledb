use std::collections::HashMap;
use crate::error::{DBError, Result};
use super::table::{Table, ColumnDef};
use super::catalog::Catalog;
use super::io::persistence::PersistenceManager;

/// 单个数据库的结构
pub struct Database {
    /// 数据库名称
    name: String,
    /// 表集合
    tables: HashMap<String, Table>,
    /// 元数据目录
    catalog: Catalog,
    /// 持久化管理器
    persistence: PersistenceManager,
}

impl Database {
    pub fn new(name: String, db_dir: &str) -> Result<Self> {
        let persistence = PersistenceManager::new(format!("{}/{}", db_dir, name))?;
        let catalog = persistence.load_metadata(&name)?;
        
        Ok(Self {
            name,
            tables: HashMap::new(),
            catalog,
            persistence,
        })
    }
    
    // 数据库内部的操作方法
    pub fn create_table(&mut self, name: String, columns: Vec<super::table::ColumnDef>) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(DBError::Schema(format!("表 '{}' 已存在", name)));
        }
        
        let table = Table::new(name.clone(), columns.clone());
        self.tables.insert(name.clone(), table);
        self.catalog.add_table_metadata(name, columns)?;
        
        Ok(())
    }
    
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if !self.tables.contains_key(name) {
            return Err(DBError::NotFound(format!("表 '{}' 不存在", name)));
        }
        
        self.tables.remove(name);
        self.catalog.remove_table_metadata(name)?;
        
        Ok(())
    }
    
    pub fn get_table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| DBError::NotFound(format!("表 '{}' 不存在", name)))
    }
    
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| DBError::NotFound(format!("表 '{}' 不存在", name)))
    }
    
    /// 加载数据库
    pub fn load(&mut self) -> Result<()> {
        // 加载目录中所有表的元数据
        for table_name in self.catalog.get_table_names() {
            let columns = self.catalog.get_table_columns(&table_name)?;
            let page_ids = self.catalog.get_table_page_ids(&table_name)?;
            
            // 创建表对象
            let mut table = Table::new(table_name.clone(), columns);
            
            // 加载表的数据页
            table.load(self.persistence.buffer_manager_mut(), page_ids)?;
            
            // 添加到表集合
            self.tables.insert(table_name, table);
        }
        
        Ok(())
    }
    
    /// 保存数据库
    pub fn save(&mut self) -> Result<()> {
        // 保存元数据
        self.persistence.save_metadata(&self.name, &self.catalog)?;
        
        // 刷新所有缓冲区页面到磁盘
        self.persistence.buffer_manager_mut().flush_all_pages()?;
        
        Ok(())
    }
}