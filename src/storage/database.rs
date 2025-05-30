use super::catalog::Catalog;
use super::io::persistence::PersistenceManager;
use super::table::{ColumnDef, Record, RecordId, Table};
use crate::error::{DBError, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

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
    pub fn new<P: AsRef<Path>>(name: String, db_path: P) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        let persistence = PersistenceManager::new(&db_path)?;
        let catalog = persistence.load_metadata(&name)?;

        Ok(Self {
            name,
            tables: HashMap::new(),
            catalog,
            persistence,
        })
    }

    // 数据库内部的操作方法
    pub fn create_table(
        &mut self,
        name: String,
        columns: Vec<super::table::ColumnDef>,
    ) -> Result<()> {
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

    // new code
    pub fn get_buffer_manager(&self) -> &super::io::buffer_manager::BufferManager {
        self.persistence.buffer_manager()
    }

    pub fn get_buffer_manager_mut(&mut self) -> &mut super::io::buffer_manager::BufferManager {
        self.persistence.buffer_manager_mut()
    }
    // new code end

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
        // 更新目录中的页ID列表
        for (table_name, table) in &self.tables {
            self.catalog
                .update_table_page_ids(table_name, table.page_ids().to_vec())?;
        }

        // 保存元数据
        self.persistence.save_metadata(&self.name, &self.catalog)?;

        // 刷新所有缓冲区页面到磁盘
        self.persistence.buffer_manager_mut().flush_all_pages()?;

        Ok(())
    }

    /// 插入记录到表中的代理方法（封装buffer_manager的访问）
    pub fn insert_record(
        &mut self,
        table_name: &str,
        values: Vec<super::table::Value>,
    ) -> Result<RecordId> {
        // 使用if let避免同时拥有两个可变引用
        if let Some(table) = self.tables.get_mut(table_name) {
            // 现在只有一个对self的可变引用，可以安全地获取buffer_manager
            let buffer_manager = self.persistence.buffer_manager_mut();
            // 调用表的insert_record方法
            table.insert_record(buffer_manager, values)
        } else {
            Err(DBError::NotFound(format!("表 '{}' 不存在", table_name)))
        }
    }

    /// 删除表中记录的代理方法
    pub fn delete_record(&mut self, table_name: &str, record_id: RecordId) -> Result<()> {
        // 检查表是否存在
        if let Some(table) = self.tables.get_mut(table_name) {
            // 获取可变的缓冲区管理器
            let buffer_manager = self.persistence.buffer_manager_mut();
            // 调用表的 delete_record 方法删除记录
            table.delete_record(buffer_manager, record_id)
        } else {
            Err(DBError::NotFound(format!("表 '{}' 不存在", table_name)))
        }
    }

    /// 更新表中记录的代理方法
    pub fn update_record(
        &mut self,
        table_name: &str,
        record_id: RecordId,
        set_pairs: &Vec<(String, super::table::Value)>,
    ) -> Result<()> {
        // 检查表是否存在
        if let Some(table) = self.tables.get_mut(table_name) {
            // 获取可变的缓冲区管理器
            let buffer_manager = self.persistence.buffer_manager_mut();
            // 调用表的 update_record 方法更新记录
            table.update_record(buffer_manager, record_id, &set_pairs)
        } else {
            Err(DBError::NotFound(format!("表 '{}' 不存在", table_name)))
        }
    }

    /// 获取表中全部记录的代理方法
    pub fn get_all_records(&mut self, table_name: &str) -> Result<Vec<Record>> {
        // 检查表是否存在
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| DBError::NotFound(format!("表 '{}' 不存在", table_name)))?;

        // 获取缓冲区管理器
        let buffer_manager = self.persistence.buffer_manager_mut();

        // 调用表的 get_all_records 方法获取所有记录
        table.get_all_records(buffer_manager)
    }
}
