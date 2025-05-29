pub mod catalog;
mod database;
mod io;
pub mod record;
pub mod table;
pub mod transaction;

use crate::error::{DBError, Result};
use catalog::Catalog;
use database::Database;
use record::{Record, RecordId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use table::{ColumnDef, Table, Value};

/// 存储引擎 - 负责数据存储和访问
pub struct StorageEngine {
    /// 多个数据库
    databases: HashMap<String, Database>,
    /// 当前选中的数据库
    current_database: Option<String>,
    /// 基础数据目录
    base_dir: PathBuf,
}

impl StorageEngine {
    /// 创建并初始化存储引擎
    ///
    /// # 参数
    /// * `base_dir` - 可选的存储基础目录，如果为None则使用默认目录"data"
    /// * `default_db_name` - 可选的默认数据库名称，如果为None则使用"default"
    pub fn new(base_dir: Option<&Path>, db_name: Option<&str>) -> Result<Self> {
        let base_dir = match base_dir {
            Some(dir) => dir.to_path_buf(),
            None => PathBuf::from("data"),
        };
        let db_name = db_name.unwrap_or("default");

        let mut storage_engine = Self {
            databases: HashMap::new(),
            current_database: None,
            base_dir,
        };

        storage_engine.load()?;

        if !storage_engine.has_database(db_name) {
            storage_engine.create_database(db_name.to_string())?;
        }

        if storage_engine.current_database().is_err() {
            storage_engine.use_database(db_name)?;
        }

        Ok(storage_engine)
    }

    /// 获取数据库目录路径
    fn get_db_path(&self, db_name: &str) -> PathBuf {
        self.base_dir.join(db_name)
    }

    /// 加载所有数据库
    fn load(&mut self) -> Result<()> {
        if !self.base_dir.exists() {
            std::fs::create_dir_all(&self.base_dir)
                .map_err(|e| DBError::IO(format!("无法创建数据库目录: {}", e)))?;
        }

        // 读取基础目录中的所有子目录
        let entries = std::fs::read_dir(&self.base_dir)
            .map_err(|e| DBError::IO(format!("无法读取数据库目录: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| DBError::IO(format!("无法读取数据库目录项: {}", e)))?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(db_name) = path.file_name().and_then(|n| n.to_str()) {
                    // 加载数据库
                    let mut database =
                        Database::new(db_name.to_string(), &self.get_db_path(db_name))?;
                    database.load()?;
                    self.databases.insert(db_name.to_string(), database);
                }
            }
        }

        Ok(())
    }

    /// 保存所有数据库
    pub fn save(&mut self) -> Result<()> {
        // 保存每个数据库
        for database in self.databases.values_mut() {
            database.save()?;
        }

        Ok(())
    }

    // 以下是数据库管理方法
    /// 创建数据库
    pub fn create_database(&mut self, name: String) -> Result<()> {
        if self.databases.contains_key(&name) {
            return Err(DBError::Schema(format!("数据库 '{}' 已存在", name)));
        }

        // 创建数据库目录
        let db_path = self.get_db_path(&name);
        let database = Database::new(name.clone(), &db_path)?;

        self.databases.insert(name.clone(), database);

        // 如果是第一个创建的数据库，自动设为当前数据库
        if self.current_database.is_none() {
            self.current_database = Some(name);
        }

        Ok(())
    }
    
    /// 删除数据库
    pub fn drop_database(&mut self, name: &str) -> Result<()> {
        if !self.databases.contains_key(name) {
            return Err(DBError::NotFound(format!("数据库 '{}' 不存在", name)));
        }

        self.databases.remove(name);

        // 如果删除的是当前数据库，重置当前数据库选择
        if self.current_database.as_deref() == Some(name) {
            self.current_database = None;
        }

        Ok(())
    }

    /// 更改当前数据库为
    pub fn use_database(&mut self, name: &str) -> Result<()> {
        if !self.databases.contains_key(name) {
            return Err(DBError::NotFound(format!("数据库 '{}' 不存在", name)));
        }

        self.current_database = Some(name.to_string());
        Ok(())
    }

    /// 是否包含某数据库
    pub fn has_database(&self, name: &str) -> bool {
        self.databases.contains_key(name)
    }

    /// 获取数据库
    pub fn get_database(&self, name: &str) -> Result<&Database> {
        self.databases
            .get(name)
            .ok_or_else(|| DBError::NotFound(format!("数据库 '{}' 不存在", name)))
    }

    /// 获取可变数据库
    pub fn get_database_mut(&mut self, name: &str) -> Result<&mut Database> {
        self.databases
            .get_mut(name)
            .ok_or_else(|| DBError::NotFound(format!("数据库 '{}' 不存在", name)))
    }

    /// 获取当前数据库的方法
    pub fn current_database(&self) -> Result<&Database> {
        const DEFAULT_DB_NAME: &str = "default";

        match &self.current_database {
            Some(name) => self
                .databases
                .get(name)
                .ok_or_else(|| DBError::NotFound(format!("当前数据库 '{}' 不存在", name))),
            None => {
                // 如果没有选择数据库但有默认数据库，则返回默认数据库
                self.databases
                    .get(DEFAULT_DB_NAME)
                    .ok_or_else(|| DBError::Other("未选择数据库且默认数据库不存在".to_string()))
            }
        }
    }

    /// 获取当前可变数据库
    pub fn current_database_mut(&mut self) -> Result<&mut Database> {
        const DEFAULT_DB_NAME: &str = "default";

        let name = match &self.current_database {
            Some(name) => name.clone(),
            None => {
                // 如果没有选择数据库但有默认数据库，则使用默认数据库
                if self.databases.contains_key(DEFAULT_DB_NAME) {
                    DEFAULT_DB_NAME.to_string()
                } else {
                    return Err(DBError::Other("未选择数据库且默认数据库不存在".to_string()));
                }
            }
        };

        self.databases
            .get_mut(&name)
            .ok_or_else(|| DBError::NotFound(format!("当前数据库 '{}' 不存在", name)))
    }

    // 以下是一些代理方法 - 转发到当前数据库
    /// 创建表
    pub fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<()> {
        let database = self.current_database_mut()?;
        database.create_table(name, columns)
    }

    /// 删除表
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let database = self.current_database_mut()?;
        database.drop_table(name)
    }

    /// 获取表
    pub fn get_table(&self, name: &str) -> Result<&Table> {
        let database = self.current_database()?;
        database.get_table(name)
    }

    /// 获取可变表
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Table> {
        let database = self.current_database_mut()?;
        database.get_table_mut(name)
    }

    /// 获取表的列定义
    pub fn get_table_columns(&self, name: &str) -> Result<Vec<ColumnDef>> {
        let database = self.current_database()?;
        let table = database.get_table(name)?;
        Ok(table.columns().to_vec())
    }

    // 以下是一些对表记录的操作
    /// 增加一行
    pub fn insert_record(&mut self, table_name: &str, values: Vec<Value>) -> Result<RecordId> {
        let database = self.current_database_mut()?;
        database.insert_record(table_name, values)
    }

    /// 删除一行
    pub fn delete_record(&mut self, table_name: &str, record_id: RecordId) -> Result<()> {
        let database = self.current_database_mut()?;
        database.delete_record(table_name, record_id)
    }

    /// 更新一行
    pub fn update_record(
        &mut self,
        table_name: &str,
        record_id: RecordId,
        set_pairs: &Vec<(String, Value)>,
    ) -> Result<()> {
        let database = self.current_database_mut()?;
        database.update_record(table_name, record_id, set_pairs)
    }

    /// 获取表中所有记录
    pub fn get_all_records(&mut self, table_name: &str) -> Result<Vec<Record>> {
        let mut database = self.current_database_mut()?;
        database.get_all_records(table_name)
    }
}

// 实现 Drop trait 以在存储引擎被销毁时自动保存数据
impl Drop for StorageEngine {
    fn drop(&mut self) {
        if let Err(e) = self.save() {
            eprintln!("保存存储引擎时出错: {}", e);
        }
    }
}
