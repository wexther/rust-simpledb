pub mod catalog;
mod database;
mod io;

pub mod table;
// pub mod record;
pub mod transaction;

use crate::error::{DBError, Result};
use catalog::Catalog;
use database::Database;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use table::{ColumnDef, Record, RecordId, Table, Value};

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
        let database = self.current_database_mut()?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::table::{ColumnDef, DataType, Value};
    use std::fs;
    use tempfile::TempDir;

    fn create_test_storage() -> (StorageEngine, TempDir) {
        let temp_dir = TempDir::new().expect("无法创建临时目录");
        let storage =
            StorageEngine::new(Some(temp_dir.path()), Some("test_db")).expect("无法创建存储引擎");
        (storage, temp_dir)
    }

    fn create_test_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int(32),
                not_null: true,
                unique: true,
                is_primary: true,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar(100),
                not_null: true,
                is_primary: false,
                unique: false,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int(32),
                not_null: false,
                is_primary: false,
                unique: false,
            },
        ]
    }

    #[test]
    fn test_storage_engine_creation() {
        let (storage, _temp_dir) = create_test_storage();

        // 验证默认数据库是否创建
        assert!(storage.has_database("test_db"));

        // 验证当前数据库是否设置正确
        assert!(storage.current_database().is_ok());
    }

    #[test]
    fn test_database_management() {
        let (mut storage, _temp_dir) = create_test_storage();

        // 测试创建数据库
        assert!(storage.create_database("new_db".to_string()).is_ok());
        assert!(storage.has_database("new_db"));

        // 测试创建重复数据库应该失败
        assert!(storage.create_database("new_db".to_string()).is_err());

        // 测试切换数据库
        assert!(storage.use_database("new_db").is_ok());

        // 测试使用不存在的数据库应该失败
        assert!(storage.use_database("non_existent").is_err());

        // 测试删除数据库
        assert!(storage.drop_database("new_db").is_ok());
        assert!(!storage.has_database("new_db"));

        // 测试删除不存在的数据库应该失败
        assert!(storage.drop_database("non_existent").is_err());
    }

    #[test]
    fn test_table_management() {
        let (mut storage, _temp_dir) = create_test_storage();
        let columns = create_test_columns();

        // 测试创建表
        assert!(
            storage
                .create_table("users".to_string(), columns.clone())
                .is_ok()
        );

        // 测试创建重复表应该失败
        assert!(
            storage
                .create_table("users".to_string(), columns.clone())
                .is_err()
        );

        // 测试获取表
        assert!(storage.get_table("users").is_ok());
        assert!(storage.get_table("non_existent").is_err());

        // 测试获取表列定义
        let retrieved_columns = storage.get_table_columns("users").unwrap();
        assert_eq!(retrieved_columns.len(), 3);
        assert_eq!(retrieved_columns[0].name, "id");
        assert_eq!(retrieved_columns[1].name, "name");
        assert_eq!(retrieved_columns[2].name, "age");

        // 测试删除表
        assert!(storage.drop_table("users").is_ok());
        assert!(storage.get_table("users").is_err());

        // 测试删除不存在的表应该失败
        assert!(storage.drop_table("non_existent").is_err());
    }

    #[test]
    fn test_record_operations() {
        let (mut storage, _temp_dir) = create_test_storage();
        let columns = create_test_columns();

        // 创建测试表
        storage.create_table("users".to_string(), columns).unwrap();

        // 测试插入记录
        let values1 = vec![
            Value::Int(1),
            Value::String("Alice".to_string()),
            Value::Int(25),
        ];
        let record_id1 = storage.insert_record("users", values1.clone()).unwrap();

        let values2 = vec![Value::Int(2), Value::String("Bob".to_string()), Value::Null];
        let record_id2 = storage.insert_record("users", values2.clone()).unwrap();

        // 测试获取所有记录
        let records = storage.get_all_records("users").unwrap();
        assert_eq!(records.len(), 2);

        // 验证记录内容
        assert_eq!(records[0].values(), &values1);
        assert_eq!(records[1].values(), &values2);

        // 测试更新记录
        let update_pairs = vec![
            ("name".to_string(), Value::String("Alice Smith".to_string())),
            ("age".to_string(), Value::Int(26)),
        ];
        assert!(
            storage
                .update_record("users", record_id1, &update_pairs)
                .is_ok()
        );

        // 验证更新后的记录
        let updated_records = storage.get_all_records("users").unwrap();
        if let Some(updated_record) = updated_records
            .iter()
            .find(|r| r.id().unwrap() == record_id1)
        {
            assert_eq!(
                updated_record.values()[1],
                Value::String("Alice Smith".to_string())
            );
            assert_eq!(updated_record.values()[2], Value::Int(26));
        } else {
            panic!("找不到更新的记录");
        }

        // 测试删除记录
        assert!(storage.delete_record("users", record_id2).is_ok());
        let remaining_records = storage.get_all_records("users").unwrap();
        assert_eq!(remaining_records.len(), 1);
        assert_eq!(remaining_records[0].id().unwrap(), record_id1);

        // 测试删除不存在的记录应该失败
        assert!(storage.delete_record("users", record_id2).is_err());
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().expect("无法创建临时目录");
        let temp_path = temp_dir.path().to_path_buf();
        let columns = create_test_columns();

        // 第一次运行：创建数据并保存
        {
            let mut storage = StorageEngine::new(Some(&temp_path), Some("persist_test")).unwrap();

            // 创建表和数据
            storage
                .create_table("users".to_string(), columns.clone())
                .unwrap();
            storage
                .insert_record(
                    "users",
                    vec![
                        Value::Int(1),
                        Value::String("Alice".to_string()),
                        Value::Int(25),
                    ],
                )
                .unwrap();
            storage
                .insert_record(
                    "users",
                    vec![
                        Value::Int(2),
                        Value::String("Bob".to_string()),
                        Value::Int(30),
                    ],
                )
                .unwrap();

            // 手动保存
            storage.save().unwrap();
        } // storage 在这里被销毁，会自动保存

        // 第二次运行：加载数据并验证
        {
            let mut storage = StorageEngine::new(Some(&temp_path), Some("persist_test")).unwrap();

            // 验证数据库和表是否存在
            assert!(storage.has_database("persist_test"));
            assert!(storage.get_table("users").is_ok());

            // 验证数据是否正确加载
            let records = storage.get_all_records("users").unwrap();
            assert_eq!(records.len(), 2);

            // 验证具体数据
            let alice_record = records
                .iter()
                .find(|r| r.values()[0] == Value::Int(1))
                .expect("找不到 Alice 的记录");
            assert_eq!(alice_record.values()[1], Value::String("Alice".to_string()));
            assert_eq!(alice_record.values()[2], Value::Int(25));

            let bob_record = records
                .iter()
                .find(|r| r.values()[0] == Value::Int(2))
                .expect("找不到 Bob 的记录");
            assert_eq!(bob_record.values()[1], Value::String("Bob".to_string()));
            assert_eq!(bob_record.values()[2], Value::Int(30));
        }
    }

    #[test]
    fn test_multiple_databases() {
        let (mut storage, _temp_dir) = create_test_storage();
        let columns = create_test_columns();

        // 创建多个数据库
        storage.create_database("db1".to_string()).unwrap();
        storage.create_database("db2".to_string()).unwrap();

        // 在 db1 中创建表
        storage.use_database("db1").unwrap();
        storage
            .create_table("table1".to_string(), columns.clone())
            .unwrap();
        storage
            .insert_record(
                "table1",
                vec![
                    Value::Int(1),
                    Value::String("DB1 Data".to_string()),
                    Value::Int(100),
                ],
            )
            .unwrap();

        // 在 db2 中创建表
        storage.use_database("db2").unwrap();
        storage.create_table("table2".to_string(), columns).unwrap();
        storage
            .insert_record(
                "table2",
                vec![
                    Value::Int(2),
                    Value::String("DB2 Data".to_string()),
                    Value::Int(200),
                ],
            )
            .unwrap();

        // 验证数据隔离
        let db2_records = storage.get_all_records("table2").unwrap();
        assert_eq!(db2_records.len(), 1);
        assert_eq!(
            db2_records[0].values()[1],
            Value::String("DB2 Data".to_string())
        );

        // 切换回 db1 验证数据
        storage.use_database("db1").unwrap();
        let db1_records = storage.get_all_records("table1").unwrap();
        assert_eq!(db1_records.len(), 1);
        assert_eq!(
            db1_records[0].values()[1],
            Value::String("DB1 Data".to_string())
        );

        // 验证在 db1 中无法访问 db2 的表
        assert!(storage.get_table("table2").is_err());
    }

    #[test]
    fn test_edge_cases() {
        let (mut storage, _temp_dir) = create_test_storage();
        let columns = create_test_columns();

        // 测试空表操作
        storage
            .create_table("empty_table".to_string(), columns.clone())
            .unwrap();
        let empty_records = storage.get_all_records("empty_table").unwrap();
        assert_eq!(empty_records.len(), 0);

        // 测试对不存在的表进行操作
        assert!(
            storage
                .insert_record("non_existent", vec![Value::Int(1)])
                .is_err()
        );
        assert!(storage.get_all_records("non_existent").is_err());
        assert!(
            storage
                .update_record(
                    "non_existent",
                    RecordId::new(1, 0 /* 我们不关心这个值 */),
                    &vec![]
                )
                .is_err()
        );
        assert!(
            storage
                .delete_record("non_existent", RecordId::new(1, 0 /* 我们不关心这个值 */))
                .is_err()
        );

        // 测试插入类型不匹配的数据
        storage
            .create_table("test_table".to_string(), columns)
            .unwrap();

        // 插入错误数量的值（应该通过，但可能在更严格的验证中失败）
        let wrong_values = vec![Value::Int(1)]; // 只有1个值，但表有3列
        // 注意：这取决于你的实现是否在存储层验证列数
        // 如果不验证，这个测试可能需要调整
    }

    #[test]
    fn test_error_handling() {
        let (mut storage, _temp_dir) = create_test_storage();

        // 测试在未选择数据库时的操作
        storage.drop_database("test_db").unwrap(); // 删除默认数据库

        // 现在应该没有当前数据库了
        // 注意：这取决于你的实现细节，可能需要调整

        // 测试各种错误情况
        assert!(storage.get_table("any_table").is_err());
        assert!(
            storage
                .create_table("any_table".to_string(), vec![])
                .is_err()
        );
    }

    #[test]
    fn test_concurrent_operations() {
        // 这是一个基础的并发测试
        // 注意：真实的并发测试需要更复杂的设置
        let (mut storage, _temp_dir) = create_test_storage();
        let columns = create_test_columns();

        storage
            .create_table("concurrent_table".to_string(), columns)
            .unwrap();

        // 快速连续插入多条记录
        for i in 0..10 {
            storage
                .insert_record(
                    "concurrent_table",
                    vec![
                        Value::Int(i),
                        Value::String(format!("User{}", i)),
                        Value::Int(20 + i),
                    ],
                )
                .unwrap();
        }

        let records = storage.get_all_records("concurrent_table").unwrap();
        assert_eq!(records.len(), 10);
    }
}
