pub mod buffer_manager;
mod disk_manager;
pub mod page;

use crate::error::{DBError, Result};
use crate::storage::catalog::Catalog;
use buffer_manager::BufferManager;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// 持久化管理器 - 负责数据库元数据和记录的持久化
pub struct PersistenceManager {
    /// 数据库目录
    db_dir: PathBuf,
    /// 缓冲池管理器
    buffer_manager: BufferManager,
}

impl PersistenceManager {
    pub fn new<P: AsRef<Path>>(db_dir: P) -> Result<Self> {
        let db_dir = db_dir.as_ref().to_path_buf();

        // 确保数据库目录存在
        fs::create_dir_all(&db_dir)
            .map_err(|e| DBError::IO(format!("无法创建数据库目录: {}", e)))?;

        // 数据文件路径
        let data_file = db_dir.join("data.db");

        // 创建缓冲池管理器
        let buffer_manager = BufferManager::new(data_file)?;

        Ok(Self {
            db_dir,
            buffer_manager,
        })
    }

    /// 保存数据库元数据
    pub fn save_metadata(&self, database_name: &str, catalog: &Catalog) -> Result<()> {
        let metadata_file = self.db_dir.join(format!("{}.meta", database_name));

        // 使用 bincode 2.x 序列化元数据
        let catalog_data = bincode::encode_to_vec(catalog, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("无法序列化元数据: {}", e)))?;

        // 写入文件
        let mut file = File::create(metadata_file)
            .map_err(|e| DBError::IO(format!("无法创建元数据文件: {}", e)))?;

        file.write_all(&catalog_data)
            .map_err(|e| DBError::IO(format!("无法写入元数据: {}", e)))?;

        file.flush()
            .map_err(|e| DBError::IO(format!("无法刷新元数据到磁盘: {}", e)))?;

        Ok(())
    }

    /// 加载数据库元数据
    pub fn load_metadata(&self, database_name: &str) -> Result<Catalog> {
        let metadata_file = self.db_dir.join(format!("{}.meta", database_name));

        // 检查文件是否存在
        if !metadata_file.exists() {
            return Ok(Catalog::new()); // 如果文件不存在，返回空的元数据
        }

        // 读取文件
        let mut file = File::open(metadata_file)
            .map_err(|e| DBError::IO(format!("无法打开元数据文件: {}", e)))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .map_err(|e| DBError::IO(format!("无法读取元数据: {}", e)))?;

        // 使用 bincode 2.x 反序列化
        let (catalog, _) = bincode::decode_from_slice(&buffer, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("无法解析元数据: {}", e)))?;

        Ok(catalog)
    }

    /// 检查数据库是否存在
    pub fn database_exists(&self, database_name: &str) -> bool {
        let metadata_file = self.db_dir.join(format!("{}.meta", database_name));
        metadata_file.exists()
    }

    /// 删除数据库元数据文件
    pub fn delete_metadata(&self, database_name: &str) -> Result<()> {
        let metadata_file = self.db_dir.join(format!("{}.meta", database_name));

        if metadata_file.exists() {
            fs::remove_file(metadata_file)
                .map_err(|e| DBError::IO(format!("无法删除元数据文件: {}", e)))?;
        }

        Ok(())
    }

    /// 列出所有数据库
    pub fn list_databases(&self) -> Result<Vec<String>> {
        let mut databases = Vec::new();

        let entries = fs::read_dir(&self.db_dir)
            .map_err(|e| DBError::IO(format!("无法读取数据库目录: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| DBError::IO(format!("无法读取目录项: {}", e)))?;

            let path = entry.path();
            if path.is_file() {
                if let Some(extension) = path.extension() {
                    if extension == "meta" {
                        if let Some(stem) = path.file_stem() {
                            if let Some(name) = stem.to_str() {
                                databases.push(name.to_string());
                            }
                        }
                    }
                }
            }
        }

        Ok(databases)
    }

    /// 备份数据库元数据
    pub fn backup_metadata(&self, database_name: &str, backup_path: &str) -> Result<()> {
        let metadata_file = self.db_dir.join(format!("{}.meta", database_name));

        if !metadata_file.exists() {
            return Err(DBError::NotFound(format!(
                "数据库 '{}' 不存在",
                database_name
            )));
        }

        // 读取原文件
        let data = fs::read(&metadata_file)
            .map_err(|e| DBError::IO(format!("无法读取元数据文件: {}", e)))?;

        // 写入备份文件
        fs::write(backup_path, data)
            .map_err(|e| DBError::IO(format!("无法写入备份文件: {}", e)))?;

        Ok(())
    }

    /// 从备份恢复数据库元数据
    pub fn restore_metadata(&self, database_name: &str, backup_path: &str) -> Result<()> {
        let metadata_file = self.db_dir.join(format!("{}.meta", database_name));

        // 验证备份文件是否是有效的 Catalog
        let backup_data =
            fs::read(backup_path).map_err(|e| DBError::IO(format!("无法读取备份文件: {}", e)))?;

        // 尝试反序列化以验证数据完整性
        let _: Catalog = bincode::decode_from_slice(&backup_data, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("备份文件损坏或格式不正确: {}", e)))?
            .0;

        // 复制备份文件到元数据文件
        fs::copy(backup_path, metadata_file)
            .map_err(|e| DBError::IO(format!("无法恢复元数据文件: {}", e)))?;

        Ok(())
    }

    /// 获取缓冲池管理器引用
    pub fn buffer_manager(&self) -> &BufferManager {
        &self.buffer_manager
    }

    /// 获取可变缓冲池管理器引用
    pub fn buffer_manager_mut(&mut self) -> &mut BufferManager {
        &mut self.buffer_manager
    }

    /// 刷新所有数据到磁盘
    pub fn flush_all(&mut self) -> Result<()> {
        self.buffer_manager.flush_all_pages()
    }

    /// 获取数据库目录路径
    pub fn db_dir(&self) -> &Path {
        &self.db_dir
    }

    /// 获取元数据文件路径
    pub fn get_metadata_path(&self, database_name: &str) -> PathBuf {
        self.db_dir.join(format!("{}.meta", database_name))
    }

    /// 获取元数据文件大小
    pub fn get_metadata_size(&self, database_name: &str) -> Result<u64> {
        let metadata_file = self.get_metadata_path(database_name);

        if !metadata_file.exists() {
            return Ok(0);
        }

        let metadata = fs::metadata(metadata_file)
            .map_err(|e| DBError::IO(format!("无法获取文件元数据: {}", e)))?;

        Ok(metadata.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::table::{ColumnDef, DataType};
    use tempfile::TempDir;

    #[test]
    fn test_metadata_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = PersistenceManager::new(temp_dir.path()).unwrap();

        // 创建测试目录
        let mut catalog = Catalog::new();
        let columns = vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Int(4),
            not_null: true,
            unique: true,
            is_primary: true,
        }];
        catalog
            .add_table_metadata("test_table".to_string(), columns)
            .unwrap();

        // 保存元数据
        persistence.save_metadata("test_db", &catalog).unwrap();

        // 加载元数据
        let loaded_catalog = persistence.load_metadata("test_db").unwrap();

        // 验证数据完整性
        assert_eq!(loaded_catalog.table_count(), 1);
        assert!(loaded_catalog.has_table("test_table"));
    }

    #[test]
    fn test_database_operations() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = PersistenceManager::new(temp_dir.path()).unwrap();

        // 测试空数据库列表
        let databases = persistence.list_databases().unwrap();
        assert!(databases.is_empty());

        // 创建并保存数据库
        let catalog = Catalog::new();
        persistence.save_metadata("db1", &catalog).unwrap();
        persistence.save_metadata("db2", &catalog).unwrap();

        // 测试数据库列表
        let mut databases = persistence.list_databases().unwrap();
        databases.sort();
        assert_eq!(databases, vec!["db1", "db2"]);

        // 测试数据库存在性检查
        assert!(persistence.database_exists("db1"));
        assert!(!persistence.database_exists("nonexistent"));

        // 测试删除数据库
        persistence.delete_metadata("db1").unwrap();
        assert!(!persistence.database_exists("db1"));
        assert!(persistence.database_exists("db2"));
    }

    #[test]
    fn test_backup_restore() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = PersistenceManager::new(temp_dir.path()).unwrap();

        // 创建测试数据
        let mut catalog = Catalog::new();
        let columns = vec![ColumnDef {
            name: "test_col".to_string(),
            data_type: DataType::Varchar(100),
            not_null: false,
            unique: false,
            is_primary: false,
        }];
        catalog
            .add_table_metadata("backup_test".to_string(), columns)
            .unwrap();
        persistence
            .save_metadata("test_backup_db", &catalog)
            .unwrap();

        // 备份
        let backup_path = temp_dir.path().join("backup.meta");
        persistence
            .backup_metadata("test_backup_db", backup_path.to_str().unwrap())
            .unwrap();

        // 删除原数据
        persistence.delete_metadata("test_backup_db").unwrap();
        assert!(!persistence.database_exists("test_backup_db"));

        // 恢复
        persistence
            .restore_metadata("test_backup_db", backup_path.to_str().unwrap())
            .unwrap();
        assert!(persistence.database_exists("test_backup_db"));

        // 验证恢复的数据
        let restored_catalog = persistence.load_metadata("test_backup_db").unwrap();
        assert!(restored_catalog.has_table("backup_test"));
    }
}
