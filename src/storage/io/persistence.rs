use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};
use crate::error::{DBError, Result};
use super::buffer_manager::BufferManager;
use crate::storage::catalog::Catalog;

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
        let buffer_manager = BufferManager::new(data_file.to_str().unwrap())?;
        
        Ok(Self {
            db_dir,
            buffer_manager,
        })
    }
    
    /// 保存数据库元数据
    pub fn save_metadata(&self, database_name: &str, catalog: &Catalog) -> Result<()> {
        let metadata_file = self.db_dir.join(format!("{}.meta", database_name));
        
        // 序列化元数据
        let catalog_data = serde_json::to_string(catalog)
            .map_err(|e| DBError::IO(format!("无法序列化元数据: {}", e)))?;
        
        // 写入文件
        let mut file = File::create(metadata_file)
            .map_err(|e| DBError::IO(format!("无法创建元数据文件: {}", e)))?;
        
        file.write_all(catalog_data.as_bytes())
            .map_err(|e| DBError::IO(format!("无法写入元数据: {}", e)))?;
        
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
        
        let mut content = String::new();
        file.read_to_string(&mut content)
            .map_err(|e| DBError::IO(format!("无法读取元数据: {}", e)))?;
        
        // 反序列化
        let catalog = serde_json::from_str(&content)
            .map_err(|e| DBError::IO(format!("无法解析元数据: {}", e)))?;
        
        Ok(catalog)
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
}