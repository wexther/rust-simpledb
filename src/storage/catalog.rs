use super::io::page::PageId;
use super::table::ColumnDef;
use crate::error::{DBError, Result};
use bincode::{Decode, Encode};
use std::collections::HashMap;

/// 目录 - 存储数据库模式信息（表结构、列定义等元数据）
#[derive(Debug, Clone, Encode, Decode)]
pub struct Catalog {
    /// 表元数据，存储表名与其对应的列定义
    tables: HashMap<String, TableMetadata>,
}

/// 表的元数据信息
#[derive(Debug, Clone, Encode, Decode)]
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

    /// 使用 bincode 2.x 序列化目录
    pub fn serialize(&self) -> Vec<u8> {
        bincode::encode_to_vec(self, bincode::config::standard()).unwrap_or_else(|e| {
            panic!("序列化Catalog失败: {}", e);
        })
    }

    /// 使用 bincode 2.x 反序列化目录
    pub fn deserialize(buffer: &[u8]) -> Result<Self> {
        match bincode::decode_from_slice(buffer, bincode::config::standard()) {
            Ok((catalog, _)) => Ok(catalog),
            Err(e) => Err(DBError::IO(format!("反序列化Catalog失败: {}", e))),
        }
    }

    /// 保存目录到文件
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let serialized = self.serialize();
        std::fs::write(path, serialized)
            .map_err(|e| DBError::IO(format!("保存目录文件失败: {}", e)))
    }

    /// 从文件加载目录
    pub fn load_from_file(path: &str) -> Result<Self> {
        let buffer =
            std::fs::read(path).map_err(|e| DBError::IO(format!("读取目录文件失败: {}", e)))?;
        Self::deserialize(&buffer)
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::table::{ColumnDef, DataType};

    #[test]
    fn test_catalog_serialization() {
        let mut catalog = Catalog::new();

        // 添加一些测试数据
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int(4),
                not_null: true,
                unique: true,
                is_primary: true,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar(255),
                not_null: false,
                unique: false,
                is_primary: false,
            },
        ];

        catalog
            .add_table_metadata("test_table".to_string(), columns)
            .unwrap();
        catalog.add_table_page_id("test_table", 1).unwrap();
        catalog.add_table_page_id("test_table", 2).unwrap();

        // 测试序列化
        let serialized = catalog.serialize();
        assert!(!serialized.is_empty());

        // 测试反序列化
        let deserialized = Catalog::deserialize(&serialized).unwrap();

        // 验证数据完整性
        assert_eq!(deserialized.table_count(), 1);
        assert!(deserialized.has_table("test_table"));

        let columns = deserialized.get_table_columns("test_table").unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");

        let page_ids = deserialized.get_table_page_ids("test_table").unwrap();
        assert_eq!(page_ids, vec![1, 2]);
    }

    #[test]
    fn test_catalog_file_operations() {
        let mut catalog = Catalog::new();
        let columns = vec![ColumnDef {
            name: "test_col".to_string(),
            data_type: DataType::Int(4),
            not_null: true,
            unique: false,
            is_primary: false,
        }];

        catalog
            .add_table_metadata("file_test_table".to_string(), columns)
            .unwrap();

        // 测试保存到文件
        let temp_path = "/tmp/test_catalog.bin";
        catalog.save_to_file(temp_path).unwrap();

        // 测试从文件加载
        let loaded_catalog = Catalog::load_from_file(temp_path).unwrap();
        assert_eq!(loaded_catalog.table_count(), 1);
        assert!(loaded_catalog.has_table("file_test_table"));

        // 清理测试文件
        std::fs::remove_file(temp_path).ok();
    }
}
