use std::collections::HashMap;
use crate::error::{DBError, Result};
use super::io::page::{PageId, Page};
use super::io::buffer_manager::BufferManager;
use super::record::{Record, RecordId, RecordPageManager};
use serde::{Serialize, Deserialize};

/// 表示列定义的结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_primary_key: bool,
}

/// 表示数据类型的枚举
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Varchar(usize),
}

/// 表示值的枚举
#[derive(Debug, Clone)]
pub enum Value {
    Int(i32),
    String(String),
    Null,
}

impl Value {
    /// 序列化值到缓冲区
    pub fn serialize(&self, buffer: &mut Vec<u8>) {
        match self {
            Value::Int(n) => {
                buffer.push(1); // 类型标记
                buffer.extend_from_slice(&n.to_le_bytes());
            }
            Value::String(s) => {
                buffer.push(2); // 类型标记
                let bytes = s.as_bytes();
                buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buffer.extend_from_slice(bytes);
            }
            Value::Null => {
                buffer.push(0); // 类型标记
            }
        }
    }
    
    /// 从缓冲区反序列化值
    pub fn deserialize(buffer: &[u8]) -> Result<(Self, usize)> {
        if buffer.is_empty() {
            return Err(DBError::IO("值数据不完整".to_string()));
        }
        
        let type_tag = buffer[0];
        let mut pos = 1;
        
        match type_tag {
            0 => Ok((Value::Null, pos)),
            1 => {
                if buffer.len() < pos + 4 {
                    return Err(DBError::IO("整数值数据不完整".to_string()));
                }
                
                let mut int_bytes = [0u8; 4];
                int_bytes.copy_from_slice(&buffer[pos..pos + 4]);
                let value = i32::from_le_bytes(int_bytes);
                pos += 4;
                
                Ok((Value::Int(value), pos))
            }
            2 => {
                if buffer.len() < pos + 4 {
                    return Err(DBError::IO("字符串值数据不完整".to_string()));
                }
                
                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&buffer[pos..pos + 4]);
                let str_len = u32::from_le_bytes(len_bytes) as usize;
                pos += 4;
                
                if buffer.len() < pos + str_len {
                    return Err(DBError::IO("字符串值数据不完整".to_string()));
                }
                
                let string_data = &buffer[pos..pos + str_len];
                let value = String::from_utf8(string_data.to_vec())
                    .map_err(|_| DBError::IO("无效的UTF-8字符串".to_string()))?;
                pos += str_len;
                
                Ok((Value::String(value), pos))
            }
            _ => Err(DBError::IO(format!("未知的值类型标记: {}", type_tag))),
        }
    }
}

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
        let primary_key_index = columns.iter()
            .position(|col| col.is_primary_key);
        
        Self {
            name,
            columns,
            page_ids: Vec::new(),
            primary_key_index,
        }
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
    pub fn insert_record(&mut self, buffer_manager: &mut BufferManager, values: Vec<Value>) -> Result<RecordId> {
        // 验证值的数量与列数是否匹配
        if values.len() != self.columns.len() {
            return Err(DBError::Schema(format!(
                "值的数量({})与列数({})不匹配",
                values.len(), self.columns.len()
            )));
        }
        
        // 创建记录
        let record = Record::new(values);
        
        // 尝试在现有页面中插入
        for &page_id in &self.page_ids {
            let mut page = buffer_manager.get_page_mut(page_id)?;
            let mut page_manager = RecordPageManager::load_from_page(&page)?;
            
            // 尝试插入记录
            match page_manager.insert_record(&mut page, &record) {
                Ok(record_id) => return Ok(record_id),
                Err(_) => continue, // 这个页面满了，尝试下一个
            }
        }
        
        // 所有现有页面都已满，创建新页面
        let new_page_id = buffer_manager.create_page()?;
        self.page_ids.push(new_page_id);
        
        // 在新页面中插入记录
        let mut page = buffer_manager.get_page_mut(new_page_id)?;
        let mut page_manager = RecordPageManager::new(new_page_id);
        
        page_manager.insert_record(&mut page, &record)
    }
    
    /// 删除记录
    pub fn delete_record(&mut self, buffer_manager: &mut BufferManager, id: RecordId) -> Result<()> {
        if !self.page_ids.contains(&id.page_id) {
            return Err(DBError::NotFound(format!("页面 {} 不属于表 {}", id.page_id, self.name)));
        }
        
        let mut page = buffer_manager.get_page_mut(id.page_id)?;
        let mut page_manager = RecordPageManager::load_from_page(&page)?;
        
        page_manager.delete_record(&mut page, id.slot)
    }
    
    /// 获取记录
    pub fn get_record(&self, buffer_manager: &mut BufferManager, id: RecordId) -> Result<Record> {
        if !self.page_ids.contains(&id.page_id) {
            return Err(DBError::NotFound(format!("页面 {} 不属于表 {}", id.page_id, self.name)));
        }
        
        let page = buffer_manager.get_page(id.page_id)?;
        let page_manager = RecordPageManager::load_from_page(page)?;
        
        page_manager.get_record(page, id.slot)
    }
    
    /// 获取表中所有记录
    pub fn get_all_records(&self, buffer_manager: &mut BufferManager) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        
        for &page_id in &self.page_ids {
            let page = buffer_manager.get_page(page_id)?;
            let page_manager = RecordPageManager::load_from_page(page)?;
            
            // 使用公共方法获取记录数量
            let record_count = page_manager.get_record_count();
            
            // 逐个槽位检查并获取记录
            for slot in 0..record_count {
                // 使用公共方法检查槽位是否有效
                if page_manager.is_slot_used(slot) {
                    match page_manager.get_record(page, slot) {
                        Ok(record) => records.push(record),
                        Err(_) => continue, // 跳过无法读取的记录
                    }
                }
            }
        }
        
        Ok(records)
    }
    
    /// 从磁盘加载表数据
    pub fn load(&mut self, buffer_manager: &mut BufferManager, page_ids: Vec<PageId>) -> Result<()> {
        self.page_ids = page_ids;
        Ok(())
    }
    
    /// 获取表的页面ID列表
    pub fn page_ids(&self) -> &[PageId] {
        &self.page_ids
    }
}