use crate::{
    error::{DBError, Result},
    storage::table::{Record, RecordId},
};

/// 页面大小（增加到32KB以提供更多缓冲空间）
pub const PAGE_SIZE: usize = 32768;

/// 页ID类型
pub type PageId = u32;

/// 重新导入 Value 类型
use crate::storage::table::Value;
type RawRecord = Vec<Value>;

/// 页面 - 直接存储记录数组，添加缓存优化
#[derive(Debug, Clone)]
pub struct Page {
    /// 页面ID
    id: PageId,
    /// 记录数组
    records: Vec<Option<RawRecord>>,
    /// 是否已被修改
    is_dirty: bool,
    /// 缓存的序列化大小（用于快速容量检查）
    cached_size: Option<usize>,
}

impl Page {
    /// 创建新的空页面
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            records: Vec::new(),
            is_dirty: false,
            cached_size: None,
        }
    }

    /// 从序列化数据创建页面
    pub fn from_data(id: PageId, data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Ok(Self::new(id));
        }

        let records = bincode::decode_from_slice::<Vec<Option<RawRecord>>, _>(
            data,
            bincode::config::standard(),
        )
        .map_err(|e| DBError::IO(format!("反序列化页面数据失败: {}", e)))?
        .0;

        Ok(Self {
            id,
            records,
            is_dirty: false,
            cached_size: None,
        })
    }

    /// 获取页面ID
    pub fn id(&self) -> PageId {
        self.id
    }

    /// 序列化页面数据（优化版本，使用缓存）
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(&self.records, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("序列化页面数据失败: {}", e)))
    }

    /// 获取当前页面序列化后的大小（使用缓存优化）
    pub fn get_serialized_size(&mut self) -> Result<usize> {
        if let Some(size) = self.cached_size {
            if !self.is_dirty {
                return Ok(size);
            }
        }
        
        let serialized = self.serialize()?;
        let size = serialized.len();
        self.cached_size = Some(size);
        Ok(size)
    }

    /// 清除缓存
    fn clear_cache(&mut self) {
        self.cached_size = None;
    }

    /// 检查页面是否被修改过
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// 清除修改标记
    pub fn clear_dirty(&mut self) {
        self.is_dirty = false;
    }

    /// 检查是否可以容纳更多记录
    pub fn can_fit(&self, additional_records_num: usize) -> Result<bool> {
        let mut test_records = self.records.clone();
        for _ in 0..additional_records_num {
            test_records.push(None);
        }

        let test_size = bincode::encode_to_vec(&test_records, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("估算页面大小失败: {}", e)))?
            .len();

        Ok(test_size <= PAGE_SIZE)
    }

    // ==================== 记录操作方法 ====================

    /// 插入记录 - 返回完整的 RecordId
    pub fn insert_record(&mut self, raw_record: RawRecord) -> Result<RecordId> {
        // 首先检查记录是否能放入当前页面
        if !self.can_fit_record(&raw_record)? {
            return Err(DBError::IO("页面空间不足，需要新页面".to_string()));
        }

        let slot = if let Some(slot) = self.records.iter().position(|r| r.is_none()) {
            slot
        } else {
            self.records.push(None);
            self.records.len() - 1
        };

        self.records[slot] = Some(raw_record);
        self.is_dirty = true;
        self.clear_cache(); // 清除缓存

        // 直接返回 RecordId
        Ok(RecordId::new(self.id, slot))
    }

    /// 删除记录 - 使用 RecordId
    pub fn delete_record(&mut self, id: RecordId) -> Result<()> {
        // 验证页面ID
        if id.page_id != self.id {
            return Err(DBError::IO("RecordId 的页面ID不匹配".to_string()));
        }

        let slot = id.slot;
        if slot >= self.records.len() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        if self.records[slot].is_none() {
            return Err(DBError::NotFound(format!("记录槽位 {} 已被删除", slot)));
        }

        self.records[slot] = None;
        self.is_dirty = true;
        self.clear_cache(); // 清除缓存
        Ok(())
    }

    /// 获取记录 - 使用 RecordId
    pub fn get_record(&self, id: RecordId) -> Result<Record> {
        // 验证页面ID
        if id.page_id != self.id {
            return Err(DBError::IO("RecordId 的页面ID不匹配".to_string()));
        }

        let slot = id.slot;
        if slot >= self.records.len() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        let raw_record = self.records[slot]
            .as_ref()
            .ok_or_else(|| DBError::NotFound(format!("记录槽位 {} 已被删除", slot)))?;

        Ok(Record::with_id(id, raw_record.clone()))
    }

    /// 获取原始记录数据
    pub fn get_raw_record(&self, slot: usize) -> Result<&RawRecord> {
        if slot >= self.records.len() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        self.records[slot]
            .as_ref()
            .ok_or_else(|| DBError::NotFound(format!("记录槽位 {} 已被删除", slot)))
    }

    /// 获取记录数量
    pub fn get_record_count(&self) -> usize {
        self.records.iter().filter(|r| r.is_some()).count()
    }

    /// 替换记录 - 使用 RecordId（带容量检查）
    pub fn replace_record(&mut self, id: RecordId, new_raw_record: RawRecord) -> Result<()> {
        if id.page_id != self.id {
            return Err(DBError::IO("RecordId 的页面ID不匹配".to_string()));
        }

        let slot = id.slot;
        if slot >= self.records.len() || self.records[slot].is_none() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        // 容量检查：计算替换后的页面大小
        let mut test_records = self.records.clone();
        test_records[slot] = Some(new_raw_record.clone());

        let new_size = bincode::encode_to_vec(&test_records, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("估算页面大小失败: {}", e)))?
            .len();

        // 增加一些缓冲空间以避免边界情况
        let max_allowed_size = PAGE_SIZE - 1024; // 保留1KB的缓冲空间
        
        if new_size > max_allowed_size {
            return Err(DBError::IO(format!(
                "替换记录后页面大小({} bytes)将超出安全限制({} bytes)，需要重新分配到新页面",
                new_size, max_allowed_size
            )));
        }

        // 执行替换
        self.records[slot] = Some(new_raw_record);
        self.is_dirty = true;
        Ok(())
    }

    /// 更新字段 - 使用 RecordId（带容量检查）
    pub fn update_field(
        &mut self,
        id: RecordId,
        field_index: usize,
        new_value: Value,
    ) -> Result<()> {
        if id.page_id != self.id {
            return Err(DBError::IO("RecordId 的页面ID不匹配".to_string()));
        }

        let slot = id.slot;
        if slot >= self.records.len() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        let record = self.records[slot]
            .as_ref()
            .ok_or_else(|| DBError::NotFound(format!("记录槽位 {} 已被删除", slot)))?;

        if field_index >= record.len() {
            return Err(DBError::IO(format!("字段索引 {} 超出范围", field_index)));
        }

        // 容量检查：创建测试记录
        let mut test_record = record.clone();
        test_record[field_index] = new_value.clone();

        let mut test_records = self.records.clone();
        test_records[slot] = Some(test_record);

        let new_size = bincode::encode_to_vec(&test_records, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("估算页面大小失败: {}", e)))?
            .len();

        if new_size > PAGE_SIZE {
            return Err(DBError::IO(format!(
                "更新字段后页面大小({} bytes)超出限制({} bytes)",
                new_size, PAGE_SIZE
            )));
        }

        // 执行更新
        let record = self.records[slot]
            .as_mut()
            .ok_or_else(|| DBError::NotFound(format!("记录槽位 {} 已被删除", slot)))?;
        record[field_index] = new_value;
        self.is_dirty = true;
        Ok(())
    }

    /// 批量更新字段 - 减少重复的容量检查
    pub fn update_fields(&mut self, id: RecordId, updates: Vec<(usize, Value)>) -> Result<()> {
        if id.page_id != self.id {
            return Err(DBError::IO("RecordId 的页面ID不匹配".to_string()));
        }

        let slot = id.slot;
        if slot >= self.records.len() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        let record = self.records[slot]
            .as_ref()
            .ok_or_else(|| DBError::NotFound(format!("记录槽位 {} 已被删除", slot)))?;

        // 创建更新后的记录副本
        let mut updated_record = record.clone();
        for (field_index, new_value) in &updates {
            if *field_index >= updated_record.len() {
                return Err(DBError::IO(format!("字段索引 {} 超出范围", field_index)));
            }
            updated_record[*field_index] = new_value.clone();
        }

        // 容量检查
        let mut test_records = self.records.clone();
        test_records[slot] = Some(updated_record.clone());

        let new_size = bincode::encode_to_vec(&test_records, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("估算页面大小失败: {}", e)))?
            .len();

        if new_size > PAGE_SIZE {
            return Err(DBError::IO(format!(
                "批量更新后页面大小({} bytes)超出限制({} bytes)",
                new_size, PAGE_SIZE
            )));
        }

        // 执行批量更新
        let record_mut = self.records[slot]
            .as_mut()
            .ok_or(DBError::IO(format!("记录槽位 {} 已被删除", slot)))?;
        for (field_index, new_value) in updates {
            record_mut[field_index] = new_value;
        }

        self.is_dirty = true;
        Ok(())
    }

    /// 高效的容量检查 - 避免完整克隆
    pub fn can_fit_record_update(&self, slot: usize, new_record: &RawRecord) -> Result<bool> {
        if slot >= self.records.len() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        // 计算旧记录大小
        let old_record_size = if let Some(old_record) = &self.records[slot] {
            Self::estimate_record_size(old_record)
        } else {
            return Err(DBError::NotFound(format!("记录槽位 {} 已被删除", slot)));
        };

        // 计算新记录大小
        let new_record_size = Self::estimate_record_size(new_record);

        // 计算当前页面大小
        let current_size = self.serialize()?.len();

        // 估算更新后的大小
        let estimated_new_size = current_size - old_record_size + new_record_size;

        Ok(estimated_new_size <= PAGE_SIZE)
    }

    /// 安全的记录替换 - 先检查容量
    pub fn try_replace_record(&mut self, id: RecordId, new_raw_record: RawRecord) -> Result<()> {
        if id.page_id != self.id {
            return Err(DBError::IO("RecordId 的页面ID不匹配".to_string()));
        }

        let slot = id.slot;

        // 先进行快速容量检查
        if !self.can_fit_record_update(slot, &new_raw_record)? {
            return Err(DBError::IO("替换记录后页面大小将超出限制".to_string()));
        }

        // 如果快速检查通过，进行精确检查
        self.replace_record(id, new_raw_record)
    }

    /// 获取页面剩余容量（字节）
    pub fn get_remaining_capacity(&self) -> Result<usize> {
        let current_size = self.serialize()?.len();
        Ok(PAGE_SIZE.saturating_sub(current_size))
    }

    /// 获取页面使用率
    pub fn get_utilization(&self) -> Result<f64> {
        let current_size = self.serialize()?.len();
        Ok(current_size as f64 / PAGE_SIZE as f64)
    }

    /// 检查记录是否存在 - 使用 RecordId
    pub fn is_record_exists(&self, id: RecordId) -> bool {
        if id.page_id != self.id {
            return false;
        }

        let slot = id.slot;
        slot < self.records.len() && self.records[slot].is_some()
    }

    /// 迭代器 - 返回 RecordId 和 Record
    pub fn iter_records(&self) -> impl Iterator<Item = (RecordId, Record)> + '_ {
        self.records
            .iter()
            .enumerate()
            .filter_map(|(slot, opt_record)| {
                opt_record.as_ref().map(|raw_record| {
                    let record_id = RecordId::new(self.id, slot);
                    let record = Record::with_id(record_id, raw_record.clone());
                    (record_id, record)
                })
            })
    }

    /// 获取所有有效记录的ID
    pub fn get_all_record_ids(&self) -> Vec<RecordId> {
        self.records
            .iter()
            .enumerate()
            .filter_map(|(slot, opt_record)| {
                if opt_record.is_some() {
                    Some(RecordId::new(self.id, slot))
                } else {
                    None
                }
            })
            .collect()
    }

    /// 估算单条记录的序列化大小
    pub fn estimate_record_size(record: &RawRecord) -> usize {
        bincode::encode_to_vec(record, bincode::config::standard())
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// 更精确且高效的容量检查
    pub fn can_fit_record(&self, record: &RawRecord) -> Result<bool> {
        // 快速估算，避免完整序列化
        let record_size = Self::estimate_record_size(record);
        let estimated_overhead = 64; // Option<T> 和 Vec 的开销
        let safety_margin = 2048; // 2KB安全边距
        
        // 使用当前记录数来估算页面使用情况
        let active_records = self.records.iter().filter(|r| r.is_some()).count();
        let estimated_current_size = active_records * 100 + 1024; // 粗略估算
        
        let estimated_new_size = estimated_current_size + record_size + estimated_overhead;
        
        // 如果快速检查失败，进行精确检查
        if estimated_new_size > PAGE_SIZE - safety_margin {
            // 只有在必要时才进行精确的序列化检查
            let current_size = self.serialize()?.len();
            let new_size = current_size + record_size + estimated_overhead;
            Ok(new_size <= PAGE_SIZE - safety_margin)
        } else {
            Ok(true)
        }
    }

    // // 保留一些内部使用的 slot 方法（私有或仅供内部使用）
    // fn is_slot_used(&self, slot: usize) -> bool {
    //     slot < self.records.len() && self.records[slot].is_some()
    // }
}
