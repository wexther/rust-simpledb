use crate::{error::{DBError, Result}, storage::table::{Record, RecordId}};

/// 页面大小（默认4KB，可根据需要调整）
pub const PAGE_SIZE: usize = 4096;

/// 页ID类型
pub type PageId = u32;

/// 重新导入 Value 类型
use crate::storage::table::Value;
type RawRecord = Vec<Value>;

/// 页面 - 直接存储记录数组
#[derive(Debug, Clone)]
pub struct Page {
    /// 页面ID
    id: PageId,
    /// 记录数组
    records: Vec<Option<RawRecord>>,
    /// 是否已被修改
    is_dirty: bool,
}

impl Page {
    /// 创建新的空页面
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            records: Vec::new(),
            is_dirty: false,
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
        })
    }

    /// 获取页面ID
    pub fn id(&self) -> PageId {
        self.id
    }

    /// 序列化页面数据
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::encode_to_vec(&self.records, bincode::config::standard())
            .map_err(|e| DBError::IO(format!("序列化页面数据失败: {}", e)))
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
        let slot = if let Some(slot) = self.records.iter().position(|r| r.is_none()) {
            slot
        } else {
            if !self.can_fit(1)? {
                return Err(DBError::IO("页面空间不足".to_string()));
            }
            self.records.push(None);
            self.records.len() - 1
        };

        self.records[slot] = Some(raw_record);
        self.is_dirty = true;
        
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
    
    /// 替换记录 - 使用 RecordId
    pub fn replace_record(&mut self, id: RecordId, new_raw_record: RawRecord) -> Result<()> {
        if id.page_id != self.id {
            return Err(DBError::IO("RecordId 的页面ID不匹配".to_string()));
        }

        let slot = id.slot;
        if slot >= self.records.len() || self.records[slot].is_none() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        self.records[slot] = Some(new_raw_record);
        self.is_dirty = true;
        Ok(())
    }

    /// 更新字段 - 使用 RecordId
    pub fn update_field(&mut self, id: RecordId, field_index: usize, new_value: Value) -> Result<()> {
        if id.page_id != self.id {
            return Err(DBError::IO("RecordId 的页面ID不匹配".to_string()));
        }

        let slot = id.slot;
        if slot >= self.records.len() {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        let record = self.records[slot]
            .as_mut()
            .ok_or_else(|| DBError::NotFound(format!("记录槽位 {} 已被删除", slot)))?;

        if field_index >= record.len() {
            return Err(DBError::IO(format!("字段索引 {} 超出范围", field_index)));
        }

        record[field_index] = new_value;
        self.is_dirty = true;
        Ok(())
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
    
    /// 更精确的容量检查
    pub fn can_fit_record(&self, record: &RawRecord) -> Result<bool> {
        let current_size = self.serialize()?.len();
        let record_size = Self::estimate_record_size(record);
        let new_size = current_size + record_size + 8; // +8 for Option overhead
        
        Ok(new_size <= PAGE_SIZE)
    }

    // 保留一些内部使用的 slot 方法（私有或仅供内部使用）
    fn is_slot_used(&self, slot: usize) -> bool {
        slot < self.records.len() && self.records[slot].is_some()
    }
}
