use super::super::io::page::{Page, PageId};
use super::super::table::{ColumnDef, Value};
use crate::error::{DBError, Result};
use std::convert::TryFrom;
use bincode::{Encode, Decode};

/// 记录ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RecordId {
    /// 页面ID
    pub page_id: PageId,
    /// 页内偏移量
    pub slot: u16,
}

impl RecordId {
    pub fn new(page_id: PageId, slot: u16) -> Self {
        Self { page_id, slot }
    }
}

/// 存储用的记录结构（纯数据，用于序列化）
#[derive(Debug, Clone, Encode, Decode)]
struct StoredRecord {
    data: Vec<Value>,
}

/// 运行时记录结构（包含ID等运行时信息）
#[derive(Debug, Clone)]
pub struct Record {
    /// 运行时ID，不参与序列化
    id: Option<RecordId>,
    /// 记录数据
    data: Vec<Value>,
}

impl Record {
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            id: None,
            data: values,
        }
    }

    pub fn with_id(id: RecordId, values: Vec<Value>) -> Self {
        Self {
            id: Some(id),
            data: values,
        }
    }

    /// 获取记录ID
    pub fn id(&self) -> Option<RecordId> {
        self.id
    }

    /// 设置记录ID
    pub fn set_id(&mut self, id: RecordId) {
        self.id = Some(id);
    }

    /// 获取记录值
    pub fn values(&self) -> &[Value] {
        &self.data
    }

    /// 获取指定位置的值
    pub fn get_value(&self, index: usize) -> Option<&Value> {
        self.data.get(index)
    }

    /// 序列化记录（只序列化数据部分）
    pub fn serialize(&self) -> Vec<u8> {
        let stored = StoredRecord {
            data: self.data.clone(),
        };
        bincode::encode_to_vec(&stored, bincode::config::standard()).unwrap_or_else(|e| {
            panic!("序列化Record失败: {}", e);
        })
    }

    /// 反序列化记录
    pub fn deserialize(buffer: &[u8]) -> Result<Self> {
        match bincode::decode_from_slice::<StoredRecord, _>(buffer, bincode::config::standard()) {
            Ok((stored, _)) => Ok(Self {
                id: None, // ID 在反序列化时为空，需要后续设置
                data: stored.data,
            }),
            Err(e) => Err(DBError::IO(format!("反序列化Record失败: {}", e))),
        }
    }
}

/// 记录页管理器 - 负责在页面中管理记录
pub struct RecordPageManager {
    /// 页面ID
    page_id: PageId,
    /// 页内记录数
    record_count: u16,
    /// 记录槽位图，表示哪些槽位被占用
    slot_bitmap: Vec<bool>,
    /// 记录在页面中的偏移量
    offsets: Vec<u16>,
}

impl RecordPageManager {
    /// 创建新的记录页管理器
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            record_count: 0,
            slot_bitmap: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// 从页面中加载记录页管理器
    pub fn load_from_page(page: &Page) -> Result<Self> {
        let page_id = page.id();
        let data = page.data();

        if data.len() < 2 {
            return Err(DBError::IO("页面数据不完整".to_string()));
        }

        // 读取记录数
        let mut record_count_bytes = [0u8; 2];
        record_count_bytes.copy_from_slice(&data[0..2]);
        let record_count = u16::from_le_bytes(record_count_bytes);

        // 读取槽位图和偏移量表
        let mut slot_bitmap = Vec::with_capacity(record_count as usize);
        let mut offsets = Vec::with_capacity(record_count as usize);

        let bitmap_bytes = (record_count + 7) / 8; // 每8个槽位占用1个字节
        let bitmap_start = 2;
        let bitmap_end = bitmap_start + bitmap_bytes as usize;

        if data.len() < bitmap_end {
            return Err(DBError::IO("页面数据不完整".to_string()));
        }

        // 解析槽位图
        for i in 0..record_count {
            let byte_index = (i / 8) as usize;
            let bit_index = (i % 8) as usize;
            let is_used = (data[bitmap_start + byte_index] & (1 << bit_index)) != 0;
            slot_bitmap.push(is_used);
        }

        // 读取偏移量表
        let offsets_start = bitmap_end;
        let offsets_end = offsets_start + (record_count as usize * 2);

        if data.len() < offsets_end {
            return Err(DBError::IO("页面数据不完整".to_string()));
        }

        for i in 0..record_count {
            let offset_pos = offsets_start + (i as usize * 2);
            let mut offset_bytes = [0u8; 2];
            offset_bytes.copy_from_slice(&data[offset_pos..offset_pos + 2]);
            let offset = u16::from_le_bytes(offset_bytes);
            offsets.push(offset);
        }

        Ok(Self {
            page_id,
            record_count,
            slot_bitmap,
            offsets,
        })
    }

    /// 保存记录页管理器到页面
    pub fn save_to_page(&self, page: &mut Page) -> Result<()> {
        let mut buffer = Vec::new();

        // 写入记录数
        buffer.extend_from_slice(&self.record_count.to_le_bytes());

        // 写入槽位图
        let mut bitmap_bytes = vec![0u8; ((self.record_count + 7) / 8) as usize];
        for (i, &is_used) in self.slot_bitmap.iter().enumerate() {
            if is_used {
                let byte_index = i / 8;
                let bit_index = i % 8;
                bitmap_bytes[byte_index] |= 1 << bit_index;
            }
        }
        buffer.extend_from_slice(&bitmap_bytes);

        // 写入偏移量表
        for &offset in &self.offsets {
            buffer.extend_from_slice(&offset.to_le_bytes());
        }

        // 写入页面
        page.write_data(0, &buffer);

        Ok(())
    }

    /// 插入记录到页面
    pub fn insert_record(&mut self, page: &mut Page, record: &Record) -> Result<RecordId> {
        let record_data = record.serialize();
        let record_size = record_data.len();

        // 检查页面剩余空间
        let header_size = self.calc_header_size(self.record_count + 1);
        let available_space = page.data().len() - header_size;

        if record_size > available_space {
            return Err(DBError::IO("页面空间不足".to_string()));
        }

        // 寻找空闲槽位
        let slot = if let Some(pos) = self.slot_bitmap.iter().position(|&used| !used) {
            // 使用现有的空闲槽位
            pos as u16
        } else {
            // 创建新槽位
            let slot = self.record_count;
            self.record_count += 1;
            self.slot_bitmap.push(true);
            self.offsets.push(header_size as u16);
            slot
        };

        // 更新槽位状态
        if slot < self.slot_bitmap.len() as u16 {
            self.slot_bitmap[slot as usize] = true;
        }

        // 记录写入位置
        let offset = if slot < self.offsets.len() as u16 {
            self.offsets[slot as usize]
        } else {
            header_size as u16
        };

        // 写入记录数据
        page.write_data(offset as usize, &record_data);

        // 更新页面头部信息
        self.save_to_page(page)?;

        // 返回记录ID
        Ok(RecordId::new(page.id(), slot))
    }

    /// 删除记录
    pub fn delete_record(&mut self, page: &mut Page, slot: u16) -> Result<()> {
        if slot >= self.record_count {
            return Err(DBError::NotFound(format!("记录槽位 {} 不存在", slot)));
        }

        // 标记槽位为未使用
        self.slot_bitmap[slot as usize] = false;

        // 更新页面头部信息
        self.save_to_page(page)?;

        Ok(())
    }

    /// 获取记录
    pub fn get_record(&self, page: &Page, slot: u16) -> Result<Record> {
        if slot >= self.record_count || !self.slot_bitmap[slot as usize] {
            return Err(DBError::NotFound(format!(
                "记录槽位 {} 不存在或已删除",
                slot
            )));
        }

        let offset = self.offsets[slot as usize] as usize;
        let record_data = page.read_data(offset, page.size() - offset);

        let mut record = Record::deserialize(record_data)?;
        record.set_id(RecordId::new(page.id(), slot));

        Ok(record)
    }

    /// 替换记录
    pub fn replace_record(
        &mut self,
        page: &mut Page,
        id: RecordId,
        new_record: &Record,
    ) -> Result<()> {
        let slot = id.slot;
        // 检查槽位是否有效
        if slot >= self.record_count || !self.slot_bitmap[slot as usize] {
            return Err(DBError::NotFound(format!(
                "记录槽位 {} 不存在或已删除",
                slot
            )));
        }

        // 序列化新记录
        let new_record_data = new_record.serialize();
        let new_record_size = new_record_data.len();

        // 计算更新后的页面头部大小
        let new_header_size = self.calc_header_size(self.record_count);
        // 计算页面剩余空间
        let available_space = page.data().len() - new_header_size;

        // 检查新记录大小是否超出可用空间
        if new_record_size > available_space {
            return Err(DBError::IO("页面空间不足，无法替换记录".to_string()));
        }

        // 获取旧记录的偏移量
        let old_offset = self.offsets[slot as usize] as usize;

        // 写入新记录数据
        page.write_data(old_offset, &new_record_data);

        // 更新页面头部信息
        self.save_to_page(page)?;

        Ok(())
    }

    /// 计算页面头部大小
    fn calc_header_size(&self, record_count: u16) -> usize {
        let bitmap_size = ((record_count + 7) / 8) as usize; // 槽位图大小
        let offsets_size = record_count as usize * 2; // 偏移量表大小

        2 + bitmap_size + offsets_size // 记录数(2字节) + 槽位图 + 偏移量表
    }

    /// 获取记录总数
    pub fn get_record_count(&self) -> u16 {
        self.record_count
    }

    /// 检查槽位是否被使用
    pub fn is_slot_used(&self, slot: u16) -> bool {
        if slot >= self.record_count {
            return false;
        }
        self.slot_bitmap[slot as usize]
    }
}
