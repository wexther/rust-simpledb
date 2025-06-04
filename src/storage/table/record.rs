use super::super::io::page::PageId;
use super::super::table::Value;
use bincode::{Decode, Encode};

/// 记录ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RecordId {
    /// 页面ID
    pub page_id: PageId,
    /// 页内索引
    pub slot: usize,
}

impl RecordId {
    pub fn new(page_id: PageId, slot: usize) -> Self {
        Self { page_id, slot }
    }
}

pub type RawRecord = Vec<Value>;

/// 运行时记录结构（包含ID等运行时信息）
#[derive(Debug, Clone)]
pub struct Record {
    /// 运行时ID，不参与序列化
    id: Option<RecordId>,
    /// 记录数据
    data: RawRecord,
}

impl Record {
    pub fn new(values: RawRecord) -> Self {
        Self {
            id: None,
            data: values,
        }
    }

    pub fn with_id(id: RecordId, values: RawRecord) -> Self {
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
    pub fn value_at(&self, index: usize) -> Option<&Value> {
        self.data.get(index)
    }

    /// 获取原始记录数据
    pub fn raw_data(&self) -> &RawRecord {
        &self.data
    }
}
