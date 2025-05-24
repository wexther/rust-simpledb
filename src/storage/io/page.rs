use std::cmp::{min,max};
use std::fmt;
use crate::error::Result;

/// 页面大小（默认4KB，可根据需要调整）
pub const PAGE_SIZE: usize = 4096;

/// 页ID类型
pub type PageId = u32;

/// 页面 - 数据存储的基本单位
#[derive(Clone)]
pub struct Page {
    /// 页面ID
    id: PageId,
    /// 页面数据
    data: [u8; PAGE_SIZE],
    /// 数据有效长度
    size: usize,
    /// 是否已被修改
    is_dirty: bool,
}

impl Page {
    /// 创建新的空页面
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: [0; PAGE_SIZE],
            size: 0,
            is_dirty: false,
        }
    }
    
    /// 创建包含数据的页面
    pub fn with_data(id: PageId, data: &[u8]) -> Self {
        let mut page = Self::new(id);
        page.write_data(0, data);
        page
    }
    
    /// 获取页面ID
    pub fn id(&self) -> PageId {
        self.id
    }
    
    /// 获取页面数据
    pub fn data(&self) -> &[u8] {
        &self.data[0..self.size]
    }
    
    /// 获取可变数据引用
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data[0..PAGE_SIZE]
    }
    
    /// 获取页面大小
    pub fn size(&self) -> usize {
        self.size
    }
    
    /// 检查页面是否被修改过
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }
    
    /// 将页面标记为已修改
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }
    
    /// 清除修改标记
    pub fn clear_dirty(&mut self) {
        self.is_dirty = false;
    }
    
    /// 从指定位置读取数据
    pub fn read_data(&self, offset: usize, len: usize) -> &[u8] {
        let end = min(offset + len, min(self.size, PAGE_SIZE));
        let start = min(offset, end);
        &self.data[start..end]
    }
    
    /// 向指定位置写入数据
    pub fn write_data(&mut self, offset: usize, data: &[u8]) -> usize {
        if offset >= PAGE_SIZE {
            return 0;
        }
        
        let bytes_to_write = min(data.len(), PAGE_SIZE - offset);
        self.data[offset..offset + bytes_to_write].copy_from_slice(&data[0..bytes_to_write]);
        self.size = min(max(offset + bytes_to_write, self.size), PAGE_SIZE);
        self.is_dirty = true;
        
        bytes_to_write
    }
    
    /// 清空页面数据
    pub fn clear(&mut self) {
        self.data.fill(0);
        self.size = 0;
        self.is_dirty = true;
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page {{ id: {}, size: {}, dirty: {} }}", self.id, self.size, self.is_dirty)
    }
}