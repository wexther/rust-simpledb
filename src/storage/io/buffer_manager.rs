use super::disk_manager::DiskManager;
use super::page::{PAGE_SIZE, Page, PageId};
use crate::error::{DBError, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// 缓冲池大小（可以根据需要调整）
const BUFFER_POOL_SIZE: usize = 1024;

/// 缓冲池管理器 - 负责页面的缓存和置换
pub struct BufferManager {
    /// 磁盘管理器
    disk_manager: DiskManager,
    /// 页面缓存
    pages: HashMap<PageId, Page>,
    /// 最近使用的页面ID
    lru_list: Vec<PageId>,
    /// 被钉住的页面（不能被置换出去）
    pinned_pages: HashSet<PageId>,
}

impl BufferManager {
    pub fn new<P: AsRef<Path>>(db_file_path: P) -> Result<Self> {
        Ok(Self {
            disk_manager: DiskManager::new(db_file_path)?,
            pages: HashMap::new(),
            lru_list: Vec::new(),
            pinned_pages: HashSet::new(),
        })
    }

    /// 获取页面，如果不在缓存中则从磁盘加载
    pub fn get_page(&mut self, page_id: PageId) -> Result<&Page> {
        if !self.pages.contains_key(&page_id) {
            // 页面不在缓存中，需要从磁盘加载
            self.load_page(page_id)?;
        }

        // 更新LRU列表
        self.update_lru(page_id);

        // 返回页面
        Ok(self.pages.get(&page_id).unwrap()) // Safe unwrap as we just ensured it exists
    }

    /// 获取可变页面引用
    pub fn get_page_mut(&mut self, page_id: PageId) -> Result<&mut Page> {
        if !self.pages.contains_key(&page_id) {
            // 页面不在缓存中，需要从磁盘加载
            self.load_page(page_id)?;
        }

        // 更新LRU列表
        self.update_lru(page_id);

        // 返回可变页面引用
        Ok(self.pages.get_mut(&page_id).unwrap()) // Safe unwrap as we just ensured it exists
    }

    /// 创建新页面
    pub fn create_page(&mut self) -> Result<PageId> {
        // 分配新页面ID
        let page_id = self.disk_manager.allocate_page()?;

        // 创建新页面对象
        let page = Page::new(page_id);

        // 如果缓存已满，需要置换页面
        if self.pages.len() >= BUFFER_POOL_SIZE {
            self.evict_page()?;
        }

        // 将新页面加入缓存
        self.pages.insert(page_id, page);
        self.update_lru(page_id);

        Ok(page_id)
    }

    /**
    将页面钉在缓冲池中（防止被置换出去）
    */
    pub fn pin_page(&mut self, page_id: PageId) -> Result<()> {
        if !self.pages.contains_key(&page_id) {
            self.load_page(page_id)?;
        }

        self.pinned_pages.insert(page_id);
        Ok(())
    }

    /// 取消页面的钉住状态
    pub fn unpin_page(&mut self, page_id: PageId) {
        self.pinned_pages.remove(&page_id);
    }

    /// 刷新单个脏页面到磁盘
    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(page) = self.pages.get_mut(&page_id) {
            if page.is_dirty() {
                self.disk_manager.write_page(page_id, &page.serialize()?)?;
                page.clear_dirty();
            }
        }
        Ok(())
    }

    /// 刷新所有脏页面到磁盘
    pub fn flush_all_pages(&mut self) -> Result<()> {
        for page_id in self.pages.keys().copied().collect::<Vec<_>>() {
            self.flush_page(page_id)?;
        }
        Ok(())
    }

    /// 从磁盘加载页面到缓冲池
    fn load_page(&mut self, page_id: PageId) -> Result<()> {
        // 如果缓冲池已满，需要置换页面
        if self.pages.len() >= BUFFER_POOL_SIZE {
            self.evict_page()?;
        }

        // 从磁盘读取页面数据
        let data = self.disk_manager.read_page(page_id)?;

        // 创建页面并加入缓冲池
        let page = Page::from_data(page_id, &data)?;
        self.pages.insert(page_id, page);

        Ok(())
    }

    /// 置换页面（使用LRU策略）
    fn evict_page(&mut self) -> Result<()> {
        // 寻找可以置换的页面（最久未使用且未被钉住的页面）
        let mut page_to_evict = None;

        for page_id in &self.lru_list {
            if !self.pinned_pages.contains(page_id) {
                page_to_evict = Some(*page_id);
                break;
            }
        }

        // 如果找到可置换页面，先将其刷新到磁盘，然后从缓存移除
        if let Some(page_id) = page_to_evict {
            self.flush_page(page_id)?;
            self.pages.remove(&page_id);
            self.lru_list.retain(|&id| id != page_id);
            Ok(())
        } else {
            // 所有页面都被钉住，无法置换
            Err(DBError::IO(
                "缓冲池已满且所有页面都被钉住，无法置换".to_string(),
            ))
        }
    }

    /// 更新LRU列表
    fn update_lru(&mut self, page_id: PageId) {
        self.lru_list.retain(|&id| id != page_id);
        self.lru_list.push(page_id);
    }
}

impl Drop for BufferManager {
    fn drop(&mut self) {
        // 确保所有脏页面都写回磁盘
        if let Err(e) = self.flush_all_pages() {
            eprintln!("关闭缓冲管理器时刷新页面失败: {}", e);
        }
    }
}
