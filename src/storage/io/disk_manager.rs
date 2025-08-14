use super::page::{PAGE_SIZE, PageId};
use crate::error::{DBError, Result};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// 磁盘管理器 - 负责页面的磁盘读写
pub struct DiskManager {
    /// 数据库文件
    file: File,
    /// 下一个可分配的页面ID
    next_page_id: PageId,
}

impl DiskManager {
    /// 创建或打开数据库文件
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // 打开或创建数据库文件
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true).truncate(true)
            .open(path)
            .map_err(|e| DBError::IO(format!("无法打开数据库文件: {}", e)))?;

        // 计算当前文件大小以确定下一个可分配的页面ID
        let file_size = file
            .metadata()
            .map_err(|e| DBError::IO(format!("无法获取文件元数据: {}", e)))?
            .len();

        let next_page_id = (file_size / PAGE_SIZE as u64) as PageId;

        Ok(Self { file, next_page_id })
    }

    /// 读取页面
    pub fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        // 计算页面在文件中的偏移量
        let offset = self.page_offset(page_id);

        // 检查偏移量是否超出文件大小
        let file_size = self
            .file
            .metadata()
            .map_err(|e| DBError::IO(format!("无法获取文件大小: {}", e)))?
            .len();

        if offset >= file_size {
            return Err(DBError::NotFound(format!("页面 {} 不存在", page_id)));
        }

        // 定位到页面位置
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| DBError::IO(format!("无法定位到页面 {}: {}", page_id, e)))?;

        // 读取页面数据
        let mut buffer = vec![0; PAGE_SIZE];
        self.file.read_exact(&mut buffer).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                DBError::IO(format!("页面 {} 数据不完整", page_id))
            } else {
                DBError::IO(format!("无法读取页面 {}: {}", page_id, e))
            }
        })?;

        Ok(buffer)
    }

    /// 写入页面
    pub fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        if data.len() > PAGE_SIZE {
            return Err(DBError::IO(format!(
                "页面数据过大: {} > {}",
                data.len(),
                PAGE_SIZE
            )));
        }

        // 计算页面在文件中的偏移量
        let offset = self.page_offset(page_id);

        // 定位到页面位置
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| DBError::IO(format!("无法定位到页面 {}: {}", page_id, e)))?;

        // 若数据小于页面大小，则创建完整大小的缓冲区
        let mut buffer = vec![0; PAGE_SIZE];
        buffer[..data.len()].copy_from_slice(data);

        // 写入页面数据
        self.file
            .write_all(&buffer)
            .map_err(|e| DBError::IO(format!("无法写入页面 {}: {}", page_id, e)))?;
        self.file
            .flush()
            .map_err(|e| DBError::IO(format!("无法刷新页面 {}: {}", page_id, e)))?;

        Ok(())
    }

    /// 分配新页面
    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;

        // 写入空页面以扩展文件
        let empty_page = vec![0; PAGE_SIZE];
        self.write_page(page_id, &empty_page)?;

        Ok(page_id)
    }

    /// 计算页面在文件中的偏移量
    fn page_offset(&self, page_id: PageId) -> u64 {
        page_id as u64 * PAGE_SIZE as u64
    }
}
