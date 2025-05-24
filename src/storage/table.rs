// Table definition for the database system

/// 表示列定义的结构
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_primary_key: bool,
}

/// 表示数据类型的枚举
#[derive(Debug, Clone)]
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

/// 表结构
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    // 可以添加其他表相关的字段
}

impl Table {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        Self { name, columns }
    }
    
    // 可以添加表相关的方法
}