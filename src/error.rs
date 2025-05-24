use std::fmt;
use std::result;

#[derive(Debug)]
pub enum DBError {
    Parse(String),
    Schema(String),
    Execution(String),
    IO(String),
    NotFound(String),
    Other(String),
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DBError::Parse(msg) => write!(f, "解析错误: {}", msg),
            DBError::Schema(msg) => write!(f, "模式错误: {}", msg),
            DBError::Execution(msg) => write!(f, "执行错误: {}", msg),
            DBError::IO(msg) => write!(f, "IO错误: {}", msg),
            DBError::NotFound(msg) => write!(f, "未找到: {}", msg),
            DBError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for DBError {}

pub type Result<T> = result::Result<T, DBError>;