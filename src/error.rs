use std::result;
use thiserror::Error;

pub type Result<T> = result::Result<T, DBError>;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("IO错误: {0}")]
    IO(String),

    #[error("解析错误: {0}")]
    Parse(String),

    #[error("模式错误: {0}")]
    Schema(String),

    #[error("执行错误: {0}")]
    Execution(String),

    #[error("资源未找到: {0}")]
    NotFound(String),

    #[error("{0}")]
    Other(String),
}

impl From<sqlparser::parser::ParserError> for DBError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        DBError::Parse(err.to_string())
    }
}

impl From<std::io::Error> for DBError {
    fn from(err: std::io::Error) -> Self {
        DBError::IO(err.to_string())
    }
}
