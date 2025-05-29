use std::{result, io};
use thiserror::Error;
use sqlparser::parser;

pub type Result<T> = result::Result<T, DBError>;

#[derive(Error, Debug)]
pub enum DBError {
    /// 使用 std::io 读写数据库文件时的报错
    #[error("{0}")]
    IO(String),
    //IO(io::Error),

    /// 由 sqlparser 解析 SQL 语句时的报错
    #[error("{0}")]
    Parse(String),
    //Parse(sqlparser::parser::ParserError),

    #[error("{0}")]
    Planner(String),

    /// 模糊不清的错误信息
    #[error("{0}")]
    Schema(String),

    /// 模糊不清的错误信息2
    #[error("{0}")]
    Execution(String),

    /// 模糊不清的错误信息3
    #[error("{0}")]
    NotFound(String),

    /// 模糊不清的错误信息4
    #[error("{0}")]
    Other(String),
}

impl From<parser::ParserError> for DBError {
    fn from(err: parser::ParserError) -> Self {
        DBError::Parse(err.to_string())
    }
}

impl From<io::Error> for DBError {
    fn from(err: io::Error) -> Self {
        DBError::IO(err.to_string())
    }
}
