pub mod executor;
pub mod planner;
pub mod result;

use self::executor::Executor;
use self::planner::QueryPlanner;
use self::result::QueryResult;
use crate::error::{DBError, Result};
use crate::storage::StorageEngine;
use sqlparser::ast::Statement;

/// 查询处理器 - 负责整个查询处理流程
pub struct QueryProcessor<'a> {
    planner: QueryPlanner,
    executor: Executor<'a>,
}

impl<'a> QueryProcessor<'a> {
    pub fn new(storage: &'a mut StorageEngine) -> Self {
        Self {
            executor: Executor::new(storage),
            planner: QueryPlanner::new(),
        }
    }

    /// 执行SQL语句，返回执行结果
    pub fn execute(&mut self, stmt: Statement) -> Result<QueryResult> {
        let plan = self.planner.plan(&stmt)?;

        self.executor.execute(plan)
        }
    }

