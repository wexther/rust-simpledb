pub mod executor;
pub mod planner;
pub mod result;

use self::executor::{DDLExecutor, DMLExecutor, Executor, QueryExecutor};
use self::planner::QueryPlanner;
use self::result::QueryResult;
use crate::error::{DBError, Result};
use crate::storage::StorageEngine;
use sqlparser::ast::Statement;

/// 查询处理器 - 负责整个查询处理流程
pub struct QueryProcessor<'a> {
    storage: &'a mut StorageEngine,
    planner: QueryPlanner,
}

impl<'a> QueryProcessor<'a> {
    pub fn new(storage: &'a mut StorageEngine) -> Self {
        Self {
            storage,
            planner: QueryPlanner::new(),
        }
    }

    /// 执行SQL语句，返回执行结果
    pub fn execute(&mut self, stmt: Statement) -> Result<QueryResult> {
        // 1. 生成查询计划
        let plan = self.planner.plan(&stmt)?;

        // 2. 根据语句类型选择合适的执行器
        match stmt {
            Statement::Query(_) => {
                let mut executor = QueryExecutor::new(self.storage);
                executor.execute(plan)
            }
            Statement::CreateTable { .. } | Statement::Drop { .. } => {
                let mut executor = DDLExecutor::new(self.storage);
                executor.execute(plan)
            }
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                let mut executor = DMLExecutor::new(self.storage);
                executor.execute(plan)
            }
            _ => Err(DBError::Schema(format!("不支持的语句类型: {:?}", stmt))),
        }
    }
}
