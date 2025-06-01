use std::fmt;

use crate::storage::table::Value;

/// 查询结果数据
#[derive(Debug)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>, // 改为 Value 类型
}

impl fmt::Display for ResultSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // 表头
        write!(f, "| ")?;
        for (i, col) in self.columns.iter().enumerate() {
            write!(f, "{}", col)?;
            if i < self.columns.len() - 1 {
                write!(f, " | ")?;
            }
        }
        writeln!(f, " |")?;

        // 分隔线
        write!(f, "| ")?;
        for (i, col) in self.columns.iter().enumerate() {
            write!(f, "{}", "-".repeat(col.len()))?;
            if i < self.columns.len() - 1 {
                write!(f, " | ")?;
            }
        }
        writeln!(f, " |")?;

        // 数据行
        for row in &self.rows {
            write!(f, "| ")?;
            for (i, cell) in row.iter().enumerate() {
                // 将 Value 转换为字符串显示
                let cell_str = match cell {
                    Value::Int(n) => n.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::String(s) => s.clone(),
                    Value::Boolean(b) => b.to_string(),
                    Value::Null => "NULL".to_string(),
                };
                write!(f, "{}", cell_str)?;
                if i < row.len() - 1 {
                    write!(f, " | ")?;
                }
            }
            writeln!(f, " |")?;
        }

        Ok(())
    }
}

/// 查询执行结果
#[derive(Debug)]
pub enum QueryResult {
    ResultSet(ResultSet),
    Success,
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryResult::ResultSet(rs) => write!(f, "{}", rs),
            QueryResult::Success => Ok(()),
        }
    }
}
