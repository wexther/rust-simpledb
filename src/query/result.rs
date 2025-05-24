use std::fmt;

/// 查询结果数据
#[derive(Debug)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
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
                write!(f, "{}", cell)?;
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
    Success(String),
    Error(String),
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryResult::ResultSet(rs) => write!(f, "{}", rs),
            QueryResult::Success(msg) => write!(f, "{}", msg),
            QueryResult::Error(msg) => write!(f, "Error: {}", msg),
        }
    }
}