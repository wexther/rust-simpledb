use crate::error::{DBError, Result};
use bincode::{Decode, Encode};

/// 表示值的枚举
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum Value {
    Int(i32),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

impl Value {
    /// 使用 bincode 2.x 序列化到缓冲区
    pub fn serialize(&self, buffer: &mut Vec<u8>) {
        let serialized = bincode::encode_to_vec(self, bincode::config::standard()).unwrap();
        buffer.extend_from_slice(&serialized);
    }

    /// 使用 bincode 2.x 从缓冲区反序列化
    pub fn deserialize(buffer: &[u8]) -> Result<(Self, usize)> {
        match bincode::decode_from_slice(buffer, bincode::config::standard()) {
            Ok((value, bytes_consumed)) => Ok((value, bytes_consumed)),
            Err(e) => Err(DBError::IO(format!("反序列化Value失败: {}", e))),
        }
    }

    // 保留现有的数学运算方法...
    pub fn add(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
            _ => Err(DBError::Execution("类型不兼容，无法相加".to_string())),
        }
    }

    pub fn subtract(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - *b as f64)),
            _ => Err(DBError::Execution("类型不兼容，无法相减".to_string())),
        }
    }

    pub fn multiply(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * *b as f64)),
            _ => Err(DBError::Execution("类型不兼容，无法相乘".to_string())),
        }
    }

    pub fn divide(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    return Err(DBError::Execution("除数不能为零".to_string()));
                }
                Ok(Value::Int(a / b))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err(DBError::Execution("除数不能为零".to_string()));
                }
                Ok(Value::Float(a / b))
            }
            (Value::Int(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err(DBError::Execution("除数不能为零".to_string()));
                }
                Ok(Value::Float(*a as f64 / b))
            }
            (Value::Float(a), Value::Int(b)) => {
                if *b == 0 {
                    return Err(DBError::Execution("除数不能为零".to_string()));
                }
                Ok(Value::Float(a / *b as f64))
            }
            _ => Err(DBError::Execution("类型不兼容，无法相除".to_string())),
        }
    }
    pub fn modulo(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    return Err(DBError::Execution("模数不能为零".to_string()));
                }
                Ok(Value::Int(a % b))
            }
            _ => Err(DBError::Execution("模运算仅支持整数".to_string())),
        }
    }

    pub fn negate(&self) -> Result<Value> {
        match self {
            Value::Int(n) => Ok(Value::Int(-n)),
            Value::Float(f) => Ok(Value::Float(-f)),
            _ => Err(DBError::Execution("只能对数值进行取负操作".to_string())),
        }
    }

    // 保留现有的比较方法...
    pub fn eq(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(false),
            (Value::Int(a), Value::Int(b)) => Ok(a == b),
            (Value::Float(a), Value::Float(b)) => Ok(a == b),
            (Value::Int(a), Value::Float(b)) => Ok(*a as f64 == *b),
            (Value::Float(a), Value::Int(b)) => Ok(*a == *b as f64),
            (Value::String(a), Value::String(b)) => Ok(a == b),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(a == b),
            _ => Err(DBError::Execution("类型不匹配，无法比较".to_string())),
        }
    }

    pub fn ne(&self, other: &Self) -> Result<bool> {
        self.eq(other).map(|result| !result)
    }

    pub fn lt(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(false),
            (Value::Int(a), Value::Int(b)) => Ok(a < b),
            (Value::Float(a), Value::Float(b)) => Ok(a < b),
            (Value::Int(a), Value::Float(b)) => Ok((*a as f64) < *b),
            (Value::Float(a), Value::Int(b)) => Ok(*a < *b as f64),
            (Value::String(a), Value::String(b)) => Ok(a < b),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(!*a && *b),
            _ => Err(DBError::Execution("类型不匹配，无法比较".to_string())),
        }
    }

    pub fn le(&self, other: &Self) -> Result<bool> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(false),
            (Value::Int(a), Value::Int(b)) => Ok(a <= b),
            (Value::Float(a), Value::Float(b)) => Ok(a <= b),
            (Value::Int(a), Value::Float(b)) => Ok(*a as f64 <= *b),
            (Value::Float(a), Value::Int(b)) => Ok(*a <= *b as f64),
            (Value::String(a), Value::String(b)) => Ok(a <= b),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(!*a || *b),
            _ => Err(DBError::Execution("类型不匹配，无法比较".to_string())),
        }
    }

    pub fn gt(&self, other: &Self) -> Result<bool> {
        other.lt(self)
    }

    pub fn ge(&self, other: &Self) -> Result<bool> {
        other.le(self)
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(n) => write!(f, "{}", n),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// 表示列定义的结构
#[derive(Debug, Clone, Encode, Decode)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,

    // 约束
    pub not_null: bool,
    pub unique: bool,
    pub is_primary: bool, // is_primary => not_null && unique
}

/// 表示数据类型的枚举
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum DataType {
    Int(u64),
    Varchar(u64),
}
