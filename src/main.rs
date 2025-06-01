use simple_db::SimpleDB;

fn main() {
    match SimpleDB::from_args() {
        Ok(mut db) => {
            if let Err(e) = db.run() {
                eprintln!("运行失败: {}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("初始化失败: {}", e);
            std::process::exit(1);
        }
    }
}
