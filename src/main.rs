use simple_db::SimpleDB;

fn main() {
    if let Err(e) = SimpleDB::from_args().and_then(|mut db| db.run()) {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
