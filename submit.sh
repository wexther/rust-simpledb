cargo build --release

mkdir simple_db 
cp src simple_db/src -r
mkdir simple_db/target
mkdir simple_db/target/release
cp target/release/simple_db simple_db/target/release/simple_db
cp Cargo.toml simple_db/Cargo.toml
cp Cargo.lock simple_db/Cargo.lock

zip -FS submit.zip -rm simple_db