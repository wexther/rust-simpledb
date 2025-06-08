Cargo build --release

rm -f -r simple_db

mkdir simple_db

cp src simple_db/src -r
cp target/release/simple_db simple_db/target/release/simple_db
cp Cargo.toml simple_db/Cargo.toml
cp Cargo.lock simple_db/Cargo.lock

zip submit.zip -r simple_db/src simple_db/target/release/simple_db simple_db/Cargo.toml simple_db/Cargo.lock

rm -f -r simple_db