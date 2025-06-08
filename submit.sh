cd ..

zip submit.zip -r simple_db/src simple_db/target/release/simple_db simple_db/Cargo.toml simple_db/Cargo.lock

mv submit.zip simple_db/submit.zip

cd simple_db