set dotenv-load := true

fmt:
    cargo fmt --all --check

lint:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo nextest run --all-features

e2e:
    cargo test --test e2e --all-features

check: fmt lint test e2e

run *args:
    cargo run -- {{args}}
