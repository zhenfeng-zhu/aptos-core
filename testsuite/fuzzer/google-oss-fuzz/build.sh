#!/bin/bash -eu

NIGHTLY_VERSION="nightly-2023-06-01"

rustup install $NIGHTLY_VERSION
cd testsuite/fuzzer

RUSTFLAGS="$RUSTFLAGS --cfg tokio_unstable" cargo +$NIGHTLY_VERSION fuzz build -O -a
for fuzzer in $(cat fuzz/Cargo.toml | grep "name = " | grep -v "fuzzer-fuzz" | cut -d'"' -f2); do
  cp ../../target/x86_64-unknown-linux-gnu/release/$fuzzer $OUT/
done