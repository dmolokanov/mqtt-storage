mod common;
use common::*;

use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::TempDir;

use mqtt_storage::Rocksdb;

criterion_group!(basic, random);
criterion_main!(basic);

fn random(c: &mut Criterion) {
    c.bench_function("rocksdb", |b| {
        b.iter_custom(|iters| {
            let mut storage = Rocksdb::new(TempDir::new().unwrap(), "q", 10);
            let start = Instant::now();
            run(&mut storage, ops(iters, "q", 10));
            start.elapsed()
        })
    });
}
