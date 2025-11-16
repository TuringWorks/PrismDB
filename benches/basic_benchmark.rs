use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

fn bench_simple_operation(c: &mut Criterion) {
    c.bench_function("simple_operation", |b| {
        b.iter(|| {
            // Simple arithmetic operation for benchmarking
            let result = black_box(42 + 24);
            result
        })
    });
}

criterion_group!(benches, bench_simple_operation);
criterion_main!(benches);
