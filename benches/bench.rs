use criterion::{black_box, criterion_group, criterion_main, Criterion};

const PATH: &str = "/home/flavio/1brc/measurements.txt";

pub fn bench(c: &mut Criterion) {
    c.bench_function("basic impl", |b| {
        b.to_async(tokio::runtime::Builder::new_multi_thread().build().unwrap())
            .iter(|| onebrc::basic::async_process(black_box(PATH.into())))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench
}
criterion_main!(benches);
