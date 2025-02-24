use criterion::{Criterion, black_box, criterion_group, criterion_main};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
//static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const PATH: &str = "./measurements.txt";

pub fn bench(c: &mut Criterion) {
    c.bench_function("basic impl", |b| {
        b.iter(|| onebrc::adv::process(black_box(PATH.into())))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench
}
criterion_main!(benches);
