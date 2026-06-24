#[global_allocator]
static GLOBAL: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn main() {
    eprintln!("smoke: process started");

    let mut values = Vec::with_capacity(1024);
    values.extend(0..1024_u64);
    drop(values);
    eprintln!("smoke: allocation round-trip completed");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build tokio runtime");
    eprintln!("smoke: tokio runtime built");

    runtime.block_on(async {
        tokio::task::spawn_blocking(|| {
            let mut values = Vec::with_capacity(1024);
            values.extend(0..1024_u64);
            values.len()
        })
        .await
        .expect("join blocking task");
    });
    eprintln!("smoke: tokio blocking task completed");
}
