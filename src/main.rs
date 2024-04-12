use anyhow::Result;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const PATH: &str = "/home/flavio/1brc/measurements.txt";

// #[tokio::main]
// async fn main() -> Result<()> {
//     basic::process(PATH.into()).await
// }

fn main() -> Result<()> {
    onebrc::adv::process(PATH.into())
}
