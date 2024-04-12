use anyhow::Result;

const PATH: &str = "./measurements.txt";

fn main() -> Result<()> {
    onebrc::adv::process(PATH.into())
}
