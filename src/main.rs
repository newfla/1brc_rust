use anyhow::Result;

const PATH: &str = "/home/flavio/1brc/measurements.txt";

fn main() -> Result<()> {
    onebrc::adv::process(PATH.into())
}
