mod proto;
pub use proto::{DocRecord, Header, Posting, PostingsList};

use ciff::JsonlToCiff;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "pisa2ciff",
    about = "Convert a PISA index to a Common Index Format [v1]"
)]
struct Args {
    #[structopt(short, long, help = "Path to jsonl file")]
    input: PathBuf,
    #[structopt(short, long, help = "Output basename")]
    output: PathBuf,
}

fn main() {
    let args = Args::from_args();

    let mut converter = JsonlToCiff::default();
    converter.input_path(args.input).output_path(args.output);

    if let Err(error) = converter.convert() {
        eprintln!("ERROR: {error}");
        std::process::exit(1);
    }
}
