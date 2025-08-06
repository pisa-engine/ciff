mod proto;
pub use proto::{DocRecord, Header, Posting, PostingsList};

use ciff::JsonlToCiff;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "jsonl2ciff",
    about = "Convert a jsonl file into the Common Index Format [v1]"
)]
struct Args {
    #[structopt(short, long, help = "Path to jsonl file")]
    input: PathBuf,
    #[structopt(short, long, help = "Output basename")]
    output: PathBuf,
    #[structopt(short, long, help = "Quantize scores to integers")]
    quantize: bool,
}

fn main() {
    let args = Args::from_args();

    let mut converter = JsonlToCiff::default();
    converter
        .input_path(args.input)
        .output_path(args.output)
        .quantize(args.quantize);

    if let Err(error) = converter.convert() {
        eprintln!("ERROR: {error}");
        std::process::exit(1);
    }
}
