mod proto;
pub use proto::{DocRecord, Header, Posting, PostingsList};

use ciff::CiffToJsonl;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "ciff2jsonl",
    about = "Convert a Common Index Format [v1] file into a jsonl file"
)]
struct Args {
    #[structopt(short, long, help = "Path to CIFF file")]
    input: PathBuf,
    #[structopt(short, long, help = "Output jsonl file")]
    output: PathBuf,
}

fn main() {
    let args = Args::from_args();

    let mut converter = CiffToJsonl::default();
    converter.input_path(args.input).output_path(args.output);

    if let Err(error) = converter.convert() {
        eprintln!("ERROR: {error}");
        std::process::exit(1);
    }
}
