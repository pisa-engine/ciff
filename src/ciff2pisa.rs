//! This program generates a PISA index from a Common Index Format (v1)
//! Refer to [`osirrc/ciff`](https://github.com/osirrc/ciff) on Github
//! for more detailed information about the format.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use ciff::CiffToPisa;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "ciff2pisa",
    about = "Generates a PISA index from a Common Index Format [v1]"
)]
struct Args {
    #[structopt(short, long, help = "Path to ciff export file")]
    ciff_file: PathBuf,
    #[structopt(short, long, help = "Output basename")]
    output: PathBuf,
    #[structopt(short, long, help = "Generate lexicon files?")]
    generate_lexicons: bool,
}

fn main() {
    let args = Args::from_args();
    let mut converter = CiffToPisa::default();
    converter
        .input_path(args.ciff_file)
        .output_paths(args.output);
    if !args.generate_lexicons {
        converter.skip_lexicons();
    }
    if let Err(error) = converter.convert() {
        eprintln!("ERROR: {}", error);
        std::process::exit(1);
    }
}
