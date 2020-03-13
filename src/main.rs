//! This program generates a PISA index from a Common Index Format [v1]
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

use ciff::convert;
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
}

fn main() {
    let args = Args::from_args();
    if let Err(error) = convert(&args.ciff_file, &args.output) {
        eprintln!("ERROR: {}", error);
        std::process::exit(1);
    }
}
