//! This program converts a PISA index to a Common Index Format (v1)
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

use ciff::PisaToCiff;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "pisa2ciff",
    about = "Convert a PISA index to a Common Index Format [v1]"
)]
struct Args {
    #[structopt(short, long, help = "Binary collection (uncompressed) basename")]
    collection: PathBuf,
    #[structopt(short, long, help = "Path to terms text file")]
    terms: PathBuf,
    #[structopt(short, long, help = "Path to documents text file")]
    documents: PathBuf,
    #[structopt(short, long, help = "Output filename")]
    output: PathBuf,
    #[structopt(long, help = "Index description")]
    description: Option<String>,
}

fn main() {
    let args = Args::from_args();
    if let Err(error) = PisaToCiff::default()
        .description(args.description.unwrap_or_default())
        .index_paths(args.collection)
        .terms_path(args.terms)
        .titles_path(args.documents)
        .output_path(args.output)
        .convert()
    {
        eprintln!("ERROR: {}", error);
        std::process::exit(1);
    }
}
