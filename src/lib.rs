//! Library supporting converting CIFF to PISA uncompressed collection format.
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

use anyhow::{anyhow, Context};
use indicatif::{ProgressBar, ProgressStyle};
use num::ToPrimitive;
use protobuf::CodedInputStream;
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

mod proto;
pub use proto::{DocRecord, Header, Posting, PostingsList};

type Result<T> = anyhow::Result<T>;

const DEFAULT_PROGRESS_TEMPLATE: &str =
    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {count}/{total} ({eta})";

/// Returns default progress style.
fn pb_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(DEFAULT_PROGRESS_TEMPLATE)
        .progress_chars("=> ")
}

/// Encodes a sequence of 4-byte unsigned integers into `writer` in native-endianness.
///
/// # Examples
///
/// ```
/// # use ciff::encode_u32_sequence;
/// # fn main() -> anyhow::Result<()> {
/// let mut buf: Vec<u8> = vec![];
/// let input = vec![4_u32, 98765];
/// encode_u32_sequence(&mut buf, 2, input)?;
///
/// #[cfg(target_endian = "little")]
/// assert_eq!(buf, &[
///     2_u8, 0, 0, 0,  // Sequence length
///     4, 0, 0, 0,     // First element
///     205, 129, 1, 0, // Second element
///     ]);
/// # Ok(())
/// # }
///
/// ```
///
/// # Errors
///
/// Passes along any IO errors.
pub fn encode_u32_sequence<N, S, W>(writer: &mut W, len: u32, sequence: S) -> io::Result<()>
where
    N: Borrow<u32>,
    S: IntoIterator<Item = N>,
    W: Write,
{
    let size: [u8; 4] = len.to_ne_bytes();
    writer.write_all(&size)?;
    for element in sequence {
        writer.write_all(&element.borrow().to_ne_bytes())?;
    }
    Ok(())
}

fn write_posting_list<DW, FW, TW>(
    posting_list: &PostingsList,
    documents: &mut DW,
    frequencies: &mut FW,
    terms: &mut TW,
) -> Result<()>
where
    DW: Write,
    FW: Write,
    TW: Write,
{
    let length = posting_list
        .get_df()
        .to_u32()
        .ok_or_else(|| anyhow!("Cannot cast to u32: {}", posting_list.get_df()))?;

    let postings = posting_list.get_postings();

    encode_u32_sequence(
        documents,
        length,
        postings.iter().scan(0_u32, |prev, p| {
            *prev += u32::try_from(p.get_docid()).expect("Negative ID");
            Some(*prev)
        }),
    )?;

    encode_u32_sequence(
        frequencies,
        length,
        postings
            .iter()
            .map(|p| u32::try_from(p.get_tf()).expect("Negative frequency")),
    )?;

    writeln!(terms, "{}", posting_list.get_term())?;
    Ok(())
}

/// Converts a CIFF index stored in `path` to a PISA "binary collection" (uncompressed inverted
/// index) with a basename `output`.
///
/// # Errors
///
/// Returns an error when:
/// - an IO error occurs,
/// - reading protobuf format fails,
/// - data format is valid but any ID, frequency, or a count is negative.
pub fn convert(input: &Path, output: &Path) -> Result<()> {
    let mut ciff_reader =
        File::open(input).with_context(|| format!("Unable to open {}", input.display()))?;
    let mut input = CodedInputStream::new(&mut ciff_reader);
    let mut documents = BufWriter::new(File::create(format!("{}.docs", output.display()))?);
    let mut frequencies = BufWriter::new(File::create(format!("{}.freqs", output.display()))?);
    let mut terms = BufWriter::new(File::create(format!("{}.terms", output.display()))?);

    // Read protobuf header
    let header = input.read_message::<Header>()?;
    let num_documents = u32::try_from(header.get_num_docs())
        .context("Number of documents must be non-negative.")?;
    println!("{}", &header);

    eprintln!("Processing postings...");
    encode_u32_sequence(&mut documents, 1, [num_documents].iter())?;
    let progress = ProgressBar::new(u64::try_from(header.get_num_postings_lists())?);
    progress.set_style(pb_style());
    progress.set_draw_delta(10);
    for _ in 0..header.get_num_postings_lists() {
        write_posting_list(
            &input.read_message::<PostingsList>()?,
            &mut documents,
            &mut frequencies,
            &mut terms,
        )?;
        progress.inc(1);
    }
    progress.finish();

    documents.flush()?;
    frequencies.flush()?;
    terms.flush()?;

    eprintln!("Processing document lengths...");
    let mut sizes = BufWriter::new(File::create(format!("{}.sizes", output.display()))?);
    let mut trecids = BufWriter::new(File::create(format!("{}.documents", output.display()))?);

    let progress = ProgressBar::new(u64::from(num_documents));
    progress.set_style(pb_style());
    progress.set_draw_delta(u64::from(num_documents) / 100);
    sizes.write_all(&num_documents.to_ne_bytes())?;

    let expected_docs: usize = header
        .get_num_docs()
        .to_usize()
        .ok_or_else(|| anyhow!("Cannot cast to usize: {}", header.get_num_docs()))?;

    for docs_seen in 0..expected_docs {
        let doc_record = input.read_message::<DocRecord>()?;

        let docid: u32 = doc_record
            .get_docid()
            .to_u32()
            .ok_or_else(|| anyhow!("Cannot cast to u32: {}", doc_record.get_docid()))?;

        let trecid = doc_record.get_collection_docid();
        let length: u32 = doc_record
            .get_doclength()
            .to_u32()
            .ok_or_else(|| anyhow!("Cannot cast to u32: {}", doc_record.get_doclength()))?;

        assert_eq!(
            docid as usize, docs_seen,
            "Document sizes must come in order"
        );

        sizes.write_all(&length.to_ne_bytes())?;
        writeln!(trecids, "{}", trecid)?;
        progress.inc(1);
    }
    progress.finish();

    Ok(())
}
