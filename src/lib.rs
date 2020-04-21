//! Library supporting converting CIFF to PISA uncompressed collection format.
//! Refer to [`osirrc/ciff`](https://github.com/osirrc/ciff) on Github
//! for more detailed information about the format.
//!
//! For more information about PISA's internal storage formats, see the
//! [documentation](https://pisa.readthedocs.io/en/latest/index.html).

#![doc(html_root_url = "https://docs.rs/ciff/0.1")]
#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]

use anyhow::{anyhow, Context};
use indicatif::ProgressIterator;
use indicatif::{ProgressBar, ProgressStyle};
use memmap::Mmap;
use num::ToPrimitive;
use protobuf::{CodedInputStream, CodedOutputStream};
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

mod proto;
pub use proto::{DocRecord, Header, Posting, PostingsList};
mod binary_collection;
pub use binary_collection::{BinaryCollection, BinarySequence, InvalidFormat};

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
    let size: [u8; 4] = len.to_le_bytes();
    writer.write_all(&size)?;
    for element in sequence {
        writer.write_all(&element.borrow().to_le_bytes())?;
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
pub fn ciff_to_pisa(input: &Path, output: &Path) -> Result<()> {
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

    eprintln!("Processing postings");
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

    eprintln!("Processing document lengths");
    let mut sizes = BufWriter::new(File::create(format!("{}.sizes", output.display()))?);
    let mut trecids = BufWriter::new(File::create(format!("{}.documents", output.display()))?);

    let progress = ProgressBar::new(u64::from(num_documents));
    progress.set_style(pb_style());
    progress.set_draw_delta(u64::from(num_documents) / 100);
    sizes.write_all(&num_documents.to_le_bytes())?;

    let expected_docs: usize = header
        .get_num_docs()
        .to_usize()
        .ok_or_else(|| anyhow!("Cannot cast num docs to usize: {}", header.get_num_docs()))?;

    for docs_seen in 0..expected_docs {
        let doc_record = input.read_message::<DocRecord>()?;

        let docid: u32 = doc_record
            .get_docid()
            .to_u32()
            .ok_or_else(|| anyhow!("Cannot cast docid to u32: {}", doc_record.get_docid()))?;

        let trecid = doc_record.get_collection_docid();
        let length: u32 = doc_record.get_doclength().to_u32().ok_or_else(|| {
            anyhow!(
                "Cannot cast doc length to u32: {}",
                doc_record.get_doclength()
            )
        })?;

        assert_eq!(
            docid as usize, docs_seen,
            "Document sizes must come in order"
        );

        sizes.write_all(&length.to_le_bytes())?;
        writeln!(trecids, "{}", trecid)?;
        progress.inc(1);
    }
    progress.finish();

    Ok(())
}

fn read_document_count(
    documents: &mut BinaryCollection,
) -> std::result::Result<u32, InvalidFormat> {
    let invalid = || InvalidFormat::new("Unable to read document count");
    documents
        .next()
        .ok_or_else(invalid)??
        .get(0)
        .ok_or_else(invalid)
}

fn header(documents_bytes: &[u8], sizes_bytes: &[u8], description: &str) -> Result<Header> {
    let mut num_postings_lists = 0;

    eprintln!("Collecting posting lists statistics");
    let progress = ProgressBar::new(documents_bytes.len() as u64);
    progress.set_style(pb_style());
    progress.set_draw_delta(100_000);
    let mut collection = BinaryCollection::try_from(documents_bytes)?;
    let num_documents = read_document_count(&mut collection)?;
    for sequence in collection {
        num_postings_lists += 1;
        let sequence = sequence?;
        progress.inc((sequence.bytes().len() + 4) as u64);
    }
    progress.finish();

    eprintln!("Computing average document length");
    let progress = ProgressBar::new(u64::from(num_documents));
    progress.set_style(pb_style());
    let sizes = BinaryCollection::try_from(sizes_bytes)?
        .next()
        .ok_or_else(|| InvalidFormat::new("Unable to read sizes"))??;
    let doclen_sum: i64 = sizes.iter().map(i64::from).progress_with(progress).sum();

    let mut header = Header::default();
    header.set_version(1);
    header.set_description(description.into());
    header.set_num_postings_lists(num_postings_lists);
    header.set_total_postings_lists(num_postings_lists);
    header.set_total_terms_in_collection(i64::from(num_postings_lists));
    header.set_num_docs(num_documents as i32);
    header.set_total_docs(num_documents as i32);
    #[allow(clippy::cast_precision_loss)]
    header.set_average_doclength(doclen_sum as f64 / f64::from(num_documents));
    Ok(header)
}

fn write_sizes(sizes_mmap: &Mmap, titles_file: &File, out: &mut CodedOutputStream) -> Result<()> {
    let titles = BufReader::new(titles_file);
    let sizes = BinaryCollection::try_from(&sizes_mmap[..])?
        .next()
        .ok_or_else(|| InvalidFormat::new("Unable to read sizes"))??;
    for ((docid, size), title) in sizes.iter().enumerate().zip(titles.lines()) {
        let mut document = DocRecord::default();
        document.set_docid(docid as i32);
        document.set_collection_docid(title?);
        document.set_doclength(size as i32);
        out.write_message_no_tag(&document)?;
    }
    Ok(())
}

fn write_postings(
    documents_mmap: &Mmap,
    frequencies_mmap: &Mmap,
    terms_file: &File,
    out: &mut CodedOutputStream,
) -> Result<()> {
    let mut documents = BinaryCollection::try_from(&documents_mmap[..])?;
    let num_documents = u64::from(read_document_count(&mut documents)?);
    let frequencies = BinaryCollection::try_from(&frequencies_mmap[..])?;
    let terms = BufReader::new(terms_file);

    eprintln!("Writing postings");
    let progress = ProgressBar::new(num_documents);
    progress.set_style(pb_style());
    progress.set_draw_delta(num_documents / 100);
    for ((term_documents, term_frequencies), term) in documents
        .zip(frequencies)
        .zip(terms.lines())
        .progress_with(progress)
    {
        let mut posting_list = PostingsList::default();
        posting_list.set_term(term?);
        let mut count = 0;
        let mut sum = 0;
        let mut last_doc = 0;
        for (docid, frequency) in term_documents?.iter().zip(term_frequencies?.iter()) {
            let mut posting = Posting::default();
            posting.set_docid(docid as i32 - last_doc);
            posting.set_tf(frequency as i32);
            posting_list.postings.push(posting);
            count += 1;
            sum += i64::from(frequency);
            last_doc = docid as i32;
        }
        posting_list.set_df(count);
        posting_list.set_cf(sum);
        out.write_message_no_tag(&posting_list)?;
    }
    Ok(())
}

/// Converts a a PISA "binary collection" (uncompressed inverted index) with a basename `input`
/// to a CIFF index stored in `output`.
///
/// # Errors
///
/// Returns an error when:
/// - an IO error occurs,
/// - writing protobuf format fails,
pub fn pisa_to_ciff(
    collection_input: &Path,
    terms_input: &Path,
    titles_input: &Path,
    output: &Path,
    description: &str,
) -> Result<()> {
    pisa_to_ciff_from_paths(
        &PathBuf::from(format!("{}.docs", collection_input.display())),
        &PathBuf::from(format!("{}.freqs", collection_input.display())),
        &PathBuf::from(format!("{}.sizes", collection_input.display())),
        terms_input,
        titles_input,
        output,
        description,
    )
}

fn pisa_to_ciff_from_paths(
    documents_path: &Path,
    frequencies_path: &Path,
    sizes_path: &Path,
    terms_path: &Path,
    titles_path: &Path,
    output: &Path,
    description: &str,
) -> Result<()> {
    let documents_file = File::open(documents_path)?;
    let frequencies_file = File::open(frequencies_path)?;
    let sizes_file = File::open(sizes_path)?;
    let terms_file = File::open(terms_path)?;
    let titles_file = File::open(titles_path)?;

    let documents_mmap = unsafe { Mmap::map(&documents_file)? };
    let frequencies_mmap = unsafe { Mmap::map(&frequencies_file)? };
    let sizes_mmap = unsafe { Mmap::map(&sizes_file)? };

    let mut writer = BufWriter::new(File::create(output)?);
    let mut out = CodedOutputStream::new(&mut writer);

    let header = header(&documents_mmap[..], &sizes_mmap[..], description)?;
    out.write_message_no_tag(&header)?;

    write_postings(&documents_mmap, &frequencies_mmap, &terms_file, &mut out)?;
    write_sizes(&sizes_mmap, &titles_file, &mut out)?;

    out.flush()?;

    Ok(())
}
