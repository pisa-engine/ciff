//! Library supporting converting CIFF to PISA uncompressed collection format.
//! Refer to [`osirrc/ciff`](https://github.com/osirrc/ciff) on Github
//! for more detailed information about the format.
//!
//! For more information about PISA's internal storage formats, see the
//! [documentation](https://pisa.readthedocs.io/en/latest/index.html).

#![doc(html_root_url = "https://docs.rs/ciff/0.1.1")]
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
    clippy::cast_possible_truncation,
    clippy::copy_iterator
)]

use anyhow::{anyhow, Context};
use indicatif::ProgressIterator;
use indicatif::{ProgressBar, ProgressStyle};
use memmap::Mmap;
use num_traits::ToPrimitive;
use protobuf::{CodedInputStream, CodedOutputStream};
use std::borrow::Borrow;
use std::convert::TryFrom;
use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

mod proto;
pub use proto::{DocRecord, Posting, PostingsList};

mod binary_collection;
pub use binary_collection::{
    BinaryCollection, BinarySequence, InvalidFormat, RandomAccessBinaryCollection,
};

mod payload_vector;
pub use payload_vector::{PayloadIter, PayloadSlice, PayloadVector};

type Result<T> = anyhow::Result<T>;

const DEFAULT_PROGRESS_TEMPLATE: &str =
    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {count}/{total} ({eta})";

/// Wraps [`proto::Header`] and additionally provides some important counts that are already cast
/// to an unsigned type.
#[derive(PartialEq, Clone, Default)]
struct Header {
    num_postings_lists: u32,
    num_documents: u32,
    /// Used for printing.
    protobuf_header: proto::Header,
}

impl Header {
    /// Reads the protobuf header, and converts to a proper-typed header to fail fast if the protobuf
    /// header contains any negative values.
    ///
    /// # Errors
    ///
    /// Returns an error if the protobuf header contains negative counts.
    fn from_stream(input: &mut CodedInputStream<'_>) -> Result<Self> {
        let header = input.read_message::<proto::Header>()?;
        let num_documents = u32::try_from(header.get_num_docs())
            .context("Number of documents must be non-negative.")?;
        let num_postings_lists = u32::try_from(header.get_num_postings_lists())
            .context("Number of documents must be non-negative.")?;
        Ok(Self {
            protobuf_header: header,
            num_documents,
            num_postings_lists,
        })
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.protobuf_header)
    }
}

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

fn check_lines_sorted<R: BufRead>(reader: R) -> io::Result<bool> {
    let mut prev = String::from("");
    for line in reader.lines() {
        let line = line?;
        if line < prev {
            return Ok(false);
        }
        prev = line;
    }
    Ok(true)
}

struct PisaIndexPaths {
    terms: PathBuf,
    documents: PathBuf,
    frequencies: PathBuf,
    sizes: PathBuf,
    titles: PathBuf,
}

impl PisaIndexPaths {
    fn from_base_path(path: &Path) -> Option<Self> {
        let file_name = path.file_name()?;
        let parent = path.parent()?;
        let format_name = |file: &OsStr, suffix| {
            let mut full_name = file.to_owned();
            full_name.push(suffix);
            full_name
        };
        Some(Self {
            terms: parent.join(format_name(file_name, ".terms")),
            documents: parent.join(format_name(file_name, ".docs")),
            frequencies: parent.join(format_name(file_name, ".freqs")),
            sizes: parent.join(format_name(file_name, ".sizes")),
            titles: parent.join(format_name(file_name, ".documents")),
        })
    }
}

fn reorder_postings(path: &Path, order: &[usize], skip_first: bool) -> Result<()> {
    let temp = TempDir::new()?;
    let tmp_path = temp.path().join("coll");
    std::fs::rename(path, &tmp_path)?;
    let mmap = unsafe { Mmap::map(&File::open(tmp_path)?)? };
    let coll = RandomAccessBinaryCollection::try_from(mmap.as_ref())?;
    let mut writer = BufWriter::new(File::create(path)?);
    if skip_first {
        let order: Vec<_> = std::iter::once(0)
            .chain(order.iter().map(|&i| i + 1))
            .collect();
        binary_collection::reorder(&coll, &order, &mut writer)?;
    } else {
        binary_collection::reorder(&coll, order, &mut writer)?;
    }
    writer.flush()?;
    Ok(())
}

fn reorder_pisa_index(paths: &PisaIndexPaths) -> Result<()> {
    let terms = BufReader::new(File::open(&paths.terms)?)
        .lines()
        .collect::<io::Result<Vec<_>>>()?;
    let mut order: Vec<_> = (0..terms.len()).collect();
    order.sort_by_key(|&i| &terms[i]);
    reorder_postings(&paths.documents, &order, true)?;
    reorder_postings(&paths.frequencies, &order, false)?;
    let mut term_writer = BufWriter::new(File::create(&paths.terms)?);
    for index in order {
        writeln!(&mut term_writer, "{}", terms[index])?;
    }
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
/// - data format is valid but any ID, frequency, or a count is negative,
/// - document records is out of order.
pub fn ciff_to_pisa(input: &Path, output: &Path) -> Result<()> {
    let index_paths =
        PisaIndexPaths::from_base_path(output).ok_or_else(|| anyhow!("invalid output path"))?;

    let mut ciff_reader =
        File::open(input).with_context(|| format!("Unable to open {}", input.display()))?;
    let mut input = CodedInputStream::new(&mut ciff_reader);
    let mut documents = BufWriter::new(File::create(&index_paths.documents)?);
    let mut frequencies = BufWriter::new(File::create(&index_paths.frequencies)?);
    let mut terms = BufWriter::new(File::create(&index_paths.terms)?);

    let header = Header::from_stream(&mut input)?;
    println!("{}", header);

    eprintln!("Processing postings");
    encode_u32_sequence(&mut documents, 1, [header.num_documents].iter())?;
    let progress = ProgressBar::new(u64::try_from(header.num_postings_lists)?);
    progress.set_style(pb_style());
    progress.set_draw_delta(10);
    for _ in 0..header.num_postings_lists {
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
    let mut sizes = BufWriter::new(File::create(&index_paths.sizes)?);
    let mut trecids = BufWriter::new(File::create(&index_paths.titles)?);

    let progress = ProgressBar::new(u64::from(header.num_documents));
    progress.set_style(pb_style());
    progress.set_draw_delta(u64::from(header.num_documents) / 100);
    sizes.write_all(&header.num_documents.to_le_bytes())?;

    for docs_seen in 0..header.num_documents {
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

        if docid != docs_seen {
            anyhow::bail!("Document sizes must come in order");
        }

        sizes.write_all(&length.to_le_bytes())?;
        writeln!(trecids, "{}", trecid)?;
        progress.inc(1);
    }
    progress.finish();

    if !check_lines_sorted(BufReader::new(File::open(&index_paths.terms)?))? {
        reorder_pisa_index(&index_paths)?;
    }

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

fn header(documents_bytes: &[u8], sizes_bytes: &[u8], description: &str) -> Result<proto::Header> {
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
    let doclen_sum: i64 = sizes(sizes_bytes)?
        .iter()
        .map(i64::from)
        .progress_with(progress)
        .sum();

    let mut header = proto::Header::default();
    header.set_version(1);
    header.set_description(description.into());
    header.set_num_postings_lists(num_postings_lists);
    header.set_total_postings_lists(num_postings_lists);
    header.set_total_terms_in_collection(doclen_sum);
    header.set_num_docs(num_documents as i32);
    header.set_total_docs(num_documents as i32);
    #[allow(clippy::cast_precision_loss)]
    header.set_average_doclength(doclen_sum as f64 / f64::from(num_documents));
    Ok(header)
}

fn sizes(memory: &[u8]) -> std::result::Result<BinarySequence<'_>, InvalidFormat> {
    BinaryCollection::try_from(memory)?
        .next()
        .ok_or_else(|| InvalidFormat::new("sizes collection is empty"))?
}

fn write_sizes(sizes_mmap: &Mmap, titles_file: &File, out: &mut CodedOutputStream) -> Result<()> {
    let titles = BufReader::new(titles_file);
    for ((docid, size), title) in sizes(sizes_mmap)?.iter().enumerate().zip(titles.lines()) {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_size_sequence() {
        let empty_memory = Vec::<u8>::new();
        let sizes = sizes(&empty_memory);
        assert!(sizes.is_err());
        assert_eq!(
            "Invalid binary collection format: sizes collection is empty",
            &format!("{}", sizes.err().unwrap())
        );

        let valid_memory: Vec<u8> = [
            5_u32.to_le_bytes(),
            1_u32.to_le_bytes(),
            2_u32.to_le_bytes(),
            3_u32.to_le_bytes(),
            4_u32.to_le_bytes(),
            5_u32.to_le_bytes(),
        ]
        .iter()
        .flatten()
        .copied()
        .collect();
        let sizes = super::sizes(&valid_memory);
        assert!(sizes.is_ok());
        assert_eq!(
            sizes.unwrap().iter().collect::<Vec<u32>>(),
            vec![1_u32, 2, 3, 4, 5]
        );
    }

    fn header_to_buf(header: &proto::Header) -> Result<Vec<u8>> {
        let mut buffer = Vec::<u8>::new();
        let mut out = CodedOutputStream::vec(&mut buffer);
        out.write_message_no_tag(header)?;
        out.flush()?;
        Ok(buffer)
    }

    #[test]
    fn test_read_default_header() -> Result<()> {
        let mut proto_header = proto::Header::default();
        proto_header.set_num_docs(17);
        proto_header.set_num_postings_lists(1234);

        let buffer = header_to_buf(&proto_header)?;

        let mut input = CodedInputStream::from_bytes(&buffer);
        let header = Header::from_stream(&mut input)?;
        assert_eq!(header.protobuf_header, proto_header);
        assert_eq!(header.num_documents, 17);
        assert_eq!(header.num_postings_lists, 1234);
        Ok(())
    }

    #[test]
    fn test_read_negative_num_documents() -> Result<()> {
        let mut proto_header = proto::Header::default();
        proto_header.set_num_docs(-17);

        let buffer = header_to_buf(&proto_header)?;

        let mut input = CodedInputStream::from_bytes(&buffer);
        assert!(Header::from_stream(&mut input).is_err());
        Ok(())
    }

    #[test]
    fn test_read_negative_num_posting_lists() -> Result<()> {
        let mut proto_header = proto::Header::default();
        proto_header.set_num_postings_lists(-1234);

        let buffer = header_to_buf(&proto_header)?;

        let mut input = CodedInputStream::from_bytes(&buffer);
        assert!(Header::from_stream(&mut input).is_err());
        Ok(())
    }
}
