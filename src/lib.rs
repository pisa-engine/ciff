//! Library supporting converting CIFF to PISA uncompressed collection format.
//! Refer to [`osirrc/ciff`](https://github.com/osirrc/ciff) on Github
//! for more detailed information about the format.
//!
//! For more information about PISA's internal storage formats, see the
//! [documentation](https://pisa.readthedocs.io/en/latest/index.html).
//!
//! # Examples
//!
//! Use [`PisaToCiff`] and [`CiffToPisa`] builders to convert from one format
//! to another.
//!
//! ```
//! # use std::path::PathBuf;
//! # use tempfile::TempDir;
//! # use ciff::{PisaToCiff, CiffToPisa};
//! # fn main() -> anyhow::Result<()> {
//! # let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
//! # let ciff_file = dir.join("tests").join("test_data").join("toy-complete-20200309.ciff");
//! # let temp = TempDir::new()?;
//! # let pisa_base_path = temp.path().join("pisa");
//! # let output = temp.path().join("output");
//! CiffToPisa::default()
//!     .input_path(ciff_file)
//!     .output_paths(&pisa_base_path)
//!     .convert()?;
//! PisaToCiff::default()
//!     .description("Hello, CIFF!")
//!     .pisa_paths(&pisa_base_path)
//!     .output_path(output)
//!     .convert()?;
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/ciff/0.3.0")]
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
use std::ffi::{OsStr, OsString};
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
pub use payload_vector::{build_lexicon, PayloadIter, PayloadSlice, PayloadVector};

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

fn append_suffix<P, S>(path: P, suffix: S) -> OsString
where
    P: AsRef<OsStr>,
    S: AsRef<OsStr>,
{
    let mut path = path.as_ref().to_owned();
    path.push(suffix);
    path
}

/// Paths to an inverted index in an uncompressed PISA format.
#[derive(Debug, Clone, Default)]
struct PisaIndexPaths {
    documents: PathBuf,
    frequencies: PathBuf,
    sizes: PathBuf,
}

impl PisaIndexPaths {
    #[must_use]
    fn from_base_path<P: AsRef<OsStr>>(path: P) -> Self {
        Self {
            documents: PathBuf::from(append_suffix(path.as_ref(), ".docs")),
            frequencies: PathBuf::from(append_suffix(path.as_ref(), ".freqs")),
            sizes: PathBuf::from(append_suffix(path.as_ref(), ".sizes")),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct PisaPaths {
    index: PisaIndexPaths,
    terms: PathBuf,
    titles: PathBuf,
    termlex: Option<PathBuf>,
    doclex: Option<PathBuf>,
}

impl PisaPaths {
    #[must_use]
    fn from_base_path<P: AsRef<OsStr>>(path: P) -> Self {
        Self {
            index: PisaIndexPaths::from_base_path(&path),
            terms: PathBuf::from(append_suffix(&path, ".terms")),
            titles: PathBuf::from(append_suffix(&path, ".documents")),
            termlex: Some(PathBuf::from(append_suffix(&path, ".termlex"))),
            doclex: Some(PathBuf::from(append_suffix(&path, ".doclex"))),
        }
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

fn reorder_pisa_index(paths: &PisaPaths) -> Result<()> {
    let terms = BufReader::new(File::open(&paths.terms)?)
        .lines()
        .collect::<io::Result<Vec<_>>>()?;
    let mut order: Vec<_> = (0..terms.len()).collect();
    order.sort_by_key(|&i| &terms[i]);
    reorder_postings(&paths.index.documents, &order, true)?;
    reorder_postings(&paths.index.frequencies, &order, false)?;
    let mut term_writer = BufWriter::new(File::create(&paths.terms)?);
    for index in order {
        writeln!(&mut term_writer, "{}", terms[index])?;
    }
    Ok(())
}

/// CIFF to PISA converter.
#[derive(Debug, Default, Clone)]
pub struct CiffToPisa {
    input: Option<PathBuf>,
    documents_path: Option<PathBuf>,
    frequencies_path: Option<PathBuf>,
    sizes_path: Option<PathBuf>,
    terms_path: Option<PathBuf>,
    titles_path: Option<PathBuf>,
    termlex_path: Option<PathBuf>,
    doclex_path: Option<PathBuf>,
}

impl CiffToPisa {
    /// Sets the CIFF path. Required.
    pub fn input_path<P: Into<PathBuf>>(&mut self, path: P) -> &mut Self {
        self.input = Some(path.into());
        self
    }

    /// Sets PISA (uncompressed) inverted index paths. Required.
    ///
    /// Paths are constructed by appending file extensions to the base path:
    ///  - `.docs` for document postings,
    ///  - `.freqs` for frequency postings,
    ///  - `.sizes` for document sizes,
    ///  - `.terms` for terms text file,
    ///  - `.documents` for document titles text file,
    ///  - `.termlex` for term lexicon,
    ///  - `.doclex` for document lexicon.
    pub fn output_paths<P: AsRef<OsStr>>(&mut self, base_path: P) -> &mut Self {
        let paths = PisaPaths::from_base_path(base_path);
        self.documents_path = Some(paths.index.documents);
        self.frequencies_path = Some(paths.index.frequencies);
        self.sizes_path = Some(paths.index.sizes);
        self.terms_path = Some(paths.terms);
        self.titles_path = Some(paths.titles);
        self.termlex_path = paths.termlex;
        self.doclex_path = paths.doclex;
        self
    }

    /// Do not construct document and term lexicons.
    pub fn skip_lexicons(&mut self) -> &mut Self {
        self.termlex_path = None;
        self.doclex_path = None;
        self
    }

    /// Builds a PISA index using the previously defined parameters.
    ///
    /// # Errors
    ///
    /// Error will be returned if:
    ///  - some required parameters are not defined,
    ///  - any I/O error occurs during reading input files or writing to the output file,
    ///  - any input file is in an incorrect format.
    pub fn convert(&self) -> Result<()> {
        let input = self
            .input
            .as_ref()
            .ok_or_else(|| anyhow!("input path undefined"))?;
        let index_output = PisaIndexPaths {
            documents: self
                .documents_path
                .clone()
                .ok_or_else(|| anyhow!("document postings path undefined"))?,
            frequencies: self
                .frequencies_path
                .clone()
                .ok_or_else(|| anyhow!("frequency postings path undefined"))?,
            sizes: self
                .sizes_path
                .clone()
                .ok_or_else(|| anyhow!("document sizes path undefined"))?,
        };
        let output = PisaPaths {
            index: index_output,
            terms: self
                .terms_path
                .clone()
                .ok_or_else(|| anyhow!("terms path undefined"))?,
            titles: self
                .titles_path
                .clone()
                .ok_or_else(|| anyhow!("terms path undefined"))?,
            termlex: self.termlex_path.clone(),
            doclex: self.doclex_path.clone(),
        };
        convert_to_pisa(input, &output)
    }
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
#[deprecated = "use CiffToPisa instead"]
pub fn ciff_to_pisa(input: &Path, output: &Path, generate_lexicons: bool) -> Result<()> {
    let mut converter = CiffToPisa::default();
    converter.input_path(input).output_paths(output);
    if !generate_lexicons {
        converter.skip_lexicons();
    }
    converter.convert()
}

fn convert_to_pisa(input: &Path, output: &PisaPaths) -> Result<()> {
    println!("{:?}", output);
    let mut ciff_reader =
        File::open(input).with_context(|| format!("Unable to open {}", input.display()))?;
    let mut input = CodedInputStream::new(&mut ciff_reader);
    let mut documents = BufWriter::new(File::create(&output.index.documents)?);
    let mut frequencies = BufWriter::new(File::create(&output.index.frequencies)?);
    let mut terms = BufWriter::new(File::create(&output.terms)?);

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
    let mut sizes = BufWriter::new(File::create(&output.index.sizes)?);
    let mut trecids = BufWriter::new(File::create(&output.titles)?);

    let progress = ProgressBar::new(u64::from(header.num_documents));
    progress.set_style(pb_style());
    progress.set_draw_delta(u64::from(header.num_documents) / 100);
    sizes.write_all(&header.num_documents.to_le_bytes())?;
    sizes.flush()?;

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
    trecids.flush()?;
    progress.finish();

    if !check_lines_sorted(BufReader::new(File::open(&output.terms)?))? {
        reorder_pisa_index(output)?;
    }

    eprintln!("Generating the document and term lexicons...");
    drop(trecids);
    if let Some(termlex) = output.termlex.as_ref() {
        build_lexicon(&output.terms, termlex)?;
    }
    if let Some(doclex) = output.doclex.as_ref() {
        build_lexicon(&output.titles, doclex)?;
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

/// PISA to CIFF converter.
#[derive(Debug, Default, Clone)]
pub struct PisaToCiff {
    documents_path: Option<PathBuf>,
    frequencies_path: Option<PathBuf>,
    sizes_path: Option<PathBuf>,
    terms_path: Option<PathBuf>,
    titles_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    description: String,
}

impl PisaToCiff {
    /// Sets CIFF index description.
    pub fn description<S: Into<String>>(&mut self, description: S) -> &mut Self {
        self.description = description.into();
        self
    }

    /// Sets PISA paths. Required.
    ///
    /// Paths are constructed by appending file extensions to the base path:
    ///  - `.docs` for document postings,
    ///  - `.freqs` for frequency postings,
    ///  - `.sizes` for document sizes,
    ///  - `.terms` for terms text file,
    ///  - `.documents` for document titles text file,
    pub fn pisa_paths<P: AsRef<OsStr>>(&mut self, base_path: P) -> &mut Self {
        let paths = PisaPaths::from_base_path(base_path);
        self.documents_path = Some(paths.index.documents);
        self.frequencies_path = Some(paths.index.frequencies);
        self.sizes_path = Some(paths.index.sizes);
        self.terms_path = Some(paths.terms);
        self.titles_path = Some(paths.titles);
        self
    }

    /// Sets PISA (uncompressed) inverted index paths. Required.
    ///
    /// Constructs paths using the given base path, appeding suffixes:
    /// `.docs`, `.freqs`, and `.sizes`.
    pub fn index_paths<P: AsRef<OsStr>>(&mut self, base_path: P) -> &mut Self {
        let PisaIndexPaths {
            documents,
            frequencies,
            sizes,
        } = PisaIndexPaths::from_base_path(base_path);
        self.documents_path = Some(documents);
        self.frequencies_path = Some(frequencies);
        self.sizes_path = Some(sizes);
        self
    }

    /// Sets the path of the term file (newline-delimited text format). Required.
    pub fn terms_path<P: Into<PathBuf>>(&mut self, path: P) -> &mut Self {
        self.terms_path = Some(path.into());
        self
    }

    /// Sets the path of the document titles file (newline-delimited text format). Required.
    pub fn titles_path<P: Into<PathBuf>>(&mut self, path: P) -> &mut Self {
        self.titles_path = Some(path.into());
        self
    }

    /// Set the output file path. Required.
    pub fn output_path<P: Into<PathBuf>>(&mut self, path: P) -> &mut Self {
        self.output_path = Some(path.into());
        self
    }

    /// Builds a CIFF index using the previously defined parameters.
    ///
    /// # Errors
    ///
    /// Error will be returned if:
    ///  - some required parameters are not defined,
    ///  - any I/O error occurs during reading input files or writing to the output file,
    ///  - any input file is in an incorrect format.
    pub fn convert(&self) -> Result<()> {
        pisa_to_ciff_from_paths(
            self.documents_path
                .as_ref()
                .ok_or_else(|| anyhow!("undefined document postings path"))?,
            self.frequencies_path
                .as_ref()
                .ok_or_else(|| anyhow!("undefined frequency postings path"))?,
            self.sizes_path
                .as_ref()
                .ok_or_else(|| anyhow!("undefined document sizes path"))?,
            self.terms_path
                .as_ref()
                .ok_or_else(|| anyhow!("undefined terms path"))?,
            self.titles_path
                .as_ref()
                .ok_or_else(|| anyhow!("undefined titles path"))?,
            self.output_path
                .as_ref()
                .ok_or_else(|| anyhow!("undefined output path"))?,
            &self.description,
        )
    }
}

/// Converts a a PISA "binary collection" (uncompressed inverted index) with a basename `input`
/// to a CIFF index stored in `output`.
///
/// # Errors
///
/// Returns an error when:
/// - an IO error occurs,
/// - writing protobuf format fails,
#[deprecated = "use PisaToCiff instead"]
pub fn pisa_to_ciff(
    collection_input: &Path,
    terms_input: &Path,
    titles_input: &Path,
    output: &Path,
    description: &str,
) -> Result<()> {
    PisaToCiff::default()
        .description(description)
        .index_paths(collection_input)
        .terms_path(terms_input)
        .titles_path(titles_input)
        .output_path(output)
        .convert()
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
