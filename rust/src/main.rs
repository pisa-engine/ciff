use indicatif::{ProgressBar, ProgressStyle};
use num::ToPrimitive;
use protobuf::CodedInputStream;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::mem::drop;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

mod proto;
use proto::PostingsList;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Returns default progress style.
fn pb_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {count}/{total} ({eta})",
        )
        .progress_chars("=> ")
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "cif2pisa",
    about = "Generates a PISA index from a Common Index Format"
)]
struct Args {
    #[structopt(short, long, help = "Path to postings file")]
    postings: PathBuf,
    #[structopt(short, long = "doclen", help = "Path to document lengths file")]
    document_lengths: PathBuf,
    #[structopt(short, long, help = "Output basename")]
    output: String,
}

fn encode_sequence<N, S, W>(writer: &mut W, len: u32, sequence: S) -> Result<()>
where
    N: ToPrimitive,
    S: IntoIterator<Item = N>,
    W: Write,
{
    let size: [u8; 4] = len.to_ne_bytes();
    writer.write_all(&size)?;
    for element in sequence {
        writer.write_all(&element.to_u32().ok_or("")?.to_ne_bytes())?;
    }
    Ok(())
}

fn write_document_lengths(input: &Path, output_basename: &str) -> Result<u32> {
    eprintln!("Processing document lengths...");
    let input_file = File::open(input)?;

    let num_documents = BufReader::new(&File::open(input)?)
        .lines()
        .count()
        .to_u32()
        .ok_or("Number of documents too large (must fit in u32)")?;

    let input = BufReader::new(&input_file);
    let mut sizes = BufWriter::new(File::create(format!("{}.sizes", output_basename))?);
    let mut trecids = BufWriter::new(File::create(format!("{}.documents", output_basename))?);

    let bar = ProgressBar::new(num_documents as u64);
    bar.set_style(pb_style());
    bar.set_draw_delta(num_documents as u64 / 100);
    sizes.write_all(&num_documents.to_ne_bytes())?;
    for (expected_docid, line) in input.lines().enumerate() {
        let line = line?;
        let mut columns = line.split('\t');
        let docid: u32 = columns.next().ok_or("Corrupted file")?.parse()?;
        let trecid = columns.next().ok_or("Corrupted file")?;
        let length: u32 = columns.next().ok_or("Corrupted file")?.parse()?;
        assert_eq!(
            docid as usize, expected_docid,
            "Document sizes must come in order"
        );
        sizes.write_all(&length.to_ne_bytes())?;
        writeln!(trecids, "{}", trecid)?;
        bar.inc(1);
    }
    bar.finish();
    Ok(num_documents)
}

fn gen(args: Args) -> Result<()> {
    let mut postings_reader = File::open(args.postings)?;
    let mut input = CodedInputStream::new(&mut postings_reader);
    let mut documents = BufWriter::new(File::create(format!("{}.docs", args.output))?);
    let mut frequencies = BufWriter::new(File::create(format!("{}.freqs", args.output))?);
    let mut terms = BufWriter::new(File::create(format!("{}.terms", args.output))?);

    let num_documents = write_document_lengths(&args.document_lengths, &args.output)?;

    eprintln!("Processing postings...");
    encode_sequence(&mut documents, 1, [0_u32].iter().copied())?;
    let bar = ProgressBar::new(262);
    bar.set_style(pb_style());
    bar.set_draw_delta(10);
    while !input.eof()? {
        let posting_list = input.read_message::<PostingsList>()?;

        let length = posting_list
            .get_df()
            .to_u32()
            .ok_or_else(|| format!("Cannot cast to u32: {}", posting_list.get_df()))?;

        let postings = posting_list.get_posting();

        encode_sequence(
            &mut documents,
            length,
            postings.iter().scan(0, |prev, p| {
                *prev += p.get_docid();
                Some(*prev)
            }),
        )?;

        encode_sequence(
            &mut frequencies,
            length,
            postings.iter().map(|p| p.get_tf()),
        )?;

        writeln!(terms, "{}", posting_list.get_term())?;

        bar.inc(1);
    }
    bar.finish();

    documents.flush()?;
    frequencies.flush()?;
    terms.flush()?;

    drop(documents);
    let mut documents = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .append(false)
        .open(format!("{}.docs", args.output))?;
    documents.seek(SeekFrom::Start(0))?;
    encode_sequence(&mut documents, 1, [num_documents].iter().copied())?;

    Ok(())
}

#[paw::main]
fn main(args: Args) {
    if let Err(error) = gen(args) {
        eprintln!("ERROR: {}", error);
        std::process::exit(1);
    }
}
