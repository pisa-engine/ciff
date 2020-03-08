use indicatif::{ProgressBar, ProgressStyle};
use num::ToPrimitive;
use protobuf::CodedInputStream;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::mem::drop;
use std::path::{PathBuf};
use structopt::StructOpt;

mod proto;
use proto::Header;
use proto::PostingsList;
use proto::DocRecord;

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
    name = "ciff2pisa",
    about = "Generates a PISA index from a Common Index Format [v1]"
)]
struct Args {
    #[structopt(short, long, help = "Path to ciff export file")]
    ciff_file: PathBuf,
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

fn print_header(hdr: Header) {
    println!("----- CIFF HEADER -----");
    println!("Version: {}", hdr.get_version());
    println!("No. Postings Lists: {}", hdr.get_num_postings_lists());
    println!("Total Postings Lists: {}", hdr.get_total_postings_lists());
    println!("No. Documents: {}", hdr.get_num_docs());
    println!("Total Documents: {}", hdr.get_total_docs());
    println!("Total Terms in Collection {}", hdr.get_total_terms_in_collection());
    println!("Average Document Length: {}", hdr.get_average_doclength());
    println!("Description: {}", hdr.get_description());
    println!("-----------------------");
}

fn gen(args: Args) -> Result<()> {
    let mut ciff_reader = File::open(args.ciff_file)?;
    let mut input = CodedInputStream::new(&mut ciff_reader);
    let mut documents = BufWriter::new(File::create(format!("{}.docs", args.output))?);
    let mut frequencies = BufWriter::new(File::create(format!("{}.freqs", args.output))?);
    let mut terms = BufWriter::new(File::create(format!("{}.terms", args.output))?);

    // Read protobuf header
    let header = input.read_message::<Header>()?;
    let num_documents = header.get_num_docs();
    print_header(header.clone());


    eprintln!("Processing postings...");
    encode_sequence(&mut documents, 1, [0_u32].iter().copied())?;
    let bar = ProgressBar::new(262);
    bar.set_style(pb_style());
    bar.set_draw_delta(10);
    let mut postings_seen = 0;
    let expected_postings = header.get_num_postings_lists();
    while postings_seen < expected_postings {
        let posting_list = input.read_message::<PostingsList>()?;

        let length = posting_list
            .get_df()
            .to_u32()
            .ok_or_else(|| format!("Cannot cast to u32: {}", posting_list.get_df()))?;

        let postings = posting_list.get_postings();

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

        postings_seen += 1;
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

    eprintln!("Processing document lengths...");

    let mut sizes = BufWriter::new(File::create(format!("{}.sizes", args.output))?);
    let mut trecids = BufWriter::new(File::create(format!("{}.documents", args.output))?);

    let bar = ProgressBar::new(num_documents as u64);
    bar.set_style(pb_style());
    bar.set_draw_delta(num_documents as u64 / 100);
    sizes.write_all(&num_documents.to_ne_bytes())?;

    let mut docs_seen: usize = 0;
    let expected_docs: usize = header
                          .get_num_docs()
                          .to_usize()
                          .ok_or_else(|| format!("Cannot cast to usize: {}", header.get_num_docs()))?;


    while docs_seen < expected_docs {

        let doc_record = input.read_message::<DocRecord>()?;

        let docid: u32 = doc_record
                  .get_docid()
                  .to_u32()
                  .ok_or_else(|| format!("Cannot cast to u32: {}", doc_record.get_docid()))?;

        let trecid = doc_record.get_collection_docid();
        let length: u32 = doc_record
                  .get_doclength()
                  .to_u32()
                  .ok_or_else(|| format!("Cannot cast to u32: {}", doc_record.get_doclength()))?;


        assert_eq!(
            docid as usize, docs_seen,
            "Document sizes must come in order"
        );
 
       sizes.write_all(&length.to_ne_bytes())?;
        writeln!(trecids, "{}", trecid)?;
        bar.inc(1);
       docs_seen += 1;
    }
    bar.finish();

    Ok(())

}

#[paw::main]
fn main(args: Args) {
    if let Err(error) = gen(args) {
        eprintln!("ERROR: {}", error);
        std::process::exit(1);
    }
}
