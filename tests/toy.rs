#![allow(deprecated)]

use ciff::{ciff_to_pisa, concat, pisa_to_ciff, CiffToPisa, PayloadSlice, PisaToCiff};
use std::fs::{read, read_to_string};
use std::path::Path;
use std::path::PathBuf;
use tempfile::TempDir;

/// Tests the toy index that can be downloaded from: https://github.com/osirrc/ciff/issues/12
#[test]
fn test_toy_index() -> anyhow::Result<()> {
    let input_path = PathBuf::from("tests/test_data/toy-complete-20200309.ciff");
    let temp = TempDir::new().unwrap();
    let output_path = temp.path().join("coll");
    CiffToPisa::default()
        .input_path(input_path)
        .output_paths(output_path)
        .convert()
        .unwrap();
    assert_eq!(
        read_to_string(temp.path().join("coll.documents"))?,
        "WSJ_1\nTREC_DOC_1\nDOC222\n"
    );
    let bytes = read(temp.path().join("coll.doclex"))?;
    let actual_titles: Vec<_> = PayloadSlice::new(&bytes).iter().collect();
    assert_eq!(
        actual_titles,
        vec![b"WSJ_1".as_ref(), b"TREC_DOC_1", b"DOC222"],
    );
    assert_eq!(
        read(temp.path().join("coll.sizes"))?,
        vec![3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0]
    );
    assert_eq!(
        read_to_string(temp.path().join("coll.terms"))?
            .lines()
            .collect::<Vec<_>>(),
        vec!["01", "03", "30", "content", "enough", "head", "simpl", "text", "veri"]
    );
    let bytes = read(temp.path().join("coll.termlex"))?;
    let actual_terms: Vec<_> = PayloadSlice::new(&bytes).iter().collect();
    assert_eq!(
        actual_terms,
        vec![
            b"01".as_ref(),
            b"03",
            b"30",
            b"content",
            b"enough",
            b"head",
            b"simpl",
            b"text",
            b"veri"
        ]
    );
    assert_eq!(
        read(temp.path().join("coll.docs"))?,
        vec![
            1, 0, 0, 0, 3, 0, 0, 0, // Number of documents
            1, 0, 0, 0, 0, 0, 0, 0, // t0
            1, 0, 0, 0, 0, 0, 0, 0, // t1
            1, 0, 0, 0, 0, 0, 0, 0, // t2
            1, 0, 0, 0, 0, 0, 0, 0, // t3
            1, 0, 0, 0, 2, 0, 0, 0, // t4
            3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t5
            2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t6
            3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t7
            1, 0, 0, 0, 1, 0, 0, 0, // t8
        ]
    );
    assert_eq!(
        read(temp.path().join("coll.freqs"))?,
        vec![
            1, 0, 0, 0, 1, 0, 0, 0, // t0
            1, 0, 0, 0, 1, 0, 0, 0, // t1
            1, 0, 0, 0, 1, 0, 0, 0, // t2
            1, 0, 0, 0, 1, 0, 0, 0, // t3
            1, 0, 0, 0, 1, 0, 0, 0, // t4
            3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, // t5
            2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, // t6
            3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, // t7
            1, 0, 0, 0, 1, 0, 0, 0, // t8
        ]
    );
    Ok(())
}

#[test]
fn test_to_and_from_ciff() -> anyhow::Result<()> {
    let input_path = PathBuf::from("tests/test_data/toy-complete-20200309.ciff");
    let temp = TempDir::new().unwrap();
    let output_path = temp.path().join("coll");
    CiffToPisa::default()
        .input_path(input_path)
        .output_paths(&output_path)
        .convert()
        .unwrap();
    let ciff_output_path = temp.path().join("ciff");
    PisaToCiff::default()
        .index_paths(&output_path)
        .terms_path(&temp.path().join("coll.terms"))
        .titles_path(&temp.path().join("coll.documents"))
        .output_path(&ciff_output_path)
        .convert()?;

    // NOTE: the constructed ciff file will not be exactly the same as the initial one.
    // The reason is that PISA index will be treated as a whole index while the statistics
    // in the initial ciff are as if it is a chunk of a larger index. Therefore, we convert
    // back to PISA to verify.

    let pisa_copy = temp.path().join("copy");
    CiffToPisa::default()
        .input_path(&ciff_output_path)
        .output_paths(&pisa_copy)
        .convert()
        .unwrap();

    let coll_basename = output_path.display().to_string();
    let copy_basename = pisa_copy.display().to_string();

    assert_eq!(
        read(format!("{}.sizes", coll_basename))?,
        read(format!("{}.sizes", copy_basename))?
    );
    assert_eq!(
        read(format!("{}.terms", coll_basename))?,
        read(format!("{}.terms", copy_basename))?
    );
    assert_eq!(
        read(format!("{}.documents", coll_basename))?,
        read(format!("{}.documents", copy_basename))?
    );
    assert_eq!(
        read(format!("{}.docs", coll_basename))?,
        read(format!("{}.docs", copy_basename))?
    );
    assert_eq!(
        read(format!("{}.freqs", coll_basename))?,
        read(format!("{}.freqs", copy_basename))?
    );

    Ok(())
}

#[test]
fn test_reorder_terms() -> anyhow::Result<()> {
    let input_path = PathBuf::from("tests/test_data/toy-complete-20200309.ciff");
    let temp = TempDir::new().unwrap();
    let pisa_path = temp.path().join("coll");
    CiffToPisa::default()
        .input_path(input_path)
        .output_paths(&pisa_path)
        .convert()
        .unwrap();

    // Rewrite the terms; later, we will check if the posting lists are in reverse order.
    std::fs::write(
        temp.path().join("coll.terms"),
        vec![
            "veri", "text", "simpl", "head", "enough", "content", "30", "03", "01",
        ]
        .join("\n"),
    )?;

    let ciff_output_path = temp.path().join("ciff");
    PisaToCiff::default()
        .index_paths(&pisa_path)
        .terms_path(&temp.path().join("coll.terms"))
        .titles_path(&temp.path().join("coll.documents"))
        .output_path(&ciff_output_path)
        .convert()?;

    // Convert back to PISA to verify list order
    let pisa_copy = temp.path().join("copy");
    CiffToPisa::default()
        .input_path(ciff_output_path)
        .output_paths(pisa_copy)
        .convert()
        .unwrap();

    assert_eq!(
        read_to_string(temp.path().join("copy.documents"))?,
        "WSJ_1\nTREC_DOC_1\nDOC222\n"
    );
    assert_eq!(
        read(temp.path().join("coll.sizes"))?,
        vec![3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0]
    );
    assert_eq!(
        read_to_string(temp.path().join("copy.terms"))?
            .lines()
            .collect::<Vec<_>>(),
        vec!["01", "03", "30", "content", "enough", "head", "simpl", "text", "veri"]
    );
    assert_eq!(
        read(temp.path().join("copy.docs"))?,
        vec![
            1, 0, 0, 0, 3, 0, 0, 0, // Number of documents
            1, 0, 0, 0, 1, 0, 0, 0, // t8
            3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t7
            2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t6
            3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, // t5
            1, 0, 0, 0, 2, 0, 0, 0, // t4
            1, 0, 0, 0, 0, 0, 0, 0, // t3
            1, 0, 0, 0, 0, 0, 0, 0, // t2
            1, 0, 0, 0, 0, 0, 0, 0, // t1
            1, 0, 0, 0, 0, 0, 0, 0, // t0
        ]
    );
    assert_eq!(
        read(temp.path().join("copy.freqs"))?,
        vec![
            1, 0, 0, 0, 1, 0, 0, 0, // t8
            3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 3, 0, 0, 0, // t7
            2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, // t6
            3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, // t5
            1, 0, 0, 0, 1, 0, 0, 0, // t4
            1, 0, 0, 0, 1, 0, 0, 0, // t3
            1, 0, 0, 0, 1, 0, 0, 0, // t2
            1, 0, 0, 0, 1, 0, 0, 0, // t1
            1, 0, 0, 0, 1, 0, 0, 0, // t0
        ]
    );

    Ok(())
}

fn assert_files_eq<P1: AsRef<Path>, P2: AsRef<Path>>(lhs: P1, rhs: P2) {
    if read(lhs.as_ref()).unwrap() != read(rhs.as_ref()).unwrap() {
        panic!(
            "Files not equal: {}, {}",
            lhs.as_ref().display(),
            rhs.as_ref().display()
        );
    }
}

#[test]
fn test_legacy_api() {
    let input_path = PathBuf::from("tests/test_data/toy-complete-20200309.ciff");
    let temp = TempDir::new().unwrap();

    let builder_output = temp.path().join("builder");
    let legacy_output = temp.path().join("legacy");

    CiffToPisa::default()
        .input_path(&input_path)
        .output_paths(&builder_output)
        .convert()
        .unwrap();
    ciff_to_pisa(&input_path, &legacy_output, true).unwrap();

    for suffix in [
        ".docs",
        ".freqs",
        ".sizes",
        ".documents",
        ".terms",
        ".doclex",
        ".termlex",
    ] {
        assert_files_eq(
            concat(&builder_output, suffix),
            concat(&legacy_output, suffix),
        );
    }

    let builder_ciff = temp.path().join("builder.ciff");
    let legacy_ciff = temp.path().join("legacy.ciff");

    PisaToCiff::default()
        .description("description")
        .pisa_paths(&builder_output)
        .output_path(&builder_ciff)
        .convert()
        .unwrap();
    pisa_to_ciff(
        &legacy_output,
        &PathBuf::from(concat(&legacy_output, ".terms")),
        &PathBuf::from(concat(&legacy_output, ".documents")),
        &legacy_ciff,
        "description",
    )
    .unwrap();

    assert_files_eq(builder_ciff, legacy_ciff);
}

#[test]
fn test_skip_lexicons() {
    let input_path = PathBuf::from("tests/test_data/toy-complete-20200309.ciff");
    let temp = TempDir::new().unwrap();
    let output = temp.path().join("builder");

    ciff_to_pisa(&input_path, &output, false).unwrap();
    assert!(!PathBuf::from(concat(&output, ".termlex")).exists());
    assert!(!PathBuf::from(concat(&output, ".doclex")).exists());

    CiffToPisa::default()
        .input_path(&input_path)
        .output_paths(&output)
        .skip_lexicons()
        .convert()
        .unwrap();
    assert!(!PathBuf::from(concat(&output, ".termlex")).exists());
    assert!(!PathBuf::from(concat(&output, ".doclex")).exists());
}
