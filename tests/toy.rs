use ciff::{ciff_to_pisa, pisa_to_ciff};
use std::fs::read;
use std::path::PathBuf;
use tempfile::TempDir;

/// Tests the toy index that can be downloaded from: https://github.com/osirrc/ciff/issues/12
#[test]
fn test_toy_index() -> anyhow::Result<()> {
    let input_path = PathBuf::from("tests/test_data/toy-complete-20200309.ciff");
    let temp = TempDir::new().unwrap();
    let output_path = temp.path().join("coll");
    match ciff_to_pisa(&input_path, &output_path) {
        Err(error) => panic!("{}", error),
        Ok(_) => {}
    }
    assert_eq!(
        std::fs::read_to_string(temp.path().join("coll.documents"))?,
        "WSJ_1\nTREC_DOC_1\nDOC222\n"
    );
    assert_eq!(
        std::fs::read(temp.path().join("coll.sizes"))?,
        vec![3, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0, 6, 0, 0, 0]
    );
    assert_eq!(
        std::fs::read_to_string(temp.path().join("coll.terms"))?
            .lines()
            .collect::<Vec<_>>(),
        vec!["01", "03", "30", "content", "enough", "head", "simpl", "text", "veri"]
    );
    assert_eq!(
        std::fs::read(temp.path().join("coll.docs"))?,
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
        std::fs::read(temp.path().join("coll.freqs"))?,
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
    match ciff_to_pisa(&input_path, &output_path) {
        Err(error) => panic!("{}", error),
        Ok(_) => {}
    }
    let ciff_output_path = temp.path().join("ciff");
    pisa_to_ciff(
        &output_path,
        &temp.path().join("coll.terms"),
        &temp.path().join("coll.documents"),
        &ciff_output_path,
        "Export of toy 3-document collection from Anserini's io.anserini.integration.TrecEndToEndTest test case",
    )?;

    // NOTE: the constructed ciff file will not be exactly the same as the initial one.
    // The reason is that PISA index will be treated as a whole index while the statistics
    // in the initial ciff are as if it is a chunk of a larger index. Therefore, we convert
    // back to PISA to verify.

    let pisa_copy = temp.path().join("copy");
    ciff_to_pisa(&ciff_output_path, &pisa_copy)?;

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
