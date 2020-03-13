use ciff::convert;
use std::path::PathBuf;
use tempfile::TempDir;

/// Tests the toy index that can be downloaded from: https://github.com/osirrc/ciff/issues/12
#[test]
fn test_toy_index() -> anyhow::Result<()> {
    let input_path = PathBuf::from("tests/test_data/toy-complete-20200309.ciff");
    let temp = TempDir::new().unwrap();
    let output_path = temp.path().join("coll");
    match convert(&input_path, &output_path) {
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
