use std::path::PathBuf;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

/// Converts a CIFF index stored in `input_file` to a PISA "binary collection"
/// (uncompressed inverted index) with a basename `output`.
#[pyfunction]
fn ciff_to_pisa_internal(input_file: &str, output: &str) -> PyResult<()> {
    ciff::ciff_to_pisa(&PathBuf::from(input_file), &PathBuf::from(output))
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

#[pyfunction]
/// Converts a PISA "binary collection" (uncompressed inverted index) to a CIFF index.
pub fn pisa_to_ciff_internal(
    collection_input: &str,
    terms_input: &str,
    titles_input: &str,
    output: &str,
    description: &str,
) -> PyResult<()> {
    ciff::pisa_to_ciff(
        &PathBuf::from(collection_input),
        &PathBuf::from(terms_input),
        &PathBuf::from(titles_input),
        &PathBuf::from(output),
        description,
    )
    .map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyciff(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ciff_to_pisa_internal, m)?)?;
    m.add_function(wrap_pyfunction!(pisa_to_ciff_internal, m)?)?;
    Ok(())
}
