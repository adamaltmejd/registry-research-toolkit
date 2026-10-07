//! Python bindings for the spike: the shape `reg-core-py` would take.

use pyo3::prelude::*;

#[pymodule]
mod reg_core_spike {
    use pyo3::prelude::*;

    #[pyfunction]
    fn fold_identity(s: &str) -> String {
        crate::fold_identity(s)
    }
    #[pyfunction]
    fn fold_search(s: &str) -> String {
        crate::fold_search(s)
    }
    #[pyfunction]
    fn normalized_search_query(s: &str) -> String {
        crate::normalized_search_query(s)
    }
    #[pyfunction]
    fn fts_match_query(s: &str) -> Option<String> {
        crate::fts_match_query(s)
    }
    #[pyfunction]
    fn py_isspace(c: char) -> bool {
        crate::py_isspace(c)
    }
    #[pyfunction]
    fn py_isalnum(c: char) -> bool {
        crate::py_isalnum(c)
    }
}
