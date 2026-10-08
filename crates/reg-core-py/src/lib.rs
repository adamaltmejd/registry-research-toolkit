//! `reg-core-py`: the Python module the build imports (`RUST_RUNTIME_SPEC.md` section 6).

use pyo3::prelude::*;

#[pymodule]
mod reg_core_py {
    use pyo3::prelude::*;

    /// `reg_core::fold_search`, the search-text fold.
    #[pyfunction]
    fn fold_search(s: &str) -> String {
        reg_core::fold_search(s)
    }
}
