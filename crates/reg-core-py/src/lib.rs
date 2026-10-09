//! `reg-core-py`: the Python module the build imports (`RUST_RUNTIME_SPEC.md` section 6).

use pyo3::prelude::*;

#[pymodule]
mod reg_core_py {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use reg_core::{GrammarError as CoreGrammarError, Period};

    pyo3::create_exception!(
        reg_core_py,
        GrammarError,
        PyValueError,
        "A refused FQID, slug or period: the input, then reg-core's grammar message."
    );

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("GrammarError", m.py().get_type::<GrammarError>())
    }

    fn refuse(value: &str, err: CoreGrammarError) -> PyErr {
        GrammarError::new_err(format!("{value:?}: {err}"))
    }

    /// A parsed FQID: `kind` is `provider`, `register`, `variable` or
    /// `classification`; `str()` gives back the one spelling.
    #[pyclass(frozen, eq, module = "reg_core_py")]
    #[derive(PartialEq)]
    struct Fqid(reg_core::Fqid);

    #[pymethods]
    impl Fqid {
        #[getter]
        fn kind(&self) -> &'static str {
            match self.0 {
                reg_core::Fqid::Provider { .. } => "provider",
                reg_core::Fqid::Register { .. } => "register",
                reg_core::Fqid::Variable { .. } => "variable",
                reg_core::Fqid::Classification { .. } => "classification",
            }
        }

        #[getter]
        fn provider(&self) -> Option<&str> {
            match &self.0 {
                reg_core::Fqid::Provider { provider }
                | reg_core::Fqid::Register { provider, .. }
                | reg_core::Fqid::Variable { provider, .. } => Some(provider),
                reg_core::Fqid::Classification { .. } => None,
            }
        }

        #[getter]
        fn register(&self) -> Option<&str> {
            match &self.0 {
                reg_core::Fqid::Register { register, .. }
                | reg_core::Fqid::Variable { register, .. } => Some(register),
                _ => None,
            }
        }

        #[getter]
        fn variable(&self) -> Option<&str> {
            match &self.0 {
                reg_core::Fqid::Variable { variable, .. } => Some(variable),
                _ => None,
            }
        }

        #[getter]
        fn classification(&self) -> Option<&str> {
            match &self.0 {
                reg_core::Fqid::Classification { classification } => Some(classification),
                _ => None,
            }
        }

        fn __str__(&self) -> String {
            self.0.to_string()
        }

        fn __repr__(&self) -> String {
            format!("Fqid({:?})", self.0.to_string())
        }

        fn __hash__(&self) -> u64 {
            let mut hasher = DefaultHasher::new();
            self.0.to_string().hash(&mut hasher);
            hasher.finish()
        }
    }

    /// Parse an FQID (`reg_core::Fqid`).
    #[pyfunction]
    fn parse_fqid(value: &str) -> PyResult<Fqid> {
        value.parse().map(Fqid).map_err(|e| refuse(value, e))
    }

    /// Refuse a string outside the slug grammar (`reg_core::is_slug`).
    #[pyfunction]
    fn check_slug(value: &str) -> PyResult<()> {
        if reg_core::is_slug(value) {
            Ok(())
        } else {
            Err(refuse(value, CoreGrammarError::InvalidRef))
        }
    }

    /// One period token; a `from..to` range is not a token.
    fn period_token(value: &str) -> Result<Period, CoreGrammarError> {
        match value.parse() {
            Ok(token @ Period::Token(_)) => Ok(token),
            _ => Err(CoreGrammarError::InvalidPeriod),
        }
    }

    /// Whether `value` is one period token.
    #[pyfunction]
    fn is_period(value: &str) -> bool {
        period_token(value).is_ok()
    }

    /// The first and last ISO day one period token covers.
    #[pyfunction]
    fn period_bounds(value: &str) -> PyResult<(String, String)> {
        period_token(value)
            .map(Period::iso_bounds)
            .map_err(|e| refuse(value, e))
    }

    /// The coarsest period token whose bounds are exactly `lo..hi`, else `lo..hi`
    /// (`reg_core::period_token_for_bounds`).
    #[pyfunction]
    fn period_token_for_bounds(lo: &str, hi: &str) -> String {
        reg_core::period_token_for_bounds(lo, hi)
    }

    /// The day after an inclusive ISO upper bound (`reg_core::next_iso_day`).
    #[pyfunction]
    fn next_iso_day(s: &str) -> String {
        reg_core::next_iso_day(s)
    }

    /// `reg_core::fold_identity`, the column identity fold.
    #[pyfunction]
    fn fold_identity(s: &str) -> String {
        reg_core::fold_identity(s)
    }

    /// `reg_core::fold_search`, the search-text fold.
    #[pyfunction]
    fn fold_search(s: &str) -> String {
        reg_core::fold_search(s)
    }
}
