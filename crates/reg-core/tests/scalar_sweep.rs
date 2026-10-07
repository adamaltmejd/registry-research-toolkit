//! Properties over every Unicode scalar value: each fold returns for every input, and
//! `fold_search` is idempotent.

fn scalars() -> impl Iterator<Item = char> {
    (0..=u32::from(char::MAX)).filter_map(char::from_u32)
}

#[test]
fn every_scalar_folds_without_panic() {
    let mut count = 0_u32;
    for c in scalars() {
        // Bare and in a context that exercises Final_Sigma and token boundaries.
        for s in [c.to_string(), format!("ΑΣ{c} x")] {
            let _ = reg_core::fold_identity(&s);
            let _ = reg_core::fold_search(&s);
            let _ = reg_core::normalized_search_query(&s);
            let _ = reg_core::fts_match_query(&s);
        }
        count += 1;
    }
    assert_eq!(count, 0x11_0000 - 0x800);
}

#[test]
fn fold_search_is_idempotent_on_every_scalar() {
    let failures: Vec<String> = scalars()
        .filter_map(|c| {
            let once = reg_core::fold_search(&c.to_string());
            let twice = reg_core::fold_search(&once);
            (once != twice).then(|| format!("U+{:04X}", u32::from(c)))
        })
        .collect();
    assert!(
        failures.is_empty(),
        "{} scalars: {}",
        failures.len(),
        failures[..failures.len().min(20)].join(", ")
    );
}
