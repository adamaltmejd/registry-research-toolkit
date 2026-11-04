//! Properties over every Unicode scalar value: each fold returns for every input.

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
            let _ = reg_core::normalized_search_query(&s);
        }
        count += 1;
    }
    assert_eq!(count, 0x11_0000 - 0x800);
}
