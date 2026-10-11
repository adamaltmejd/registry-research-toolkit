//! One Unicode version for all of `reg-core`'s Unicode data (`crates/DESIGN.md`, "Text folds").

#[test]
fn unicode_data_is_on_one_version() {
    let ours = reg_core::UNICODE_VERSION;
    assert_eq!(ours, (17, 0, 0));
    assert_eq!(char::UNICODE_VERSION, ours, "Rust std");
    assert_eq!(
        unicode_normalization::UNICODE_VERSION,
        ours,
        "unicode-normalization"
    );
    let (major, minor, update) = unicode_properties::UNICODE_VERSION;
    assert_eq!(
        (major, minor, update),
        (ours.0.into(), ours.1.into(), ours.2.into()),
        "unicode-properties"
    );
}
