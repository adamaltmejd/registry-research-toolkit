//! The seeded generator of the property loops.

/// `SplitMix64`: a fixed-seed generator, so a failure reproduces.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `lo..=hi` (the modulo bias is irrelevant here).
    pub fn range(&mut self, lo: u16, hi: u16) -> u16 {
        let span = u64::from(hi - lo) + 1;
        lo + u16::try_from(self.next() % span).expect("below span")
    }
}
