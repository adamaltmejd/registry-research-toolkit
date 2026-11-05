//! The generator's random numbers: xoshiro256** seeded by `SplitMix64`, and the few
//! draws it needs. Written out rather than taken from `rand`, whose generators and
//! distributions may change their output between releases: a mock's bytes must depend
//! only on the spec, the seed and this crate (crates/reg-mock/DESIGN.md, "Determinism").

/// One random stream.
pub struct Rng([u64; 4]);

impl Rng {
    /// The stream for `seed` and a `label` (a file, a column, the population), so that
    /// adding a column or a file changes no other stream.
    #[must_use]
    pub fn new(seed: u64, label: &str) -> Self {
        // FNV-1a of the label, mixed into the seed.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in label.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        let mut state = seed ^ hash;
        let mut next = || {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        Self([next(), next(), next(), next()])
    }

    pub fn next_u64(&mut self) -> u64 {
        let s = &mut self.0;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        // 53 random bits: exact in an f64.
        #[allow(clippy::cast_precision_loss)]
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit
    }

    /// Uniform in `[0, n)`; `n` must be above 0.
    pub fn below(&mut self, n: u64) -> u64 {
        // simplify: modulo bias below 2^-40 for the n this crate draws (< 2^24).
        self.next_u64() % n
    }

    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }

    /// A count with mean `mean`: Knuth's method below 30, else a rounded normal.
    pub fn poisson(&mut self, mean: f64) -> u64 {
        if mean <= 0.0 {
            return 0;
        }
        if mean < 30.0 {
            let limit = (-mean).exp();
            let (mut k, mut p) = (0, self.unit());
            while p > limit {
                k += 1;
                p *= self.unit();
            }
            return k;
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let k = (mean + mean.sqrt() * self.normal()).round().max(0.0) as u64;
        k
    }

    /// A standard normal draw (Box–Muller).
    pub fn normal(&mut self) -> f64 {
        let u = 1.0 - self.unit();
        let v = self.unit();
        (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()
    }

    /// `x` rounded to a neighbouring integer with probability by its fraction, so many
    /// scaled counts keep their total.
    pub fn round(&mut self, x: f64) -> u64 {
        let floor = x.floor();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let n = floor as u64 + u64::from(self.chance(x - floor));
        n
    }
}
