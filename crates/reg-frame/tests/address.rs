//! The address grammar's normalization/serialization law, over generated source text.

use reg_frame::address::Address;
use std::fmt::Write;

struct Rng(u64);

impl Rng {
    fn below(&mut self, limit: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        usize::try_from(self.0 >> 32).expect("32 bits fit usize") % limit
    }

    fn pick<'a>(&mut self, choices: &'a [&str]) -> &'a str {
        choices[self.below(choices.len())]
    }

    fn slug(&mut self) -> String {
        let mut text = String::from("t");
        for _ in 0..self.below(8) {
            text.push(char::from(b'a' + u8::try_from(self.below(26)).unwrap()));
            if self.below(4) == 0 {
                text.push_str("-1");
            }
        }
        text
    }

    fn literal(&mut self) -> String {
        if self.below(8) == 0 {
            return "<none>".to_owned();
        }
        let mut text = String::new();
        for _ in 0..=self.below(5) {
            text.push_str(self.pick(&[
                "a", "WHERE", "where", ".", "..", "0", "'", "\"", "Å", "ß", "İ",
            ]));
            if self.below(5) == 0 {
                let scalar = u32::try_from(self.below(0x11_0000)).unwrap();
                if let Some(c) = char::from_u32(scalar) {
                    text.push(c);
                }
            }
        }
        text
    }

    fn spelling(&mut self) -> String {
        match self.below(5) {
            0 => self.literal(),
            1 => {
                let alternatives: Vec<_> = (0..=self.below(4)).map(|_| self.literal()).collect();
                format!("{{{}}}", alternatives.join(","))
            }
            2 => {
                let prefix = self.literal();
                let suffix = self.literal();
                let alternatives: Vec<_> = (0..=self.below(3)).map(|_| self.literal()).collect();
                format!("{prefix}{{{}}}{suffix}", alternatives.join(","))
            }
            3 => {
                let lo = self.below(10);
                let hi = lo + self.below(8);
                format!("{}{{{lo:02}..{hi:02}}}{}", self.literal(), self.literal())
            }
            _ => {
                let alternatives: Vec<_> = (0..=self.below(3)).map(|_| self.literal()).collect();
                let lo = self.below(10);
                let hi = lo + self.below(4);
                format!("{{{}}}{{{lo:02}..{hi:02}}}", alternatives.join(","))
            }
        }
    }

    fn period(&mut self) -> String {
        let year = 1900 + self.below(200);
        match self.below(9) {
            0 => year.to_string(),
            1 => format!("{year}-{:02}", 1 + self.below(12)),
            2 => format!("{year}-{:02}-{:02}", 1 + self.below(12), 1 + self.below(28)),
            3 => format!("{year}-Q{}", 1 + self.below(4)),
            4 => format!("{year}-H{}", 1 + self.below(2)),
            5 => format!("{}{year}", self.pick(&["HT", "VT", "LA"])),
            6 => format!("..{year}"),
            7 => format!("{year}.."),
            _ => format!("{year}..2099"),
        }
    }

    fn suffix(&mut self) -> String {
        let mut text = String::new();
        if self.below(2) == 0 {
            let periods: Vec<_> = (0..=self.below(3)).map(|_| self.period()).collect();
            write!(text, " @{}", periods.join(",")).unwrap();
        }
        for i in 0..self.below(4) {
            let join = if i == 0 { " where " } else { " and " };
            let field = self.pick(&["label", "type", "unit", "key", "release"]);
            let op = if self.below(2) == 0 { "=" } else { "^=" };
            let value = format!(
                "{}{}",
                self.literal(),
                self.pick(&["", " where ", " and ", "/@", "\t", "\""])
            );
            write!(text, "{join}{field} {op} '{value}'").unwrap();
        }
        text
    }

    fn address(&mut self, form: usize) -> String {
        match form {
            0..=1 => {
                let table = if self.below(4) == 0 {
                    self.pick(&["release", "var", "table"]).to_owned()
                } else {
                    self.slug()
                };
                let before = self.pick(&["", " ", "\t"]);
                let after = self.pick(&["", " ", "\t"]);
                format!(
                    "{table}{before}:{after}{}{}",
                    self.spelling(),
                    self.suffix()
                )
            }
            2 | 3 => {
                let kind = if form == 2 { "var" } else { "table" };
                let id = if self.below(3) == 0 {
                    "where".to_owned()
                } else {
                    self.slug()
                };
                format!("{kind}: {id}")
            }
            _ => format!(
                "release:{}/{}{}{}",
                self.slug(),
                self.literal(),
                self.suffix(),
                self.pick(&["", " ", "/", "'\"", "\n"])
            ),
        }
    }
}

// Fails when normalization writes a spelling, brace body or quoted guard the parser
// interprets differently (e.g. treating literal '..' alternatives as a numeric range).
// The oracle is the grammar law parse(serialize(parse(s))) == parse(s); existing
// artifact/CLI cases do not sample this compositional address space.
#[test]
fn accepted_addresses_round_trip() {
    let mut rng = Rng(0x05EE_DADD_2E55);
    let mut accepted = [0; 4];
    for i in 0..20_000 {
        let text = rng.address(i % 5);
        let Ok(address) = text.parse::<Address>() else {
            continue;
        };
        let kind = match &address {
            Address::Occurrence { .. } => 0,
            Address::Variable(_) => 1,
            Address::Table(_) => 2,
            Address::Release { .. } => 3,
        };
        accepted[kind] += 1;
        let display = address.to_string();
        assert_eq!(
            display.parse(),
            Ok(address.clone()),
            "source {text:?}, normalized {display:?}"
        );
        let serialized = serde_json::to_string(&address).unwrap();
        assert_eq!(
            serde_json::from_str::<Address>(&serialized).unwrap(),
            address,
            "source {text:?}, serialized {serialized:?}"
        );
    }
    // Refusal-heavy candidates must still exercise every address form, not pass by
    // rejecting all generated inputs in a branch of the grammar.
    assert!(
        accepted.into_iter().all(|count| count > 100),
        "accepted by form: {accepted:?}"
    );
}
