//! The synthetic population every mock file draws from: persons born, immigrating,
//! dying and emigrating by Sweden's demography (SCB, 1968–), so that a person's age,
//! sex and county agree across files and registers hold plausible people (the young in
//! education data, the old among pensioners). Firms are a flat list. Nothing here comes
//! from data rows: only the public aggregates in `data/`.

use std::collections::BTreeMap;

use crate::rng::Rng;

const DEMOGRAPHY: &str = include_str!("../data/demography.csv");
const COUNTY: &str = include_str!("../data/county.csv");

/// The first synthetic person key. Keys are small integers from a reserved range,
/// conspicuously unlike a personnummer, and fit a Stata `long`.
pub const PERSON_KEY_BASE: i64 = 900_000_000;
pub const FIRM_KEY_BASE: i64 = 800_000_000;

/// One year's numbers for one sex and age (age 100 is "100 or older").
#[derive(Clone, Copy, Default)]
struct Cell {
    population: f64,
    deaths: f64,
    immigrants: f64,
    emigrants: f64,
}

/// The embedded tables.
pub struct Demography {
    /// (year, sex, age) → numbers.
    cells: BTreeMap<(i32, u8, u8), Cell>,
    /// year → (county code, population), counties in code order.
    counties: BTreeMap<i32, Vec<(String, f64)>>,
    first: i32,
    last: i32,
}

impl Demography {
    /// # Panics
    ///
    /// If the embedded tables are malformed, which `data/refresh.sh` never writes.
    #[must_use]
    pub fn embedded() -> Self {
        let mut cells = BTreeMap::new();
        for line in DEMOGRAPHY.lines().skip(1) {
            let f: Vec<&str> = line.split(',').collect();
            let num = |i: usize| f[i].parse::<f64>().expect("a count");
            cells.insert(
                (
                    f[0].parse().expect("a year"),
                    f[1].parse().expect("a sex"),
                    f[2].parse().expect("an age"),
                ),
                Cell {
                    population: num(3),
                    deaths: num(4),
                    immigrants: num(5),
                    emigrants: num(6),
                },
            );
        }
        let mut counties: BTreeMap<i32, Vec<(String, f64)>> = BTreeMap::new();
        for line in COUNTY.lines().skip(1) {
            let f: Vec<&str> = line.split(',').collect();
            counties
                .entry(f[0].parse().expect("a year"))
                .or_default()
                .push((f[1].to_owned(), f[2].parse().expect("a count")));
        }
        let first = cells.keys().next().expect("demography rows").0;
        let last = cells.keys().next_back().expect("demography rows").0;
        Self {
            cells,
            counties,
            first,
            last,
        }
    }

    /// The year whose numbers stand for `year`: the nearest the tables hold.
    fn year(&self, year: i32) -> i32 {
        year.clamp(self.first, self.last)
    }

    fn cell(&self, year: i32, sex: u8, age: i32) -> Cell {
        let age = u8::try_from(age.clamp(0, 100)).expect("clamped");
        self.cells
            .get(&(self.year(year), sex, age))
            .copied()
            .unwrap_or_default()
    }

    fn total(&self, year: i32) -> f64 {
        let year = self.year(year);
        self.cells
            .range((year, 0, 0)..=(year, u8::MAX, u8::MAX))
            .map(|(_, c)| c.population)
            .sum()
    }

    fn county(&self, year: i32, rng: &mut Rng) -> String {
        let counties = &self.counties[&self.year(year)];
        let total: f64 = counties.iter().map(|(_, n)| n).sum();
        let mut x = rng.unit() * total;
        for (code, n) in counties {
            if x < *n {
                return code.clone();
            }
            x -= n;
        }
        counties.last().expect("counties").0.clone()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    Death,
    Emigration,
}

pub struct Person {
    pub key: i64,
    /// 1 men, 2 women: SCB's coding.
    pub sex: u8,
    /// Days from 1970-01-01.
    pub birth: i64,
    pub county: String,
    /// The year the person became resident: born, immigrated, or the first year.
    pub entered: i32,
    pub exit: Option<(i64, Exit)>,
}

impl Person {
    #[must_use]
    pub fn birth_year(&self) -> i32 {
        year_of(self.birth)
    }

    /// Resident at some time in `year`.
    #[must_use]
    pub fn resident(&self, year: i32) -> bool {
        self.entered <= year && self.exit.is_none_or(|(day, _)| year_of(day) >= year)
    }
}

pub struct Firm {
    pub key: i64,
    pub county: String,
}

pub struct Population {
    pub persons: Vec<Person>,
    pub firms: Vec<Firm>,
}

impl Population {
    /// About `size` persons resident at the start of `first`, then each year's births,
    /// immigrants, deaths and emigrants through `last`, scaled from Sweden's numbers.
    /// One firm per 20 persons.
    ///
    /// # Panics
    ///
    /// Past `i64::MAX` persons, which the size cap rules out.
    #[must_use]
    pub fn simulate(demography: &Demography, seed: u64, size: u64, first: i32, last: i32) -> Self {
        let mut rng = Rng::new(seed, "population");
        #[allow(clippy::cast_precision_loss)]
        let scale = size as f64 / demography.total(first - 1);
        let mut persons = Vec::new();
        let add = |persons: &mut Vec<Person>, rng: &mut Rng, sex: u8, birth: i64, entered: i32| {
            let county = demography.county(entered, rng);
            let key = PERSON_KEY_BASE + 1 + i64::try_from(persons.len()).expect("fits");
            persons.push(Person {
                key,
                sex,
                birth,
                county,
                entered,
                exit: None,
            });
        };
        // Residents on 31 December of the year before.
        for sex in [1, 2] {
            for age in 0..=100 {
                let n = rng.round(scale * demography.cell(first - 1, sex, age).population);
                for _ in 0..n {
                    let birth = day_in(first - 1 - age, &mut rng);
                    add(&mut persons, &mut rng, sex, birth, first);
                }
            }
        }
        for year in first..=last {
            // Survivors to 31 December plus the year's infant deaths.
            for sex in [1, 2] {
                let infants = demography.cell(year, sex, 0);
                for _ in 0..rng.round(scale * (infants.population + infants.deaths)) {
                    let birth = day_in(year, &mut rng);
                    add(&mut persons, &mut rng, sex, birth, year);
                }
                for age in 1..=100 {
                    let n = rng.round(scale * demography.cell(year, sex, age).immigrants);
                    for _ in 0..n {
                        let birth = day_in(year - age, &mut rng);
                        add(&mut persons, &mut rng, sex, birth, year);
                    }
                }
            }
            for person in persons.iter_mut().filter(|p| p.resident(year)) {
                if person.exit.is_some() {
                    continue;
                }
                let age = year - person.birth_year();
                let cell = demography.cell(year, person.sex, age);
                let rate = |n: f64| {
                    if n > 0.0 {
                        n / (cell.population + n)
                    } else {
                        0.0
                    }
                };
                let exit = if rng.chance(rate(cell.deaths)) {
                    Exit::Death
                } else if rng.chance(rate(cell.emigrants)) {
                    Exit::Emigration
                } else {
                    continue;
                };
                let day = day_in(year, &mut rng).max(person.birth);
                person.exit = Some((day, exit));
            }
        }
        let firms = (0..(size / 20).max(1))
            .map(|i| Firm {
                key: FIRM_KEY_BASE + 1 + i64::try_from(i).expect("fits"),
                county: demography.county(last, &mut rng),
            })
            .collect();
        Self { persons, firms }
    }
}

/// A uniformly drawn day of `year`, in days from 1970-01-01.
///
/// # Panics
///
/// Never: a year has 365 or 366 days.
pub fn day_in(year: i32, rng: &mut Rng) -> i64 {
    let start = days_from_civil(year, 1, 1);
    let len = days_from_civil(year + 1, 1, 1) - start;
    start + i64::try_from(rng.below(u64::try_from(len).expect("positive"))).expect("fits")
}

/// Days from 1970-01-01 of a civil date (Howard Hinnant's `days_from_civil`).
#[must_use]
fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let y = i64::from(year) - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(month);
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The year of a day number.
#[must_use]
pub fn year_of(day: i64) -> i32 {
    // An estimate within a year, corrected.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    let mut year = 1970 + (day as f64 / 365.2425).floor() as i32;
    while days_from_civil(year, 1, 1) > day {
        year -= 1;
    }
    while days_from_civil(year + 1, 1, 1) <= day {
        year += 1;
    }
    year
}
