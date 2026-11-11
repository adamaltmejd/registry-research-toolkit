//! Periods in plain words, for a researcher reading an order finding (#2409, #2415).

use crate::ops::Period;

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

/// Periods in plain words at their grain: whole school years as `2004/2005`
/// (`reg_webapp/frontend/DESIGN.md` → Time, #2315, #2389), whole years as `2017`,
/// whole months as `July to December 2018` or `2017 to June 2018`, else days
/// (`15 October 2019`); an open end as `2019 onwards` or `every year through 2019`;
/// several periods joined with "and".
pub(super) fn plain(periods: &[Period]) -> String {
    let words: Vec<String> = periods
        .iter()
        .map(|p| {
            let last = p.end.as_deref().map(reg_core::prev_iso_day);
            match (p.start.as_deref(), last.as_deref()) {
                (Some(first), Some(last)) => plain_span(first, last),
                (Some(first), None) => format!("{} onwards", plain_start(first)),
                (None, Some(last)) => format!("every year through {}", plain_end(last)),
                (None, None) => "every year".to_owned(),
            }
        })
        .collect();
    match words.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
        _ => words.concat(),
    }
}

/// One inclusive span of ISO days in words (see [`plain`]).
fn plain_span(first: &str, last: &str) -> String {
    let year = |iso: &str| -> i32 { iso[..4].parse().expect("an ISO year") };
    if first.ends_with("-07-01") && last.ends_with("-06-30") {
        let (from, to) = (year(first), year(last) - 1);
        let school = |y: i32| format!("{y}/{}", y + 1);
        return if from == to {
            school(from)
        } else {
            format!("{} to {}", school(from), school(to))
        };
    }
    let month = |iso: &str| MONTHS[iso[5..7].parse::<usize>().expect("an ISO month") - 1];
    let month_end = |iso: &str| reg_core::next_iso_day(iso).ends_with("-01");
    if first == last {
        let day: u32 = first[8..].parse().expect("an ISO day");
        return format!("{day} {} {}", month(first), &first[..4]);
    }
    if year(first) == year(last) {
        let y = year(first);
        if first.ends_with("-01-01") && last.ends_with("-12-31") {
            return y.to_string();
        }
        if first.ends_with("-01") && month_end(last) {
            return if first[5..7] == last[5..7] {
                format!("{} {y}", month(first))
            } else {
                format!("{} to {} {y}", month(first), month(last))
            };
        }
    }
    format!("{} to {}", plain_start(first), plain_end(last))
}

/// A span's first day in words: its year or month when it starts one, else the day.
pub(super) fn plain_start(first: &str) -> String {
    let day: u32 = first[8..].parse().expect("an ISO day");
    let month = MONTHS[first[5..7].parse::<usize>().expect("an ISO month") - 1];
    match (&first[5..], day) {
        ("01-01", _) => first[..4].to_owned(),
        (_, 1) => format!("{month} {}", &first[..4]),
        _ => format!("{day} {month} {}", &first[..4]),
    }
}

/// A span's last day in words: its year or month when it ends one, else the day.
fn plain_end(last: &str) -> String {
    let day: u32 = last[8..].parse().expect("an ISO day");
    let month = MONTHS[last[5..7].parse::<usize>().expect("an ISO month") - 1];
    if last.ends_with("-12-31") {
        last[..4].to_owned()
    } else if reg_core::next_iso_day(last).ends_with("-01") {
        format!("{month} {}", &last[..4])
    } else {
        format!("{day} {month} {}", &last[..4])
    }
}

/// [`plain`] for a sentence saying the days have no data, where an unbounded period
/// reads "any year": "no data for every year" would say some years have data.
pub(super) fn plain_none(periods: &[Period]) -> String {
    if periods.iter().all(|p| p.start.is_none() && p.end.is_none()) {
        "any year".to_owned()
    } else {
        plain(periods)
    }
}
