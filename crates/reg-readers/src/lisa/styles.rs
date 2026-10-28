//! The visual structure calamine does not expose: source fonts and bottom borders.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;

use quick_xml::events::Event;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Style {
    pub bold: bool,
    pub italic: bool,
    pub size: f64,
    pub bottom: bool,
}

pub(super) struct Styles {
    cells: BTreeMap<(String, usize, usize), Style>,
}

type Attributes = BTreeMap<String, String>;

impl Styles {
    pub fn load(path: &Path) -> Result<Self, String> {
        let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
        let mut zip = zip::ZipArchive::new(file).map_err(|e| e.to_string())?;
        let mut read = |name: &str| -> Result<Vec<u8>, String> {
            let mut bytes = Vec::new();
            zip.by_name(name)
                .map_err(|e| format!("{name}: {e}"))?
                .read_to_end(&mut bytes)
                .map_err(|e| format!("{name}: {e}"))?;
            Ok(bytes)
        };
        let formats = formats(&read("xl/styles.xml")?)?;
        let mut targets = BTreeMap::new();
        walk(&read("xl/_rels/workbook.xml.rels")?, |name, enter, a| {
            if enter && name == "Relationship" {
                targets.insert(required(a, "Id")?, required(a, "Target")?);
            }
            Ok(())
        })?;
        let mut sheets = Vec::new();
        walk(&read("xl/workbook.xml")?, |name, enter, a| {
            if enter && name == "sheet" {
                sheets.push((required(a, "name")?, required(a, "r:id")?));
            }
            Ok(())
        })?;
        let mut cells = BTreeMap::new();
        for (sheet, id) in sheets {
            let target = targets
                .get(&id)
                .ok_or_else(|| format!("sheet {sheet:?}: no relationship {id}"))?;
            let path = if let Some(absolute) = target.strip_prefix('/') {
                absolute.to_owned()
            } else {
                format!("xl/{target}")
            };
            walk(&read(&path)?, |name, enter, a| {
                if enter && name == "c" {
                    let address = required(a, "r")?;
                    let (row, col) = coordinates(&address)?;
                    let index = index(a, "s")?;
                    let style = formats
                        .get(index)
                        .ok_or_else(|| format!("{sheet}!{address}: unknown style {index}"))?;
                    cells.insert((sheet.clone(), row, col), *style);
                }
                Ok(())
            })?;
        }
        Ok(Self { cells })
    }

    /// Coordinates are one-based rows and zero-based columns, like the LISA reader.
    pub fn cell(&self, sheet: &str, row: usize, col: usize) -> Result<Style, String> {
        self.cells
            .get(&(sheet.to_owned(), row, col))
            .copied()
            .ok_or_else(|| format!("sheet {sheet:?}, row {row}, column {col}: missing cell style"))
    }
}

fn formats(bytes: &[u8]) -> Result<Vec<Style>, String> {
    let mut fonts = Vec::<Style>::new();
    let mut borders = Vec::<bool>::new();
    let mut xfs = Vec::new();
    let mut section = String::new();
    walk(bytes, |name, enter, a| {
        if matches!(name, "fonts" | "borders" | "cellXfs") {
            section = if enter {
                name.to_owned()
            } else {
                String::new()
            };
        }
        if !enter {
            return Ok(());
        }
        match (section.as_str(), name) {
            ("fonts", "font") => fonts.push(Style::default()),
            ("fonts", "b" | "i" | "sz") => {
                let font = fonts.last_mut().ok_or("font property without a font")?;
                match name {
                    "b" => font.bold = enabled(a),
                    "i" => font.italic = enabled(a),
                    _ => {
                        font.size = required(a, "val")?
                            .parse()
                            .map_err(|e| format!("invalid font size: {e}"))?;
                    }
                }
            }
            ("borders", "border") => borders.push(false),
            ("borders", "bottom") => {
                *borders.last_mut().ok_or("bottom border without a border")? =
                    a.contains_key("style");
            }
            ("cellXfs", "xf") => xfs.push((index(a, "fontId")?, index(a, "borderId")?)),
            _ => (),
        }
        Ok(())
    })?;
    xfs.into_iter()
        .map(|(font, border)| {
            Ok(Style {
                bottom: *borders
                    .get(border)
                    .ok_or_else(|| format!("unknown border {border}"))?,
                ..*fonts
                    .get(font)
                    .ok_or_else(|| format!("unknown font {font}"))?
            })
        })
        .collect()
}

fn enabled(a: &Attributes) -> bool {
    a.get("val")
        .is_none_or(|v| !matches!(v.as_str(), "0" | "false"))
}

fn required(a: &Attributes, key: &str) -> Result<String, String> {
    a.get(key)
        .cloned()
        .ok_or_else(|| format!("missing XML attribute {key}"))
}

fn index(a: &Attributes, key: &str) -> Result<usize, String> {
    a.get(key).map_or(Ok(0), |v| {
        v.parse().map_err(|e| format!("invalid {key}: {e}"))
    })
}

fn coordinates(address: &str) -> Result<(usize, usize), String> {
    let split = address
        .find(|c: char| c.is_ascii_digit())
        .ok_or_else(|| format!("invalid cell address {address:?}"))?;
    let (letters, digits) = address.split_at(split);
    let mut col = 0usize;
    for byte in letters.bytes() {
        if !byte.is_ascii_uppercase() {
            return Err(format!("invalid cell address {address:?}"));
        }
        col = col
            .checked_mul(26)
            .and_then(|n| n.checked_add(usize::from(byte - b'A') + 1))
            .ok_or("cell column overflow")?;
    }
    let row = digits
        .parse::<usize>()
        .map_err(|e| format!("invalid cell address {address:?}: {e}"))?;
    if row == 0 || col == 0 {
        return Err(format!("invalid cell address {address:?}"));
    }
    Ok((row, col - 1))
}

fn walk(
    bytes: &[u8],
    mut visit: impl FnMut(&str, bool, &Attributes) -> Result<(), String>,
) -> Result<(), String> {
    let mut xml = quick_xml::Reader::from_reader(bytes);
    loop {
        match xml.read_event().map_err(|e| e.to_string())? {
            Event::Start(e) | Event::Empty(e) => {
                let mut attrs = BTreeMap::new();
                for a in e.attributes() {
                    let a = a.map_err(|e| e.to_string())?;
                    let key = a.key.as_ref().to_owned();
                    let value = a
                        .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                        .map_err(|e| e.to_string())?
                        .into_owned();
                    attrs.insert(key, value);
                }
                let local = e.local_name();
                let name = local.as_ref();
                visit(name, true, &attrs)?;
            }
            Event::End(e) => {
                let local = e.local_name();
                visit(local.as_ref(), false, &BTreeMap::new())?;
            }
            Event::Eof => return Ok(()),
            _ => (),
        }
    }
}
