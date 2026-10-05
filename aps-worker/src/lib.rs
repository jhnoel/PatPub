pub mod pack;

use patpubrender::render::biblio::extract_biblio;
use patpubrender::{aps_record_starts, parse_patent_aps, render_markdown_with_template};
use serde_json::{Map, Value};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const SECTION_TAGS: &[&str] = &[
    "PATN", "INVT", "ASSG", "PRIR", "REIS", "RLAP", "PARN", "CLAS", "UREF", "FREF", "OREF", "LREP",
    "PCTA", "ABST", "GOVT", "BSUM", "DRWD", "DETD", "CLMS", "DCLM",
];

#[derive(Debug, Default)]
struct RawScan {
    wku: Option<String>,
    patn_apd: Option<String>,
    patn_isd: Option<String>,
    apt: Option<String>,
    art: Option<String>,
    prir_apds: Vec<String>,
    prir_cnts: Vec<String>,
}

#[derive(Debug, Default)]
pub struct SampleTracker {
    got_first: bool,
    got_design: bool,
    got_reissue: bool,
    got_plant: bool,
    got_code_fence: bool,
    got_priority: bool,
    entries: Vec<(String, String)>,
}

impl SampleTracker {
    pub fn maybe_record(
        &mut self,
        zip_name: &str,
        patent_number: &str,
        description: Option<&str>,
        has_priority_date: bool,
        emitted_ordinal: usize,
    ) {
        let mut add = |patent: &str| {
            self.entries
                .push((zip_name.to_string(), patent.to_string()));
        };

        if !self.got_first {
            self.got_first = true;
            add(patent_number);
        }
        if !self.got_design && patent_number.starts_with('D') {
            self.got_design = true;
            add(patent_number);
        }
        if !self.got_reissue && patent_number.starts_with("RE") {
            self.got_reissue = true;
            add(patent_number);
        }
        if !self.got_plant && patent_number.starts_with("PP") {
            self.got_plant = true;
            add(patent_number);
        }
        if !self.got_code_fence
            && description.is_some_and(|text| text.lines().any(|line| line == "```"))
        {
            self.got_code_fence = true;
            add(patent_number);
        }
        if !self.got_priority && has_priority_date {
            self.got_priority = true;
            add(patent_number);
        }
        if emitted_ordinal > 0 && emitted_ordinal % 100_000 == 0 {
            add(patent_number);
        }
    }

    pub fn write_jsonl(&self, path: &Path) -> io::Result<()> {
        let mut file = File::create(path)?;
        for (zip, patent_number) in &self.entries {
            let line = serde_json::json!({
                "zip": zip,
                "patent_number": patent_number,
            });
            writeln!(file, "{line}")?;
        }
        Ok(())
    }
}

pub fn decode_zip_text(path: &Path) -> io::Result<String> {
    let file = File::open(path)?;
    let mut archive = zip::ZipArchive::new(file).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to open zip {}: {err}", path.display()),
        )
    })?;

    let mut member_name = None;
    for i in 0..archive.len() {
        let name = archive.by_index(i).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to read zip index {i} in {}: {err}", path.display()),
            )
        })?;
        let name = name.name().to_string();
        if name.starts_with("pftaps") && name.ends_with(".txt") {
            member_name = Some(name);
            break;
        }
    }

    let member_name = member_name.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no pftaps*.txt member in {}", path.display()),
        )
    })?;

    let mut member = archive.by_name(&member_name).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to read {member_name} in {}: {err}", path.display()),
        )
    })?;
    let mut bytes = Vec::new();
    member.read_to_end(&mut bytes)?;
    Ok(decode_bytes_as_text(&bytes))
}

pub fn decode_bytes_as_text(bytes: &[u8]) -> String {
    match String::from_utf8(bytes.to_vec()) {
        Ok(text) => text,
        Err(err) => err.into_bytes().into_iter().map(|b| b as char).collect(),
    }
}

pub fn record_to_json(record: &str, source_stem: &str) -> Result<Map<String, Value>, String> {
    let doc = parse_patent_aps(record).map_err(|err| err.to_string())?;
    let biblio = extract_biblio(&doc);
    let raw = scan_raw_record(record);

    let patent_number = biblio
        .doc_number
        .clone()
        .ok_or_else(|| "missing WKU".to_string())?;

    let mut obj = Map::new();
    obj.insert("doc_kind".to_string(), Value::String("aps".to_string()));
    obj.insert(
        "kind_code".to_string(),
        Value::String(kind_code(&patent_number).to_string()),
    );
    obj.insert(
        "patent_number".to_string(),
        Value::String(patent_number.clone()),
    );

    if let Some(date) = format_date(raw.patn_isd.as_deref()) {
        obj.insert("publication_date".to_string(), Value::String(date));
    }
    if let Some(date) = format_date(raw.patn_apd.as_deref()) {
        obj.insert("filing_date".to_string(), Value::String(date));
    }
    insert_opt_string(&mut obj, "application_number", biblio.application_number);
    insert_opt_string(&mut obj, "application_type", raw.apt);
    insert_opt_string(&mut obj, "art_unit", raw.art);
    insert_opt_string(&mut obj, "title", biblio.invention_title);

    if let Ok(text) = render_markdown_with_template(&doc, "{{abstract}}") {
        insert_trimmed_string(&mut obj, "abstract", text);
    }
    if let Ok(text) = render_markdown_with_template(&doc, "{{description}}") {
        insert_trimmed_string(&mut obj, "description", text);
    }
    if let Ok(text) = render_markdown_with_template(&doc, "{{claims}}") {
        insert_trimmed_string(&mut obj, "claims", text);
    }

    insert_string_array(&mut obj, "inventor", biblio.inventor_names);
    insert_string_array(&mut obj, "assignee", biblio.assignee_names);
    insert_string_array(&mut obj, "us_class", biblio.us_classifications);
    insert_string_array(&mut obj, "ipc", biblio.ipc_classifications);

    let priority_dates: Vec<String> = raw
        .prir_apds
        .iter()
        .filter_map(|apd| format_date(Some(apd.as_str())))
        .collect();
    if let Some(earliest) = priority_dates.into_iter().min() {
        obj.insert("priority_date".to_string(), Value::String(earliest));
    }
    insert_string_array(&mut obj, "priority_country", raw.prir_cnts);

    obj.insert("source_stem".to_string(), Value::String(source_stem.to_string()));
    obj.insert(
        "source_generation".to_string(),
        Value::String("aps-greenbook-v1".to_string()),
    );

    Ok(obj)
}

pub fn export_zip_text(
    text: &str,
    source_stem: &str,
    stdout: &mut impl Write,
    mut sample: Option<&mut SampleTracker>,
    already_emitted: usize,
) -> (usize, usize) {
    let starts = aps_record_starts(text.as_bytes());
    let mut bounds = starts;
    bounds.push(text.len());

    let mut emitted = 0usize;
    let mut failures = 0usize;

    for window in bounds.windows(2) {
        let record = &text[window[0]..window[1]];
        if record.trim().is_empty() {
            continue;
        }

        match record_to_json(record, source_stem) {
            Ok(obj) => {
                emitted += 1;
                if let Some(tracker) = sample.as_mut() {
                    let patent_number = obj
                        .get("patent_number")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let description = obj.get("description").and_then(Value::as_str);
                    let has_priority = obj.contains_key("priority_date");
                    tracker.maybe_record(
                        &format!("{source_stem}.zip"),
                        patent_number,
                        description,
                        has_priority,
                        already_emitted + emitted,
                    );
                }
                let line = serde_json::Value::Object(obj);
                let _ = writeln!(stdout, "{line}");
            }
            Err(_) => failures += 1,
        }
    }

    (emitted, failures)
}

pub fn sorted_zip_paths(dir: &Path, limit: Option<usize>) -> io::Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "zip"))
        .collect();
    paths.sort();
    if let Some(limit) = limit {
        paths.truncate(limit);
    }
    Ok(paths)
}

fn kind_code(doc_key: &str) -> &'static str {
    if doc_key.starts_with("RE") {
        "E"
    } else if doc_key.starts_with("PP") {
        "PP"
    } else if doc_key.starts_with('D') {
        "S"
    } else {
        "B1"
    }
}

fn scan_raw_record(record: &str) -> RawScan {
    let mut scan = RawScan::default();
    let mut current_section = String::from("PATN");

    for line in record.lines() {
        if line.trim().is_empty() {
            continue;
        }

        let tag = match line_tag(line) {
            None => continue,
            Some(tag) if SECTION_TAGS.contains(&tag) => {
                current_section = tag.to_string();
                continue;
            }
            Some(tag) => tag,
        };

        let value = line_value(line).trim().to_string();
        if value.is_empty() {
            continue;
        }

        match (current_section.as_str(), tag) {
            ("PATN", "WKU") if scan.wku.is_none() => scan.wku = Some(value),
            ("PATN", "APD") if scan.patn_apd.is_none() => scan.patn_apd = Some(value),
            ("PATN", "ISD") if scan.patn_isd.is_none() => scan.patn_isd = Some(value),
            ("PATN", "APT") if scan.apt.is_none() => scan.apt = Some(value),
            ("PATN", "ART") if scan.art.is_none() => scan.art = Some(value),
            ("PRIR", "APD") => scan.prir_apds.push(value),
            ("PRIR", "CNT") => scan.prir_cnts.push(value),
            _ => {}
        }
    }

    scan
}

fn line_tag(line: &str) -> Option<&str> {
    if line.starts_with(' ') {
        return None;
    }
    let tag = line.get(0..4).unwrap_or(line).trim_end();
    if tag.is_empty() { None } else { Some(tag) }
}

fn line_value(line: &str) -> &str {
    line.get(5..).unwrap_or("").trim_end()
}

fn format_date(raw: Option<&str>) -> Option<String> {
    let digits = raw?.trim();
    if digits.len() != 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let year = &digits[0..4];
    let month: u32 = digits[4..6].parse().ok()?;
    let day: u32 = digits[6..8].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // TODO: reject calendar-impossible Green Book dates before emit.
    // Day 1–31 still lets through 1988-02-31, 1991-02-29, 1991-02-30, and
    // 1991-09-31. Quickwit drops those documents at ingest.
    Some(format!("{year}-{month:02}-{day:02}T00:00:00Z"))
}

fn insert_opt_string(map: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value.filter(|v| !v.is_empty()) {
        map.insert(key.to_string(), Value::String(value));
    }
}

fn insert_trimmed_string(map: &mut Map<String, Value>, key: &str, value: String) {
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        map.insert(key.to_string(), Value::String(trimmed.to_string()));
    }
}

fn insert_string_array(map: &mut Map<String, Value>, key: &str, values: Vec<String>) {
    if values.is_empty() {
        return;
    }
    map.insert(
        key.to_string(),
        Value::Array(values.into_iter().map(Value::String).collect()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const UTILITY: &str = "\
PATN
WKU  043757022
SRC  6
APN  2745224
APT  1
ART  353
APD  19810617
TTL  Device for mounting an earmuff on a helmet
ISD  19830308
INVT
NAM  Lundin; Tord R.
CTY  Billesholm
CNT  SEX
ASSG
NAM  Gullfiber AB
CLAS
OCL    2423
XCL    2209
ICL  A42B  300
ABST
PAL  An attachment device in the form of a bearing housing (10) and an arm (38)
      projecting out through an opening (44) in the housing.
BSUM
PAR  The present invention is concerned with an attachment device.
DETD
PAR  The attachment device of FIG. 1 includes a bearing housing 10.
CLMS
STM  I claim:
NUM  1.
PAR  1. An attachment device including a bearing housing.
";

    const DESIGN: &str = "\
PATN
WKU  D02681420
APN  1989073
APT  4
APD  19801021
TTL  Protective garment for an ice hockey player
ISD  19830308
DCLM
PAR  The ornamental design for a protective garment for an ice hockey player, as
      shown.
";

    const PRIORITY: &str = "\
PATN
WKU  050000001
APD  19900101
ISD  19900605
TTL  Priority example
PRIR
CNT  GBX
APD  19870914
APN  1044836
";

    fn json_for(record: &str) -> Map<String, Value> {
        record_to_json(record, "fixture").expect("record parses")
    }

    fn s(obj: &Map<String, Value>, key: &str) -> String {
        obj.get(key).and_then(Value::as_str).unwrap().to_string()
    }

    #[test]
    fn utility_record_json() {
        let obj = json_for(UTILITY);
        assert_eq!(s(&obj, "patent_number"), "4375702");
        assert_eq!(s(&obj, "kind_code"), "B1");
        assert_eq!(s(&obj, "doc_kind"), "aps");
        assert_eq!(s(&obj, "source_generation"), "aps-greenbook-v1");
        assert_eq!(s(&obj, "publication_date"), "1983-03-08T00:00:00Z");
        assert_eq!(s(&obj, "filing_date"), "1981-06-17T00:00:00Z");
        assert_eq!(s(&obj, "application_number"), "2745224");
        assert_eq!(s(&obj, "application_type"), "1");
        assert_eq!(s(&obj, "art_unit"), "353");
        assert_eq!(
            s(&obj, "title"),
            "Device for mounting an earmuff on a helmet"
        );

        let abstract_text = s(&obj, "abstract");
        assert!(abstract_text.contains("projecting out through an opening"));
        assert!(!abstract_text.contains("PAL"));

        let claims = s(&obj, "claims");
        assert!(claims.contains("bearing housing"));
        assert!(!claims.contains("I claim:"));

        let description = s(&obj, "description");
        assert!(description.contains("The present invention is concerned with an attachment device."));
        assert!(description.contains("The attachment device of FIG. 1 includes a bearing housing 10."));

        let inventors = obj.get("inventor").and_then(Value::as_array).unwrap();
        assert_eq!(inventors[0], Value::String("Tord R. Lundin".to_string()));
        let assignees = obj.get("assignee").and_then(Value::as_array).unwrap();
        assert_eq!(assignees[0], Value::String("Gullfiber AB".to_string()));

        let us_class = obj.get("us_class").and_then(Value::as_array).unwrap();
        assert!(us_class.iter().any(|v| v.as_str() == Some("2423")));

        let ipc = obj.get("ipc").and_then(Value::as_array).unwrap();
        assert!(ipc.iter().any(|v| v.as_str() == Some("A42B 300")));
    }

    #[test]
    fn design_record_json() {
        let obj = json_for(DESIGN);
        assert_eq!(s(&obj, "patent_number"), "D268142");
        assert_eq!(s(&obj, "kind_code"), "S");
        let claims = s(&obj, "claims");
        assert!(claims.contains("ornamental design"));
        assert!(obj.get("abstract").is_none());
    }

    #[test]
    fn priority_record_json() {
        let obj = json_for(PRIORITY);
        assert_eq!(s(&obj, "priority_date"), "1987-09-14T00:00:00Z");
        assert_eq!(s(&obj, "filing_date"), "1990-01-01T00:00:00Z");
        let countries = obj.get("priority_country").and_then(Value::as_array).unwrap();
        assert_eq!(countries.len(), 1);
        assert_eq!(countries[0], Value::String("GBX".to_string()));
    }

    #[test]
    fn wku_doc_key_from_library() {
        let obj = json_for("PATN\nWKU  06332220&\nISD  19900605\n");
        assert_eq!(s(&obj, "patent_number"), "6332220");
    }

    #[test]
    fn rejects_invalid_dates() {
        assert!(format_date(Some("19810000")).is_none());
        assert!(format_date(Some("19811301")).is_none());
        assert!(format_date(Some("19810132")).is_none());
        assert_eq!(
            format_date(Some("19830308")),
            Some("1983-03-08T00:00:00Z".to_string())
        );
    }
}
