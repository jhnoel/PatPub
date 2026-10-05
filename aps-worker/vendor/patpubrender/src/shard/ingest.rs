//! Bulk ingest — render a USPTO weekly bulk ZIP into a shard plus a
//! `.biblio.jsonl` sidecar and a `.manifest.json`.
//!
//! It reads the weekly ZIP, splits it into per-doc records, parses + renders
//! each, and hands the Markdown to a [`ShardWriter`](super::ShardWriter). The
//! frame/index format itself is owned by the codec; this only drives it.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use super::ShardWriter;
use crate::error::{DetectError, ParseError};
use crate::json::json_string;

#[derive(Debug)]
pub struct ShardStats {
    /// Every document written to the shard — patent bodies plus supplementals.
    pub docs_written: usize,
    pub docs_skipped: usize,
    /// Supplemental documents (sequence listings) written to the shard,
    /// included in `docs_written`.
    pub supplemental_written: usize,
    /// `sequence-cwu` records skipped. Zero now that they render; kept so the
    /// manifest contract (and any regression) stays observable.
    pub sequence_cwu_skipped: usize,
    /// Standalone supplemental `table` records — skipped until they gain an
    /// adapter (phase 2 of docs/design/render-supplemental-roots.md).
    pub table_skipped: usize,
    /// Well-formed patent docs the parser cannot yet handle (unknown schema
    /// version, ambiguous version markers, or unhandled structure). FIXABLE by
    /// adding/extending a source adapter — real patents we dropped, not corrupt
    /// data. Detail is written to `<stem>.unsupported.tsv`.
    pub unsupported_skipped: usize,
    /// Genuinely malformed / unparseable XML (corrupt source bytes). Not fixable.
    pub malformed_skipped: usize,
    pub zst_path: String,
    pub idx_path: String,
    pub biblio_path: String,
    pub manifest_path: String,
    /// Path to the fixable-gap report, present only when `unsupported_skipped > 0`.
    pub unsupported_path: Option<String>,
}

/// How a record that failed to render is classified.
#[derive(Debug, PartialEq, Eq)]
enum SkipKind {
    /// Well-formed supplemental record without an adapter yet (standalone
    /// `table`). Normal.
    Supplemental,
    /// A patent doc the parser cannot yet handle — FIXABLE by adding an adapter.
    Unsupported,
    /// Corrupt / unparseable XML. Not fixable.
    Malformed,
}

fn classify_skip(error: &ParseError) -> SkipKind {
    match error {
        error if error.is_supplemental_root() => SkipKind::Supplemental,
        ParseError::Detect(DetectError::UnknownFormat)
        | ParseError::Detect(DetectError::ConflictingVersionMarkers)
        | ParseError::Detect(DetectError::UnsupportedRoot(_))
        | ParseError::UnsupportedStructure(_) => SkipKind::Unsupported,
        ParseError::Detect(DetectError::MalformedXml) | ParseError::MalformedXml(_) => {
            SkipKind::Malformed
        }
    }
}

/// Render a USPTO weekly bulk zip into a zstd-per-doc shard + biblio sidecar.
pub fn render_shard_from_zip(
    zip_path: &str,
    out_dir: &str,
    limit: Option<usize>,
) -> Result<ShardStats, Box<dyn std::error::Error>> {
    let zip_path = Path::new(zip_path);
    let stem = zip_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or("Cannot derive stem from zip path")?;
    let source_file = zip_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or("Cannot derive source file from zip path")?;
    let doc_kind = doc_kind_from_source_file(source_file)?;

    let out_dir = Path::new(out_dir);
    let biblio_path = out_dir.join(format!("{stem}.biblio.jsonl"));
    let manifest_path = out_dir.join(format!("{stem}.manifest.json"));

    fs::create_dir_all(out_dir)?;

    let zip_file = fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(zip_file)?;

    let xml_index = (0..archive.len())
        .find(|&i| {
            let Ok(entry) = archive.by_index(i) else {
                return false;
            };
            !entry.is_dir() && has_xml_name(entry.name())
        })
        .ok_or("No XML entry found in zip")?;

    // The Green Book "APS" product ships a single `.txt` entry per weekly zip
    // (no `.XML`); detect it so we use the plain-text record splitter + parser.
    let is_aps = {
        let entry = archive.by_index(xml_index)?;
        has_aps_name(entry.name())
    };

    let mut writer = ShardWriter::create(out_dir, stem)?;
    let mut biblio_file = fs::File::create(&biblio_path)?;

    let mut docs_written = 0usize;
    let mut supplemental_written = 0usize;
    let mut sequence_cwu_skipped = 0usize;
    let mut table_skipped = 0usize;
    let mut unsupported_skipped = 0usize;
    let mut malformed_skipped = 0usize;
    // Fixable-gap detail: one `doc_key\treason` line per unsupported patent doc.
    let mut unsupported_records: Vec<String> = Vec::new();
    // Every doc_key written so far. The downstream loader PK is
    // `pdp:<stem>:<doc_key>`, so a duplicate key in one shard crashes ingest;
    // supplemental keys are disambiguated against this set before writing.
    let mut written_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

    {
        let entry = archive.by_index(xml_index)?;
        let mut handle = |record_ordinal: usize,
                          doc_string: &str|
         -> Result<bool, Box<dyn std::error::Error>> {
            if limit.is_some_and(|lim| docs_written >= lim) {
                return Ok(false);
            }

            let doc_key = if is_aps {
                derive_aps_doc_key(doc_string, record_ordinal)
            } else {
                derive_doc_key(doc_string, record_ordinal)
            };

            let parsed = if is_aps {
                crate::parse_patent_aps(doc_string)
            } else {
                crate::parse_patent_xml(doc_string)
            };
            let doc = match parsed {
                Ok(doc) => doc,
                Err(e) => {
                    match classify_skip(&e) {
                        // Well-formed supplemental record without an adapter yet.
                        // Normal; the manifest carries the per-root totals.
                        SkipKind::Supplemental => match e.supplemental_root() {
                            Some("sequence-cwu") => sequence_cwu_skipped += 1,
                            _ => table_skipped += 1,
                        },
                        // FIXABLE: a real patent in a schema/structure we can't yet
                        // parse. Record it so the gap is identifiable + re-renderable.
                        SkipKind::Unsupported => {
                            unsupported_skipped += 1;
                            unsupported_records.push(format!("{doc_key}\t{e}"));
                        }
                        // Corrupt source bytes. Not fixable.
                        SkipKind::Malformed => {
                            eprintln!("  skip malformed doc {record_ordinal} (key={doc_key}): {e}");
                            malformed_skipped += 1;
                        }
                    }
                    return Ok(true);
                }
            };
            let is_supplemental = doc.source_format == crate::SourceFormat::UsptoSequenceCwu;
            // A supplemental record must not be labeled with the per-ZIP
            // doc_kind ("grant"/"application"): the downstream loader keys its
            // patent_number/dedup logic on the supplemental vocabulary
            // (grant_sequence_listing etc.), and its key must never collide
            // with the parent patent's.
            let (doc_key, record_doc_kind) = if is_supplemental {
                let key = unique_doc_key(format!("{doc_key}-seqlst"), &written_keys);
                let kind = if doc_kind == "grant" {
                    "grant_sequence_listing"
                } else {
                    "application_sequence_listing"
                };
                (key, kind)
            } else {
                (doc_key, doc_kind)
            };
            let md = crate::render_markdown(&doc);

            writer.append(&doc_key, md.as_bytes())?;
            let biblio = crate::render::biblio::extract_biblio(&doc);
            writeln!(
                biblio_file,
                "{}",
                biblio.to_json_line(&doc_key, record_doc_kind, source_file)
            )?;
            written_keys.insert(doc_key);

            docs_written += 1;
            if is_supplemental {
                supplemental_written += 1;
            }
            Ok(true)
        };

        if is_aps {
            stream_aps_documents(entry, &mut handle)?;
        } else {
            stream_documents(entry, &mut handle)?;
        }
    }

    // Persist the fixable-gap inventory next to the shard, only when non-empty.
    let unsupported_path = if unsupported_records.is_empty() {
        None
    } else {
        let path = out_dir.join(format!("{stem}.unsupported.tsv"));
        let mut report = String::from("doc_key\treason\n");
        for line in &unsupported_records {
            report.push_str(line);
            report.push('\n');
        }
        fs::write(&path, report)?;
        Some(path.to_string_lossy().to_string())
    };

    write_manifest_atomic(
        &manifest_path,
        ShardManifestData {
            stem,
            source_file,
            doc_kind,
            patents_written: (docs_written - supplemental_written) as u64,
            supplemental_written: supplemental_written as u64,
            sequence_cwu_skipped: sequence_cwu_skipped as u64,
            table_skipped: table_skipped as u64,
            unsupported_skipped: unsupported_skipped as u64,
            malformed_skipped: malformed_skipped as u64,
        },
    )?;

    Ok(ShardStats {
        docs_written,
        docs_skipped: sequence_cwu_skipped
            + table_skipped
            + unsupported_skipped
            + malformed_skipped,
        supplemental_written,
        sequence_cwu_skipped,
        table_skipped,
        unsupported_skipped,
        malformed_skipped,
        zst_path: out_dir
            .join(format!("{stem}.zst"))
            .to_string_lossy()
            .to_string(),
        idx_path: out_dir
            .join(format!("{stem}.idx"))
            .to_string_lossy()
            .to_string(),
        biblio_path: biblio_path.to_string_lossy().to_string(),
        manifest_path: manifest_path.to_string_lossy().to_string(),
        unsupported_path,
    })
}

/// Make `key` unique against the doc_keys already written to this shard by
/// appending `-2`, `-3`, … — a patent occasionally carries more than one
/// sequence listing, and the downstream loader PK cannot tolerate a repeat.
fn unique_doc_key(key: String, written: &std::collections::HashSet<String>) -> String {
    if !written.contains(&key) {
        return key;
    }
    (2..)
        .map(|n| format!("{key}-{n}"))
        .find(|candidate| !written.contains(candidate))
        .expect("some numeric suffix is always free")
}

/// Extract the doc_key from a document string. Prefers the root element's
/// `file="..."` attribute stem, then publication/patent number, then ordinal.
fn derive_doc_key(doc_string: &str, ordinal: usize) -> String {
    if let Some(root_tag) = root_start_tag(doc_string)
        && let Some(file_value) = attr_value(root_tag, "file")
    {
        if let Some(dot_pos) = file_value.rfind('.') {
            return file_value[..dot_pos].to_string();
        }
        return file_value.to_string();
    }

    for tag in &["<doc-number>", "<publication-number>", "<patent-number>"] {
        if let Some(pos) = doc_string.find(*tag) {
            let rest = &doc_string[pos + tag.len()..];
            if let Some(end) = rest.find('<') {
                let value = rest[..end].trim();
                if !value.is_empty() {
                    return value.to_string();
                }
            }
        }
    }
    (ordinal + 1).to_string()
}

fn root_start_tag(input: &str) -> Option<&str> {
    let mut cursor = input.trim_start();

    while let Some(start) = cursor.find('<') {
        let content = &cursor[start + 1..];

        if content.starts_with('?') {
            let end = content.find("?>")?;
            cursor = &content[end + 2..];
            continue;
        }

        if content.starts_with("!DOCTYPE") {
            let end = content
                .find("]>")
                .map(|idx| idx + 2)
                .or_else(|| content.find('>').map(|idx| idx + 1))?;
            cursor = &content[end..];
            continue;
        }

        if content.starts_with("!--") {
            let end = content.find("-->")?;
            cursor = &content[end + 3..];
            continue;
        }

        if content.starts_with('!') || content.starts_with('/') {
            return None;
        }

        let end = content.find('>')?;
        return Some(&cursor[start..start + end + 2]);
    }

    None
}

fn attr_value<'a>(tag: &'a str, name: &str) -> Option<&'a str> {
    let double = format!("{name}=\"");
    if let Some(pos) = tag.find(&double) {
        let rest = &tag[pos + double.len()..];
        return rest.find('"').map(|end| &rest[..end]);
    }

    let single = format!("{name}='");
    if let Some(pos) = tag.find(&single) {
        let rest = &tag[pos + single.len()..];
        return rest.find('\'').map(|end| &rest[..end]);
    }

    None
}

fn has_xml_name(name: &str) -> bool {
    name.ends_with(".XML") || name.ends_with(".xml") || has_aps_name(name)
}

/// The Green Book "APS" weekly zip holds a single `.txt` entry (e.g.
/// `pftaps19830308_wk10.txt`), not an `.XML` file.
fn has_aps_name(name: &str) -> bool {
    let base = name.rsplit('/').next().unwrap_or(name);
    let lower = base.to_ascii_lowercase();
    lower.ends_with(".txt") && lower.starts_with("pftaps")
}

fn doc_kind_from_source_file(
    source_file: &str,
) -> Result<&'static str, Box<dyn std::error::Error>> {
    let lower = source_file.to_ascii_lowercase();
    let stem = lower
        .strip_suffix(".zip")
        .or_else(|| lower.strip_suffix(".ZIP"))
        .unwrap_or(&lower);

    if stem.starts_with("ipg") || stem.starts_with("pg") || stem.starts_with("pftaps") {
        Ok("grant")
    } else if stem.starts_with("ipa") || stem.starts_with("pa") {
        Ok("application")
    } else {
        Err(format!("Cannot derive doc_kind from source file '{source_file}'").into())
    }
}

struct ShardManifestData<'a> {
    stem: &'a str,
    source_file: &'a str,
    doc_kind: &'a str,
    patents_written: u64,
    supplemental_written: u64,
    sequence_cwu_skipped: u64,
    table_skipped: u64,
    unsupported_skipped: u64,
    malformed_skipped: u64,
}

fn write_manifest_atomic(
    path: &Path,
    manifest: ShardManifestData<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp_path = tmp_manifest_path(path);
    let json = format!(
        concat!(
            "{{\n",
            "  \"format_version\": 1,\n",
            "  \"stem\": {},\n",
            "  \"source_file\": {},\n",
            "  \"doc_kind\": {},\n",
            "  \"patents_written\": {},\n",
            "  \"supplemental_written\": {},\n",
            "  \"sequence_cwu_skipped\": {},\n",
            "  \"table_skipped\": {},\n",
            "  \"unsupported_skipped\": {},\n",
            "  \"malformed_skipped\": {}\n",
            "}}\n"
        ),
        json_string(manifest.stem),
        json_string(manifest.source_file),
        json_string(manifest.doc_kind),
        manifest.patents_written,
        manifest.supplemental_written,
        manifest.sequence_cwu_skipped,
        manifest.table_skipped,
        manifest.unsupported_skipped,
        manifest.malformed_skipped
    );

    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn tmp_manifest_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("manifest.json");
    path.with_file_name(format!(".{file_name}.tmp"))
}

/// Derive a stable doc_key for an APS record from its `WKU` (patent number)
/// line, falling back to the ordinal. The key mirrors the cleaned patent
/// number form so it joins back to the biblio sidecar.
fn derive_aps_doc_key(record: &str, ordinal: usize) -> String {
    for line in record.lines() {
        if let Some(rest) = line.strip_prefix("WKU") {
            let value = rest.trim();
            if !value.is_empty() {
                return crate::source::aps::aps_doc_key_from_wku(value);
            }
        }
    }
    (ordinal + 1).to_string()
}

/// Stream the concatenated Green Book "APS" text, invoking `f` once per record.
/// Records are delimited by a bare `PATN` line; the weekly file header precedes
/// the first record and is discarded.
fn stream_aps_documents<R: Read>(
    mut reader: R,
    mut f: impl FnMut(usize, &str) -> Result<bool, Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;

    let text: String = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(err) => err.into_bytes().iter().map(|&b| b as char).collect(),
    };

    let starts = crate::aps_record_starts(text.as_bytes());
    let mut bounds = starts;
    bounds.push(text.len());

    let mut ordinal = 0usize;
    for window in bounds.windows(2) {
        let (start, end) = (window[0], window[1]);
        let record = &text[start..end];
        if record.trim().is_empty() {
            continue;
        }
        if !f(ordinal, record)? {
            return Ok(());
        }
        ordinal += 1;
    }

    Ok(())
}

fn stream_documents<R: Read>(
    mut reader: R,
    mut f: impl FnMut(usize, &str) -> Result<bool, Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    let mut found_first_record = false;
    let mut ordinal = 0usize;

    loop {
        let read = reader.read(&mut chunk)?;
        let eof = read == 0;
        if read > 0 {
            buffer.extend_from_slice(&chunk[..read]);
        }

        if !found_first_record {
            if let Some(start) = find_next_record_start(&buffer, 0) {
                if start > 0 {
                    buffer.drain(..start);
                }
                found_first_record = true;
            } else if eof {
                return Ok(());
            } else {
                trim_to_search_tail(&mut buffer);
                continue;
            }
        }

        loop {
            if let Some(next_start) = find_next_record_start(&buffer, XML_DECL.len()) {
                let doc_bytes: Vec<u8> = buffer.drain(..next_start).collect();
                if !process_doc_bytes(doc_bytes, ordinal, &mut f)? {
                    return Ok(());
                }
                ordinal += 1;
            } else if eof {
                if !buffer.iter().all(u8::is_ascii_whitespace) {
                    let doc_bytes = std::mem::take(&mut buffer);
                    process_doc_bytes(doc_bytes, ordinal, &mut f)?;
                }
                return Ok(());
            } else {
                break;
            }
        }
    }
}

fn process_doc_bytes(
    doc_bytes: Vec<u8>,
    ordinal: usize,
    f: &mut impl FnMut(usize, &str) -> Result<bool, Box<dyn std::error::Error>>,
) -> Result<bool, Box<dyn std::error::Error>> {
    // USPTO weeklies are nominally UTF-8, but older grants (the ST.32 / v2.5 era)
    // carry stray Latin-1 / Windows-1252 bytes. A single such byte must not abort
    // the whole run, and the patent is real — so on a UTF-8 error fall back to a
    // total ISO-8859-1 decode (every byte maps to a code point) rather than
    // erroring. Genuinely corrupt XML still fails downstream at the parse step.
    let doc = match String::from_utf8(doc_bytes) {
        Ok(text) => text,
        Err(err) => err.into_bytes().iter().map(|&b| b as char).collect(),
    };
    if doc.trim().is_empty() {
        return Ok(true);
    }
    f(ordinal, &doc)
}

const XML_DECL: &[u8] = b"<?xml";

fn trim_to_search_tail(buffer: &mut Vec<u8>) {
    const KEEP: usize = 1024;
    if buffer.len() > KEEP {
        buffer.drain(..buffer.len() - KEEP);
    }
}

fn find_next_record_start(buffer: &[u8], from: usize) -> Option<usize> {
    let mut cursor = from;
    while let Some(rel) = find_bytes(buffer, XML_DECL, cursor) {
        if is_record_start_at(buffer, rel) {
            return Some(rel);
        }
        cursor = rel + XML_DECL.len();
    }
    None
}

fn is_record_start_at(buffer: &[u8], pos: usize) -> bool {
    if !buffer[pos..].starts_with(XML_DECL) {
        return false;
    }

    let after_xml = pos + XML_DECL.len();
    if buffer
        .get(after_xml)
        .is_none_or(|byte| !byte.is_ascii_whitespace())
    {
        return false;
    }

    let Some(pi_end) = find_bytes(buffer, b"?>", after_xml) else {
        return false;
    };
    let mut cursor = pi_end + 2;
    skip_ws(buffer, &mut cursor);

    while buffer
        .get(cursor..)
        .is_some_and(|tail| tail.starts_with(b"<!--"))
    {
        let Some(comment_end) = find_bytes(buffer, b"-->", cursor + 4) else {
            return false;
        };
        cursor = comment_end + 3;
        skip_ws(buffer, &mut cursor);
    }

    if buffer
        .get(cursor..)
        .is_some_and(|tail| tail.starts_with(b"<!DOCTYPE"))
    {
        let mut name_start = cursor + b"<!DOCTYPE".len();
        skip_ws(buffer, &mut name_start);
        return buffer
            .get(name_start)
            .is_some_and(|byte| is_xml_name_start(*byte));
    }

    buffer.get(cursor) == Some(&b'<')
        && buffer
            .get(cursor + 1)
            .is_some_and(|byte| is_xml_name_start(*byte))
}

fn skip_ws(buffer: &[u8], cursor: &mut usize) {
    while buffer.get(*cursor).is_some_and(u8::is_ascii_whitespace) {
        *cursor += 1;
    }
}

fn is_xml_name_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn find_bytes(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from > haystack.len() || needle.len() > haystack.len() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|pos| from + pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_key_uses_root_file_attribute_only() {
        let xml = r#"<?xml version="1.0"?>
<!DOCTYPE us-patent-grant SYSTEM "us-patent-grant-v47.dtd">
<us-patent-grant lang="EN" dtd-version="v4.7 2022-02-17">
  <description file="WRONG.XML" />
  <doc-number>7654321</doc-number>
</us-patent-grant>"#;

        assert_eq!(derive_doc_key(xml, 0), "7654321");
    }

    #[test]
    fn splitter_ignores_non_record_xml_processing_instruction_text() {
        let xml = concat!(
            "<?xml version=\"1.0\"?><us-patent-grant></us-patent-grant>",
            "text <?xml-stylesheet type=\"text/xsl\"?> text",
            "<?xml version=\"1.0\"?><sequence-cwu></sequence-cwu>"
        );

        let mut docs = Vec::new();
        stream_documents(xml.as_bytes(), |_, doc| {
            docs.push(doc.to_string());
            Ok(true)
        })
        .unwrap();

        assert_eq!(docs.len(), 2);
        assert!(docs[0].contains("xml-stylesheet"));
        assert!(docs[1].contains("sequence-cwu"));
    }

    const GRANT_RECORD: &str = concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<!DOCTYPE us-patent-grant SYSTEM \"us-patent-grant-v47-2022-02-17.dtd\" [ ]>\n",
        "<us-patent-grant lang=\"EN\" dtd-version=\"v4.7 2022-02-17\" ",
        "file=\"US12667087-20260630.XML\">\n",
        "<us-bibliographic-data-grant>\n",
        "<publication-reference><document-id><country>US</country>",
        "<doc-number>12667087</doc-number><kind>B2</kind><date>20260630</date>",
        "</document-id></publication-reference>\n",
        "<invention-title id=\"d2e43\">Widget</invention-title>\n",
        "</us-bibliographic-data-grant>\n",
        "<abstract id=\"abstract\"><p id=\"p-0001\">An abstract.</p></abstract>\n",
        "</us-patent-grant>\n"
    );

    const SEQUENCE_CWU_RECORD: &str = concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<sequence-cwu id=\"SEQLST-1\">\n",
        "<publication-reference><document-id><country>US</country>",
        "<doc-number>12667087</doc-number><kind>B2</kind><date>20260630</date>",
        "</document-id></publication-reference>\n",
        "<s400>acgtacgtacgt acgtacgta 21</s400>\n",
        "</sequence-cwu>\n"
    );

    const TABLE_RECORD: &str =
        "<?xml version=\"1.0\"?>\n<table frame=\"none\"><row><entry>x</entry></row></table>\n";

    #[test]
    fn sequence_cwu_parses_and_renders() {
        let doc = crate::parse_patent_xml(SEQUENCE_CWU_RECORD)
            .expect("sequence-cwu must parse as a first-class document");
        assert_eq!(doc.source_format, crate::SourceFormat::UsptoSequenceCwu);

        let md = crate::render_markdown(&doc);
        assert!(md.contains("acgtacgtacgt"), "payload must render:\n{md}");
    }

    #[test]
    fn shard_writes_sequence_cwu_with_supplemental_doc_kind() {
        let dir = tempfile::tempdir().unwrap();
        let zip_path = dir.path().join("ipg260630.zip");
        {
            let mut zip = zip::ZipWriter::new(fs::File::create(&zip_path).unwrap());
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            zip.start_file("ipg260630.xml", options).unwrap();
            // A grant followed by two of its sequence listings (exercises the
            // doc_key disambiguation) and a standalone table (still skipped).
            for record in [
                GRANT_RECORD,
                SEQUENCE_CWU_RECORD,
                SEQUENCE_CWU_RECORD,
                TABLE_RECORD,
            ] {
                zip.write_all(record.as_bytes()).unwrap();
            }
            zip.finish().unwrap();
        }

        let out_dir = dir.path().join("out");
        let stats =
            render_shard_from_zip(zip_path.to_str().unwrap(), out_dir.to_str().unwrap(), None)
                .unwrap();

        assert_eq!(stats.docs_written, 3);
        assert_eq!(stats.supplemental_written, 2);
        assert_eq!(stats.sequence_cwu_skipped, 0);
        assert_eq!(stats.table_skipped, 1);
        assert_eq!(stats.unsupported_skipped, 0);
        assert_eq!(stats.malformed_skipped, 0);

        // Biblio sidecar: the patent keeps the per-ZIP doc_kind, the listings
        // get the supplemental vocabulary plus identity for the loader's
        // patent_number derivation, and doc_keys never repeat.
        let biblio = fs::read_to_string(&stats.biblio_path).unwrap();
        let lines: Vec<&str> = biblio.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("\"doc_key\":\"US12667087-20260630\""));
        assert!(lines[0].contains("\"doc_kind\":\"grant\""));
        assert!(lines[1].contains("\"doc_key\":\"12667087-seqlst\""));
        assert!(lines[1].contains("\"doc_kind\":\"grant_sequence_listing\""));
        assert!(lines[1].contains("\"doc_number\":\"12667087\""));
        assert!(lines[1].contains("\"kind_code\":\"B2\""));
        assert!(lines[1].contains("\"publication_date\":\"20260630\""));
        assert!(lines[2].contains("\"doc_key\":\"12667087-seqlst-2\""));

        // The listing text is addressable through the index, like any patent.
        let idx_text = fs::read_to_string(&stats.idx_path).unwrap();
        let entries = super::super::parse_shard_index("ipg260630", &idx_text).unwrap();
        let entry = entries
            .iter()
            .find(|entry| entry.doc_key == "12667087-seqlst")
            .expect("sequence listing must be indexed");
        let frame = super::super::read_frame(
            Path::new(&stats.zst_path),
            entry.pointer.offset,
            entry.pointer.length,
        )
        .unwrap();
        let text = String::from_utf8(frame).unwrap();
        assert!(text.contains("acgtacgtacgt"), "{text}");
        assert!(text.contains("patent_number: US12667087B2"), "{text}");

        let manifest = fs::read_to_string(&stats.manifest_path).unwrap();
        assert!(manifest.contains("\"patents_written\": 1"));
        assert!(manifest.contains("\"supplemental_written\": 2"));
        assert!(manifest.contains("\"sequence_cwu_skipped\": 0"));
        assert!(manifest.contains("\"table_skipped\": 1"));
    }

    #[test]
    fn unique_doc_key_suffixes_repeats() {
        let mut written = std::collections::HashSet::new();
        assert_eq!(unique_doc_key("k-seqlst".to_string(), &written), "k-seqlst");
        written.insert("k-seqlst".to_string());
        assert_eq!(
            unique_doc_key("k-seqlst".to_string(), &written),
            "k-seqlst-2"
        );
        written.insert("k-seqlst-2".to_string());
        assert_eq!(
            unique_doc_key("k-seqlst".to_string(), &written),
            "k-seqlst-3"
        );
    }

    #[test]
    fn table_is_supplemental_skip() {
        let error = crate::parse_patent_xml("<?xml version=\"1.0\"?><table/>")
            .expect_err("table should not parse as a patent document");

        assert_eq!(classify_skip(&error), SkipKind::Supplemental);
        assert_eq!(
            error.to_string(),
            "source detection failed: supplemental XML root 'table'"
        );
    }

    #[test]
    fn unknown_root_is_unsupported_skip() {
        let error = crate::parse_patent_xml("<?xml version=\"1.0\"?><not-a-patent/>")
            .expect_err("unknown root should not parse as a patent document");

        assert_eq!(classify_skip(&error), SkipKind::Unsupported);
        assert_eq!(
            error.to_string(),
            "source detection failed: unsupported XML root 'not-a-patent'"
        );
    }
}
