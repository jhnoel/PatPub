use blake2::Blake2b512;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use zstd::stream::write::Encoder as ZstdEncoder;

const RENDERER_PROFILE: &str = "aps-greenbook-markdown-v1:zstd-9-checksum";

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub enum PackError {
    Io(io::Error),
    Json(serde_json::Error),
    Zstd(String),
    InvalidInput(String),
    ImmutableConflict(String),
}

impl From<io::Error> for PackError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for PackError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl std::fmt::Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Json(err) => write!(f, "{err}"),
            Self::Zstd(msg) => write!(f, "zstd error: {msg}"),
            Self::InvalidInput(msg) => write!(f, "{msg}"),
            Self::ImmutableConflict(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for PackError {}

#[derive(Debug, Default)]
pub struct WeekStats {
    pub docs: usize,
    pub skipped_dates: usize,
    pub duplicates: usize,
    pub pack_sha256: String,
}

pub fn pack_ndjson(ndjson: &Path, zip_dir: &Path, out_dir: &Path) -> Result<(), PackError> {
    fs::create_dir_all(out_dir.join("ndjson"))?;
    fs::create_dir_all(out_dir.join("packs"))?;

    let file = File::open(ndjson)?;
    let reader = io::BufReader::new(file);

    let mut zip_hashes: HashMap<String, String> = HashMap::new();
    let mut finished_stems: HashSet<String> = HashSet::new();
    let mut current_stem: Option<String> = None;
    let mut current_records: Vec<Map<String, Value>> = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: Map<String, Value> = serde_json::from_str(&line)?;
        let stem = record
            .get("source_stem")
            .and_then(Value::as_str)
            .ok_or_else(|| PackError::InvalidInput("record missing source_stem".into()))?
            .to_string();

        if finished_stems.contains(&stem) {
            return Err(PackError::InvalidInput(format!(
                "source_stem {stem} reappeared after its week was finished"
            )));
        }

        match current_stem.as_ref() {
            None => {
                current_stem = Some(stem);
                current_records.push(record);
            }
            Some(active) if active == &stem => current_records.push(record),
            Some(active) => {
                let stats = pack_week(
                    active,
                    &current_records,
                    zip_dir,
                    out_dir,
                    &mut zip_hashes,
                )?;
                println!(
                    "{active} docs={} skipped_dates={} duplicates={} pack={}",
                    stats.docs, stats.skipped_dates, stats.duplicates, stats.pack_sha256
                );
                finished_stems.insert(active.clone());
                current_stem = Some(stem);
                current_records = vec![record];
            }
        }
    }

    if let Some(stem) = current_stem {
        let stats = pack_week(&stem, &current_records, zip_dir, out_dir, &mut zip_hashes)?;
        println!(
            "{stem} docs={} skipped_dates={} duplicates={} pack={}",
            stats.docs, stats.skipped_dates, stats.duplicates, stats.pack_sha256
        );
    }

    Ok(())
}

fn pack_week(
    stem: &str,
    records: &[Map<String, Value>],
    zip_dir: &Path,
    out_dir: &Path,
    zip_hashes: &mut HashMap<String, String>,
) -> Result<WeekStats, PackError> {
    let zip_sha256 = match zip_hashes.get(stem) {
        Some(hash) => hash.clone(),
        None => {
            let zip_path = zip_dir.join(format!("{stem}.zip"));
            let hash = sha256_file(&zip_path)?;
            zip_hashes.insert(stem.to_string(), hash.clone());
            hash
        }
    };

    let source_generation = source_generation(&zip_sha256)?;
    let pack_path = out_dir.join("packs").join(format!("{stem}.zst"));
    let ndjson_path = out_dir.join("ndjson").join(format!("{stem}.ndjson"));
    let pack_temp = unique_temp_path(&pack_path);
    let ndjson_temp = unique_temp_path(&ndjson_path);

    let result = (|| -> Result<WeekStats, PackError> {
        let mut pack = BufWriter::new(File::create(&pack_temp)?);
        let mut documents: Vec<Map<String, Value>> = Vec::new();
        let mut seen_uids = HashSet::new();
        let mut stats = WeekStats::default();
        let mut pack_offset = 0u64;

        for record in records {
            match record_to_document(record, &source_generation)? {
                Some((mut document, markdown)) => {
                    let publication_uid = document["publication_uid"]
                        .as_str()
                        .expect("publication_uid")
                        .to_string();
                    if !seen_uids.insert(publication_uid) {
                        stats.duplicates += 1;
                        continue;
                    }

                    let frame = compress_source_frame(markdown.as_bytes())?;
                    let offset = pack_offset;
                    pack.write_all(&frame)?;
                    pack_offset = pack_offset
                        .checked_add(frame.len() as u64)
                        .ok_or_else(|| PackError::InvalidInput("source pack exceeded u64".into()))?;

                    document.insert(
                        "source_revision".into(),
                        Value::String(format!(
                            "sha256:{}",
                            hex_string(&Sha256::digest(markdown.as_bytes()))
                        )),
                    );
                    document.insert("offset".into(), Value::Number(offset.into()));
                    document.insert("length".into(), Value::Number((frame.len() as u64).into()));
                    document.insert(
                        "uncompressed_length".into(),
                        Value::Number((markdown.len() as u64).into()),
                    );
                    document.insert(
                        "source_fingerprint".into(),
                        Value::String(format!(
                            "blake2b-128:{}",
                            hex_string(&Blake2b512::digest(&frame)[..16])
                        )),
                    );
                    documents.push(document);
                    stats.docs += 1;
                }
                None => stats.skipped_dates += 1,
            }
        }

        pack.flush()?;
        pack.get_ref().sync_all()?;

        let pack_sha256 = sha256_file(&pack_temp)?;
        stats.pack_sha256 = format!("sha256:{pack_sha256}");
        let pack_key = format!("source/v1/packs/{pack_sha256}.zst");

        let mut ndjson = BufWriter::new(File::create(&ndjson_temp)?);
        for mut document in documents {
            document.insert("source_pack".into(), Value::String(stats.pack_sha256.clone()));
            document.insert("source_pack_key".into(), Value::String(pack_key.clone()));
            serde_json::to_writer(&mut ndjson, &document)?;
            ndjson.write_all(b"\n")?;
        }
        ndjson.flush()?;
        ndjson.get_ref().sync_all()?;

        publish_immutable_file(&pack_temp, &pack_path, &pack_sha256)?;
        let ndjson_sha256 = sha256_file(&ndjson_temp)?;
        publish_immutable_file(&ndjson_temp, &ndjson_path, &ndjson_sha256)?;

        Ok(stats)
    })();

    let _ = fs::remove_file(&pack_temp);
    let _ = fs::remove_file(&ndjson_temp);
    result
}

fn record_to_document(
    record: &Map<String, Value>,
    source_generation: &str,
) -> Result<Option<(Map<String, Value>, String)>, PackError> {
    let patent_number = required_string(record, "patent_number")?;
    let kind_code = required_string(record, "kind_code")?;
    let publication_uid = format!("US:{patent_number}:{kind_code}");

    let publication_date = match record.get("publication_date").and_then(Value::as_str) {
        Some(raw) => normalize_date(raw)?,
        None => return Ok(None),
    };

    let title = optional_string(record, "title");
    let abstract_text = optional_string(record, "abstract");
    let description = optional_string(record, "description");
    let claims = optional_string(record, "claims");
    let application_number = optional_string(record, "application_number");
    let applicant = string_array(record, "inventor");
    let assignee = string_array(record, "assignee");
    let ipc = string_array(record, "ipc");
    let us_class = string_array(record, "us_class");

    let markdown = canonical_markdown(
        &publication_uid,
        &title,
        &abstract_text,
        &description,
        &claims,
    );

    let mut document = Map::new();
    document.insert("publication_uid".into(), Value::String(publication_uid.clone()));
    document.insert("publication_date".into(), Value::String(publication_date));
    if let Some(filing_date) = record
        .get("filing_date")
        .and_then(Value::as_str)
        .map(normalize_date)
        .transpose()?
    {
        document.insert("filing_date".into(), Value::String(filing_date));
    }
    document.insert("title".into(), Value::String(title));
    document.insert("abstract".into(), Value::String(abstract_text));
    document.insert("claims".into(), Value::String(claims));
    document.insert("description".into(), Value::String(description));
    document.insert(
        "publication_number".into(),
        Value::String(patent_number.clone()),
    );
    document.insert("publication_key".into(), Value::String("US".into()));
    document.insert("patent_number".into(), Value::String(patent_number));
    document.insert("patent_key".into(), Value::String("US".into()));
    document.insert("application_number".into(), Value::String(application_number));
    document.insert("doc_kind".into(), Value::String("grant".into()));
    document.insert("applicant".into(), Value::Array(applicant));
    document.insert("assignee".into(), Value::Array(assignee));
    document.insert("ipc".into(), Value::Array(ipc));
    document.insert("us_class".into(), Value::Array(us_class));
    document.insert("shard".into(), Value::String(publication_uid));
    document.insert(
        "source_generation".into(),
        Value::String(source_generation.into()),
    );

    Ok(Some((document, markdown)))
}

pub fn canonical_markdown(
    publication_uid: &str,
    title: &str,
    abstract_text: &str,
    description: &str,
    claims: &str,
) -> String {
    let mut parts = vec![
        format!("<!-- publication_uid: {publication_uid} -->"),
        format!("# {title}"),
    ];
    for section in [abstract_text, description, claims] {
        if !section.is_empty() {
            parts.push(section.to_string());
        }
    }
    parts.join("\n")
}

fn normalize_date(raw: &str) -> Result<String, PackError> {
    if raw.len() == 10 && raw.as_bytes().get(4) == Some(&b'-') && raw.as_bytes().get(7) == Some(&b'-')
    {
        return Ok(format!("{raw}T00:00:00Z"));
    }
    Ok(raw.to_string())
}

fn required_string(record: &Map<String, Value>, key: &str) -> Result<String, PackError> {
    record
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| PackError::InvalidInput(format!("record missing {key}")))
}

fn optional_string(record: &Map<String, Value>, key: &str) -> String {
    record
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn string_array(record: &Map<String, Value>, key: &str) -> Vec<Value> {
    record
        .get(key)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(|value| Value::String(value.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

pub fn source_generation(raw_sha256: &str) -> Result<String, PackError> {
    let body = format!(
        "{{\"raw_sha256\":\"{raw_sha256}\",\"renderer_profile\":\"{RENDERER_PROFILE}\"}}\n"
    );
    Ok(format!(
        "sha256:{}",
        hex_string(&Sha256::digest(body.as_bytes()))
    ))
}

pub fn compress_source_frame(markdown: &[u8]) -> Result<Vec<u8>, PackError> {
    let mut encoder = ZstdEncoder::new(Vec::new(), 9)
        .map_err(|error| PackError::Zstd(format!("create encoder: {error}")))?;
    encoder
        .include_checksum(true)
        .map_err(|error| PackError::Zstd(format!("enable checksum: {error}")))?;
    encoder
        .include_contentsize(true)
        .map_err(|error| PackError::Zstd(format!("enable content size: {error}")))?;
    encoder
        .include_dictid(false)
        .map_err(|error| PackError::Zstd(format!("disable dictionary id: {error}")))?;
    encoder
        .set_pledged_src_size(Some(markdown.len() as u64))
        .map_err(|error| PackError::Zstd(format!("set content size: {error}")))?;
    encoder
        .write_all(markdown)
        .map_err(|error| PackError::Zstd(format!("compress frame: {error}")))?;
    encoder
        .finish()
        .map_err(|error| PackError::Zstd(format!("finish frame: {error}")))
}

fn sha256_file(path: &Path) -> Result<String, PackError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_string(&hasher.finalize()))
}

fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unique_temp_path(path: &Path) -> PathBuf {
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!(".{name}.tmp-{}-{counter}", std::process::id()))
}

fn publish_immutable_file(
    source: &Path,
    destination: &Path,
    expected_sha256: &str,
) -> Result<(), PackError> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    if destination.exists() {
        let actual = sha256_file(destination)?;
        if actual != expected_sha256 {
            return Err(PackError::ImmutableConflict(format!(
                "immutable object conflict at {}",
                destination.display()
            )));
        }
        let _ = fs::remove_file(source);
        return Ok(());
    }
    match fs::rename(source, destination) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let actual = sha256_file(destination)?;
            if actual == expected_sha256 {
                let _ = fs::remove_file(source);
                Ok(())
            } else {
                Err(PackError::ImmutableConflict(format!(
                    "immutable object conflict at {}",
                    destination.display()
                )))
            }
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_fixture_zip(path: &Path) {
        const FIXTURE_ZIP: &[u8] = include_bytes!("../fixtures/pack_fixture.zip");
        fs::write(path, FIXTURE_ZIP).expect("write zip bytes");
    }

    fn fixture_record(patent_number: &str, source_stem: &str) -> Map<String, Value> {
        let mut record = Map::new();
        record.insert("patent_number".into(), Value::String(patent_number.into()));
        record.insert("kind_code".into(), Value::String("B1".into()));
        record.insert("title".into(), Value::String("Fixture title".into()));
        record.insert("abstract".into(), Value::String("Fixture abstract".into()));
        record.insert("claims".into(), Value::String("Fixture claims".into()));
        record.insert(
            "description".into(),
            Value::String("Fixture description".into()),
        );
        record.insert(
            "publication_date".into(),
            Value::String("2020-01-02".into()),
        );
        record.insert("filing_date".into(), Value::String("2019-01-01".into()));
        record.insert("application_number".into(), Value::String("16123456".into()));
        record.insert(
            "inventor".into(),
            Value::Array(vec![Value::String("Ada Lovelace".into())]),
        );
        record.insert("source_stem".into(), Value::String(source_stem.into()));
        record
    }

    #[test]
    fn packs_two_records_for_one_week() {
        let temp = std::env::temp_dir().join(format!(
            "aps-backfill-pack-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp);
        fs::create_dir_all(&temp).expect("temp dir");

        let zip_dir = temp.join("zips");
        let out_dir = temp.join("out");
        fs::create_dir_all(&zip_dir).expect("zip dir");
        let stem = "pftaps199001_wk01";
        let zip_path = zip_dir.join(format!("{stem}.zip"));
        write_fixture_zip(&zip_path);
        assert!(zip_path.is_file());

        let ndjson_path = temp.join("input.ndjson");
        let mut ndjson = File::create(&ndjson_path).expect("ndjson");
        for patent in ["10000001", "10000002"] {
            let line = serde_json::Value::Object(fixture_record(patent, stem));
            writeln!(ndjson, "{line}").expect("write ndjson line");
        }

        pack_ndjson(&ndjson_path, &zip_dir, &out_dir).expect("pack");

        let output = fs::read_to_string(out_dir.join("ndjson").join(format!("{stem}.ndjson")))
            .expect("read ndjson");
        let first: Map<String, Value> =
            serde_json::from_str(output.lines().next().expect("first line")).expect("parse line");

        assert_eq!(
            first.get("publication_uid").and_then(Value::as_str),
            Some("US:10000001:B1")
        );
        assert_eq!(first.get("doc_kind").and_then(Value::as_str), Some("grant"));
        assert!(
            first
                .get("source_pack_key")
                .and_then(Value::as_str)
                .is_some_and(|key| key.ends_with(".zst"))
        );
        assert!(
            first
                .get("source_generation")
                .and_then(Value::as_str)
                .is_some_and(|value| value.starts_with("sha256:"))
        );

        let _ = fs::remove_dir_all(&temp);
    }
}
