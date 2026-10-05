mod pap;

use blake2::Blake2b512;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use zip::ZipArchive;
use zstd::stream::{read::Decoder as ZstdDecoder, write::Encoder as ZstdEncoder};

pub const ALLOWED_PRODUCTS: &[&str] = &["PTGRXML", "APPXML"];
/// Stable renderer identity included in generation manifests.
pub const RENDERER_PROFILE: &str = "uspto-xml-markdown-v2:zstd-9-checksum";
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("ZIP error: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("XML error: {0}")]
    Xml(#[from] quick_xml::Error),
    #[error("Zstandard error: {0}")]
    Zstd(String),
    #[error("source-pack validation failed: {0}")]
    SourcePack(String),
    #[error("remote source changed: {0}")]
    RemoteChanged(String),
    #[error("ingestion error: {0}")]
    Ingestion(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceFile {
    pub product: String,
    pub source_id: String,
    pub url: String,
    pub expected_bytes: Option<u64>,
    pub expected_sha256: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl SourceFile {
    pub fn work_id(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.product.as_bytes());
        hasher.update([0]);
        hasher.update(self.source_id.as_bytes());
        hasher.update([0]);
        hasher.update(self.url.as_bytes());
        hex_string(&hasher.finalize())[..32].to_owned()
    }
}

pub struct RangeResponse {
    pub status: u16,
    pub body: Box<dyn Read>,
    pub etag: Option<String>,
    pub range_start: u64,
    pub total_bytes: Option<u64>,
}

pub trait RangeSource {
    fn fetch(&mut self, offset: u64, if_match: Option<&str>) -> Result<RangeResponse, Error>;
}

pub struct FileSource {
    path: PathBuf,
    chunk_size: usize,
    etag: String,
}

impl FileSource {
    pub fn new(path: impl Into<PathBuf>, chunk_size: usize) -> Result<Self, Error> {
        let path = path.into();
        let etag = sha256_file(&path)?;
        Ok(Self {
            path,
            chunk_size,
            etag,
        })
    }
}

impl RangeSource for FileSource {
    fn fetch(&mut self, offset: u64, if_match: Option<&str>) -> Result<RangeResponse, Error> {
        if if_match.is_some_and(|value| value != self.etag) {
            return Err(Error::RemoteChanged("file validator changed".into()));
        }
        let total = fs::metadata(&self.path)?.len();
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; self.chunk_size];
        let read = file.read(&mut bytes)?;
        bytes.truncate(read);
        Ok(RangeResponse {
            status: if offset == 0 { 200 } else { 206 },
            body: Box::new(Cursor::new(bytes)),
            etag: Some(self.etag.clone()),
            range_start: offset,
            total_bytes: Some(total),
        })
    }
}

#[derive(Debug)]
pub struct LocalStore {
    pub root: PathBuf,
}

impl LocalStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, Error> {
        let root = root.into();
        for name in ["partial", "staged", "raw", "manifests", "source-batches"] {
            fs::create_dir_all(root.join(name))?;
        }
        Ok(Self { root })
    }

    fn safe(&self, key: &str) -> Result<PathBuf, Error> {
        if key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(Error::Ingestion(format!("unsafe blob key: {key}")));
        }
        Ok(self.root.join(key))
    }

    fn partial_path(&self, work_id: &str) -> PathBuf {
        self.root.join("partial").join(format!("{work_id}.part"))
    }
    fn staged_path(&self, work_id: &str) -> PathBuf {
        self.root.join("staged").join(format!("{work_id}.zip"))
    }

    fn stage_block(&self, work_id: &str, offset: u64, data: &[u8]) -> Result<(), Error> {
        let path = self.partial_path(work_id);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.sync_data()?;
        Ok(())
    }

    fn partial_size(&self, work_id: &str) -> Result<u64, Error> {
        let path = self.partial_path(work_id);
        Ok(path.metadata().map(|metadata| metadata.len()).unwrap_or(0))
    }

    fn reset_partial(&self, work_id: &str) -> Result<(), Error> {
        let _ = fs::remove_file(self.partial_path(work_id));
        Ok(())
    }

    fn commit_staging(&self, work_id: &str) -> Result<PathBuf, Error> {
        let staged = self.staged_path(work_id);
        if staged.exists() {
            return Ok(staged);
        }
        fs::rename(self.partial_path(work_id), &staged)?;
        Ok(staged)
    }

    fn clear_staged(&self, work_id: &str) {
        let _ = fs::remove_file(self.staged_path(work_id));
    }

    fn put_immutable(&self, key: &str, source: &Path) -> Result<bool, Error> {
        let destination = self.safe(key)?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        match fs::hard_link(source, &destination) {
            Ok(()) => {
                fs::remove_file(source)?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn write_json_if_absent<T: Serialize>(&self, key: &str, value: &T) -> Result<bool, Error> {
        let destination = self.safe(key)?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = unique_temp_path(&destination);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        match fs::hard_link(&temporary, &destination) {
            Ok(()) => {
                fs::remove_file(temporary)?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(temporary);
                Ok(false)
            }
            Err(error) => {
                let _ = fs::remove_file(temporary);
                Err(error.into())
            }
        }
    }

    fn exists(&self, key: &str) -> Result<bool, Error> {
        Ok(self.safe(key)?.exists())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    source: SourceFile,
    status: String,
    #[serde(flatten)]
    result: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let path = path.into();
        if !path.exists() {
            fs::write(&path, b"{}\n")?;
        }
        Ok(Self { path })
    }

    fn read(&self) -> Result<BTreeMap<String, State>, Error> {
        Ok(serde_json::from_slice(&fs::read(&self.path)?)?)
    }

    fn write(&self, states: &BTreeMap<String, State>) -> Result<(), Error> {
        let temporary = unique_temp_path(&self.path);
        fs::write(&temporary, serde_json::to_vec_pretty(states)?)?;
        fs::rename(temporary, &self.path)?;
        Ok(())
    }

    fn get(&self, work_id: &str) -> Result<Option<State>, Error> {
        Ok(self.read()?.remove(work_id))
    }

    fn put(&self, work_id: String, state: State) -> Result<(), Error> {
        let mut states = self.read()?;
        states.insert(work_id, state);
        self.write(&states)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawManifest {
    pub raw_key: String,
    pub sha256: String,
    pub bytes: u64,
    pub member_count: usize,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub source_id: String,
    pub source_url: String,
    pub product: String,
}

pub struct LandingWorker {
    pub store: LocalStore,
    pub state: StateStore,
    pub chunk_size: usize,
}

impl LandingWorker {
    fn save_staging(
        &self,
        source: &SourceFile,
        bytes: u64,
        etag: Option<&str>,
    ) -> Result<(), Error> {
        let mut result = BTreeMap::new();
        result.insert("bytes_downloaded".into(), serde_json::Value::from(bytes));
        if let Some(etag) = etag {
            result.insert("etag".into(), serde_json::Value::from(etag));
        }
        self.state.put(
            source.work_id(),
            State {
                source: source.clone(),
                status: "staging".into(),
                result,
            },
        )
    }

    pub fn run<S: RangeSource>(
        &self,
        source: &SourceFile,
        client: &mut S,
    ) -> Result<RawManifest, Error> {
        if !ALLOWED_PRODUCTS.contains(&source.product.as_str()) {
            return Err(Error::Ingestion(format!(
                "product not allowed: {}",
                source.product
            )));
        }
        let work_id = source.work_id();
        let existing = self.state.get(&work_id)?;
        if let Some(state) = existing.as_ref().filter(|state| state.status == "landed") {
            let result: RawManifest = serde_json::from_value(serde_json::to_value(&state.result)?)?;
            if !matches_source(source, &result) {
                return Err(Error::Ingestion(
                    "landed source conflicts with manifest".into(),
                ));
            }
            return Ok(result);
        }
        let staged = self.store.staged_path(&work_id);
        if staged.exists() {
            let staged_etag = existing
                .as_ref()
                .and_then(|state| state.result.get("etag"))
                .and_then(|value| value.as_str())
                .map(str::to_owned)
                .or_else(|| source.etag.clone());
            let validator_conflict = source
                .etag
                .as_ref()
                .zip(staged_etag.as_ref())
                .is_some_and(|(current, staged)| current != staged);
            if !validator_conflict {
                if let Ok(result) =
                    self.publish_staged(source, &staged, staged.metadata()?.len(), staged_etag)
                {
                    return Ok(result);
                }
            }
            self.store.clear_staged(&work_id);
        }

        let mut offset = self.store.partial_size(&work_id)?;
        let mut pinned_etag = existing
            .as_ref()
            .and_then(|state| state.result.get("etag"))
            .and_then(|value| value.as_str())
            .map(str::to_owned)
            .or_else(|| source.etag.clone());
        if offset > 0 && pinned_etag.is_none() {
            self.store.reset_partial(&work_id)?;
            offset = 0;
        }
        let state_source = existing.as_ref().map(|state| &state.source);
        if state_source.is_some_and(|previous| {
            previous.expected_bytes != source.expected_bytes
                || previous.expected_sha256 != source.expected_sha256
        }) {
            self.store.reset_partial(&work_id)?;
            offset = 0;
            pinned_etag = source.etag.clone();
        }
        self.save_staging(source, offset, pinned_etag.as_deref())?;

        loop {
            let mut response = client.fetch(
                offset,
                if offset > 0 {
                    pinned_etag.as_deref()
                } else {
                    source.etag.as_deref()
                },
            )?;
            if pinned_etag.is_some() && response.etag != pinned_etag {
                return Err(Error::RemoteChanged("publisher validator changed".into()));
            }
            if pinned_etag.is_none() {
                pinned_etag = response.etag.clone();
            }
            if offset > 0 && response.status == 200 {
                let mut remaining = offset;
                let mut staged_prefix = File::open(self.store.partial_path(&work_id))?;
                while remaining > 0 {
                    let mut buffer = vec![0; self.chunk_size.min(remaining as usize)];
                    staged_prefix.read_exact(&mut buffer)?;
                    let mut received = vec![0; buffer.len()];
                    response.body.read_exact(&mut received).map_err(|_| {
                        Error::Ingestion("full response ended before staged prefix".into())
                    })?;
                    if received != buffer {
                        return Err(Error::RemoteChanged(
                            "resumed response prefix differs from staged bytes".into(),
                        ));
                    }
                    remaining -= buffer.len() as u64;
                }
                loop {
                    let mut buffer = vec![0; self.chunk_size];
                    let read = response.body.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    self.store.stage_block(&work_id, offset, &buffer[..read])?;
                    offset += read as u64;
                    self.save_staging(source, offset, pinned_etag.as_deref())?;
                }
                break;
            }
            if response.status != 200 && response.status != 206 || response.range_start != offset {
                return Err(Error::Ingestion("malformed HTTP range response".into()));
            }
            let mut buffer = vec![0; self.chunk_size];
            let read = response.body.read(&mut buffer)?;
            if read == 0 {
                return Err(Error::Ingestion("empty HTTP response before EOF".into()));
            }
            self.store.stage_block(&work_id, offset, &buffer[..read])?;
            offset += read as u64;
            self.save_staging(source, offset, pinned_etag.as_deref())?;
            if source
                .expected_bytes
                .is_some_and(|expected| offset > expected)
            {
                return Err(Error::Ingestion(
                    "download exceeded expected byte count".into(),
                ));
            }
            if response.total_bytes.is_some_and(|total| offset >= total)
                || source
                    .expected_bytes
                    .is_some_and(|expected| offset == expected)
            {
                break;
            }
        }
        if source
            .expected_bytes
            .is_some_and(|expected| offset != expected)
        {
            return Err(Error::Ingestion("byte count mismatch".into()));
        }
        let staged = self.store.commit_staging(&work_id)?;
        self.publish_staged(source, &staged, offset, pinned_etag)
    }

    fn publish_staged(
        &self,
        source: &SourceFile,
        staged: &Path,
        bytes: u64,
        etag: Option<String>,
    ) -> Result<RawManifest, Error> {
        if source
            .expected_bytes
            .is_some_and(|expected| expected != bytes)
        {
            self.store.clear_staged(&source.work_id());
            return Err(Error::Ingestion("byte count mismatch".into()));
        }
        let member_count = match validate_zip(staged) {
            Ok(value) => value,
            Err(error) => {
                self.store.clear_staged(&source.work_id());
                return Err(error);
            }
        };
        let sha256 = sha256_file(staged)?;
        if source
            .expected_sha256
            .as_deref()
            .is_some_and(|expected| expected != sha256)
        {
            self.store.clear_staged(&source.work_id());
            return Err(Error::Ingestion("SHA-256 mismatch".into()));
        }
        let raw_key = format!(
            "raw/uspto/{}/{}/{}.zip",
            source.product, source.source_id, sha256
        );
        if self.store.exists(&raw_key)? {
            if sha256_file(&self.store.safe(&raw_key)?)? != sha256 {
                return Err(Error::Ingestion("immutable raw conflict".into()));
            }
            self.store.clear_staged(&source.work_id());
        } else {
            let created = self.store.put_immutable(&raw_key, staged)?;
            if !created {
                if sha256_file(&self.store.safe(&raw_key)?)? != sha256 {
                    return Err(Error::Ingestion("immutable raw conflict".into()));
                }
                self.store.clear_staged(&source.work_id());
            }
        }
        let manifest = RawManifest {
            raw_key,
            sha256,
            bytes,
            member_count,
            etag,
            last_modified: source.last_modified.clone(),
            source_id: source.source_id.clone(),
            source_url: source.url.clone(),
            product: source.product.clone(),
        };
        let manifest_key = format!(
            "manifests/raw/{}/{}/{}.json",
            source.product, source.source_id, manifest.sha256
        );
        self.store.write_json_if_absent(&manifest_key, &manifest)?;
        let result = serde_json::to_value(&manifest)?
            .as_object()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        self.state.put(
            source.work_id(),
            State {
                source: source.clone(),
                status: "landed".into(),
                result,
            },
        )?;
        Ok(manifest)
    }
}

pub fn validate_zip(path: &Path) -> Result<usize, Error> {
    let file = File::open(path)?;
    let mut archive = ZipArchive::new(file)?;
    if archive.len() > 200_000 {
        return Err(Error::Ingestion("archive has too many members".into()));
    }
    let mut declared = 0u64;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let lower_name = entry.name().to_ascii_lowercase();
        if lower_name.ends_with(".tif") || lower_name.ends_with(".tiff") {
            return Err(Error::Ingestion(format!(
                "image member is not allowed: {}",
                entry.name()
            )));
        }
        declared = declared.saturating_add(entry.size());
        if declared > 64 * 1024 * 1024 * 1024 {
            return Err(Error::Ingestion(
                "archive expands beyond configured limit".into(),
            ));
        }
        io::copy(&mut entry, &mut io::sink())?;
    }
    Ok(archive.len())
}

fn matches_source(source: &SourceFile, manifest: &RawManifest) -> bool {
    manifest.product == source.product
        && manifest.source_id == source.source_id
        && manifest.source_url == source.url
        && source
            .expected_bytes
            .is_none_or(|value| value == manifest.bytes)
        && source
            .expected_sha256
            .as_ref()
            .is_none_or(|value| value == &manifest.sha256)
        && source
            .etag
            .as_ref()
            .is_none_or(|value| Some(value) == manifest.etag.as_ref())
        && source
            .last_modified
            .as_ref()
            .is_none_or(|value| Some(value) == manifest.last_modified.as_ref())
}

fn sha256_file(path: &Path) -> Result<String, Error> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 1024 * 1024];
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBatch {
    pub batch_id: String,
    pub renderer_profile: String,
    pub source_count: usize,
    pub sources: Vec<RawManifest>,
}

pub fn assemble_source_batch(
    store: &LocalStore,
    manifests: &mut [RawManifest],
    renderer_profile: &str,
) -> Result<SourceBatch, Error> {
    manifests.sort_by(|left, right| {
        (left.product.as_str(), left.source_id.as_str())
            .cmp(&(right.product.as_str(), right.source_id.as_str()))
    });
    for manifest in manifests.iter() {
        if !store.exists(&manifest.raw_key)? {
            return Err(Error::Ingestion(format!(
                "missing raw object: {}",
                manifest.raw_key
            )));
        }
    }
    #[derive(Serialize)]
    struct Identity<'a> {
        sources: &'a [RawManifest],
        renderer_profile: &'a str,
    }
    let identity = serde_json::to_vec(&Identity {
        sources: manifests,
        renderer_profile,
    })?;
    let mut hasher = Sha256::new();
    hasher.update(identity);
    let digest = hex_string(&hasher.finalize());
    let batch = SourceBatch {
        batch_id: format!("sha256:{digest}"),
        renderer_profile: renderer_profile.into(),
        source_count: manifests.len(),
        sources: manifests.to_vec(),
    };
    store.write_json_if_absent(&format!("source-batches/{digest}.json"), &batch)?;
    Ok(batch)
}

/// A source-pack pointer stored in every full-text document.
///
/// `offset` and `length` are byte offsets into the immutable Zstandard pack
/// named by `source_pack_key`; `source_pack` binds that key to its content
/// identity. Every frame contains exactly one canonical document, so a
/// serving worker can satisfy a hit with one Blob range read.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourcePointer {
    pub publication_uid: String,
    pub source_revision: String,
    pub source_generation: String,
    pub source_pack: String,
    pub source_pack_key: String,
    pub offset: u64,
    pub length: u64,
    pub uncompressed_length: u64,
    pub source_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourcePackManifest {
    pub format_version: u32,
    pub pack_sha256: String,
    pub pack_key: String,
    pub pack_bytes: u64,
    pub frame_count: usize,
    pub source_generation: String,
    pub renderer_profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourcePackEntry {
    publication_uid: String,
    source_revision: String,
    offset: u64,
    length: u64,
    uncompressed_length: u64,
    source_fingerprint: String,
}

/// Project a USPTO XML archive into Quickwit documents and an immutable source
/// pack. The ZIP members are sorted by name before rendering, and each
/// canonical Markdown document is compressed independently with deterministic
/// Zstandard settings (checksum and pledged content size enabled).
///
/// The pack is written before the final NDJSON is emitted because the
/// content-addressed `source_pack` value depends on the complete pack bytes.
/// The intermediate records are streamed through a temporary file, keeping
/// memory bounded by one publication at a time.
pub fn project_xml_archive_to_source_pack(
    archive_path: &Path,
    output_path: &Path,
    pack_path: &Path,
    source_generation: &str,
) -> Result<SourcePackManifest, Error> {
    if source_generation.trim().is_empty() {
        return Err(Error::SourcePack("source generation is empty".into()));
    }
    let file = File::open(archive_path)?;
    let mut archive = ZipArchive::new(file)?;
    let mut members = Vec::new();
    for index in 0..archive.len() {
        let member = archive.by_index(index)?;
        if member.name().to_ascii_lowercase().ends_with(".xml") {
            members.push((member.name().to_owned(), index));
        }
    }
    members.sort_by(|left, right| left.0.cmp(&right.0));
    if members.is_empty() {
        return Err(Error::Ingestion(
            "archive contains no XML publication".into(),
        ));
    }

    let intermediate_path = unique_temp_path(output_path);
    let pack_temporary = unique_temp_path(pack_path);
    let output_temporary = unique_temp_path(output_path);
    let result = (|| {
        let mut intermediate = BufWriter::new(File::create(&intermediate_path)?);
        let mut pack = BufWriter::new(File::create(&pack_temporary)?);
        let mut entries = Vec::new();
        let mut publication_uids = BTreeSet::new();
        let mut pack_offset = 0u64;

        for (_, index) in members {
            let mut member = archive.by_index(index)?;
            for_each_xml_publication(&mut member, |publication| {
                if !publication_uids.insert(publication.publication_uid.clone()) {
                    return Err(Error::SourcePack(format!(
                        "duplicate publication UID: {}",
                        publication.publication_uid
                    )));
                }
                let markdown = canonical_markdown(&publication);
                let frame = compress_source_frame(markdown.as_bytes())?;
                let offset = pack_offset;
                pack.write_all(&frame)?;
                pack_offset = pack_offset
                    .checked_add(frame.len() as u64)
                    .ok_or_else(|| Error::SourcePack("source pack exceeded u64".into()))?;
                entries.push(SourcePackEntry {
                    publication_uid: publication.publication_uid.clone(),
                    source_revision: format!(
                        "sha256:{}",
                        hex_string(&Sha256::digest(markdown.as_bytes()))
                    ),
                    offset,
                    length: frame.len() as u64,
                    uncompressed_length: markdown.len() as u64,
                    source_fingerprint: format!(
                        "blake2b-128:{}",
                        hex_string(&Blake2b512::digest(&frame)[..16])
                    ),
                });
                let document = xml_document_value(&publication, source_generation);
                serde_json::to_writer(
                    &mut intermediate,
                    &serde_json::json!({
                        "document": document,
                        "entry": entries.last().expect("entry was just appended"),
                    }),
                )?;
                intermediate.write_all(b"\n")?;
                Ok(())
            })?;
        }
        intermediate.flush()?;
        pack.flush()?;
        pack.get_ref().sync_all()?;
        let pack_sha256 = sha256_file(&pack_temporary)?;
        let pack_bytes = fs::metadata(&pack_temporary)?.len();
        let manifest = SourcePackManifest {
            format_version: 1,
            pack_sha256: format!("sha256:{pack_sha256}"),
            pack_key: format!("source/v1/packs/{pack_sha256}.zst"),
            pack_bytes,
            frame_count: entries.len(),
            source_generation: source_generation.into(),
            renderer_profile: RENDERER_PROFILE.into(),
        };

        publish_immutable_file(&pack_temporary, pack_path, &pack_sha256)?;

        let input = File::open(&intermediate_path)?;
        let mut output = BufWriter::new(File::create(&output_temporary)?);
        let lines = io::BufReader::new(input).lines();
        for line in lines {
            let line = line?;
            let record: serde_json::Value = serde_json::from_str(&line)?;
            let mut document = record
                .get("document")
                .cloned()
                .ok_or_else(|| Error::SourcePack("intermediate record has no document".into()))?;
            let entry: SourcePackEntry =
                serde_json::from_value(record.get("entry").cloned().ok_or_else(|| {
                    Error::SourcePack("intermediate record has no entry".into())
                })?)?;
            let object = document
                .as_object_mut()
                .ok_or_else(|| Error::SourcePack("document is not a JSON object".into()))?;
            object.insert(
                "source_pack".into(),
                serde_json::json!(manifest.pack_sha256),
            );
            object.insert(
                "source_pack_key".into(),
                serde_json::json!(format!(
                    "source/v1/packs/{}.zst",
                    &manifest.pack_sha256["sha256:".len()..]
                )),
            );
            object.insert(
                "source_revision".into(),
                serde_json::json!(entry.source_revision),
            );
            object.insert("offset".into(), serde_json::json!(entry.offset));
            object.insert("length".into(), serde_json::json!(entry.length));
            object.insert(
                "uncompressed_length".into(),
                serde_json::json!(entry.uncompressed_length),
            );
            object.insert(
                "source_fingerprint".into(),
                serde_json::json!(entry.source_fingerprint),
            );
            serde_json::to_writer(&mut output, &document)?;
            output.write_all(b"\n")?;
        }
        output.flush()?;
        output.get_ref().sync_all()?;
        let output_sha256 = sha256_file(&output_temporary)?;
        publish_immutable_file(&output_temporary, output_path, &output_sha256)?;
        Ok(manifest)
    })();
    let _ = fs::remove_file(&intermediate_path);
    let _ = fs::remove_file(&pack_temporary);
    let _ = fs::remove_file(&output_temporary);
    result
}

/// Backwards-compatible projection entry point. The pack is emitted next to
/// the NDJSON as `<output>.zst`; callers that publish source packs should use
/// [`project_xml_archive_to_source_pack`] and upload both immutable objects.
pub fn project_xml_archive(
    archive_path: &Path,
    output_path: &Path,
    source_generation: &str,
) -> Result<usize, Error> {
    let pack_path = output_path.with_extension("zst");
    Ok(project_xml_archive_to_source_pack(
        archive_path,
        output_path,
        &pack_path,
        source_generation,
    )?
    .frame_count)
}

#[derive(Debug, Clone)]
/// Normalized publication fields shared by the grant and application XML
/// profiles. The source-pack and hydration contracts do not depend on the
/// input product's XML layout.
struct XmlPublication {
    publication_uid: String,
    publication_date: String,
    filing_date: String,
    title: String,
    abstract_text: String,
    claims: String,
    description: String,
    publication_number: String,
    publication_key: String,
    application_number: String,
    applicant: Vec<String>,
    assignee: Vec<String>,
    ipc: Vec<String>,
    us_class: Vec<String>,
    publication_kind: String,
    markdown: String,
}

fn xml_document_value(publication: &XmlPublication, source_generation: &str) -> serde_json::Value {
    serde_json::json!({
        "publication_uid": publication.publication_uid,
        "publication_date": publication.publication_date,
        "filing_date": publication.filing_date,
        "title": publication.title,
        "abstract": publication.abstract_text,
        "claims": publication.claims,
        "description": publication.description,
        "publication_number": publication.publication_number,
        "publication_key": publication.publication_key,
        "patent_number": publication.publication_number,
        "patent_key": publication.publication_key,
        "application_number": publication.application_number,
        "doc_kind": if publication.publication_kind.starts_with('A') { "application" } else { "grant" },
        "applicant": publication.applicant,
        "assignee": publication.assignee,
        "ipc": publication.ipc,
        "us_class": publication.us_class,
        "shard": publication.publication_uid,
        "offset": 0,
        "length": 0,
        "uncompressed_length": 0,
        "source_generation": source_generation,
    })
}

/// USPTO bulk ZIP members may contain many complete XML documents. Keep only
/// the current document in memory and reject an incomplete final document.
fn for_each_xml_publication<R: io::Read>(
    input: R,
    mut emit: impl FnMut(XmlPublication) -> Result<(), Error>,
) -> Result<(), Error> {
    use quick_xml::events::Event;
    use quick_xml::{Reader, Writer};

    const MAX_PUBLICATION_XML_BYTES: usize = 512 * 1024 * 1024;
    let mut reader = Reader::from_reader(io::BufReader::new(input));
    let mut buffer = Vec::new();
    let mut document = Writer::new(Vec::new());
    let mut elements: Vec<Vec<u8>> = Vec::new();
    let mut root_name: Vec<u8> = Vec::new();
    let mut count = 0usize;
    loop {
        let event = reader.read_event_into(&mut buffer)?;
        match &event {
            Event::Eof => {
                if !elements.is_empty() {
                    return Err(Error::Ingestion("truncated XML publication".into()));
                }
                break;
            }
            Event::Start(start) => {
                if elements.is_empty() {
                    root_name = start.name().as_ref().to_vec();
                }
                elements.push(start.name().as_ref().to_vec());
            }
            Event::End(end) => {
                let Some(start) = elements.pop() else {
                    return Err(Error::Ingestion("unmatched XML end tag".into()));
                };
                if start != end.name().as_ref() {
                    return Err(Error::Ingestion("mismatched XML end tag".into()));
                }
            }
            Event::Empty(_) if elements.is_empty() => {
                return Err(Error::Ingestion("empty XML publication root".into()));
            }
            Event::Text(text) if elements.is_empty() => {
                if !text.as_ref().iter().all(u8::is_ascii_whitespace) {
                    return Err(Error::Ingestion("text outside XML publication".into()));
                }
            }
            Event::CData(_) if elements.is_empty() => {
                return Err(Error::Ingestion("CDATA outside XML publication".into()));
            }
            _ => {}
        }
        if !elements.is_empty() || matches!(event, Event::End(_)) {
            document.write_event(event.into_owned())?;
            if document.get_ref().len() > MAX_PUBLICATION_XML_BYTES {
                return Err(Error::Ingestion(
                    "single XML publication exceeds 512 MiB".into(),
                ));
            }
            if elements.is_empty() {
                // USPTO grant bundles can append a separate sequence listing
                // after a grant. It is not another publication or search hit.
                if root_name.as_slice() != b"sequence-cwu" {
                    emit(parse_xml_publication(document.get_ref())?)?;
                    count += 1;
                }
                document = Writer::new(Vec::new());
                root_name.clear();
            }
        }
        buffer.clear();
    }
    if count == 0 {
        return Err(Error::Ingestion(
            "XML ZIP member contains no publication".into(),
        ));
    }
    Ok(())
}

fn parse_xml_publication(bytes: &[u8]) -> Result<XmlPublication, Error> {
    let xml = tolerate_html_entities(&String::from_utf8_lossy(bytes));
    if xml.contains("<patent-application-publication") {
        return pap::parse_pap_v15_application(&xml);
    }
    let document = patpubrender::parse_patent_xml(&xml)
        .map_err(|error| Error::Ingestion(error.to_string()))?;
    let biblio = patpubrender::render::biblio::extract_biblio(&document);
    let section = |template: &str| {
        patpubrender::render_markdown_with_template(&document, template)
            .map_err(|error| Error::Ingestion(error.to_string()))
    };
    let title = biblio.invention_title.unwrap_or_default();
    let publication_number = biblio
        .doc_number
        .clone()
        .filter(|value| !value.is_empty())
        .or(biblio.patent_number.clone())
        .unwrap_or_default();
    if publication_number.is_empty() || title.is_empty() {
        return Err(Error::Ingestion(
            "XML publication is missing publication number or title".into(),
        ));
    }
    let publication_kind = biblio
        .kind_code
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "B2".into())
        .to_ascii_uppercase();
    let publication_key = biblio
        .country_code
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "US".into())
        .to_ascii_uppercase();
    Ok(XmlPublication {
        publication_uid: format!("{publication_key}:{publication_number}:{publication_kind}"),
        publication_date: date_rfc3339(&biblio.publication_date.unwrap_or_default()),
        filing_date: date_rfc3339(&biblio.filing_date.unwrap_or_default()),
        title,
        abstract_text: section("{{abstract}}")?,
        claims: section("{{claims}}")?,
        description: section("{{description}}")?,
        publication_number,
        publication_key,
        application_number: biblio.application_number.unwrap_or_default(),
        applicant: if biblio.applicant_names.is_empty() {
            biblio.inventor_names
        } else {
            biblio.applicant_names
        },
        assignee: biblio.assignee_names,
        ipc: biblio.ipc_classifications,
        us_class: biblio.us_classifications,
        publication_kind,
        markdown: patpubrender::render_markdown(&document),
    })
}

fn tolerate_html_entities(xml: &str) -> String {
    if !xml.contains('&') {
        return xml.to_owned();
    }
    let bytes = xml.as_bytes();
    let mut out = String::with_capacity(xml.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'&' {
            let start = index;
            while index < bytes.len() && bytes[index] != b'&' {
                index += 1;
            }
            out.push_str(&xml[start..index]);
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_alphanumeric() {
            end += 1;
        }
        if end < bytes.len() && bytes[end] == b';' && end > start && bytes[start].is_ascii_alphabetic()
        {
            let name = &xml[start..end];
            if matches!(name, "amp" | "lt" | "gt" | "quot" | "apos") {
                out.push('&');
                index += 1;
                continue;
            }
            match html_entity(name) {
                Some(value) => out.push_str(value),
                None => out.push_str(name),
            }
            index = end + 1;
            continue;
        }
        out.push('&');
        index += 1;
    }
    out
}

fn html_entity(name: &str) -> Option<&'static str> {
    Some(match name {
        "copy" => "©",
        "reg" => "®",
        "trade" => "™",
        "nbsp" => " ",
        "mdash" => "—",
        "ndash" => "–",
        "bull" => "•",
        "deg" => "°",
        "plusmn" => "±",
        "times" => "×",
        "divide" => "÷",
        "micro" => "µ",
        "middot" => "·",
        "hellip" => "…",
        "lsquo" | "rsquo" => "'",
        "ldquo" | "rdquo" => "\"",
        _ => return None,
    })
}

fn date_rfc3339(value: &str) -> String {
    if value.len() >= 8 && value.is_ascii() && value.as_bytes()[..8].iter().all(u8::is_ascii_digit)
    {
        format!(
            "{}-{}-{}T00:00:00Z",
            &value[0..4],
            &value[4..6],
            &value[6..8]
        )
    } else {
        value.to_owned()
    }
}

fn canonical_markdown(publication: &XmlPublication) -> String {
    // The identity comment is part of the canonical bytes. It lets the
    // hydration path verify that a range read belongs to the hit without
    // consulting a second metadata service. The body is patpubrender Markdown.
    format!(
        "<!-- publication_uid: {} -->\n{}",
        publication.publication_uid, publication.markdown
    )
}

fn compress_source_frame(markdown: &[u8]) -> Result<Vec<u8>, Error> {
    let mut encoder = ZstdEncoder::new(Vec::new(), 9)
        .map_err(|error| Error::Zstd(format!("create encoder: {error}")))?;
    encoder
        .include_checksum(true)
        .map_err(|error| Error::Zstd(format!("enable checksum: {error}")))?;
    encoder
        .include_contentsize(true)
        .map_err(|error| Error::Zstd(format!("enable content size: {error}")))?;
    encoder
        .include_dictid(false)
        .map_err(|error| Error::Zstd(format!("disable dictionary id: {error}")))?;
    encoder
        .set_pledged_src_size(Some(markdown.len() as u64))
        .map_err(|error| Error::Zstd(format!("set content size: {error}")))?;
    encoder
        .write_all(markdown)
        .map_err(|error| Error::Zstd(format!("compress frame: {error}")))?;
    encoder
        .finish()
        .map_err(|error| Error::Zstd(format!("finish frame: {error}")))
}

fn publish_immutable_file(
    source: &Path,
    destination: &Path,
    expected_sha256: &str,
) -> Result<(), Error> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    if destination.exists() {
        let actual = sha256_file(destination)?;
        if actual != expected_sha256 {
            return Err(Error::SourcePack(format!(
                "immutable object conflict at {}",
                destination.display()
            )));
        }
        return Ok(());
    }
    match fs::hard_link(source, destination) {
        Ok(()) => {
            fs::remove_file(source)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let actual = sha256_file(destination)?;
            if actual == expected_sha256 {
                let _ = fs::remove_file(source);
                Ok(())
            } else {
                Err(Error::SourcePack(format!(
                    "immutable object conflict at {}",
                    destination.display()
                )))
            }
        }
        Err(error) => Err(error.into()),
    }
}

/// Read exactly one independently compressed frame from a source pack.
///
/// This deliberately seeks to the supplied range before reading, matching
/// the HTTP `Range: bytes=start-end` operation used by the serving router.
/// It validates the compressed frame fingerprint, frame boundary, bounded
/// decompression size, canonical text revision, and publication identity.
pub fn hydrate_source_pack_range(
    pack_path: &Path,
    pointer: &SourcePointer,
) -> Result<String, Error> {
    validate_source_hash("source_pack", &pointer.source_pack, "sha256:", 64)?;
    if pointer.source_pack_key
        != format!(
            "source/v1/packs/{}.zst",
            &pointer.source_pack["sha256:".len()..]
        )
    {
        return Err(Error::SourcePack(
            "source pack key does not match identity".into(),
        ));
    }
    validate_source_hash(
        "source_generation",
        &pointer.source_generation,
        "sha256:",
        64,
    )?;
    validate_source_hash("source_revision", &pointer.source_revision, "sha256:", 64)?;
    validate_source_hash(
        "source_fingerprint",
        &pointer.source_fingerprint,
        "blake2b-128:",
        32,
    )?;
    if pointer.length == 0 || pointer.uncompressed_length == 0 {
        return Err(Error::SourcePack("empty source frame pointer".into()));
    }
    let expected_pack_key = format!(
        "source/v1/packs/{}.zst",
        &pointer.source_pack["sha256:".len()..]
    );
    if pointer.source_pack_key != expected_pack_key {
        return Err(Error::SourcePack(
            "source_pack_key does not match source_pack".into(),
        ));
    }
    const MAX_SOURCE_FRAME_BYTES: u64 = 64 * 1024 * 1024;
    if pointer.length > MAX_SOURCE_FRAME_BYTES {
        return Err(Error::SourcePack(
            "compressed source frame exceeds read limit".into(),
        ));
    }
    let total = fs::metadata(pack_path)?.len();
    let end = pointer
        .offset
        .checked_add(pointer.length)
        .ok_or_else(|| Error::SourcePack("source frame range overflow".into()))?;
    if end > total {
        return Err(Error::SourcePack(format!(
            "source frame range {}..{} exceeds pack size {}",
            pointer.offset, end, total
        )));
    }
    let mut file = File::open(pack_path)?;
    file.seek(SeekFrom::Start(pointer.offset))?;
    let length: usize = pointer
        .length
        .try_into()
        .map_err(|_| Error::SourcePack("source frame is too large".into()))?;
    let mut compressed = vec![0u8; length];
    file.read_exact(&mut compressed)?;

    let fingerprint = hex_string(&Blake2b512::digest(&compressed)[..16]);
    if fingerprint != pointer.source_fingerprint["blake2b-128:".len()..] {
        return Err(Error::SourcePack(
            "compressed frame fingerprint mismatch".into(),
        ));
    }
    let frame_size = zstd::zstd_safe::find_frame_compressed_size(&compressed)
        .map_err(|code| Error::Zstd(zstd::zstd_safe::get_error_name(code).to_string()))?;
    if frame_size as u64 != pointer.length {
        return Err(Error::SourcePack(format!(
            "pointer contains {} bytes but frame consumes {}",
            pointer.length, frame_size
        )));
    }
    let declared_size = zstd::zstd_safe::get_frame_content_size(&compressed)
        .map_err(|_| Error::SourcePack("invalid zstandard content size".into()))?
        .ok_or_else(|| Error::SourcePack("source frame omits content size".into()))?;
    if declared_size != pointer.uncompressed_length {
        return Err(Error::SourcePack(format!(
            "declared content size {} differs from pointer {}",
            declared_size, pointer.uncompressed_length
        )));
    }
    const MAX_SOURCE_TEXT_BYTES: u64 = 64 * 1024 * 1024;
    if pointer.uncompressed_length > MAX_SOURCE_TEXT_BYTES {
        return Err(Error::SourcePack(
            "source text exceeds decompression limit".into(),
        ));
    }
    let mut decoder = ZstdDecoder::new(Cursor::new(compressed))
        .map_err(|error| Error::Zstd(format!("create decoder: {error}")))?;
    let mut markdown = Vec::with_capacity(pointer.uncompressed_length as usize);
    decoder
        .read_to_end(&mut markdown)
        .map_err(|error| Error::Zstd(format!("decompress source frame: {error}")))?;
    if markdown.len() as u64 != pointer.uncompressed_length {
        return Err(Error::SourcePack("decompressed size mismatch".into()));
    }
    let revision = hex_string(&Sha256::digest(&markdown));
    if revision != pointer.source_revision["sha256:".len()..] {
        return Err(Error::SourcePack("canonical text revision mismatch".into()));
    }
    let text = String::from_utf8(markdown)
        .map_err(|_| Error::SourcePack("canonical source is not UTF-8".into()))?;
    let identity = format!("<!-- publication_uid: {} -->", pointer.publication_uid);
    if text.lines().next() != Some(identity.as_str()) {
        return Err(Error::SourcePack("publication identity mismatch".into()));
    }
    Ok(text)
}

fn validate_source_hash(
    name: &str,
    value: &str,
    prefix: &str,
    hex_len: usize,
) -> Result<(), Error> {
    let hex = value
        .strip_prefix(prefix)
        .ok_or_else(|| Error::SourcePack(format!("{name} must use {prefix}<hex> format")))?;
    if hex.len() != hex_len || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::SourcePack(format!("invalid {name}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;
    use zip::write::SimpleFileOptions;

    fn fixture_zip(include_tiff: bool) -> Vec<u8> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("publication.json", SimpleFileOptions::default())
            .unwrap();
        writer
            .write_all(br#"{"publication_uid":"US:1:A1"}"#)
            .unwrap();
        if include_tiff {
            writer
                .start_file("image.tiff", SimpleFileOptions::default())
                .unwrap();
            writer.write_all(b"image").unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    fn fixture_xml_zip() -> Vec<u8> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("fixture.xml", SimpleFileOptions::default())
            .unwrap();
        writer
            .write_all(
                br#"<?xml version="1.0"?>
<us-patent-grant lang="EN" dtd-version="v4.7 2022-02-17">
  <us-bibliographic-data-grant>
  <publication-reference><document-id><country>US</country><doc-number>9999999</doc-number><date>20260102</date></document-id></publication-reference>
  <application-reference><document-id><doc-number>17123456</doc-number><date>20250101</date></document-id></application-reference>
  <invention-title>Adaptive fixture controller</invention-title>
  <parties><applicants><applicant><addressbook><orgname>Acme Research</orgname></addressbook></applicant></applicants><assignees><assignee><addressbook><orgname>Acme Labs</orgname></addressbook></assignee></assignees></parties>
  <classifications-ipcr><classification-ipcr><section>A</section><class>01</class><subclass>B</subclass><main-group>1</main-group><subgroup>00</subgroup></classification-ipcr></classifications-ipcr>
  <classification-national><country>US</country><main-classification>123</main-classification></classification-national>
  </us-bibliographic-data-grant>
  <abstract>A fixture controller for signal processing.</abstract>
  <description>The controller adjusts a signal.</description>
  <claims><claim><claim-text>A controller comprising a signal processor.</claim-text></claim></claims>
</us-patent-grant>"#,
            )
            .unwrap();
        writer.finish().unwrap().into_inner()
    }

    fn zip_xml(bytes: &[u8]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file("bulk.xml", SimpleFileOptions::default())
            .unwrap();
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn projects_each_concatenated_publication_independently() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("bulk.zip");
        let output = directory.path().join("bulk.ndjson");
        let mut source_zip = ZipArchive::new(Cursor::new(fixture_xml_zip())).unwrap();
        let mut first = String::new();
        source_zip
            .by_name("fixture.xml")
            .unwrap()
            .read_to_string(&mut first)
            .unwrap();
        let second = first
            .replace("9999999", "8888888")
            .replace("Adaptive fixture controller", "Second invention");
        fs::write(&archive, zip_xml(format!("{first}\n{second}").as_bytes())).unwrap();
        assert_eq!(
            project_xml_archive(&archive, &output, "generation").unwrap(),
            2
        );
        let records: Vec<serde_json::Value> = fs::read_to_string(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records[0]["publication_uid"], "US:9999999:B2");
        assert_eq!(records[0]["title"], "Adaptive fixture controller");
        assert_eq!(records[1]["publication_uid"], "US:8888888:B2");
        assert_eq!(records[1]["title"], "Second invention");
    }

    #[test]
    fn skips_separate_sequence_listing_in_grant_bundle() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("with-sequence.zip");
        let output = directory.path().join("with-sequence.ndjson");
        let mut source_zip = ZipArchive::new(Cursor::new(fixture_xml_zip())).unwrap();
        let mut grant = String::new();
        source_zip.by_name("fixture.xml").unwrap().read_to_string(&mut grant).unwrap();
        let listing = r#"<?xml version="1.0"?><sequence-cwu><publication-reference><document-id><doc-number>9999999</doc-number></document-id></publication-reference><number>1</number></sequence-cwu>"#;
        fs::write(&archive, zip_xml(format!("{grant}\n{listing}").as_bytes())).unwrap();
        assert_eq!(project_xml_archive(&archive, &output, "generation").unwrap(), 1);
        assert_eq!(fs::read_to_string(output).unwrap().lines().count(), 1);
    }

    #[test]
    fn rejects_truncated_or_mismatched_bulk_xml() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("bad.zip");
        let output = directory.path().join("bad.ndjson");
        for xml in [
            "<us-patent-grant><invention-title>Broken</invention-title>",
            "<us-patent-grant><invention-title>Broken</us-patent-grant>",
        ] {
            fs::write(&archive, zip_xml(xml.as_bytes())).unwrap();
            assert!(project_xml_archive(&archive, &output, "generation").is_err());
            assert!(!output.exists());
        }
    }

    #[test]
    fn projects_xml_archive_into_search_document() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("fixture.zip");
        let output = directory.path().join("fixture.ndjson");
        fs::write(&archive, fixture_xml_zip()).unwrap();

        assert_eq!(
            project_xml_archive(&archive, &output, "release-test").unwrap(),
            1
        );
        let document: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(output).unwrap()).unwrap();
        assert_eq!(document["publication_number"], "9999999");
        assert_eq!(document["source_generation"], "release-test");
        assert_eq!(document["title"], "Adaptive fixture controller");
        assert!(
            document["claims"]
                .as_str()
                .unwrap()
                .contains("A controller comprising a signal processor.")
        );
        assert_eq!(document["applicant"][0], "Acme Research");
        assert_eq!(document["assignee"][0], "Acme Labs");
        assert_eq!(document["ipc"][0], "A01B1/00");
        assert_eq!(document["us_class"][0], "123");
        assert_eq!(document["publication_uid"], "US:9999999:B2");
        assert_eq!(document["source_fingerprint"].as_str().unwrap().len(), 44);
        assert_eq!(document["source_revision"].as_str().unwrap().len(), 71);
        assert!(
            document["source_pack"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );
    }

    #[test]
    fn supports_application_publications_and_rejects_non_uspto_xml() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("application.zip");
        let output = directory.path().join("application.ndjson");
        let mut source_zip = ZipArchive::new(Cursor::new(fixture_xml_zip())).unwrap();
        let mut source_xml = String::new();
        source_zip
            .by_name("fixture.xml")
            .unwrap()
            .read_to_string(&mut source_xml)
            .unwrap();
        let application_xml = source_xml
            .replace("us-patent-grant", "us-patent-application")
            .replace(
                "us-bibliographic-data-grant",
                "us-bibliographic-data-application",
            )
            .replace(
                "dtd-version=\"v4.7 2022-02-17\"",
                "dtd-version=\"v4.0 2004-12-02\"",
            )
            .replace(
                "<date>20260102</date>",
                "<kind>A1</kind><date>20260102</date>",
            );
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("application.xml", SimpleFileOptions::default())
            .unwrap();
        writer.write_all(application_xml.as_bytes()).unwrap();
        fs::write(&archive, writer.finish().unwrap().into_inner()).unwrap();
        project_xml_archive(&archive, &output, "sha256:application").unwrap();
        let document: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(output).unwrap()).unwrap();
        assert_eq!(document["publication_uid"], "US:9999999:A1");
        assert_eq!(document["doc_kind"], "application");

        let bad_archive = directory.path().join("bad.zip");
        let bad_output = directory.path().join("bad.ndjson");
        let bad_xml = application_xml.replace("us-patent-application", "not-a-patent");
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("bad.xml", SimpleFileOptions::default())
            .unwrap();
        writer.write_all(bad_xml.as_bytes()).unwrap();
        fs::write(&bad_archive, writer.finish().unwrap().into_inner()).unwrap();
        assert!(project_xml_archive(&bad_archive, &bad_output, "sha256:bad").is_err());
    }

    #[test]
    fn hydrates_one_range_and_rejects_identity_or_frame_tampering() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("fixture.zip");
        let output = directory.path().join("fixture.ndjson");
        let pack = directory.path().join("fixture.zst");
        fs::write(&archive, fixture_xml_zip()).unwrap();
        let manifest = project_xml_archive_to_source_pack(
            &archive,
            &output,
            &pack,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let document: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(output).unwrap()).unwrap();
        let pointer: SourcePointer = serde_json::from_value(document).unwrap();
        assert_eq!(pointer.source_pack, manifest.pack_sha256);
        let text = hydrate_source_pack_range(&pack, &pointer).unwrap();
        assert!(text.contains("Adaptive fixture controller"));
        assert!(text.starts_with("<!-- publication_uid: US:9999999:B2 -->"));

        let mut wrong_identity = pointer.clone();
        wrong_identity.publication_uid = "US:0000000:B2".into();
        assert!(matches!(
            hydrate_source_pack_range(&pack, &wrong_identity),
            Err(Error::SourcePack(message)) if message.contains("identity")
        ));

        let mut wrong_fingerprint = pointer;
        wrong_fingerprint.source_fingerprint =
            "blake2b-128:00000000000000000000000000000000".into();
        assert!(matches!(
            hydrate_source_pack_range(&pack, &wrong_fingerprint),
            Err(Error::SourcePack(message)) if message.contains("fingerprint")
        ));
    }

    #[test]
    fn source_pack_bytes_are_deterministic_and_immutable() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("fixture.zip");
        fs::write(&archive, fixture_xml_zip()).unwrap();
        let output_a = directory.path().join("a.ndjson");
        let pack_a = directory.path().join("a.zst");
        let output_b = directory.path().join("b.ndjson");
        let pack_b = directory.path().join("b.zst");
        let manifest_a =
            project_xml_archive_to_source_pack(&archive, &output_a, &pack_a, "sha256:generation")
                .unwrap();
        let manifest_b =
            project_xml_archive_to_source_pack(&archive, &output_b, &pack_b, "sha256:generation")
                .unwrap();
        assert_eq!(manifest_a, manifest_b);
        assert_eq!(fs::read(pack_a).unwrap(), fs::read(pack_b).unwrap());
        assert_eq!(fs::read(output_a).unwrap(), fs::read(output_b).unwrap());
    }

    #[test]
    fn lands_real_file_source_and_is_idempotent() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("fixture.zip");
        fs::write(&archive, fixture_zip(false)).unwrap();
        let store = LocalStore::new(directory.path().join("blob")).unwrap();
        let state = StateStore::new(directory.path().join("state.json")).unwrap();
        let worker = LandingWorker {
            store,
            state,
            chunk_size: 7,
        };
        let mut source_client = FileSource::new(&archive, 7).unwrap();
        let source = SourceFile {
            product: "PTGRXML".into(),
            source_id: "fixture.zip@1".into(),
            url: "file://fixture.zip".into(),
            expected_bytes: Some(archive.metadata().unwrap().len()),
            expected_sha256: None,
            etag: None,
            last_modified: None,
        };
        let first = worker.run(&source, &mut source_client).unwrap();
        let second = worker.run(&source, &mut source_client).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.member_count, 1);
    }

    #[test]
    fn rejects_tiff_members() {
        let directory = tempdir().unwrap();
        let archive = directory.path().join("fixture.zip");
        fs::write(&archive, fixture_zip(true)).unwrap();
        let store = LocalStore::new(directory.path().join("blob")).unwrap();
        let state = StateStore::new(directory.path().join("state.json")).unwrap();
        let worker = LandingWorker {
            store,
            state,
            chunk_size: 64,
        };
        let mut source_client = FileSource::new(&archive, 64).unwrap();
        let source = SourceFile {
            product: "PTGRXML".into(),
            source_id: "fixture.zip@1".into(),
            url: "file://fixture.zip".into(),
            expected_bytes: Some(archive.metadata().unwrap().len()),
            expected_sha256: None,
            etag: None,
            last_modified: None,
        };
        assert!(
            matches!(worker.run(&source, &mut source_client), Err(Error::Ingestion(message)) if message.contains("image member"))
        );
    }

    struct NoRangeSource {
        bytes: Vec<u8>,
        etag: String,
    }

    impl RangeSource for NoRangeSource {
        fn fetch(&mut self, _offset: u64, if_match: Option<&str>) -> Result<RangeResponse, Error> {
            if if_match.is_some_and(|value| value != self.etag) {
                return Err(Error::RemoteChanged("changed".into()));
            }
            Ok(RangeResponse {
                status: 200,
                body: Box::new(Cursor::new(self.bytes.clone())),
                etag: Some(self.etag.clone()),
                range_start: 0,
                total_bytes: Some(self.bytes.len() as u64),
            })
        }
    }

    #[test]
    fn progresses_when_source_ignores_range() {
        let directory = tempdir().unwrap();
        let bytes = fixture_zip(false);
        let store = LocalStore::new(directory.path().join("blob")).unwrap();
        let state = StateStore::new(directory.path().join("state.json")).unwrap();
        let worker = LandingWorker {
            store,
            state,
            chunk_size: 7,
        };
        let mut source_client = NoRangeSource {
            bytes: bytes.clone(),
            etag: "v1".into(),
        };
        let source = SourceFile {
            product: "PTGRXML".into(),
            source_id: "no-range.zip@1".into(),
            url: "fixture://no-range".into(),
            expected_bytes: Some(bytes.len() as u64),
            expected_sha256: None,
            etag: Some("v1".into()),
            last_modified: None,
        };
        let result = worker.run(&source, &mut source_client).unwrap();
        assert_eq!(result.bytes, bytes.len() as u64);
    }

    #[test]
    fn parses_patdoc_grant() {
        let publication = parse_xml_publication(
            br#"<PATDOC DTD="2.5" STATUS="Build 20020101">
<SDOBI>
<B100><B110><DNUM><PDAT>D0452599</PDAT></DNUM></B110><B130><PDAT>S1</PDAT></B130><B140><DATE><PDAT>20020101</PDAT></DATE></B140><B190><PDAT>US</PDAT></B190></B100>
<B200><B210><DNUM><PDAT>29129833</PDAT></DNUM></B210><B220><DATE><PDAT>20000922</PDAT></DATE></B220></B200>
<B500><B510><B511><PDAT>0101</PDAT></B511></B510><B520><B521><PDAT>D 1106</PDAT></B521></B520>
<B540><STEXT><PDAT>Confection item</PDAT></STEXT></B540></B500>
<B700>
<B720><B721><PARTY-US><NAM><FNM><PDAT>Ada</PDAT></FNM><SNM><STEXT><PDAT>Lovelace</PDAT></STEXT></SNM></NAM></PARTY-US></B721></B720>
<B730><B731><PARTY-US><NAM><ONM><STEXT><PDAT>Acme Labs</PDAT></STEXT></ONM></NAM></PARTY-US></B731></B730>
</B700>
</SDOBI>
<SDOAB><BTEXT><PARA><PTEXT><PDAT>A braided confection.</PDAT></PTEXT></PARA></BTEXT></SDOAB>
<SDODE><DETDESC><BTEXT><PARA><PTEXT><PDAT>The braid is tied.</PDAT></PTEXT></PARA></BTEXT></DETDESC></SDODE>
<SDOCL><CL><CLM><PARA><PTEXT><PDAT>The ornamental design for a confection item.</PDAT></PTEXT></PARA></CLM></CL></SDOCL>
</PATDOC>"#,
        )
        .unwrap();
        assert_eq!(publication.publication_uid, "US:D0452599:S1");
        assert_eq!(publication.title, "Confection item");
        assert_eq!(publication.publication_date, "2002-01-01T00:00:00Z");
        assert_eq!(publication.filing_date, "2000-09-22T00:00:00Z");
        assert_eq!(publication.application_number, "29129833");
        assert!(publication.abstract_text.contains("A braided confection."));
        assert!(publication.description.contains("The braid is tied."));
        assert!(publication.claims.contains("The ornamental design for a confection item."));
        assert_eq!(publication.applicant, vec!["Ada Lovelace".to_string()]);
        assert_eq!(publication.assignee, vec!["Acme Labs".to_string()]);
        assert_eq!(publication.ipc, vec!["0101".to_string()]);
        assert_eq!(publication.us_class, vec!["D 1106".to_string()]);
    }

    #[test]
    fn parses_pap_v15_application() {
        let publication = parse_xml_publication(
            br#"<?xml version="1.0" encoding="UTF-8"?>
<patent-application-publication>
  <subdoc-bibliographic-information>
    <document-id>
      <doc-number>20010000001</doc-number>
      <kind-code>A1</kind-code>
      <document-date>20010315</document-date>
    </document-id>
    <domestic-filing-data>
      <application-number><doc-number>09725796</doc-number></application-number>
      <filing-date>20001129</filing-date>
    </domestic-filing-data>
    <technical-information>
      <classification-ipc>
        <classification-ipc-primary><ipc>B08B003/00</ipc></classification-ipc-primary>
      </classification-ipc>
      <classification-us>
        <classification-us-primary>
          <uspc><class>134</class><subclass>031000</subclass></uspc>
        </classification-us-primary>
      </classification-us>
      <title-of-invention>Solvent mixture</title-of-invention>
    </technical-information>
    <inventors>
      <inventor>
        <name><given-name>Ada</given-name><family-name>Lovelace</family-name></name>
      </inventor>
    </inventors>
  </subdoc-bibliographic-information>
  <subdoc-abstract>
    <paragraph>Solvent mixture for cleaning surfaces.</paragraph>
  </subdoc-abstract>
  <subdoc-description>
    <detailed-description><section><paragraph>The solvent removes residue.</paragraph></section></detailed-description>
  </subdoc-description>
  <subdoc-claims>
    <claim><claim-text>1. A solvent mixture comprising water.</claim-text></claim>
  </subdoc-claims>
</patent-application-publication>"#,
        )
        .unwrap();
        assert_eq!(publication.publication_uid, "US:20010000001:A1");
        assert_eq!(publication.publication_kind, "A1");
        assert_eq!(publication.title, "Solvent mixture");
        assert!(publication.abstract_text.contains("Solvent"));
        assert!(publication.claims.contains("A solvent mixture comprising water."));
    }
}
