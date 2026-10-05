use std::env;
use std::fs;
use std::path::PathBuf;

use uspto_ingestion_worker::{
    FileSource, LandingWorker, LocalStore, SourceFile, SourcePointer, StateStore,
    hydrate_source_pack_range, project_xml_archive_to_source_pack,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let first = args.next().ok_or(
        "usage: worker <archive.zip> [root] [source-id] | worker project <archive.zip> <out.ndjson> [source-generation] [pack.zst] | worker hydrate <pack.zst> <pointer.json>",
    )?;
    if first == "project" {
        let archive = PathBuf::from(args.next().ok_or("missing archive path")?);
        let output = PathBuf::from(args.next().ok_or("missing output path")?);
        let source_generation = args.next().unwrap_or_else(|| "fixture-v1".into());
        let pack = args
            .next()
            .map(PathBuf::from)
            .unwrap_or_else(|| output.with_extension("zst"));
        let manifest =
            project_xml_archive_to_source_pack(&archive, &output, &pack, &source_generation)?;
        println!(
            "projected_documents={} output={} source_pack={} pack_bytes={}",
            manifest.frame_count,
            output.display(),
            manifest.pack_sha256,
            manifest.pack_bytes
        );
        return Ok(());
    }
    if first == "hydrate" {
        let pack = PathBuf::from(args.next().ok_or("missing source pack path")?);
        let pointer_path = PathBuf::from(args.next().ok_or("missing pointer JSON path")?);
        let pointer: SourcePointer = serde_json::from_slice(&fs::read(pointer_path)?)?;
        let text = hydrate_source_pack_range(&pack, &pointer)?;
        print!("{text}");
        return Ok(());
    }
    let archive = PathBuf::from(first);
    let root = PathBuf::from(args.next().unwrap_or_else(|| ".rust-ingestion".into()));
    let source_id = args.next().unwrap_or_else(|| {
        archive
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned()
    });
    let bytes = fs::metadata(&archive)?.len();
    let mut source_client = FileSource::new(&archive, 8 * 1024 * 1024)?;
    let product = env::var("USPTO_PRODUCT").unwrap_or_else(|_| "PTGRXML".into());
    if !uspto_ingestion_worker::ALLOWED_PRODUCTS.contains(&product.as_str()) {
        return Err(format!("unsupported USPTO_PRODUCT: {product}").into());
    }
    let source = SourceFile {
        product,
        source_id,
        url: format!("file://{}", archive.display()),
        expected_bytes: Some(bytes),
        expected_sha256: None,
        etag: None,
        last_modified: None,
    };
    let worker = LandingWorker {
        store: LocalStore::new(root.join("blob"))?,
        state: StateStore::new(root.join("state.json"))?,
        chunk_size: 8 * 1024 * 1024,
    };
    let result = worker.run(&source, &mut source_client)?;
    println!(
        "raw_key={} sha256={} bytes={} members={}",
        result.raw_key, result.sha256, result.bytes, result.member_count
    );
    Ok(())
}
