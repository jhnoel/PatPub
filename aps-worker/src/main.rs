use aps_backfill::{
    decode_zip_text, export_zip_text, pack::pack_ndjson, record_to_json, sorted_zip_paths,
    SampleTracker,
};
use serde_json::Value;
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process;

fn main() {
    if let Err(code) = run() {
        process::exit(code);
    }
}

fn run() -> Result<(), i32> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        eprintln!("usage: aps-backfill export <zip-dir> [--sample <path>] [--limit <n-zips>]");
        eprintln!("       aps-backfill show <zip-file> <patent-number>");
        eprintln!("       aps-backfill pack <ndjson> <zip-dir> <out-dir>");
        return Err(1);
    };

    match command.as_str() {
        "export" => cmd_export(&mut args),
        "show" => cmd_show(&mut args),
        "pack" => cmd_pack(&mut args),
        _ => {
            eprintln!("unknown command: {command}");
            Err(1)
        }
    }
}

fn cmd_export(args: &mut impl Iterator<Item = String>) -> Result<(), i32> {
    let Some(zip_dir) = args.next() else {
        eprintln!("export requires <zip-dir>");
        return Err(1);
    };

    let mut sample_path: Option<PathBuf> = None;
    let mut limit: Option<usize> = None;

    let positional: Vec<String> = args.collect();
    let mut i = 0;
    while i < positional.len() {
        match positional[i].as_str() {
            "--sample" => {
                i += 1;
                sample_path = Some(PathBuf::from(
                    positional.get(i).ok_or_else(|| {
                        eprintln!("--sample requires a path");
                        1
                    })?,
                ));
            }
            "--limit" => {
                i += 1;
                let raw = positional.get(i).ok_or_else(|| {
                    eprintln!("--limit requires a number");
                    1
                })?;
                limit = Some(raw.parse().map_err(|_| {
                    eprintln!("invalid --limit value: {raw}");
                    1
                })?);
            }
            other => {
                eprintln!("unknown export flag: {other}");
                return Err(1);
            }
        }
        i += 1;
    }

    let zip_paths = sorted_zip_paths(Path::new(&zip_dir), limit).map_err(|err| {
        eprintln!("failed to read zip dir {}: {err}", zip_dir);
        1
    })?;

    let mut stdout = io::stdout().lock();
    let mut sample = sample_path.as_ref().map(|_| SampleTracker::default());
    let mut total_docs = 0usize;
    let mut total_failures = 0usize;

    for zip_path in zip_paths {
        let zip_name = zip_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown.zip");
        let source_stem = zip_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("unknown");

        let text = match decode_zip_text(&zip_path) {
            Ok(text) => text,
            Err(err) => {
                eprintln!("{err}");
                return Err(1);
            }
        };

        let (emitted, failures) = export_zip_text(
            &text,
            source_stem,
            &mut stdout,
            sample.as_mut(),
            total_docs,
        );
        total_docs += emitted;
        total_failures += failures;
        eprintln!("{zip_name}: docs={total_docs} failures={total_failures}");
    }

    if let (Some(path), Some(tracker)) = (sample_path.as_ref(), sample.as_ref()) {
        if let Err(err) = tracker.write_jsonl(path) {
            eprintln!("failed to write sample sidecar {}: {err}", path.display());
            return Err(1);
        }
    }

    Ok(())
}

fn cmd_show(args: &mut impl Iterator<Item = String>) -> Result<(), i32> {
    let zip_file = args.next().ok_or_else(|| {
        eprintln!("show requires <zip-file>");
        1
    })?;
    let patent_number = args.next().ok_or_else(|| {
        eprintln!("show requires <patent-number>");
        1
    })?;

    let path = Path::new(&zip_file);
    let source_stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("unknown");

    let text = decode_zip_text(path).map_err(|err| {
        eprintln!("{err}");
        1
    })?;

    let starts = patpubrender::aps_record_starts(text.as_bytes());
    let mut bounds = starts;
    bounds.push(text.len());

    for window in bounds.windows(2) {
        let record = &text[window[0]..window[1]];
        if record.trim().is_empty() {
            continue;
        }
        let obj = match record_to_json(record, source_stem) {
            Ok(obj) => obj,
            Err(_) => continue,
        };
        if obj
            .get("patent_number")
            .and_then(Value::as_str)
            .is_some_and(|value| value == patent_number)
        {
            let line = Value::Object(obj);
            println!("{line}");
            return Ok(());
        }
    }

    Err(1)
}

fn cmd_pack(args: &mut impl Iterator<Item = String>) -> Result<(), i32> {
    let ndjson = args.next().ok_or_else(|| {
        eprintln!("pack requires <ndjson>");
        1
    })?;
    let zip_dir = args.next().ok_or_else(|| {
        eprintln!("pack requires <zip-dir>");
        1
    })?;
    let out_dir = args.next().ok_or_else(|| {
        eprintln!("pack requires <out-dir>");
        1
    })?;

    pack_ndjson(
        Path::new(&ndjson),
        Path::new(&zip_dir),
        Path::new(&out_dir),
    )
    .map_err(|err| {
        eprintln!("{err}");
        1
    })
}
