# Third-party notices

`aps-worker/vendor/patpubrender/` contains vendored patpubrender source,
Copyright 2026 Jamie Noel, licensed under Apache License 2.0. Its
[license](aps-worker/vendor/patpubrender/LICENSE) and
[notice](aps-worker/vendor/patpubrender/NOTICE) are preserved with the source.

Runtime and build dependencies are listed in `pyproject.toml`, `uv.lock`, and
Rust `Cargo.toml`/`Cargo.lock` files; container base images are listed in the
Dockerfiles. Those components retain their own licenses and notices. Binary and
container distributions must include the notices required by their dependencies.

Small synthetic fixtures are covered by this repository's Apache License 2.0.
USPTO archives, production source packs, indexes, and snapshots are not included.
