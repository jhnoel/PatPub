# Vendored patpubrender

patpubrender parses USPTO patent document formats into a common document model
and renders Markdown. PatPub uses its APS parser from the `aps-worker` crate.
This source is kept locally so production builds do not require another checkout.

The original source modules and attribution are retained. Dependency versions
are locked by `aps-worker/Cargo.lock`; use the parent crate's maintenance tests:

```sh
cargo test --locked --manifest-path aps-worker/Cargo.toml
```

Copyright 2026 Jamie Noel. Licensed under [Apache License 2.0](LICENSE);
see [NOTICE](NOTICE). This project is not affiliated with or endorsed by the USPTO.
