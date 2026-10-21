![owservable](https://avatars0.githubusercontent.com/u/87773159?s=75)

# owservable-rs

Rust implementation of the owservable reactive stack. Speaks the same websocket wire protocol as the TypeScript v3 packages (`@owservable/*` on npm, now maintenance-only), so existing frontends keep working.

| Crate | Purpose |
| --- | --- |
| [`owservable-core`](crates/core) | wire protocol, per-socket client, reactive stores, `ObservableBackend` trait |
| [`owservable-mongodb`](crates/mongodb) | MongoDB change streams |
| [`owservable-postgres`](crates/postgres) | PostgreSQL LISTEN/NOTIFY |
| [`owservable-sqlite`](crates/sqlite) | SQLite journal-trigger live updates |

> Work in progress — not yet published.

## Development

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

Protocol parity fixtures in `crates/core/tests/golden` are recorded from the TypeScript implementation:

```bash
cd tools/record-golden && pnpm i && pnpm record
```

## License

Unlicense — see [LICENSE](LICENSE).
