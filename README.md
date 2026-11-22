# Shopping Sessions: Rust pipeline

This is a standalone multithreaded Rust binary. The root `Dockerfile` builds
the Linux amd64 submission; the runtime has no network dependency. It reads
the input mount and writes `user_daily/` and `campaign_daily/` under the
output mount. There are no generated datasets or expected answers in the
repository.

## Processing

- Memory-map the CSVs, scan line-aligned chunks in parallel, and validate
  CSV syntax before deduplication. The vector parser is selected at runtime;
  the scalar parser handles other machines and exceptional row lengths.
- Resolve connected user IDs by union-find. Only aliases need entries in the
  lookup table. Short ASCII IDs use an exact compact key; other IDs are
  checked against their full bytes, not joined by hash alone.
- Index event IDs in buckets without copying the input records. Invalid
  candidates still take part in deduplication so an earlier invalid record
  cannot be rescued by a later valid duplicate.
- Group by person and sessionize their sorted events. Compute last-touch
  attribution and distinct campaign assists in one sweep for short
  histories, and by interval union for larger histories. Identical copies
  of a click do not alter either the winning click or a campaign's assist.
- Write the Parquet parts directly. Set `PIPELINE_PROFILE=1` for per-phase
  timings on stderr.

## Local verification

Download the challenge's starter, generator and dev answer from the [Shopping
Sessions track](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md).
The following uses the DuckDB starter only for the official check scripts,
not for the pipeline. Its fixture runner and comparator require Python with
Polars installed.

```sh
# Run from this repository. Paths below point to the unzipped downloads.
STARTER=/absolute/path/to/starter-duckdb
GENERATOR=/absolute/path/to/generator
ANSWER=/absolute/path/to/dev-answer
DEV=/absolute/path/to/generated-dev

cargo test --release --locked
cargo build --release --locked
cargo build --release --locked --manifest-path "$GENERATOR/Cargo.toml"
"$GENERATOR/target/release/generate" \
  --seed 1 --events 1000000 --clicks 250000 --output "$DEV"
NCPU=8 python3 "$STARTER/scripts/run_fixtures.py" "$STARTER/fixtures" \
  -- "$PWD/target/release/pipeline"
NCPU=8 DATA_DIR="$DEV" OUTPUT_DIR="$STARTER/out" target/release/pipeline
make -C "$STARTER" check-dev ANSWER="$ANSWER"
```
