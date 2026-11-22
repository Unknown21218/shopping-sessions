# Shopping Sessions: Rust pipeline

This is a standalone multithreaded Rust binary. The root `Dockerfile` builds
the Linux amd64 submission; the runtime has no network dependency. It reads
the input mount and writes `user_daily/` and `campaign_daily/` under the
output mount. There are no generated datasets or expected answers in the
repository.

## Processing

- Memory-map the CSVs, scan line-aligned chunks in parallel, and validate
  CSV syntax before deduplication. Select the AVX-512 or AVX2 event scanner
  at runtime; other CPUs and exceptional rows use the portable parser.
- Build the linked-ID components while parsing. Only aliases need entries
  in the lookup table. Short ASCII IDs use exact compact keys; other IDs
  are compared by their full bytes.
- Route event payloads directly to person shards. A separate compact index
  retains event IDs and ingestion-order information, so deduplication
  only marks losing copies and does not reroute payloads. Invalid
  candidates still participate, and hash collisions are checked by byte
  comparison.
- Group by person and sort short histories by time. For heavy histories
  with a bounded window, aggregate inside half-open 30-minute buckets and
  check the exact gap between successive nonempty buckets. Compute
  last-touch attribution and distinct campaign assists in one sweep for
  short histories, and by interval union for larger histories. Identical
  copies of a click do not change attribution or assists.
- Write the Parquet parts directly. Set `PIPELINE_PROFILE=1` for per-phase
  timings on stderr or `PIPELINE_AVX2=1` to test the AVX2 event scanner on
  a machine that also supports AVX-512.

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
