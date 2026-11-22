# Shopping Sessions: Rust pipeline

This is a standalone multithreaded Rust binary. The root `Dockerfile` builds
the Linux amd64 submission; the runtime has no network dependency. It reads
the input mount and writes `user_daily/` and `campaign_daily/` under the
output mount. There are no generated datasets or expected answers in the
repository.

## Processing

- Memory-map the CSVs, scan line-aligned chunks in parallel, and validate
  CSV syntax before deduplication. Select the AVX-512 or AVX2 scanner
  for events and clicks at runtime, and decode common 13-digit epoch
  timestamps with SIMD. Long rows, chunk tails, and unsupported CPUs
  use the portable CSV splitter.
- Build the linked-ID components while parsing. Only aliases need entries
  in the lookup table. Short ASCII IDs use exact compact keys and a
  worker-local cache; other IDs are compared by their full bytes.
- Route event payloads directly to person shards. A separate compact index
  retains event IDs and ingestion-order information, so deduplication
  only marks losing copies and does not reroute payloads. Invalid
  candidates still participate, and hash collisions are checked by byte
  comparison.
- Keep event timestamps and purchase metadata packed during grouping and
  sorting, with a wide fallback for longer windows. A single event needs
  no sort; for heavy histories with a bounded window, aggregate inside
  half-open 30-minute buckets, using a bitmap to skip empty buckets and
  comparing actual timestamps in occupied ones. A single session needs
  no click sort: scan once for last-touch
  attribution and distinct assisting campaigns. For longer histories,
  sweep sorted clicks and combine overlapping assist intervals per
  campaign. Identical copies of a click do not change the results.
- Write Parquet parts directly. Use 32-bit user counts only when the
  total input byte size proves they fit; keep 64-bit counts otherwise
  and 64-bit revenue in either case. Set `PIPELINE_PROFILE=1` for phase
  timings on stderr or `PIPELINE_AVX2=1` to select AVX2 on a machine
  that also supports AVX-512.

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
