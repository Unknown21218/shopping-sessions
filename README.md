# Shopping Sessions: Rust pipeline

Single multi-threaded binary (memchr + memmap2 + parquet, no Arrow).
Reads /data, writes /output/user_daily and /output/campaign_daily.

Phases: parse events (mmap, 8 MB line-aligned chunks) -> union-find over links (side thread)
-> parse clicks + dedup events by event_id hash (bucketed) -> bucket by person, sort, sessionize,
credit/assist, write one Parquet part per worker.

Local check:  python3 scripts/run_fixtures.py fixtures -- target/release/pipeline
