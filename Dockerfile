# Stage 1: compile the Rust pipeline (build has network; the run does not).
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# Stage 2: tiny runtime image. Same Debian release as the build stage (glibc).
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/pipeline /app/pipeline
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
USER 1000:1000
WORKDIR /app
CMD ["/app/pipeline"]
