FROM rust:1.88-bookworm

WORKDIR /app
COPY . /app


RUN mv Cargo.docker.toml Cargo.toml

RUN apt-get update && \
    apt-get install -y libssl-dev pkg-config && \
    cargo build --release
