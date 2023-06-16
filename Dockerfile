FROM valdocarbonaboyz/debian-protocol-buffer:latest as builder

RUN apt update && apt install --yes pkg-config gcc curl unzip libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev clang liburing-dev

WORKDIR /srv/www
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/local/rustup \
    set -eux; \
    rustup install stable; \
    rustup default stable; \
    cargo build --release; \
    objcopy --compress-debug-sections target/release/carbonmeter-api ./carbonmeter-api; \
    objcopy --compress-debug-sections target/release/build-event-store ./build-event-store;

FROM debian:bullseye-slim as production-runtime

RUN set -eux; \
    export DEBIAN_FRONTEND=noninteractive; \
    echo "deb http://deb.debian.org/debian unstable main" >> /etc/apt/sources.list; \
    apt update; \
    apt install --yes pkg-config gcc ca-certificates openssl libssl-dev protobuf-compiler=3.21.12-3 libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev clang liburing-dev supervisor; \
    apt clean autoclean; \
    apt autoremove --yes; \
    rm -rf /var/lib/apt/* /var/lib/dpkg/* /var/lib/cache/* /var/lib/log/*; \
    echo "Installed base utils!"

WORKDIR /srv/www

COPY --from=builder /srv/www/carbonmeter-api ./carbonmeter-api
COPY --from=builder /srv/www/build-event-store ./build-event-store
COPY ./supervisor.conf ./supervisor.conf

CMD ["./carbonmeter-api"] 
