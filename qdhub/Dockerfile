# syntax=docker/dockerfile:1
# Production: build binary and run server (glibc-based, for DuckDB)
FROM golang:latest AS builder
WORKDIR /build
# Retry apt to tolerate transient 502 from Debian mirror.
# Cache mounts persist .deb / lists on the build host between builds (not in image); installed packages still go into the image layer unchanged.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=qdhub-builder-apt \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=qdhub-builder-apt-lists \
    for i in 1 2 3; do apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates && rm -rf /var/lib/apt/lists/* && break; sleep 20; done
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked,id=qdhub-gomod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod,sharing=locked,id=qdhub-gomod \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked,id=qdhub-gocache \
    CGO_ENABLED=1 go build -ldflags="-s -w" -o /qdhub .

FROM debian:latest
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=qdhub-runtime-apt \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked,id=qdhub-runtime-apt-lists \
    apt-get update && apt-get install -y --no-install-recommends ca-certificates wget tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Shanghai
ENV QDHUB_ENV=production
# 8GB 级生产机基线：heap 水位早于默认(4096/6144)触发子任务降档；GOMEMLIMIT 约束 Go 堆，DuckDB/cgo 仍可能额外占 RSS，需配合 compose mem_limit
ENV GOMEMLIMIT=5GiB
ENV QDHUB_SYNC_MEM_HIGH_MB=2048
ENV QDHUB_SYNC_MEM_CRITICAL_MB=4096
ENV QDHUB_SUBTASK_BATCH_PAUSE_HIGH_MS=100
ENV QDHUB_SUBTASK_BATCH_PAUSE_CRITICAL_MS=400
WORKDIR /app
COPY --from=builder /qdhub .
# Default config can be overridden by volume or env
COPY configs ./configs
COPY migrations ./migrations
# data/ is created at runtime (volume or bind)
EXPOSE 8080
ENTRYPOINT ["./qdhub", "server"]
CMD ["--config", "configs/config.yaml"]
