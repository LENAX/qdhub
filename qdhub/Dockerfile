# Production: build binary and run server (glibc-based, for DuckDB)
FROM golang:1.26-bookworm AS builder
WORKDIR /build
# Retry apt to tolerate transient 502 from Debian mirror
RUN for i in 1 2 3; do apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates && rm -rf /var/lib/apt/lists/* && break; sleep 20; done
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /qdhub .

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Shanghai
ENV QDHUB_ENV=production
WORKDIR /app
COPY --from=builder /qdhub .
# Default config can be overridden by volume or env
COPY configs ./configs
COPY migrations ./migrations
# data/ is created at runtime (volume or bind)
EXPOSE 8080
ENTRYPOINT ["./qdhub", "server"]
CMD ["--config", "configs/config.yaml"]
