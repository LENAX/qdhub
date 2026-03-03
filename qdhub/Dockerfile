# Production: build binary and run server (glibc-based, for DuckDB)
FROM golang:1.24 AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /qdhub .

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /qdhub .
# Default config can be overridden by volume or env
COPY configs ./configs
COPY migrations ./migrations
# data/ is created at runtime (volume or bind)
EXPOSE 8080
ENTRYPOINT ["./qdhub", "server"]
CMD ["--config", "configs/config.yaml"]
