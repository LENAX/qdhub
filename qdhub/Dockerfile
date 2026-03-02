# Production: build binary and run server
FROM golang:1.24-alpine AS builder
WORKDIR /build
RUN apk add --no-cache gcc musl-dev
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /qdhub .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /qdhub .
# Default config can be overridden by volume or env
COPY configs ./configs
COPY migrations ./migrations
# data/ is created at runtime (volume or bind)
EXPOSE 8080
ENTRYPOINT ["./qdhub", "server"]
CMD ["--config", "configs/config.yaml"]
