# syntax=docker/dockerfile:1.6
FROM golang:1.24-alpine AS builder
WORKDIR /src

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GO111MODULE=on

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build \
      -trimpath \
      -ldflags="-s -w -X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildTime=${BUILD_TIME}" \
      -o /out/afd \
      ./cmd/afd

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app

COPY --from=builder /out/afd /usr/local/bin/afd
COPY config.yaml /app/config.yaml

RUN addgroup -S nexus && adduser -S nexus -G nexus && \
    mkdir -p /data/downloads && chown -R nexus:nexus /data
USER nexus

ENV AFD_NODE_DATA_DIR=/data \
    AFD_API_HOST=0.0.0.0 \
    AFD_DOWNLOAD_BT_DATA_DIR=/data/bt-data \
    AFD_DOWNLOAD_BT_TORRENT_FILES_DIR=/data/torrents

EXPOSE 8080/tcp
EXPOSE 50051/tcp
EXPOSE 50052/udp

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/afd"]
CMD ["serve", "-c", "/app/config.yaml"]
