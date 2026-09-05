# syntax=docker/dockerfile:1

# ---- build stage -----------------------------------------------------------
FROM golang:1.26-alpine AS build

ARG VERSION=dev
WORKDIR /src

# cache module downloads
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

# pure Go static binary: works on any libc (glibc / musl)
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOFLAGS=-trimpath \
    go build -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/ocgate ./cmd/ocgate

# ---- runtime stage ---------------------------------------------------------
FROM alpine:3.22

# ca-certificates for HTTPS upstreams, tzdata for log timestamps
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S ocgate \
    && adduser -S -G ocgate -H ocgate

COPY --from=build /out/ocgate /usr/local/bin/ocgate
COPY config.example.yml /etc/ocgate/config.example.yml

USER ocgate

EXPOSE 8080
VOLUME ["/etc/ocgate"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -q --spider "http://127.0.0.1:${OCGATE_PORT:-8080}/healthz" || exit 1

ENTRYPOINT ["/usr/local/bin/ocgate"]
CMD ["-config", "/etc/ocgate/config.yml"]
