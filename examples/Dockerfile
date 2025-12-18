FROM golang:1.24-alpine AS builder

WORKDIR /build

# Copy the entire project
COPY . .

# Build the custom collector
WORKDIR /build/examples
RUN apk add --no-cache git && \
    go run go.opentelemetry.io/collector/cmd/builder@latest --config builder-config.yaml

FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=builder /build/examples/dist/otelcol-pgplan /usr/local/bin/otelcol-pgplan

EXPOSE 4317 4318 4319 13133

ENTRYPOINT ["/usr/local/bin/otelcol-pgplan"]
CMD ["--config=/etc/otel-collector-config.yaml"]
