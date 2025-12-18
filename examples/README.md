# OpenTelemetry Collector with PostgreSQL Plan Connector

This directory contains example configurations for building and running a custom OpenTelemetry Collector with the PostgreSQL EXPLAIN plan connector.

## Prerequisites

- Go 1.24 or later
- OpenTelemetry Collector Builder (OCB)
- PostgreSQL with `auto_explain` configured

## Installation

### 1. Install OpenTelemetry Collector Builder

```bash
go install go.opentelemetry.io/collector/cmd/builder@latest
```

The binary will be installed as `builder` in your `$GOPATH/bin` directory.

### 2. Build the Custom Collector

From this directory:

```bash
builder --config builder-config.yaml
```

This will:
- Generate collector source code
- Download all required dependencies
- Build the custom collector binary at `./dist/otelcol-pgplan`

### 3. Run the Collector

```bash
./dist/otelcol-pgplan --config collector-config.yaml
```

The collector will start with:
- OTLP receiver on ports 4317 (gRPC) and 4318 (HTTP)
- Debug exporter logging to console
- pgplan connector converting logs to traces

## Configuration Files

### builder-config.yaml

Defines the custom collector build:
- **dist**: Output configuration (name, path, version)
- **receivers**: OTLP receiver for ingesting logs
- **processors**: Batch processor for efficient data handling
- **exporters**: Debug and OTLP exporters
- **connectors**: pgplan connector (local path reference)

### collector-config.yaml

Runtime configuration for the collector:
- **Logs pipeline**: OTLP receiver → pgplan connector → debug exporter
- **Traces pipeline**: pgplan connector → batch processor → OTLP/debug exporters

## Testing with Sample Data

### Create a Sample Log with EXPLAIN JSON

Create a file `sample-log.json`:

```json
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          {
            "key": "service.name",
            "value": { "stringValue": "postgresql" }
          }
        ]
      },
      "scopeLogs": [
        {
          "logRecords": [
            {
              "body": {
                "stringValue": "[{\"Plan\":{\"Node Type\":\"Seq Scan\",\"Relation Name\":\"users\",\"Startup Cost\":0.0,\"Total Cost\":10.5,\"Plan Rows\":100,\"Plan Width\":8,\"Actual Startup Time\":0.5,\"Actual Total Time\":5.2,\"Actual Rows\":95,\"Actual Loops\":1},\"Execution Time\":5.5}]"
              },
              "timeUnixNano": "1640000000000000000"
            }
          ]
        }
      ]
    }
  ]
}
```

### Send the Log to the Collector

Using `curl`:

```bash
curl -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d @sample-log.json
```

### Expected Output

In the collector logs (debug exporter), you should see:
1. The original log record
2. Generated trace spans representing the query execution plan

## PostgreSQL Configuration

To generate auto_explain logs in JSON format, add to `postgresql.conf`:

```ini
# Enable auto_explain module
shared_preload_libraries = 'auto_explain'

# Log queries taking longer than 1 second
auto_explain.log_min_duration = 1000

# Include execution statistics
auto_explain.log_analyze = on
auto_explain.log_buffers = on
auto_explain.log_timing = on
auto_explain.log_verbose = on

# Use JSON format (required for the connector)
auto_explain.log_format = json

# Log level
auto_explain.log_level = log
```

Restart PostgreSQL after configuration changes.

## Collector Pipeline Flow

```
┌─────────────┐
│   Logs      │
│  (OTLP)     │
└──────┬──────┘
       │
       v
┌─────────────────────────┐
│  pgplan connector       │
│  - Extract EXPLAIN JSON │
│  - Convert to traces    │
└──────┬─────────┬────────┘
       │         │
       v         v
  ┌─────┐   ┌────────┐
  │Debug│   │ Traces │
  └─────┘   │(OTLP)  │
            └────┬───┘
                 │
                 v
            ┌─────────┐
            │  Batch  │
            └────┬────┘
                 │
                 v
        ┌────────┴────────┐
        │                 │
    ┌───v────┐      ┌────v────┐
    │  OTLP  │      │  Debug  │
    └────────┘      └─────────┘
```

## Customization

### Add More Receivers

To collect logs from files, add the filelog receiver to `builder-config.yaml`:

```yaml
receivers:
  - gomod: go.opentelemetry.io/collector/receiver/otlpreceiver v0.117.0
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver v0.117.0
```

And update `collector-config.yaml`:

```yaml
receivers:
  filelog:
    include: ["/var/log/postgresql/*.log"]
    operators:
      - type: json_parser
        parse_from: body
```

### Configure Connector for Attribute Extraction

If your logs have EXPLAIN JSON in a specific attribute:

```yaml
connectors:
  pgplan:
    source:
      type: attribute
      attribute_key: "pg.explain"
    conversion:
      service_name: postgresql
      db_name_attribute: "db.name"
```

### Add More Exporters

To send traces to Jaeger:

```yaml
exporters:
  - gomod: go.opentelemetry.io/collector/exporter/otlpexporter v0.117.0
  # Add Jaeger exporter
```

## Troubleshooting

### Build Fails

- Ensure Go 1.24+ is installed: `go version`
- Check that OCB is installed: `builder version`
- Verify network access to download dependencies

### Collector Starts but No Traces Generated

- Check that logs contain valid EXPLAIN JSON
- Verify the source configuration matches your log structure
- Enable `on_error: log` in connector config to see error messages
- Check debug exporter output for both logs and traces

### Invalid EXPLAIN JSON

The connector expects PostgreSQL EXPLAIN output in JSON format:

```json
[
  {
    "Plan": {
      "Node Type": "...",
      ...
    },
    "Execution Time": 1.23
  }
]
```

Enable `auto_explain.log_format = json` in PostgreSQL configuration.

## Performance Considerations

- Use `batch` processor to reduce network overhead
- Configure `auto_explain.log_min_duration` appropriately (don't log all queries)
- Consider sampling if query volume is high
- Monitor collector resource usage

## Docker Demo

Want to see the complete pipeline in action? Check out the Docker Compose demo:

```bash
# Start PostgreSQL with auto_explain + collectors + Jaeger
docker compose up -d --build

# Run test queries
docker compose exec -T postgres psql -U postgres -d testdb < test-queries.sql

# View traces at http://localhost:16686
```

See [DOCKER.md](./DOCKER.md) for complete documentation of the Docker setup.

The demo includes:
- PostgreSQL with auto_explain configured
- OTel Collector agent collecting logs
- Custom collector with pgplan connector
- Jaeger for trace visualization
- Sample database and queries

## Next Steps

- Try the Docker demo to see the full pipeline
- Integrate with your observability backend (Jaeger, Tempo, etc.)
- Set up log collection from PostgreSQL servers
- Configure alerting based on query performance patterns
- Create dashboards to visualize query execution plans

## Resources

- [OpenTelemetry Collector Documentation](https://opentelemetry.io/docs/collector/)
- [OCB Documentation](https://opentelemetry.io/docs/collector/custom-collector/)
- [PostgreSQL auto_explain Documentation](https://www.postgresql.org/docs/current/auto-explain.html)
- [otlppgplan Repository](https://github.com/middle-management/otlppgplan)
