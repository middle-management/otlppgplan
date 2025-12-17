# PostgreSQL EXPLAIN Plan Connector

A logs-to-traces connector for OpenTelemetry Collector that converts PostgreSQL `auto_explain` logs containing EXPLAIN JSON into distributed traces.

## Overview

This connector processes log records containing PostgreSQL query execution plans (from `EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS)` or `auto_explain`) and converts them into OpenTelemetry traces. This allows you to visualize query execution plans in standard tracing tools like Jaeger, Zipkin, or Grafana Tempo.

## Features

- Extracts EXPLAIN JSON from log body or specific attributes
- Converts query execution plans to hierarchical trace spans
- Preserves resource attributes from logs to traces
- Correlates generated traces with original log trace context
- Configurable error handling (drop silently or log errors)
- Supports all PostgreSQL plan node types
- Handles parallel query execution and CTEs

## Configuration

### Basic Configuration

```yaml
connectors:
  pgplan:
    source:
      type: body              # Extract from log body (default)
    conversion:
      service_name: postgresql
      include_plan_json: false
      expand_loops: false
    on_error: drop            # "drop" or "log"
```

### Extract from Attribute

```yaml
connectors:
  pgplan:
    source:
      type: attribute
      attribute_key: "pg.explain"  # Required when type is "attribute"
    conversion:
      service_name: postgresql
      db_name_attribute: "db.name"  # Optional: extract DB name from this attribute
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source.type` | string | `"body"` | Where to extract EXPLAIN JSON from: `"body"` or `"attribute"` |
| `source.attribute_key` | string | - | Attribute key to extract from (required if type is `"attribute"`) |
| `conversion.service_name` | string | `"postgresql"` | Service name for generated traces |
| `conversion.db_name_attribute` | string | - | Log attribute containing database name |
| `conversion.include_plan_json` | bool | `false` | Include raw plan JSON in trace attributes |
| `conversion.expand_loops` | bool | `false` | Create separate spans for loop iterations |
| `on_error` | string | `"drop"` | Error handling: `"drop"` (silent) or `"log"` (log errors) |

## Usage

### 1. Configure PostgreSQL auto_explain

Add to `postgresql.conf`:

```ini
shared_preload_libraries = 'auto_explain'
auto_explain.log_min_duration = 1000  # Log queries taking > 1s
auto_explain.log_analyze = on
auto_explain.log_buffers = on
auto_explain.log_format = json
auto_explain.log_timing = on
```

### 2. Configure OpenTelemetry Collector

```yaml
receivers:
  filelog:
    include: ["/var/log/postgresql/*.log"]
    operators:
      - type: json_parser
        parse_from: body

connectors:
  pgplan:
    source:
      type: body
    conversion:
      service_name: postgresql

processors:
  batch:

exporters:
  otlp:
    endpoint: jaeger:4317

service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [pgplan]  # Send logs to connector
    
    traces:
      receivers: [pgplan]  # Receive traces from connector
      processors: [batch]
      exporters: [otlp]
```

### 3. Build Custom Collector with OCB

See the [examples directory](../../examples/) for OCB configuration and build instructions.

## How It Works

1. **Log Ingestion**: Logs containing PostgreSQL EXPLAIN JSON are received by the collector
2. **Extraction**: The connector extracts EXPLAIN JSON from the log body or specified attribute
3. **Conversion**: Uses the `otlppgplan` library to convert the plan into OTLP trace format
4. **Enrichment**: Propagates resource attributes and correlates with log trace context
5. **Emission**: Emitted traces flow through the traces pipeline to exporters

## Example Input and Output

### Input Log (PostgreSQL auto_explain)

```json
{
  "message": "duration: 15.234 ms plan: ...",
  "body": "[{\"Plan\":{\"Node Type\":\"Seq Scan\",\"Relation Name\":\"users\"},\"Execution Time\":15.234}]"
}
```

### Output Trace

The connector generates a trace with spans representing:
- Root span: Overall query execution
- Planning span: Query planning phase
- Execution span: Query execution phase
- Node spans: Individual plan nodes (Seq Scan, Join, etc.)

Each span includes:
- Timing information (actual startup/total time)
- Cost estimates
- Row counts and loops
- Buffer usage statistics
- I/O timing

## Requirements

- OpenTelemetry Collector Builder (OCB) for building custom collectors
- PostgreSQL with `auto_explain` configured to output JSON format
- Log receiver capable of parsing PostgreSQL logs (e.g., `filelog`, `syslog`)

## Limitations

- Only supports JSON format EXPLAIN output
- Does not modify or forward original log records
- Error logs with invalid JSON are dropped (or logged if `on_error: log`)

## Development

### Running Tests

```bash
cd connector/pgplanconnector
go test -v ./...
```

### Building

This connector is designed to be built as part of a custom OpenTelemetry Collector using OCB. See the [examples](../../examples/) directory for build configuration.

## Related Projects

- [otlppgplan](https://github.com/middle-management/otlppgplan) - Core library for converting PostgreSQL plans to OTLP traces
- [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector)
- [OpenTelemetry Collector Contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib)

## License

Same as the parent `otlppgplan` project.
