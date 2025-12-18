# Docker Compose Demo: PostgreSQL auto_explain → OTLP Traces

This demo shows the complete pipeline from PostgreSQL auto_explain logs to OpenTelemetry traces visualized in Jaeger.

## Architecture

```
┌──────────────┐
│  PostgreSQL  │ auto_explain outputs JSON to stderr
│  with        │
│  auto_explain│
└──────┬───────┘
       │ logs
       ▼
┌──────────────────┐
│ OTel Collector   │ filelog receiver
│ (Agent)          │ - Parses Docker logs
│                  │ - Extracts EXPLAIN JSON
│                  │ - Sends as OTLP logs
└──────┬───────────┘
       │ OTLP logs (HTTP)
       ▼
┌──────────────────┐
│ OTel Collector   │ pgplan connector
│ (Custom)         │ - Converts logs to traces
│ with pgplan      │ - Enriches with metadata
└──────┬───────────┘
       │ OTLP traces
       ▼
┌──────────────────┐
│     Jaeger       │ Trace visualization
│                  │ UI: http://localhost:16686
└──────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- Go 1.24+ (for building the collector image)

## Quick Start

1. **Start all services**:
```bash
cd examples
docker compose up -d --build
```

This will start:
- PostgreSQL (port 5432) with auto_explain enabled
- OTel Collector Agent (collects logs)
- Custom OTel Collector with pgplan connector
- Jaeger UI (port 16686)

2. **Wait for services to be ready**:
```bash
# Check service health
docker compose ps

# Watch logs
docker compose logs -f otel-collector-pgplan
```

3. **Run test queries**:
```bash
# Connect to PostgreSQL
docker compose exec postgres psql -U postgres -d testdb

# Or run queries from file
docker compose exec -T postgres psql -U postgres -d testdb < test-queries.sql
```

4. **View traces in Jaeger**:
   - Open http://localhost:16686
   - Select service: `postgresql`
   - Click "Find Traces"
   - You should see traces for each query execution plan!

## What's Happening

### 1. PostgreSQL Configuration

The PostgreSQL container is configured with these auto_explain settings:

```ini
shared_preload_libraries=auto_explain
auto_explain.log_min_duration=0          # Log all queries (for demo)
auto_explain.log_analyze=on              # Include actual execution stats
auto_explain.log_buffers=on              # Include buffer usage
auto_explain.log_timing=on               # Include timing info
auto_explain.log_format=json             # Output in JSON format
auto_explain.log_nested_statements=on    # Include nested statements
```

Any query executed will have its execution plan logged to stderr in JSON format.

### 2. Log Collection & Parsing

The OTel Collector Agent:
- Monitors Docker container logs via `filelog` receiver
- Parses Docker JSON log format
- Extracts auto_explain output using regex
- Finds the JSON array containing the EXPLAIN plan
- Sends the EXPLAIN JSON as OTLP log body

### 3. Trace Conversion

The custom collector with pgplan connector:
- Receives OTLP logs
- Extracts EXPLAIN JSON from log body
- Converts plan nodes to trace spans
- Adds timing, cost, and buffer statistics as span attributes
- Emits OTLP traces

### 4. Visualization

Jaeger receives the traces and displays:
- Query execution timeline
- Hierarchical plan node structure
- Timing for each operation
- Span attributes with detailed statistics

## Example Queries

The `test-queries.sql` file contains various query types:

1. **Simple index scan** - Single table lookup
2. **Join with aggregation** - Multi-table join with GROUP BY
3. **Complex analytics** - Multiple joins, aggregations, ORDER BY
4. **View query** - Query against a view
5. **Subquery** - Nested SELECT
6. **CTE** - Common Table Expression

Each generates a different execution plan that you can visualize as a trace.

## Viewing Trace Details

In Jaeger, click on a trace to see:

### Span Details
- **Span name**: Node type (e.g., "Nested Loop", "Index Scan")
- **Duration**: Actual execution time
- **Tags/Attributes**:
  - `db.postgresql.node_type`: Plan node type
  - `db.postgresql.relation`: Table name
  - `db.postgresql.actual_rows`: Rows returned
  - `db.postgresql.actual_loops`: Number of executions
  - `db.postgresql.startup_cost`: Estimated startup cost
  - `db.postgresql.total_cost`: Estimated total cost
  - `db.postgresql.shared_hit_blocks`: Buffer cache hits
  - `db.postgresql.shared_read_blocks`: Disk reads
  - And many more...

### Trace Hierarchy
The trace structure mirrors the query execution plan:
```
DB SELECT (root span)
├── DB PLAN (planning phase)
└── DB EXECUTION (execution phase)
    └── Hash Join
        ├── Seq Scan on users
        └── Hash
            └── Index Scan on orders
```

## Troubleshooting

### No traces appearing

1. **Check PostgreSQL logs**:
```bash
docker compose logs postgres | grep "plan:"
```
You should see auto_explain output.

2. **Check collector agent logs**:
```bash
docker compose logs otel-collector-agent | grep "EXPLAIN"
```

3. **Check pgplan connector logs**:
```bash
docker compose logs otel-collector-pgplan | grep "pgplan"
```
Look for errors in parsing or conversion.

4. **Verify Jaeger connection**:
```bash
curl http://localhost:16686/api/services
```

### Collector build fails

If the Docker build fails, you can build the collector locally first:
```bash
cd examples
go run go.opentelemetry.io/collector/cmd/builder@latest --config builder-config.yaml
docker compose up -d
```

### PostgreSQL logs not captured

The filelog receiver needs access to Docker container logs. Ensure:
- Docker is running
- Log files are in `/var/lib/docker/containers/`
- On macOS/Windows with Docker Desktop, you may need to adjust the path

Alternative: Use the `dockerstats` receiver or mount Docker socket.

## Stopping the Demo

```bash
# Stop all services
docker compose down

# Stop and remove volumes (clean slate)
docker compose down -v
```

## Customization

### Change auto_explain threshold

Edit `compose.yaml`:
```yaml
- -c
- auto_explain.log_min_duration=1000  # Only log queries > 1s
```

### Export to different backend

Edit `collector-config-docker.yaml` to change the exporter:
```yaml
exporters:
  otlp/tempo:
    endpoint: tempo:4317
    tls:
      insecure: true
```

### Add more collectors

You can add additional exporters in the custom collector config to send traces to multiple backends simultaneously.

## Production Considerations

For production use:

1. **Increase auto_explain threshold**: `log_min_duration=1000` (log only slow queries)
2. **Add sampling**: Don't convert every query plan to traces
3. **Use proper authentication**: Add TLS and auth for OTLP endpoints
4. **Resource limits**: Set memory/CPU limits in Docker Compose
5. **Log rotation**: Configure PostgreSQL log rotation
6. **Monitoring**: Add health checks and metrics collection

## Files

- `compose.yaml` - Docker Compose configuration
- `Dockerfile` - Custom collector image
- `otel-collector-agent-config.yaml` - Log collection config
- `collector-config-docker.yaml` - pgplan collector config
- `postgres-init.sql` - Database initialization
- `test-queries.sql` - Sample queries to generate traces

## Learn More

- [PostgreSQL auto_explain](https://www.postgresql.org/docs/current/auto-explain.html)
- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
- [Jaeger](https://www.jaegertracing.io/)
- [pgplan connector](../connector/pgplanconnector/README.md)
