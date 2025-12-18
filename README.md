# otlppgplan

Convert PostgreSQL query execution plans to OpenTelemetry distributed traces.

## Overview

`otlppgplan` is a Go library that transforms PostgreSQL `EXPLAIN` output (in JSON format) into OpenTelemetry Protocol (OTLP) traces. This allows you to visualize and analyze database query performance using standard observability tools like Jaeger, Zipkin, Grafana Tempo, or any OTLP-compatible backend.

Each plan node becomes a span in the trace, with timing information, cost estimates, buffer statistics, and execution details as span attributes.

## Features

- **Complete EXPLAIN Support**: Handles all PostgreSQL plan node types (Seq Scan, Index Scan, Joins, Aggregates, CTEs, etc.)
- **Auto_explain Format**: Natively parses PostgreSQL auto_explain logs with log prefix and "Query Text" field
- **Distributed Tracing**: Automatically extracts W3C traceparent from SQL comments to link database queries to parent traces
- **Session Correlation**: Tracks trace context by PostgreSQL PID to correlate function calls and nested queries
- **Accurate Timing**: Converts actual execution times to wall-clock trace timestamps
- **Query Text Extraction**: Automatically extracts and includes SQL query text as span attribute
- **Duration Parsing**: Extracts duration from PostgreSQL log prefix when available
- **Loop Handling**: Properly scales timing for nodes with `Actual Loops > 1`
- **CTE Deduplication**: Avoids double-counting CTE initialization costs
- **Parallel Queries**: Captures worker information and parallel execution details
- **Buffer Statistics**: Includes buffer hits, reads, and I/O timing
- **Hierarchical Structure**: Preserves the plan tree structure as nested spans
- **Child Boost**: Ensures parent span time >= sum of children for timeline consistency

## Installation

```bash
go get github.com/middle-management/otlppgplan
```

## Quick Start

### Live Demo with Docker

Try the complete pipeline with one command:

```bash
cd examples
./demo.sh
```

This starts PostgreSQL with auto_explain, collectors, and Jaeger. View traces at http://localhost:16686

See [examples/DOCKER.md](examples/DOCKER.md) for details.

## Supported Input Formats

The library automatically detects and handles multiple input formats:

### 1. PostgreSQL auto_explain logs (recommended)

PostgreSQL auto_explain logs with the full log prefix and "Query Text" field:

```
2025-12-18 08:20:34.162 UTC [3476] LOG:  duration: 2.166 ms  plan:
	{
	  "Query Text": "SELECT o.order_id, o.customer_id, c.name\n           FROM orders o\n           JOIN customers c ON o.customer_id = c.customer_id\n           WHERE o.order_date > '2024-01-01';",
	  "Plan": {
	    "Node Type": "Hash Join",
	    "Startup Cost": 22.50,
	    "Total Cost": 86.62,
	    ...
	  }
	}
```

**Benefits**: Query text and duration are automatically extracted from the log.

### 2. EXPLAIN (FORMAT JSON) output

Standard PostgreSQL EXPLAIN JSON output (array format):

```json
[{
  "Plan": {
    "Node Type": "Seq Scan",
    "Relation Name": "users",
    ...
  },
  "Planning Time": 0.833,
  "Execution Time": 5.5
}]
```

**Use case**: Direct EXPLAIN output from PostgreSQL queries.

The library automatically:
- Strips PostgreSQL log prefixes (timestamps, PIDs, LOG level)
- Extracts duration from log prefix when available
- Extracts query text from "Query Text" field
- Falls back gracefully between formats

### 3. Distributed Tracing with traceparent

Include W3C traceparent in SQL comments to link database execution plans to parent traces:

```sql
/*traceparent='00-bb4599c810d56f74fbbed0a03b0681a0-3c615a97b2013cde-01'*/
SELECT o.order_id, o.customer_id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date > '2024-01-01';
```

**Benefits**: 
- Generated trace spans automatically use the same trace ID
- Root query span becomes a child of the parent span
- Complete end-to-end tracing from application through database execution
- Works seamlessly with OpenTelemetry instrumentation
- Automatic session correlation for function calls (see [SESSION_CORRELATION.md](SESSION_CORRELATION.md))

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/middle-management/otlppgplan"
    "go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
    // Read EXPLAIN JSON from PostgreSQL
    explainJSON := []byte(`[{
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "users",
            "Startup Cost": 0.00,
            "Total Cost": 10.50,
            "Plan Rows": 100,
            "Actual Startup Time": 0.5,
            "Actual Total Time": 5.2,
            "Actual Rows": 95,
            "Actual Loops": 1
        },
        "Planning Time": 0.833,
        "Execution Time": 5.5
    }]`)

    // Convert to traces
    ctx := context.Background()
    traces, err := otlppgplan.ConvertExplainJSONToTraces(ctx, explainJSON, otlppgplan.ConvertOptions{
        ServiceName: "postgresql",
        DBName:      "mydb",
    })
    if err != nil {
        panic(err)
    }

    // Export traces (example: marshal to JSON)
    marshaler := &ptrace.JSONMarshaler{}
    tracesJSON, err := marshaler.MarshalTraces(traces)
    if err != nil {
        panic(err)
    }

    fmt.Println(string(tracesJSON))
}
```

### Getting EXPLAIN JSON from PostgreSQL

```sql
EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS, TIMING)
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2024-01-01';
```

The output will be JSON that can be passed directly to `ConvertExplainJSONToTraces`.

## Configuration Options

```go
type ConvertOptions struct {
    // ServiceName for the generated traces (default: "postgresql")
    ServiceName string
    
    // DBName is the database name
    DBName string
    
    // Statement is the SQL query text
    Statement string
    
    // Operation type (e.g., "SELECT", "INSERT")
    Operation string
    
    // ServerAddress is the database server address
    ServerAddress string
    
    // ServerPort is the database server port
    ServerPort int
    
    // IncludePlanJSON includes the raw plan JSON in span attributes
    IncludePlanJSON bool
    
    // ExpandLoops creates separate spans for each loop iteration (when Actual Loops > 1)
    ExpandLoops bool
    
    // IDGenerator for custom trace/span ID generation (default: random)
    IDGenerator IDGenerator
    
    // BaseTime is the base timestamp for the trace (default: now)
    BaseTime time.Time
}
```

## Span Attributes

All span attributes are prefixed with `db.postgresql.`:

### Timing Attributes
- `planning_time_ms` - Planning phase duration
- `execution_time_ms` - Execution phase duration
- `actual_startup_time_ms` - Node startup time
- `actual_total_time_ms` - Node total time (raw)
- `derived_total_time_ms` - Loop-scaled time
- `exclusive_time_ms` - Time excluding children

### Plan Attributes
- `node_type` - Plan node type (e.g., "Seq Scan", "Hash Join")
- `startup_cost` - Estimated startup cost
- `total_cost` - Estimated total cost
- `plan_rows` - Estimated row count
- `plan_width` - Estimated row width in bytes

### Execution Attributes
- `actual_rows` - Actual rows returned
- `actual_loops` - Number of times node executed
- `rows_removed_by_filter` - Rows filtered out
- `rows_removed_by_join_filter` - Rows removed by join condition

### Buffer Statistics
- `shared_hit_blocks` - Buffer cache hits
- `shared_read_blocks` - Blocks read from disk
- `local_*_blocks` - Local buffer usage
- `temp_*_blocks` - Temporary file usage

### I/O Timing
- `io_read_time_ms` - I/O read time
- `io_write_time_ms` - I/O write time

### Identity Attributes
- `relation` - Table name
- `schema` - Schema name
- `alias` - Table alias
- `index` - Index name
- `join_type` - Join type (Inner, Left, Right, Full)
- `parent_relationship` - Relationship to parent (Inner, Outer, SubPlan, InitPlan)

### Worker Attributes (Parallel Queries)
- `workers_count` - Number of parallel workers
- `worker.<n>.number` - Worker number
- `worker.<n>.startup_time` - Worker startup time
- `worker.<n>.total_time` - Worker total time
- `worker.<n>.rows` - Rows processed by worker
- `worker.<n>.loops` - Worker loop count

## Use Cases

### 1. Performance Analysis
Visualize query execution plans in trace viewers to identify bottlenecks:
- Which nodes take the most time?
- Are indexes being used effectively?
- Where are sequential scans happening?

### 2. Query Optimization
Compare traces before and after optimization:
- See the impact of adding indexes
- Verify query rewrites improve performance
- Track execution time improvements over time

### 3. Production Monitoring
Use with PostgreSQL `auto_explain` to automatically capture slow queries:
```ini
# postgresql.conf
shared_preload_libraries = 'auto_explain'
auto_explain.log_min_duration = 1000  # Log queries > 1s
auto_explain.log_analyze = on
auto_explain.log_buffers = on
auto_explain.log_format = json
```

### 4. CI/CD Testing
Include query plan traces in your test suite:
- Catch query plan regressions
- Ensure critical queries use proper indexes
- Verify query performance meets SLAs

## OpenTelemetry Collector Integration

This library includes an **OpenTelemetry Collector connector** for processing auto_explain logs in real-time.

### Connector Features
- Converts PostgreSQL auto_explain logs to traces automatically
- Supports log body or attribute extraction
- Propagates resource attributes from logs to traces
- Correlates with existing trace context
- Configurable error handling

### Building with the Connector

```bash
# See examples/README.md for detailed instructions
cd examples
go run go.opentelemetry.io/collector/cmd/builder@latest --config builder-config.yaml
./dist/otelcol-pgplan --config collector-config.yaml
```

See the [connector documentation](connector/pgplanconnector/README.md) for details.

## CLI Tool

A simple CLI tool is included for converting EXPLAIN JSON files:

```bash
# Build
go build ./cmd/otlppgplan

# Use
cat query_plan.json | ./otlppgplan > traces.json
```

## Examples

See the `testdata/examples/` directory for sample PostgreSQL plans and their corresponding traces:

- `ctelooped.json` - CTE with loops
- `parallellappend.json` - Parallel append
- `parallellsort.json` - Parallel sort
- `pgindex.json` - Index usage example
- `rewritetwoqueries.json` - Query rewrite example

## How It Works

### Conversion Process

1. **Parse JSON**: Unmarshal PostgreSQL EXPLAIN JSON into Go structs
2. **Create Root Span**: Overall query span with total duration
3. **Add Planning Span**: Separate span for planning phase
4. **Add Execution Span**: Separate span for execution phase
5. **Process Plan Tree**: Recursively create spans for each plan node
6. **Derive Timing**: Apply loop scaling, CTE deduplication, and child boost
7. **Set Timestamps**: Calculate wall-clock timestamps based on actual times

### Timing Algorithm

The library uses a three-pass algorithm to compute accurate span timings:

**Pass 1: Loop Scaling**
```
DerivedTime = ActualTime × ActualLoops
```

**Pass 2: CTE Deduplication**
- Identify CTE InitPlan and CTE Scan nodes
- Reduce CTE Scan times to avoid double-counting initialization

**Pass 3: Child Boost**
- Ensure parent time >= sum of children times
- Maintains timeline consistency

### Span Hierarchy Example

```
DB SELECT (root)
├── DB PLAN (planning phase)
└── DB EXECUTION (execution phase)
    └── Nested Loop [Left]
        ├── Hash Join [Left]
        │   ├── Seq Scan on table_a
        │   └── Hash
        │       └── Index Scan on table_b
        └── Materialize (130 loops)
            └── Seq Scan on table_c
```

## Testing

```bash
# Run tests
go test -v ./...

# Update snapshots
SNAPSHOT_UPDATE=1 go test -v ./...
```

The test suite uses snapshot-based testing to ensure consistent output across changes.

## Requirements

- Go 1.24 or later
- PostgreSQL 9.2+ (for EXPLAIN JSON format)
- OpenTelemetry Collector pdata v1.48.0+

## Contributing

Contributions are welcome! Please ensure:
- Tests pass
- Code follows Go conventions
- Documentation is updated
- Snapshots are regenerated if output changes

## License

[Your License Here]

## Related Projects

- [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector)
- [PostgreSQL](https://www.postgresql.org/)
- [pgMustard](https://www.pgmustard.com/) - PostgreSQL query plan visualizer
- [pev2](https://github.com/dalibo/pev2) - PostgreSQL Explain Visualizer

## Resources

- [PostgreSQL EXPLAIN Documentation](https://www.postgresql.org/docs/current/sql-explain.html)
- [PostgreSQL auto_explain](https://www.postgresql.org/docs/current/auto-explain.html)
- [OpenTelemetry Tracing Specification](https://opentelemetry.io/docs/specs/otel/trace/)
- [OTLP Specification](https://opentelemetry.io/docs/specs/otlp/)

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/middle-management/otlppgplan).
