package otlppgplan

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// -----------------------------
// Types: Postgres EXPLAIN JSON
// -----------------------------

// ExplainDocument is the top-level JSON output from EXPLAIN (FORMAT JSON).
// Postgres returns: [ { "Plan": {...}, "Execution Time": 12.34, ... } ]
type ExplainDocument []ExplainRoot

type ExplainRoot struct {
	Plan          PlanNode `json:"Plan"`
	PlanningTime  *float64 `json:"Planning Time,omitempty"`  // ms
	ExecutionTime *float64 `json:"Execution Time,omitempty"` // ms
	// There are many more optional keys (Triggers, JIT, Settings, etc.)
}

type PlanNode struct {
	NodeType string `json:"Node Type"`

	// Common identity fields
	RelationName *string `json:"Relation Name,omitempty"`
	Schema       *string `json:"Schema,omitempty"`
	Alias        *string `json:"Alias,omitempty"`
	IndexName    *string `json:"Index Name,omitempty"`
	JoinType     *string `json:"Join Type,omitempty"`

	ParentRelationship *string `json:"Parent Relationship,omitempty"`

	// Cost estimates
	StartupCost *float64 `json:"Startup Cost,omitempty"`
	TotalCost   *float64 `json:"Total Cost,omitempty"`
	PlanRows    *float64 `json:"Plan Rows,omitempty"`
	PlanWidth   *float64 `json:"Plan Width,omitempty"`

	// Actual execution stats (ANALYZE)
	ActualStartupTime *float64 `json:"Actual Startup Time,omitempty"` // ms
	ActualTotalTime   *float64 `json:"Actual Total Time,omitempty"`   // ms
	ActualRows        *float64 `json:"Actual Rows,omitempty"`
	ActualLoops       *float64 `json:"Actual Loops,omitempty"`

	RowsRemovedByFilter       *float64 `json:"Rows Removed by Filter,omitempty"`
	RowsRemovedByJoinFilter   *float64 `json:"Rows Removed by Join Filter,omitempty"`
	RowsRemovedByIndexRecheck *float64 `json:"Rows Removed by Index Recheck,omitempty"`

	// Buffers (if BUFFERS)
	SharedHitBlocks     *float64 `json:"Shared Hit Blocks,omitempty"`
	SharedReadBlocks    *float64 `json:"Shared Read Blocks,omitempty"`
	SharedDirtiedBlocks *float64 `json:"Shared Dirtied Blocks,omitempty"`
	SharedWrittenBlocks *float64 `json:"Shared Written Blocks,omitempty"`

	LocalHitBlocks     *float64 `json:"Local Hit Blocks,omitempty"`
	LocalReadBlocks    *float64 `json:"Local Read Blocks,omitempty"`
	LocalDirtiedBlocks *float64 `json:"Local Dirtied Blocks,omitempty"`
	LocalWrittenBlocks *float64 `json:"Local Written Blocks,omitempty"`

	TempReadBlocks    *float64 `json:"Temp Read Blocks,omitempty"`
	TempWrittenBlocks *float64 `json:"Temp Written Blocks,omitempty"`

	// I/O timing (if track_io_timing + BUFFERS)
	IOTimingRead  *float64 `json:"I/O Read Time,omitempty"`  // ms
	IOTimingWrite *float64 `json:"I/O Write Time,omitempty"` // ms

	// Sort fields (if present)
	SortMethod    *string  `json:"Sort Method,omitempty"`
	SortSpaceUsed *float64 `json:"Sort Space Used,omitempty"` // kB
	SortSpaceType *string  `json:"Sort Space Type,omitempty"`

	// Nested nodes
	Plans []PlanNode `json:"Plans,omitempty"`
}

// -----------------------------
// Converter
// -----------------------------

type ConvertOptions struct {
	// Resource attributes
	DBName string // db.name

	// Span attributes
	Statement       string // db.statement (optional; consider redaction)
	Operation       string // db.operation (SELECT/INSERT/...)
	PeerAddress     string // server.address (optional)
	PeerPort        int    // server.port (optional)
	IncludePlanJSON bool   // attach raw plan JSON string to root span (can be huge)

	// Timestamp control for testing
	BaseTime *time.Time // optional base time for deterministic timestamps (defaults to time.Now() if nil)

	// ID control for deterministic testing
	IDGenerator
}

// Converter holds the state for converting PostgreSQL EXPLAIN JSON to traces
type Converter struct {
	opts        ConvertOptions
	traceID     pcommon.TraceID
	rootSpanID  pcommon.SpanID
	baseTime    time.Time
	idGenerator IDGenerator
}

// NewConverter creates a new Converter instance
func NewConverter(opts ConvertOptions) *Converter {
	c := &Converter{
		opts:        opts,
		idGenerator: &RandomIDGenerator{},
	}

	// Initialize deterministic values if provided
	if opts.BaseTime != nil {
		c.baseTime = *opts.BaseTime
	}

	if opts.IDGenerator != nil {
		c.idGenerator = opts.IDGenerator
	}

	return c
}

// ConvertExplainJSONToTraces converts PostgreSQL EXPLAIN JSON to OpenTelemetry traces
func ConvertExplainJSONToTraces(ctx context.Context, explainJSON []byte, opts ConvertOptions) (ptrace.Traces, error) {
	converter := NewConverter(opts)
	return converter.Convert(ctx, explainJSON)
}

// Convert performs the conversion using the converter's options
func (c *Converter) Convert(ctx context.Context, explainJSON []byte) (ptrace.Traces, error) {
	var doc ExplainDocument
	if err := json.Unmarshal(explainJSON, &doc); err != nil {
		return ptrace.Traces{}, fmt.Errorf("parse explain json: %w", err)
	}
	if len(doc) == 0 {
		return ptrace.Traces{}, fmt.Errorf("empty explain document")
	}

	root := doc[0]

	tr := ptrace.NewTraces()
	rs := tr.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("otlppgplan")

	spans := ss.Spans()

	// Use pre-initialized IDs from converter
	var traceID pcommon.TraceID
	var rootSpanID pcommon.SpanID

	if !c.traceID.IsEmpty() {
		traceID = c.traceID
	} else {
		traceID = c.idGenerator.NewTraceID()
	}

	if !c.rootSpanID.IsEmpty() {
		rootSpanID = c.rootSpanID
	} else {
		rootSpanID = c.idGenerator.NewSpanID()
	}

	// Use pre-initialized base time from converter
	var start pcommon.Timestamp
	if !c.baseTime.IsZero() {
		start = pcommon.NewTimestampFromTime(c.baseTime)
	} else {
		start = pcommon.NewTimestampFromTime(time.Now().UTC())
	}

	// Root query span duration: prefer Execution Time; else fall back to plan node total time
	rootDurMS := firstNonNil(root.ExecutionTime, root.Plan.ActualTotalTime, 0)
	end := addMS(start, rootDurMS)

	q := spans.AppendEmpty()
	q.SetTraceID(traceID)
	q.SetSpanID(rootSpanID)
	q.SetName(querySpanName(c.opts.Operation))
	q.SetKind(ptrace.SpanKindClient)
	q.SetStartTimestamp(start)
	q.SetEndTimestamp(end)

	attrs := q.Attributes()
	attrs.PutStr("db.system", "postgresql")
	if c.opts.DBName != "" {
		attrs.PutStr("db.name", c.opts.DBName)
	}
	if c.opts.Statement != "" {
		attrs.PutStr("db.statement", c.opts.Statement)
	}
	if c.opts.Operation != "" {
		attrs.PutStr("db.operation", c.opts.Operation)
	}
	if c.opts.PeerAddress != "" {
		attrs.PutStr("server.address", c.opts.PeerAddress)
	}
	if c.opts.PeerPort != 0 {
		attrs.PutInt("server.port", int64(c.opts.PeerPort))
	}
	if root.PlanningTime != nil {
		attrs.PutDouble("db.postgresql.planning_time_ms", *root.PlanningTime)
	}
	if root.ExecutionTime != nil {
		attrs.PutDouble("db.postgresql.execution_time_ms", *root.ExecutionTime)
	}
	if c.opts.IncludePlanJSON {
		// Warning: can be extremely large; consider only attaching for slow queries.
		attrs.PutStr("db.postgresql.plan_json", string(explainJSON))
	}

	// Emit plan-node spans recursively.
	c.emitPlanNodeSpans(spans, traceID, rootSpanID, start, root.Plan)

	return tr, nil
}

func (c *Converter) emitPlanNodeSpans(spans ptrace.SpanSlice, traceID pcommon.TraceID, parentSpanID pcommon.SpanID, parentStart pcommon.Timestamp, node PlanNode) {
	spanID := c.idGenerator.NewSpanID()

	// Duration: node's "Actual Total Time" if present, else 0.
	durMS := firstNonNil(node.ActualTotalTime, nil, 0)
	nodeStart := parentStart
	nodeEnd := addMS(nodeStart, durMS)

	s := spans.AppendEmpty()
	s.SetTraceID(traceID)
	s.SetSpanID(spanID)
	s.SetParentSpanID(parentSpanID)
	s.SetName(formatNodeSpanName(node))
	s.SetKind(ptrace.SpanKindInternal)
	s.SetStartTimestamp(nodeStart)
	s.SetEndTimestamp(nodeEnd)

	a := s.Attributes()
	a.PutStr("db.system", "postgresql")
	a.PutStr("db.postgresql.node_type", node.NodeType)

	putStrPtr(a, "db.postgresql.relation", node.RelationName)
	putStrPtr(a, "db.postgresql.schema", node.Schema)
	putStrPtr(a, "db.postgresql.alias", node.Alias)
	putStrPtr(a, "db.postgresql.index", node.IndexName)
	putStrPtr(a, "db.postgresql.join_type", node.JoinType)
	putStrPtr(a, "db.postgresql.parent_relationship", node.ParentRelationship)

	putF64Ptr(a, "db.postgresql.startup_cost", node.StartupCost)
	putF64Ptr(a, "db.postgresql.total_cost", node.TotalCost)
	putF64Ptr(a, "db.postgresql.plan_rows", node.PlanRows)
	putF64Ptr(a, "db.postgresql.plan_width", node.PlanWidth)

	putF64Ptr(a, "db.postgresql.actual_startup_time_ms", node.ActualStartupTime)
	putF64Ptr(a, "db.postgresql.actual_total_time_ms", node.ActualTotalTime)
	putF64Ptr(a, "db.postgresql.actual_rows", node.ActualRows)
	putF64Ptr(a, "db.postgresql.actual_loops", node.ActualLoops)

	putF64Ptr(a, "db.postgresql.rows_removed_by_filter", node.RowsRemovedByFilter)
	putF64Ptr(a, "db.postgresql.rows_removed_by_join_filter", node.RowsRemovedByJoinFilter)
	putF64Ptr(a, "db.postgresql.rows_removed_by_index_recheck", node.RowsRemovedByIndexRecheck)

	// Buffers
	putF64Ptr(a, "db.postgresql.shared_hit_blocks", node.SharedHitBlocks)
	putF64Ptr(a, "db.postgresql.shared_read_blocks", node.SharedReadBlocks)
	putF64Ptr(a, "db.postgresql.shared_dirtied_blocks", node.SharedDirtiedBlocks)
	putF64Ptr(a, "db.postgresql.shared_written_blocks", node.SharedWrittenBlocks)

	putF64Ptr(a, "db.postgresql.local_hit_blocks", node.LocalHitBlocks)
	putF64Ptr(a, "db.postgresql.local_read_blocks", node.LocalReadBlocks)
	putF64Ptr(a, "db.postgresql.local_dirtied_blocks", node.LocalDirtiedBlocks)
	putF64Ptr(a, "db.postgresql.local_written_blocks", node.LocalWrittenBlocks)

	putF64Ptr(a, "db.postgresql.temp_read_blocks", node.TempReadBlocks)
	putF64Ptr(a, "db.postgresql.temp_written_blocks", node.TempWrittenBlocks)

	// I/O timing
	putF64Ptr(a, "db.postgresql.io_read_time_ms", node.IOTimingRead)
	putF64Ptr(a, "db.postgresql.io_write_time_ms", node.IOTimingWrite)

	// Sort
	putStrPtr(a, "db.postgresql.sort_method", node.SortMethod)
	putF64Ptr(a, "db.postgresql.sort_space_used_kb", node.SortSpaceUsed)
	putStrPtr(a, "db.postgresql.sort_space_type", node.SortSpaceType)

	// Exclusive time (best-effort): total - sum(child totals)
	if node.ActualTotalTime != nil && len(node.Plans) > 0 {
		var childSum float64
		for _, c := range node.Plans {
			if c.ActualTotalTime != nil {
				childSum += *c.ActualTotalTime
			}
		}
		excl := *node.ActualTotalTime - childSum
		if excl < 0 {
			excl = 0
		}
		a.PutDouble("db.postgresql.exclusive_time_ms", excl)
	}

	// Recurse
	for _, child := range node.Plans {
		c.emitPlanNodeSpans(spans, traceID, spanID, nodeStart, child)
	}
}

// -----------------------------
// Helpers
// -----------------------------

func querySpanName(op string) string {
	if op == "" {
		return "DB QUERY"
	}
	return "DB " + op
}

func formatNodeSpanName(n PlanNode) string {
	// Examples:
	// - "Seq Scan orders"
	// - "Index Scan orders (orders_pkey)"
	// - "Hash Join"
	base := n.NodeType
	if n.RelationName != nil && *n.RelationName != "" {
		base = fmt.Sprintf("%s %s", base, *n.RelationName)
	}
	if n.IndexName != nil && *n.IndexName != "" {
		base = fmt.Sprintf("%s (%s)", base, *n.IndexName)
	}
	if n.JoinType != nil && *n.JoinType != "" && isJoinNode(n.NodeType) {
		base = fmt.Sprintf("%s [%s]", base, *n.JoinType)
	}
	return base
}

func isJoinNode(nodeType string) bool {
	switch nodeType {
	case "Nested Loop", "Hash Join", "Merge Join":
		return true
	default:
		return false
	}
}

func addMS(ts pcommon.Timestamp, ms float64) pcommon.Timestamp {
	// Convert ms to ns
	ns := int64(ms * 1e6)
	return pcommon.Timestamp(uint64(int64(ts) + ns))
}

func firstNonNil(primary *float64, secondary *float64, fallback float64) float64 {
	if primary != nil {
		return *primary
	}
	if secondary != nil {
		return *secondary
	}
	return fallback
}

func putStrPtr(m pcommon.Map, key string, v *string) {
	if v != nil && *v != "" {
		m.PutStr(key, *v)
	}
}

func putF64Ptr(m pcommon.Map, key string, v *float64) {
	if v != nil {
		m.PutDouble(key, *v)
	}
}

type IDGenerator interface {
	NewTraceID() pcommon.TraceID
	NewSpanID() pcommon.SpanID
}

type RandomIDGenerator struct {
	tid [16]byte
	sid [8]byte
}

func (g *RandomIDGenerator) NewTraceID() pcommon.TraceID {
	_, _ = rand.Read(g.tid[:])
	return pcommon.TraceID(g.tid)
}

func (g *RandomIDGenerator) NewSpanID() pcommon.SpanID {
	_, _ = rand.Read(g.sid[:])
	// make it non-zero-ish
	if binary.LittleEndian.Uint64(g.sid[:]) == 0 {
		g.sid[0] = 1
	}
	return pcommon.SpanID(g.sid)
}
