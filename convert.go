package otlppgplan

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
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

// AutoExplainLog is the format output by PostgreSQL auto_explain
// Example:
//
//	{
//	  "Query Text": "SELECT ...",
//	  "Plan": { "Node Type": "Seq Scan", ... }
//	}
type AutoExplainLog struct {
	QueryText     string   `json:"Query Text"`
	Plan          PlanNode `json:"Plan"`
	PlanningTime  *float64 `json:"Planning Time,omitempty"`  // ms
	ExecutionTime *float64 `json:"Execution Time,omitempty"` // ms
}

type PlanNode struct {
	NodeType string `json:"Node Type"`

	// Common identity fields
	RelationName  *string `json:"Relation Name,omitempty"`
	Schema        *string `json:"Schema,omitempty"`
	Alias         *string `json:"Alias,omitempty"`
	IndexName     *string `json:"Index Name,omitempty"`
	JoinType      *string `json:"Join Type,omitempty"`
	SubplanName   *string `json:"Subplan Name,omitempty"`
	CTEName       *string `json:"CTE Name,omitempty"`
	ParallelAware *bool   `json:"Parallel Aware,omitempty"`

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

	// Parallel worker details (if present)
	Workers []PlanWorker `json:"Workers,omitempty"`

	// Derived fields (not from JSON)
	DerivedTotalTime float64 `json:"-"`
}

type PlanWorker struct {
	WorkerNumber      *int     `json:"Worker Number,omitempty"`
	ActualStartupTime *float64 `json:"Actual Startup Time,omitempty"` // ms
	ActualTotalTime   *float64 `json:"Actual Total Time,omitempty"`   // ms
	ActualRows        *float64 `json:"Actual Rows,omitempty"`
	ActualLoops       *float64 `json:"Actual Loops,omitempty"`
}

// -----------------------------
// Converter
// -----------------------------

type ConvertOptions struct {
	// Resource attributes
	ServiceName string // service.name
	DBName      string // db.name

	// Span attributes
	Statement       string // db.statement (optional; consider redaction)
	Operation       string // db.operation (SELECT/INSERT/...)
	PeerAddress     string // server.address (optional)
	PeerPort        int    // server.port (optional)
	IncludePlanJSON bool   // attach raw plan JSON string to root span (can be huge)
	ExpandLoops     bool   // emit synthetic child spans for loop iterations (uses derived totals)

	// Timestamp control for testing
	BaseTime *time.Time // optional base time for deterministic timestamps (defaults to time.Now() if nil)

	// ID control for deterministic testing
	IDGenerator
}

// Converter holds the state for converting PostgreSQL EXPLAIN JSON to traces
type Converter struct {
	opts         ConvertOptions
	traceID      pcommon.TraceID
	rootSpanID   pcommon.SpanID
	parentSpanID pcommon.SpanID // Parent span ID from traceparent (if present)
	baseTime     time.Time
	idGenerator  IDGenerator
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

// SessionTraceContext holds trace context information for correlating multiple queries in a session
type SessionTraceContext struct {
	TraceID      [16]byte // Trace ID to use for this session
	ParentSpanID [8]byte  // Parent span ID for root spans
}

// ConvertWithSessionContext converts EXPLAIN JSON using session-level trace context
// This allows multiple queries from the same session (e.g., function calls) to share the same trace
func ConvertWithSessionContext(ctx context.Context, explainJSON []byte, opts ConvertOptions, sessionCtx *SessionTraceContext) (ptrace.Traces, int, error) {
	converter := NewConverter(opts)

	// Parse to extract PID and check for traceparent
	cleanJSON, _, pid, err := parseAutoExplainLog(explainJSON)
	if err != nil {
		return ptrace.Traces{}, 0, fmt.Errorf("parse auto_explain log: %w", err)
	}

	// Try to parse as auto_explain format to check for traceparent
	var autoExplain AutoExplainLog
	hasTraceparent := false
	if err := json.Unmarshal(cleanJSON, &autoExplain); err == nil && autoExplain.QueryText != "" {
		if traceIDHex, spanIDHex := extractTraceparent(autoExplain.QueryText); traceIDHex != "" {
			// This query has a traceparent - update session context
			if traceIDBytes, err := hex.DecodeString(traceIDHex); err == nil && len(traceIDBytes) == 16 {
				copy(sessionCtx.TraceID[:], traceIDBytes)
				hasTraceparent = true
			}
			if spanIDBytes, err := hex.DecodeString(spanIDHex); err == nil && len(spanIDBytes) == 8 {
				copy(sessionCtx.ParentSpanID[:], spanIDBytes)
			}
		}
	}

	// Use session context if we don't have a traceparent in this specific query
	if !hasTraceparent && sessionCtx != nil {
		// Check if session context has a trace ID
		var zeroTraceID [16]byte
		if sessionCtx.TraceID != zeroTraceID {
			converter.traceID = pcommon.TraceID(sessionCtx.TraceID)
			converter.parentSpanID = pcommon.SpanID(sessionCtx.ParentSpanID)
		}
	}

	traces, err := converter.Convert(ctx, explainJSON)
	return traces, pid, err
}

// PostgreSQL log prefix pattern: "2025-12-18 08:20:34.162 UTC [3476] LOG:  duration: 2.166 ms  plan:"
var pgLogPrefixPattern = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \w+ \[(\d+)\] LOG:\s+)?duration:\s+([\d.]+)\s+ms\s+plan:\s*`)

// Traceparent pattern in SQL comments: /*traceparent='00-trace_id-span_id-flags'*/
var traceparentPattern = regexp.MustCompile(`/\*\s*traceparent\s*=\s*'([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})'\s*\*/`)

// parseAutoExplainLog parses a PostgreSQL auto_explain log entry
// Format: "2025-12-18 08:20:34.162 UTC [3476] LOG:  duration: 2.166 ms  plan:\n\t{...json...}"
// Returns: JSON bytes, duration in ms, PID (0 if not found), error
func parseAutoExplainLog(input []byte) (jsonData []byte, durationMS float64, pid int, err error) {
	// Find the log prefix and extract duration and PID
	match := pgLogPrefixPattern.FindSubmatch(input)
	if match != nil {
		// Extract PID from capture group 2 (if present)
		if len(match) > 2 && len(match[2]) > 0 {
			pidStr := string(match[2])
			pid, _ = strconv.Atoi(pidStr) // Ignore error, pid stays 0
		}

		// Extract duration from capture group 3
		durationStr := string(match[3])
		durationMS, err = strconv.ParseFloat(durationStr, 64)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("parse duration: %w", err)
		}

		// Remove the prefix, leaving just the JSON
		jsonData = pgLogPrefixPattern.ReplaceAll(input, []byte{})
	} else {
		// No prefix found, assume it's just JSON
		jsonData = input
		durationMS = 0
		pid = 0
	}

	// Clean up whitespace (tabs, newlines) from the JSON
	jsonData = []byte(strings.TrimSpace(string(jsonData)))

	return jsonData, durationMS, pid, nil
}

// extractTraceparent extracts W3C traceparent from SQL comment
// Format: /*traceparent='00-trace_id-span_id-flags'*/
// Returns: trace_id (32 hex chars), span_id (16 hex chars), or empty strings if not found
func extractTraceparent(queryText string) (traceID string, spanID string) {
	match := traceparentPattern.FindStringSubmatch(queryText)
	if len(match) >= 4 {
		// match[1] = version (00)
		// match[2] = trace_id (32 hex)
		// match[3] = span_id (16 hex)
		// match[4] = flags (02)
		return match[2], match[3]
	}
	return "", ""
}

// Convert performs the conversion using the converter's options
func (c *Converter) Convert(ctx context.Context, explainJSON []byte) (ptrace.Traces, error) {
	// Parse auto_explain log format (with optional PostgreSQL log prefix)
	cleanJSON, logDurationMS, _, err := parseAutoExplainLog(explainJSON)
	if err != nil {
		return ptrace.Traces{}, fmt.Errorf("parse auto_explain log: %w", err)
	}

	// Try to parse as auto_explain format first (single object with "Query Text")
	var autoExplain AutoExplainLog
	if err := json.Unmarshal(cleanJSON, &autoExplain); err == nil && autoExplain.QueryText != "" {
		// Successfully parsed as auto_explain format
		// Use the query text from the JSON
		if c.opts.Statement == "" {
			c.opts.Statement = autoExplain.QueryText
		}

		// Extract traceparent from SQL comment if present
		if traceIDHex, spanIDHex := extractTraceparent(autoExplain.QueryText); traceIDHex != "" {
			// Parse trace ID (32 hex chars = 16 bytes)
			if traceIDBytes, err := hex.DecodeString(traceIDHex); err == nil && len(traceIDBytes) == 16 {
				var tid [16]byte
				copy(tid[:], traceIDBytes)
				c.traceID = pcommon.TraceID(tid)
			}

			// Parse span ID (16 hex chars = 8 bytes) - this becomes the parent of our root span
			if spanIDBytes, err := hex.DecodeString(spanIDHex); err == nil && len(spanIDBytes) == 8 {
				var sid [8]byte
				copy(sid[:], spanIDBytes)
				c.parentSpanID = pcommon.SpanID(sid)
			}
		}

		// If we extracted duration from log prefix and there's no execution time in JSON, use it
		if logDurationMS > 0 && autoExplain.ExecutionTime == nil {
			autoExplain.ExecutionTime = &logDurationMS
		}

		// Convert to ExplainRoot format
		root := ExplainRoot{
			Plan:          autoExplain.Plan,
			PlanningTime:  autoExplain.PlanningTime,
			ExecutionTime: autoExplain.ExecutionTime,
		}

		return c.convertFromRoot(ctx, root, cleanJSON)
	}

	// Fall back to original array format: [ { "Plan": {...}, "Execution Time": ... } ]
	var doc ExplainDocument
	if err := json.Unmarshal(cleanJSON, &doc); err != nil {
		return ptrace.Traces{}, fmt.Errorf("parse explain json: %w", err)
	}
	if len(doc) == 0 {
		return ptrace.Traces{}, fmt.Errorf("empty explain document")
	}

	root := doc[0]

	// If we extracted duration from log prefix and there's no execution time in JSON, use it
	if logDurationMS > 0 && root.ExecutionTime == nil {
		root.ExecutionTime = &logDurationMS
	}

	return c.convertFromRoot(ctx, root, cleanJSON)
}

// convertFromRoot performs the conversion from an ExplainRoot structure
func (c *Converter) convertFromRoot(ctx context.Context, root ExplainRoot, planJSON []byte) (ptrace.Traces, error) {

	tr := ptrace.NewTraces()
	rs := tr.ResourceSpans().AppendEmpty()
	rattrs := rs.Resource().Attributes()
	if c.opts.ServiceName != "" {
		rattrs.PutStr("service.name", c.opts.ServiceName)
	}
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("otlppgplan")

	spans := ss.Spans()

	var traceID pcommon.TraceID
	if !c.traceID.IsEmpty() {
		traceID = c.traceID
	} else {
		traceID = c.idGenerator.NewTraceID()
	}

	var rootSpanID pcommon.SpanID
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

	// Preprocess plan for derived timings (loops, CTE de-dup, child boost).
	processPlanDerived(&root.Plan)

	// Root query span duration includes planning (if present) plus execution.
	planningMS := firstNonNil(root.PlanningTime, nil, 0)
	executionMS := firstNonNil(root.ExecutionTime, root.Plan.ActualTotalTime, 0)
	rootDurMS := planningMS + executionMS
	end := addMS(start, rootDurMS)

	q := spans.AppendEmpty()
	q.SetTraceID(traceID)
	q.SetSpanID(rootSpanID)
	// If we have a parent span ID from traceparent, set it
	if !c.parentSpanID.IsEmpty() {
		q.SetParentSpanID(c.parentSpanID)
	}
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
		attrs.PutStr("db.postgresql.plan_json", string(planJSON))
	}

	execStart := start
	if planningMS > 0 {
		execStart = addMS(start, planningMS)
		c.emitPlanningSpan(spans, traceID, rootSpanID, start, planningMS)
	}
	execSpanID := rootSpanID
	if executionMS > 0 {
		execSpanID = c.emitExecutionSpan(spans, traceID, rootSpanID, execStart, executionMS)
	}
	execEnd := addMS(execStart, executionMS)

	// Emit plan-node spans recursively.
	c.emitPlanNodeSpans(spans, traceID, execSpanID, execStart, execEnd, root.Plan)

	return tr, nil
}

func (c *Converter) emitPlanningSpan(spans ptrace.SpanSlice, traceID pcommon.TraceID, parentSpanID pcommon.SpanID, start pcommon.Timestamp, durMS float64) {
	s := spans.AppendEmpty()
	s.SetTraceID(traceID)
	s.SetSpanID(c.idGenerator.NewSpanID())
	s.SetParentSpanID(parentSpanID)
	s.SetName("DB PLAN")
	s.SetKind(ptrace.SpanKindInternal)
	s.SetStartTimestamp(start)
	s.SetEndTimestamp(addMS(start, durMS))

	a := s.Attributes()
	a.PutStr("db.system", "postgresql")
	a.PutDouble("db.postgresql.planning_time_ms", durMS)
}

func (c *Converter) emitExecutionSpan(spans ptrace.SpanSlice, traceID pcommon.TraceID, parentSpanID pcommon.SpanID, start pcommon.Timestamp, durMS float64) pcommon.SpanID {
	s := spans.AppendEmpty()
	spanID := c.idGenerator.NewSpanID()
	s.SetTraceID(traceID)
	s.SetSpanID(spanID)
	s.SetParentSpanID(parentSpanID)
	s.SetName("DB EXECUTION")
	s.SetKind(ptrace.SpanKindInternal)
	s.SetStartTimestamp(start)
	s.SetEndTimestamp(addMS(start, durMS))

	a := s.Attributes()
	a.PutStr("db.system", "postgresql")
	a.PutDouble("db.postgresql.execution_time_ms", durMS)

	return spanID
}

func (c *Converter) emitPlanNodeSpans(spans ptrace.SpanSlice, traceID pcommon.TraceID, parentSpanID pcommon.SpanID, execBase, execEnd pcommon.Timestamp, node PlanNode) {
	spanID := c.idGenerator.NewSpanID()

	// Wall-clock: use reported offsets from execution start (startup/total). Duration uses raw Actual Total Time.
	// InitPlans execute at the start of the execution phase, not offset by their startup time.
	isInitPlan := node.ParentRelationship != nil && *node.ParentRelationship == "InitPlan"
	nodeStart := execBase
	if !isInitPlan && node.ActualStartupTime != nil {
		nodeStart = addMS(execBase, *node.ActualStartupTime)
	}
	// Duration: raw actual total time (no loop scaling) to reflect wall-clock.
	nodeDur := firstNonNil(node.ActualTotalTime, nil, 0)
	selfDur := exclusiveFromActuals(node)
	if selfDur <= 0 {
		selfDur = nodeDur
	}
	nodeEnd := addMS(nodeStart, selfDur)
	if nodeEnd < nodeStart {
		nodeEnd = nodeStart
	}

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
	// Always add derived_total_time_ms for consistency, even if it equals actual_total_time_ms
	a.PutDouble("db.postgresql.derived_total_time_ms", node.DerivedTotalTime)
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
	excl := exclusiveFromActuals(node)
	if excl > 0 {
		a.PutDouble("db.postgresql.exclusive_time_ms", excl)
	}

	// Recurse
	for _, child := range node.Plans {
		c.emitPlanNodeSpans(spans, traceID, spanID, execBase, execEnd, child)
	}

	// Synthetic loop iteration spans (optional)
	if c.opts.ExpandLoops && node.DerivedTotalTime > 0 && node.ActualLoops != nil && *node.ActualLoops > 1 {
		c.emitLoopIterations(spans, traceID, spanID, nodeStart, node.DerivedTotalTime, int(*node.ActualLoops))
	}

	// Add worker information as attributes instead of creating child spans
	if len(node.Workers) > 0 {
		a.PutInt("db.postgresql.workers_count", int64(len(node.Workers)))
		for i, w := range node.Workers {
			prefix := fmt.Sprintf("db.postgresql.worker.%d", i)
			if w.WorkerNumber != nil {
				a.PutInt(prefix+".number", int64(*w.WorkerNumber))
			}
			if w.ActualStartupTime != nil {
				a.PutDouble(prefix+".actual_startup_time_ms", *w.ActualStartupTime)
			}
			if w.ActualTotalTime != nil {
				a.PutDouble(prefix+".actual_total_time_ms", *w.ActualTotalTime)
			}
			if w.ActualRows != nil {
				a.PutDouble(prefix+".actual_rows", *w.ActualRows)
			}
			if w.ActualLoops != nil {
				a.PutDouble(prefix+".actual_loops", *w.ActualLoops)
			}
		}
	}
}

func (c *Converter) emitLoopIterations(spans ptrace.SpanSlice, traceID pcommon.TraceID, parentSpanID pcommon.SpanID, start pcommon.Timestamp, totalMS float64, loops int) {
	if loops <= 1 || totalMS <= 0 {
		return
	}
	// Simple even split; best-effort visualization only.
	iterDur := totalMS / float64(loops)
	iterStart := start
	for i := 0; i < loops; i++ {
		iterEnd := addMS(iterStart, iterDur)
		s := spans.AppendEmpty()
		s.SetTraceID(traceID)
		s.SetSpanID(c.idGenerator.NewSpanID())
		s.SetParentSpanID(parentSpanID)
		s.SetName(fmt.Sprintf("loop %d/%d", i+1, loops))
		s.SetKind(ptrace.SpanKindInternal)
		s.SetStartTimestamp(iterStart)
		s.SetEndTimestamp(iterEnd)

		a := s.Attributes()
		a.PutStr("db.system", "postgresql")
		a.PutInt("db.postgresql.loop_index", int64(i+1))
		a.PutInt("db.postgresql.loop_count", int64(loops))
		a.PutDouble("db.postgresql.loop_duration_ms", iterDur)

		iterStart = iterEnd
	}
}

// processPlanDerived computes derived totals with loop scaling, CTE de-duplication, and child boosting.
func processPlanDerived(root *PlanNode) {
	// Pass 1: compute base derived totals with loop scaling.
	computeDerivedTotals(root)

	// Pass 2: collect CTE init/scan nodes.
	cteInits := make(map[string]*PlanNode)
	cteScans := make(map[string][]*PlanNode)
	collectCTENodes(root, cteInits, cteScans)
	adjustCTETotals(cteInits, cteScans)

	// Pass 3: child boost to keep parents at least sum of children.
	applyChildBoost(root)
}

func computeDerivedTotals(n *PlanNode) {
	for i := range n.Plans {
		computeDerivedTotals(&n.Plans[i])
	}

	n.DerivedTotalTime = firstNonNil(n.ActualTotalTime, nil, 0)
	if n.ActualLoops != nil && *n.ActualLoops > 1 {
		n.DerivedTotalTime = n.DerivedTotalTime * *n.ActualLoops
	}
}

func collectCTENodes(n *PlanNode, inits map[string]*PlanNode, scans map[string][]*PlanNode) {
	// InitPlan with Subplan Name "CTE <name>"
	if n.ParentRelationship != nil && *n.ParentRelationship == "InitPlan" && n.SubplanName != nil && strings.HasPrefix(*n.SubplanName, "CTE ") {
		name := strings.TrimPrefix(*n.SubplanName, "CTE ")
		inits[name] = n
	}

	// CTE Scan node
	if n.NodeType == "CTE Scan" && n.CTEName != nil {
		name := *n.CTEName
		scans[name] = append(scans[name], n)
	}

	for i := range n.Plans {
		collectCTENodes(&n.Plans[i], inits, scans)
	}
}

func adjustCTETotals(inits map[string]*PlanNode, scans map[string][]*PlanNode) {
	for name, scanNodes := range scans {
		initNode := inits[name]
		if initNode == nil {
			continue
		}
		initTotal := initNode.DerivedTotalTime
		if initTotal == 0 {
			continue
		}

		var scanSum float64
		for _, s := range scanNodes {
			scanSum += s.DerivedTotalTime
		}
		if scanSum == 0 {
			continue
		}

		for _, s := range scanNodes {
			orig := s.DerivedTotalTime
			newVal := orig * (1 - initTotal/scanSum)
			if newVal < 0 {
				newVal = 0
			}
			s.DerivedTotalTime = newVal
		}
	}
}

func applyChildBoost(n *PlanNode) {
	for i := range n.Plans {
		applyChildBoost(&n.Plans[i])
	}

	var childSum float64
	for i := range n.Plans {
		childSum += n.Plans[i].DerivedTotalTime
	}
	if n.DerivedTotalTime == 0 && childSum > 0 {
		n.DerivedTotalTime = childSum
	}
}

func exclusiveFromActuals(node PlanNode) float64 {
	if node.ActualTotalTime == nil || len(node.Plans) == 0 {
		return 0
	}
	var childSum float64
	for _, c := range node.Plans {
		if c.ActualTotalTime != nil {
			childSum += *c.ActualTotalTime
		}
	}
	excl := *node.ActualTotalTime - childSum
	if excl < 0 {
		return 0
	}
	return excl
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
