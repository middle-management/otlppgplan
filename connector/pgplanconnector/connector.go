package pgplanconnector

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/middle-management/otlppgplan"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

var _ connector.Logs = (*logsToTracesConnector)(nil)

// logsToTracesConnector converts PostgreSQL EXPLAIN logs to traces
type logsToTracesConnector struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Traces
	// traceContextCache stores trace ID and parent span ID by session identifier
	// Key format: "service.instance.id:pid" or just "pid" if no instance ID
	traceContextCache map[string]traceContext
	// functionContextCache stores parsed function call stacks by session (outer->inner)
	functionContextCache map[string][]string
}

type traceContext struct {
	traceID      string
	parentSpanID string
}

var (
	traceparentRegex = regexp.MustCompile(`/\*\s*traceparent\s*=\s*'([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})'\s*\*/`)
	pidPrefixRegex   = regexp.MustCompile(`\[(\d+)\]`)
	contextFuncRegex = regexp.MustCompile(`PL/pgSQL function ([^(]+)\(`)
)

// Capabilities declares that this connector does not mutate logs
func (c *logsToTracesConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Start is called when the connector starts
func (c *logsToTracesConnector) Start(_ context.Context, _ component.Host) error {
	c.logger.Info("Starting pgplan logs-to-traces connector")
	c.traceContextCache = make(map[string]traceContext)
	c.functionContextCache = make(map[string][]string)
	return nil
}

// Shutdown is called when the connector stops
func (c *logsToTracesConnector) Shutdown(_ context.Context) error {
	c.logger.Info("Shutting down pgplan logs-to-traces connector")
	return nil
}

// ConsumeLogs processes log records and emits traces
func (c *logsToTracesConnector) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	// Collect all traces to emit in a single batch
	allTraces := ptrace.NewTraces()

	resourceLogs := ld.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		resourceLog := resourceLogs.At(i)
		scopeLogs := resourceLog.ScopeLogs()

		for j := 0; j < scopeLogs.Len(); j++ {
			scopeLog := scopeLogs.At(j)
			logRecords := scopeLog.LogRecords()

			for k := 0; k < logRecords.Len(); k++ {
				logRecord := logRecords.At(k)

				// Seed session cache from logs that carry traceparent in STATEMENT/CONTEXT
				// lines even if they don't have EXPLAIN JSON.
				c.seedTraceContext(resourceLog.Resource(), logRecord)
				c.seedFunctionContext(resourceLog.Resource(), logRecord)

				// Extract EXPLAIN JSON from log record
				explainJSON, err := c.extractExplainJSON(logRecord)
				if err != nil {
					c.handleError(fmt.Errorf("failed to extract EXPLAIN JSON: %w", err))
					continue
				}

				if explainJSON == "" {
					continue // No EXPLAIN data in this log
				}

				// Convert EXPLAIN JSON to traces
				traces, err := c.convertToTraces(ctx, explainJSON, resourceLog, logRecord)
				if err != nil {
					c.handleError(fmt.Errorf("failed to convert EXPLAIN to traces: %w", err))
					continue
				}

				// Append traces to batch
				traces.ResourceSpans().MoveAndAppendTo(allTraces.ResourceSpans())
			}
		}
	}

	// Emit all collected traces
	if allTraces.SpanCount() > 0 {
		return c.nextConsumer.ConsumeTraces(ctx, allTraces)
	}

	return nil
}

// extractExplainJSON extracts the EXPLAIN JSON from a log record
func (c *logsToTracesConnector) extractExplainJSON(record plog.LogRecord) (string, error) {
	switch c.config.Source.Type {
	case "body":
		return c.extractFromBody(record)
	case "attribute":
		return c.extractFromAttribute(record)
	default:
		return "", fmt.Errorf("unknown source type: %s", c.config.Source.Type)
	}
}

func (c *logsToTracesConnector) extractFromBody(record plog.LogRecord) (string, error) {
	body := record.Body()

	switch body.Type() {
	case pcommon.ValueTypeStr:
		// Body is a string - assume it's the EXPLAIN JSON
		bodyStr := body.Str()
		c.logger.Debug("Extracting from string body", zap.Int("length", len(bodyStr)))
		return bodyStr, nil

	case pcommon.ValueTypeMap:
		// Body is structured - might need to navigate to find EXPLAIN data
		// For now, try to marshal the entire map as JSON
		bodyMap := body.Map()
		c.logger.Debug("Extracting from map body", zap.Int("keys", bodyMap.Len()))
		bodyBytes, err := json.Marshal(bodyMap.AsRaw())
		if err != nil {
			return "", fmt.Errorf("failed to marshal body map: %w", err)
		}
		return string(bodyBytes), nil

	case pcommon.ValueTypeSlice:
		// Body might be an array (EXPLAIN JSON is an array)
		bodySlice := body.Slice()
		c.logger.Debug("Extracting from slice body", zap.Int("length", bodySlice.Len()))
		bodyBytes, err := json.Marshal(bodySlice.AsRaw())
		if err != nil {
			return "", fmt.Errorf("failed to marshal body slice: %w", err)
		}
		return string(bodyBytes), nil

	default:
		return "", fmt.Errorf("unsupported body type: %v", body.Type())
	}
}

func (c *logsToTracesConnector) extractFromAttribute(record plog.LogRecord) (string, error) {
	attr, ok := record.Attributes().Get(c.config.Source.AttributeKey)
	if !ok {
		return "", nil // Attribute not present
	}

	if attr.Type() != pcommon.ValueTypeStr {
		return "", fmt.Errorf("attribute %s is not a string", c.config.Source.AttributeKey)
	}

	return attr.Str(), nil
}

// convertToTraces uses otlppgplan library to convert EXPLAIN JSON to traces
func (c *logsToTracesConnector) convertToTraces(
	ctx context.Context,
	explainJSON string,
	resourceLog plog.ResourceLogs,
	logRecord plog.LogRecord,
) (ptrace.Traces, error) {
	// Build conversion options
	opts := otlppgplan.ConvertOptions{
		ServiceName:     c.config.Conversion.ServiceName,
		IncludePlanJSON: c.config.Conversion.IncludePlanJSON,
		ExpandLoops:     c.config.Conversion.ExpandLoops,
	}

	// Extract database name from log attributes if configured
	if c.config.Conversion.DBNameAttribute != "" {
		if dbName, ok := logRecord.Attributes().Get(c.config.Conversion.DBNameAttribute); ok {
			opts.DBName = dbName.Str()
		}
	}

	// Build session key for trace context caching
	// Use service.instance.id + process.pid if available, otherwise just use a hash
	sessionKey := c.buildSessionKey(resourceLog.Resource(), logRecord)

	// Get or create session context
	sessCtx, ok := c.traceContextCache[sessionKey]
	if !ok {
		sessCtx = traceContext{}
	}

	// Convert with session context
	var sessionContext otlppgplan.SessionTraceContext
	// Copy existing trace context if available (stored as raw bytes)
	if len(sessCtx.traceID) >= 16 {
		copy(sessionContext.TraceID[:], sessCtx.traceID[:16])
	}
	if len(sessCtx.parentSpanID) >= 8 {
		copy(sessionContext.ParentSpanID[:], sessCtx.parentSpanID[:8])
	}

	traces, pid, err := otlppgplan.ConvertWithSessionContext(ctx, []byte(explainJSON), opts, &sessionContext)
	if err != nil {
		return ptrace.Traces{}, err
	}

	// Update cache with potentially new trace context
	if pid > 0 {
		// Use PID-based key if we extracted it
		sessionKey = fmt.Sprintf("pid:%d", pid)
	}
	c.traceContextCache[sessionKey] = traceContext{
		traceID:      string(sessionContext.TraceID[:]),
		parentSpanID: string(sessionContext.ParentSpanID[:]),
	}

	// Apply function context as synthetic spans (outer->inner)
	if funcs, ok := c.functionContextCache[sessionKey]; ok && len(funcs) > 0 {
		c.applyFunctionContext(traces, funcs)
	}

	// Optionally: propagate resource attributes from logs to traces
	c.propagateResourceAttributes(resourceLog.Resource(), traces)

	// Optionally: correlate with log's trace context if present
	c.correlateTraceContext(logRecord, traces)

	return traces, nil
}

// buildSessionKey creates a unique key for tracking session trace context
func (c *logsToTracesConnector) buildSessionKey(resource pcommon.Resource, logRecord plog.LogRecord) string {
	// Prefer PID from log prefix/attribute if available.
	if pid := extractPID(logRecord); pid > 0 {
		if instanceID, ok := resource.Attributes().Get("service.instance.id"); ok {
			return fmt.Sprintf("%s:pid:%d", instanceID.Str(), pid)
		}
		return fmt.Sprintf("pid:%d", pid)
	}

	// Try to use service.instance.id from resource
	if instanceID, ok := resource.Attributes().Get("service.instance.id"); ok {
		return instanceID.Str()
	}

	// Try to use process.pid from log attributes
	if pid, ok := logRecord.Attributes().Get("process.pid"); ok {
		return fmt.Sprintf("pid:%d", pid.Int())
	}

	// Fallback: use a combination of available attributes
	// This is a best-effort approach
	return "default"
}

// propagateResourceAttributes copies relevant attributes from log resource to trace resource
func (c *logsToTracesConnector) propagateResourceAttributes(logResource pcommon.Resource, traces ptrace.Traces) {
	// Define attributes to propagate (avoid conflicts)
	propagateKeys := []string{
		"service.name",
		"service.namespace",
		"service.instance.id",
		"deployment.environment",
		"host.name",
	}

	resourceSpans := traces.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		traceResource := resourceSpans.At(i).Resource()

		for _, key := range propagateKeys {
			if val, ok := logResource.Attributes().Get(key); ok {
				// Only set if not already present in trace resource
				if _, exists := traceResource.Attributes().Get(key); !exists {
					val.CopyTo(traceResource.Attributes().PutEmpty(key))
				}
			}
		}
	}
}

// correlateTraceContext links generated traces to log's trace context if present
func (c *logsToTracesConnector) correlateTraceContext(logRecord plog.LogRecord, traces ptrace.Traces) {
	// If the log has a trace_id and span_id, we could:
	// 1. Use the same trace_id for generated spans
	// 2. Make the root span a child of the log's span
	// 3. Add a span link

	// For now, we'll just add the correlation as an attribute
	if !logRecord.TraceID().IsEmpty() {
		resourceSpans := traces.ResourceSpans()
		for i := 0; i < resourceSpans.Len(); i++ {
			scopeSpans := resourceSpans.At(i).ScopeSpans()
			for j := 0; j < scopeSpans.Len(); j++ {
				spans := scopeSpans.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					span := spans.At(k)
					span.Attributes().PutStr("origin.log.trace_id", logRecord.TraceID().String())
					if !logRecord.SpanID().IsEmpty() {
						span.Attributes().PutStr("origin.log.span_id", logRecord.SpanID().String())
					}
				}
			}
		}
	}
}

// handleError logs errors based on configuration
func (c *logsToTracesConnector) handleError(err error) {
	if c.config.OnError == "log" {
		c.logger.Error("Error processing log record", zap.Error(err))
	}
	// If "drop", silently ignore
}

// seedTraceContext captures traceparent from non-EXPLAIN logs (STATEMENT/CONTEXT)
// so nested auto_explain plans for the same PID can reuse the trace.
func (c *logsToTracesConnector) seedTraceContext(resource pcommon.Resource, logRecord plog.LogRecord) {
	if logRecord.Body().Type() != pcommon.ValueTypeStr {
		return
	}

	body := logRecord.Body().Str()
	matches := traceparentRegex.FindStringSubmatch(body)
	if len(matches) < 4 {
		return
	}

	traceID := matches[2]
	spanID := matches[3]

	sessionKey := c.buildSessionKey(resource, logRecord)
	if sessionKey == "default" {
		if pid := extractPID(logRecord); pid > 0 {
			sessionKey = fmt.Sprintf("pid:%d", pid)
		}
	}

	// Store raw bytes so ConvertWithSessionContext can reuse them.
	c.traceContextCache[sessionKey] = traceContext{
		traceID:      traceID,
		parentSpanID: spanID,
	}
}

// extractPID tries to pull a PID from attributes or the log prefix.
func extractPID(logRecord plog.LogRecord) int {
	if pidAttr, ok := logRecord.Attributes().Get("process.pid"); ok {
		return int(pidAttr.Int())
	}

	if logRecord.Body().Type() == pcommon.ValueTypeStr {
		if m := pidPrefixRegex.FindStringSubmatch(logRecord.Body().Str()); len(m) >= 2 {
			if pid, err := strconv.Atoi(m[1]); err == nil {
				return pid
			}
		}
	}

	return 0
}

// seedFunctionContext parses CONTEXT logs to capture PL/pgSQL call stack.
func (c *logsToTracesConnector) seedFunctionContext(resource pcommon.Resource, logRecord plog.LogRecord) {
	if logRecord.Body().Type() != pcommon.ValueTypeStr {
		return
	}

	body := logRecord.Body().Str()
	if !strings.Contains(body, "CONTEXT:") {
		return
	}

	lines := strings.Split(body, "\n")
	var funcs []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "PL/pgSQL function") {
			continue
		}
		if m := contextFuncRegex.FindStringSubmatch(line); len(m) >= 2 {
			funcs = append(funcs, strings.TrimSpace(m[1]))
		}
	}
	if len(funcs) == 0 {
		return
	}

	// Reverse to outer -> inner order
	for i, j := 0, len(funcs)-1; i < j; i, j = i+1, j-1 {
		funcs[i], funcs[j] = funcs[j], funcs[i]
	}

	sessionKey := c.buildSessionKey(resource, logRecord)
	if sessionKey == "default" {
		if pid := extractPID(logRecord); pid > 0 {
			sessionKey = fmt.Sprintf("pid:%d", pid)
		}
	}

	c.functionContextCache[sessionKey] = funcs
}

// applyFunctionContext emits synthetic spans for PL/pgSQL functions listed in the context stack.
func (c *logsToTracesConnector) applyFunctionContext(traces ptrace.Traces, funcs []string) {
	rs := traces.ResourceSpans()
	for i := 0; i < rs.Len(); i++ {
		ss := rs.At(i).ScopeSpans()
		for j := 0; j < ss.Len(); j++ {
			spans := ss.At(j).Spans()
			if spans.Len() == 0 {
				continue
			}

			// First span is the query root span.
			root := spans.At(0)
			parentID := root.SpanID()
			traceID := root.TraceID()
			start := root.StartTimestamp()
			end := root.EndTimestamp()

			for _, fn := range funcs {
				s := spans.AppendEmpty()
				s.SetTraceID(traceID)
				s.SetSpanID(newSpanID())
				s.SetParentSpanID(parentID)
				s.SetName("FUNC " + fn)
				s.SetKind(ptrace.SpanKindInternal)
				s.SetStartTimestamp(start)
				s.SetEndTimestamp(end)

				a := s.Attributes()
				a.PutStr("db.system", "postgresql")
				a.PutStr("db.postgresql.function", fn)

				parentID = s.SpanID()
			}
		}
	}
}

// newSpanID provides a non-zero span ID for synthetic spans.
func newSpanID() pcommon.SpanID {
	var sid [8]byte
	_, _ = rand.Read(sid[:])
	if binary.LittleEndian.Uint64(sid[:]) == 0 {
		sid[0] = 1
	}
	return pcommon.SpanID(sid)
}
