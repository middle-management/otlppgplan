package pgplanconnector

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestLogsToTracesConnector(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)

	tracesSink := &consumertest.TracesSink{}

	connector, err := factory.CreateLogsToTraces(
		context.Background(),
		connectortest.NewNopSettings(component.MustNewType("pgplan")),
		cfg,
		tracesSink,
	)
	require.NoError(t, err)
	require.NotNil(t, connector)

	err = connector.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Create test log with EXPLAIN JSON
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()

	// Simple EXPLAIN JSON (minimal valid example)
	explainJSON := `[{"Plan":{"Node Type":"Seq Scan","Relation Name":"test"},"Execution Time":1.5}]`
	lr.Body().SetStr(explainJSON)

	err = connector.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	// Verify traces were generated
	assert.Eventually(t, func() bool {
		return tracesSink.SpanCount() > 0
	}, time.Second, 10*time.Millisecond)

	err = connector.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestLogsToTracesConnector_WithAttribute(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Source.Type = "attribute"
	cfg.Source.AttributeKey = "pg.explain"

	tracesSink := &consumertest.TracesSink{}

	connector, err := factory.CreateLogsToTraces(
		context.Background(),
		connectortest.NewNopSettings(component.MustNewType("pgplan")),
		cfg,
		tracesSink,
	)
	require.NoError(t, err)
	require.NotNil(t, connector)

	err = connector.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Create test log with EXPLAIN JSON in attribute
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("Some log message")

	// Add EXPLAIN JSON as attribute
	explainJSON := `[{"Plan":{"Node Type":"Seq Scan","Relation Name":"test"},"Execution Time":1.5}]`
	lr.Attributes().PutStr("pg.explain", explainJSON)

	err = connector.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	// Verify traces were generated
	assert.Eventually(t, func() bool {
		return tracesSink.SpanCount() > 0
	}, time.Second, 10*time.Millisecond)

	err = connector.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestLogsToTracesConnector_NoExplainData(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)

	tracesSink := &consumertest.TracesSink{}

	connector, err := factory.CreateLogsToTraces(
		context.Background(),
		connectortest.NewNopSettings(component.MustNewType("pgplan")),
		cfg,
		tracesSink,
	)
	require.NoError(t, err)

	err = connector.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Create test log WITHOUT EXPLAIN JSON
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("Regular log message without EXPLAIN data")

	err = connector.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	// Verify no traces were generated
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, tracesSink.SpanCount())

	err = connector.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestLogsToTracesConnector_InvalidJSON(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.OnError = "drop" // Should silently drop errors

	tracesSink := &consumertest.TracesSink{}

	connector, err := factory.CreateLogsToTraces(
		context.Background(),
		connectortest.NewNopSettings(component.MustNewType("pgplan")),
		cfg,
		tracesSink,
	)
	require.NoError(t, err)

	err = connector.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Create test log with invalid JSON
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr(`{invalid json}`)

	err = connector.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err) // Should not return error

	// Verify no traces were generated
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, tracesSink.SpanCount())

	err = connector.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid body source",
			config: &Config{
				Source:  SourceConfig{Type: "body"},
				OnError: "drop",
			},
			wantErr: false,
		},
		{
			name: "valid attribute source",
			config: &Config{
				Source: SourceConfig{
					Type:         "attribute",
					AttributeKey: "pg.explain",
				},
				OnError: "drop",
			},
			wantErr: false,
		},
		{
			name: "invalid source type",
			config: &Config{
				Source: SourceConfig{Type: "invalid"},
			},
			wantErr: true,
		},
		{
			name: "attribute source missing key",
			config: &Config{
				Source: SourceConfig{Type: "attribute"},
			},
			wantErr: true,
		},
		{
			name: "invalid on_error",
			config: &Config{
				Source:  SourceConfig{Type: "body"},
				OnError: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFactory(t *testing.T) {
	factory := NewFactory()

	t.Run("Type", func(t *testing.T) {
		assert.Equal(t, "pgplan", factory.Type().String())
	})

	t.Run("CreateDefaultConfig", func(t *testing.T) {
		cfg := factory.CreateDefaultConfig()
		require.NotNil(t, cfg)
		// Validate the config directly
		assert.NoError(t, cfg.(*Config).Validate())
	})

	t.Run("CreateLogsToTraces", func(t *testing.T) {
		cfg := factory.CreateDefaultConfig()
		tracesSink := &consumertest.TracesSink{}

		connector, err := factory.CreateLogsToTraces(
			context.Background(),
			connectortest.NewNopSettings(component.MustNewType("pgplan")),
			cfg,
			tracesSink,
		)
		require.NoError(t, err)
		require.NotNil(t, connector)
		assert.NotNil(t, connector.Capabilities())
	})
}

func TestLogsToTracesConnector_SessionCorrelation(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	tracesSink := &consumertest.TracesSink{}

	connector, err := factory.CreateLogsToTraces(
		context.Background(),
		connectortest.NewNopSettings(component.MustNewType("pgplan")),
		cfg,
		tracesSink,
	)
	require.NoError(t, err)

	err = connector.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer connector.Shutdown(context.Background())

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	// Log 1: WITH traceparent
	lr1 := sl.LogRecords().AppendEmpty()
	lr1.Attributes().PutInt("process.pid", 573)
	lr1.Body().SetStr(`2025-12-18 10:15:01.123 UTC [573] LOG:  duration: 5.234 ms  plan:
{
  "Query Text": "/*traceparent='00-aabbccdd11223344aabbccdd11223344-1122334455667788-01'*/ SELECT * FROM process_order(500);",
  "Plan": {"Node Type": "Result", "Startup Cost": 0.00, "Total Cost": 0.26, "Actual Startup Time": 0.015, "Actual Total Time": 5.123, "Actual Rows": 1, "Actual Loops": 1}
}`)

	// Log 2: WITHOUT traceparent (nested query)
	lr2 := sl.LogRecords().AppendEmpty()
	lr2.Attributes().PutInt("process.pid", 573)
	lr2.Body().SetStr(`2025-12-18 10:15:01.456 UTC [573] LOG:  duration: 2.100 ms  plan:
{
  "Query Text": "SELECT * FROM get_order_details(p_order_id)",
  "Plan": {"Node Type": "Function Scan", "Relation Name": "get_order_details", "Startup Cost": 0.25, "Total Cost": 10.25, "Actual Startup Time": 1.234, "Actual Total Time": 2.100, "Actual Rows": 1, "Actual Loops": 1}
}`)

	// Log 3: WITHOUT traceparent (another nested query)
	lr3 := sl.LogRecords().AppendEmpty()
	lr3.Attributes().PutInt("process.pid", 573)
	lr3.Body().SetStr(`2025-12-18 10:15:01.789 UTC [573] LOG:  duration: 1.500 ms  plan:
{
  "Query Text": "SELECT user_id FROM orders WHERE id = $1",
  "Plan": {"Node Type": "Index Scan", "Relation Name": "orders", "Index Name": "orders_pkey", "Startup Cost": 0.28, "Total Cost": 8.30, "Actual Startup Time": 0.011, "Actual Total Time": 1.500, "Actual Rows": 1, "Actual Loops": 1}
}`)

	err = connector.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return tracesSink.SpanCount() >= 9 // 3 logs * (query + exec + plan node)
	}, time.Second, 10*time.Millisecond)

	allTraces := tracesSink.AllTraces()
	require.NotEmpty(t, allTraces)

	expectedTraceID := "aabbccdd11223344aabbccdd11223344"
	expectedParentSpanID := "1122334455667788"

	uniqueTraceIDs := make(map[string]struct{})
	rootSpansWithParent := 0

	for _, tr := range allTraces {
		rs := tr.ResourceSpans()
		for i := 0; i < rs.Len(); i++ {
			ss := rs.At(i).ScopeSpans()
			for j := 0; j < ss.Len(); j++ {
				spans := ss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					span := spans.At(k)
					uniqueTraceIDs[span.TraceID().String()] = struct{}{}
					if span.ParentSpanID().String() == expectedParentSpanID {
						rootSpansWithParent++
					}
				}
			}
		}
	}

	require.Len(t, uniqueTraceIDs, 1, "all spans should share a single trace ID")
	for traceID := range uniqueTraceIDs {
		assert.Equal(t, expectedTraceID, traceID)
	}
	assert.Equal(t, 3, rootSpansWithParent, "expected one root span per log record to inherit the parent span ID")
}

func TestLogsToTracesConnector_ContextSeedsTraceparent(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	tracesSink := &consumertest.TracesSink{}

	connector, err := factory.CreateLogsToTraces(
		context.Background(),
		connectortest.NewNopSettings(component.MustNewType("pgplan")),
		cfg,
		tracesSink,
	)
	require.NoError(t, err)

	err = connector.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer connector.Shutdown(context.Background())

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	// STATEMENT log with traceparent but no plan JSON. Seeds the session cache.
	statement := sl.LogRecords().AppendEmpty()
	statement.Body().SetStr(`2025-12-18 09:21:59.384 UTC [573] STATEMENT: /*traceparent='00-aabbccdd11223344aabbccdd1122334455667788-01'*/
SELECT * FROM process_order(500);`)

	// CONTEXT log capturing function nesting.
	contextLog := sl.LogRecords().AppendEmpty()
	contextLog.Body().SetStr(`2025-12-18 09:23:16.327 UTC [573] CONTEXT:  SQL statement "SELECT o.id, u.name, o.total FROM orders o JOIN users u ON o.user_id = u.id WHERE o.id = p_order_id"
	PL/pgSQL function get_order_details(integer) line 3 at RETURN QUERY
	SQL statement "SELECT *                          FROM get_order_details(p_order_id)"
	PL/pgSQL function process_order(integer) line 8 at SQL statement`)

	// Following auto_explain log lacks traceparent but shares PID.
	plan := sl.LogRecords().AppendEmpty()
	plan.Body().SetStr(`2025-12-18 09:23:16.327 UTC [573] LOG:  duration: 0.609 ms  plan:
{
  "Query Text": "SELECT o.id, u.name, o.total FROM orders o JOIN users u ON o.user_id = u.id WHERE o.id = p_order_id",
  "Plan": {"Node Type": "Nested Loop", "Actual Startup Time": 0.605, "Actual Total Time": 0.606, "Actual Rows": 1, "Actual Loops": 1}
}`)

	err = connector.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return tracesSink.SpanCount() > 0
	}, time.Second, 10*time.Millisecond)

	allTraces := tracesSink.AllTraces()
	require.NotEmpty(t, allTraces)

	spans := allTraces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans()
	require.GreaterOrEqual(t, spans.Len(), 3)

	traceID := spans.At(0).TraceID().String()
	assert.Equal(t, "aabbccdd11223344aabbccdd11223344", traceID)

	foundFunc := map[string]bool{}
	for i := 0; i < spans.Len(); i++ {
		s := spans.At(i)
		if strings.HasPrefix(s.Name(), "FUNC ") {
			foundFunc[s.Name()] = true
		}
	}

	assert.True(t, foundFunc["FUNC process_order"], "expected outer function span")
	assert.True(t, foundFunc["FUNC get_order_details"], "expected nested function span")
}

func TestLogsToTracesConnector_NestedFunctionsSnapshot(t *testing.T) {
	content, err := os.ReadFile("testdata/nested_functions.txt")
	require.NoError(t, err)

	entries := splitNestedLogEntries(string(content))
	require.NotEmpty(t, entries)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for _, entry := range entries {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(entry)
	}

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	tracesSink := &consumertest.TracesSink{}

	conn, err := factory.CreateLogsToTraces(
		context.Background(),
		connectortest.NewNopSettings(component.MustNewType("pgplan")),
		cfg,
		tracesSink,
	)
	require.NoError(t, err)

	require.NoError(t, conn.Start(context.Background(), componenttest.NewNopHost()))
	defer conn.Shutdown(context.Background())

	require.NoError(t, conn.ConsumeLogs(context.Background(), logs))

	assert.Eventually(t, func() bool {
		return tracesSink.SpanCount() > 0
	}, time.Second, 10*time.Millisecond)

	allTraces := tracesSink.AllTraces()
	require.NotEmpty(t, allTraces)
	uniqueTraceIDs := map[string]struct{}{}
	for _, tr := range allTraces {
		rs := tr.ResourceSpans()
		for i := 0; i < rs.Len(); i++ {
			ss := rs.At(i).ScopeSpans()
			for j := 0; j < ss.Len(); j++ {
				spans := ss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					uniqueTraceIDs[spans.At(k).TraceID().String()] = struct{}{}
				}
			}
		}
	}
	assert.Len(t, uniqueTraceIDs, 1, "expected all plan logs to share a single trace")

	normalized := normalizeTracesForSnapshot(allTraces)

	jsonBytes, err := (&ptrace.JSONMarshaler{}).MarshalTraces(normalized)
	require.NoError(t, err)

	snapshotPath := filepath.Join("testdata", "__snapshots__", "nested_functions.connector.snap.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(snapshotPath), 0o755))

	compareWithSnapshot(t, snapshotPath, jsonBytes)
}

func splitNestedLogEntries(content string) []string {
	re := regexp.MustCompile(`(?m)^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \w+ \[\d+\] (LOG:|CONTEXT:|ERROR:|HINT:|STATEMENT:)`)
	matches := re.FindAllStringIndex(content, -1)
	if len(matches) == 0 {
		return nil
	}

	var entries []string
	for i := 0; i < len(matches); i++ {
		start := matches[i][0]
		end := len(content)
		if i+1 < len(matches) {
			end = matches[i+1][0]
		}
		entries = append(entries, strings.TrimSpace(content[start:end]))
	}
	return entries
}

func normalizeTracesForSnapshot(all []ptrace.Traces) ptrace.Traces {
	merged := ptrace.NewTraces()
	for _, tr := range all {
		tr.ResourceSpans().MoveAndAppendTo(merged.ResourceSpans())
	}
	if merged.SpanCount() == 0 {
		return merged
	}

	remapTraceAndSpanIDs(merged)
	normalizeTimestamps(merged)
	return merged
}

func remapTraceAndSpanIDs(tr ptrace.Traces) {
	traceMap := map[string]pcommon.TraceID{}
	spanMap := map[string]pcommon.SpanID{}
	var traceCounter uint64 = 1
	var spanCounter uint64 = 1

	nextTraceID := func(old string) pcommon.TraceID {
		if tid, ok := traceMap[old]; ok {
			return tid
		}
		var raw [16]byte
		binary.LittleEndian.PutUint64(raw[:8], traceCounter)
		binary.LittleEndian.PutUint64(raw[8:], traceCounter)
		traceCounter++
		tid := pcommon.TraceID(raw)
		traceMap[old] = tid
		return tid
	}

	nextSpanID := func(old string) pcommon.SpanID {
		if sid, ok := spanMap[old]; ok {
			return sid
		}
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], spanCounter)
		spanCounter++
		sid := pcommon.SpanID(raw)
		spanMap[old] = sid
		return sid
	}

	rs := tr.ResourceSpans()
	for i := 0; i < rs.Len(); i++ {
		ss := rs.At(i).ScopeSpans()
		for j := 0; j < ss.Len(); j++ {
			spans := ss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				span.SetTraceID(nextTraceID(span.TraceID().String()))
				if !span.ParentSpanID().IsEmpty() {
					span.SetParentSpanID(nextSpanID(span.ParentSpanID().String()))
				}
				span.SetSpanID(nextSpanID(span.SpanID().String()))
			}
		}
	}
}

func normalizeTimestamps(tr ptrace.Traces) {
	var minStart pcommon.Timestamp
	first := true

	rs := tr.ResourceSpans()
	for i := 0; i < rs.Len(); i++ {
		ss := rs.At(i).ScopeSpans()
		for j := 0; j < ss.Len(); j++ {
			spans := ss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				start := spans.At(k).StartTimestamp()
				if first || start < minStart {
					minStart = start
					first = false
				}
			}
		}
	}

	if first {
		return
	}

	for i := 0; i < rs.Len(); i++ {
		ss := rs.At(i).ScopeSpans()
		for j := 0; j < ss.Len(); j++ {
			spans := ss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				start := span.StartTimestamp() - minStart
				dur := span.EndTimestamp() - span.StartTimestamp()
				span.SetStartTimestamp(start)
				span.SetEndTimestamp(start + dur)
			}
		}
	}
}

func compareWithSnapshot(t *testing.T, snapshotPath string, actual []byte) {
	t.Helper()

	if expected, err := os.ReadFile(snapshotPath); err == nil {
		if string(expected) != string(actual) {
			if os.Getenv("SNAPSHOT_UPDATE") == "1" {
				require.NoError(t, os.WriteFile(snapshotPath, actual, 0o644))
				t.Logf("Updated snapshot file: %s", snapshotPath)
			} else {
				t.Fatalf("snapshot mismatch for %s; run SNAPSHOT_UPDATE=1 go test ./connector/pgplanconnector -run %s", snapshotPath, t.Name())
			}
		}
		return
	} else if !os.IsNotExist(err) {
		require.NoError(t, err)
	}

	require.NoError(t, os.WriteFile(snapshotPath, actual, 0o644))
	t.Logf("Created snapshot file: %s", snapshotPath)
}
