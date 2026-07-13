//go:build js && wasm

// Command wasm exposes the otlppgplan converter to JavaScript for the
// browser playground. It registers a global function:
//
//	otlppgplanConvert(input, optionsJSON) -> { otlp: string } | { error: string }
//
// input is anything the library accepts: auto_explain notice/log text
// ("duration: X ms plan: {…}"), bare auto_explain JSON, or
// EXPLAIN (FORMAT JSON) output. The result is OTLP/JSON
// ({"resourceSpans": […]}), i.e. exactly what the library would send to a
// collector's /v1/traces endpoint.
package main

import (
	"context"
	"encoding/json"
	"strings"
	"syscall/js"
	"time"

	"github.com/middle-management/otlppgplan"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type options struct {
	ServiceName string  `json:"serviceName"`
	DBName      string  `json:"dbName"`
	Layout      string  `json:"layout"`     // "flame" | "waterfall"
	BaseTimeMS  float64 `json:"baseTimeMs"` // epoch ms for the trace start
}

func convert(_ js.Value, args []js.Value) any {
	fail := func(msg string) any { return map[string]any{"error": msg} }
	if len(args) < 1 {
		return fail("missing input argument")
	}
	input := args[0].String()

	var o options
	if len(args) > 1 && args[1].Type() == js.TypeString && args[1].String() != "" {
		if err := json.Unmarshal([]byte(args[1].String()), &o); err != nil {
			return fail("parse options: " + err.Error())
		}
	}

	opts := otlppgplan.ConvertOptions{
		ServiceName: o.ServiceName,
		DBName:      o.DBName,
		Layout:      otlppgplan.SpanLayout(o.Layout),
	}
	if o.BaseTimeMS > 0 {
		t := time.UnixMilli(int64(o.BaseTimeMS)).UTC()
		opts.BaseTime = &t
	}

	traces, err := otlppgplan.ConvertExplainJSONToTraces(context.Background(), []byte(input), opts)
	if err != nil {
		return fail(err.Error())
	}
	nameRootSpanByOperation(traces)

	marshaler := &ptrace.JSONMarshaler{}
	out, err := marshaler.MarshalTraces(traces)
	if err != nil {
		return fail("marshal OTLP: " + err.Error())
	}
	return map[string]any{"otlp": string(out)}
}

// nameRootSpanByOperation renames the generic "DB QUERY" root span to
// "DB <OPERATION>" when the statement's leading keyword is recognizable,
// mirroring what callers of the library typically do via ConvertOptions.
func nameRootSpanByOperation(traces ptrace.Traces) {
	known := map[string]bool{
		"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
		"MERGE": true, "WITH": true, "CALL": true, "EXECUTE": true,
		"DECLARE": true, "CREATE": true, "REFRESH": true,
	}
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		ss := traces.ResourceSpans().At(i).ScopeSpans()
		for j := 0; j < ss.Len(); j++ {
			spans := ss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if span.Name() != "DB QUERY" {
					continue
				}
				stmt, ok := span.Attributes().Get("db.statement")
				if !ok {
					continue
				}
				fields := strings.Fields(stmt.Str())
				if len(fields) == 0 {
					continue
				}
				op := strings.ToUpper(fields[0])
				if known[op] {
					span.SetName("DB " + op)
					span.Attributes().PutStr("db.operation", op)
				}
			}
		}
	}
}

func main() {
	js.Global().Set("otlppgplanConvert", js.FuncOf(convert))
	js.Global().Set("otlppgplanReady", js.ValueOf(true))
	select {}
}
