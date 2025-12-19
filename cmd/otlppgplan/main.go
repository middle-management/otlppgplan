package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"os"
	"strings"

	"github.com/middle-management/otlppgplan"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	ctx := context.Background()
	planJSON, err := io.ReadAll(os.Stdin)
	if err != nil {
		die(err)
	}

	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "otlppgplan"
	}
	resourceAttrs := parseResourceAttributes(os.Getenv("OTEL_RESOURCE_ATTRIBUTES"))

	opts := otlppgplan.ConvertOptions{
		ServiceName: serviceName,
		ExpandLoops: false,
	}

	// Honor OTEL_PROPAGATORS and TRACEPARENT env to seed the trace ID.
	var sessCtx otlppgplan.SessionTraceContext
	if propagators := os.Getenv("OTEL_PROPAGATORS"); propagators == "" || strings.Contains(propagators, "tracecontext") {
		if tp := os.Getenv("TRACEPARENT"); tp != "" {
			if tid, sid, ok := parseTraceparent(tp); ok {
				copy(sessCtx.TraceID[:], tid)
				copy(sessCtx.ParentSpanID[:], sid)
			}
		}
	}

	traces, _, err := otlppgplan.ConvertWithSessionContext(ctx, planJSON, opts, &sessCtx)
	if err != nil {
		die(err)
	}

	// Apply resource attributes from OTEL_RESOURCE_ATTRIBUTES (overwrites existing keys).
	if len(resourceAttrs) > 0 {
		rs := traces.ResourceSpans()
		for i := 0; i < rs.Len(); i++ {
			attrs := rs.At(i).Resource().Attributes()
			for k, v := range resourceAttrs {
				attrs.PutStr(k, v)
			}
		}
	}

	m := &ptrace.JSONMarshaler{}
	b, err := m.MarshalTraces(traces)
	if err != nil {
		die(err)
	}
	_, err = io.Copy(os.Stdout, bytes.NewReader(b))
	if err != nil {
		die(err)
	}
}

func die(err error) {
	_, _ = io.WriteString(os.Stderr, err.Error()+"\n")
	os.Exit(1)
}

func parseTraceparent(tp string) (traceID []byte, spanID []byte, ok bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 {
		return nil, nil, false
	}
	if tid, err := hex.DecodeString(parts[1]); err == nil && len(tid) == 16 {
		if sid, err := hex.DecodeString(parts[2]); err == nil && len(sid) == 8 {
			return tid, sid, true
		}
	}
	return nil, nil, false
}

func parseResourceAttributes(raw string) map[string]string {
	res := make(map[string]string)
	if raw == "" {
		return res
	}
	segments := strings.Split(raw, ",")
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		kv := strings.SplitN(seg, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if key == "" {
			continue
		}
		res[key] = val
	}
	return res
}
