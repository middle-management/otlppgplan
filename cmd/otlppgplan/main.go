package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
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

	traces, err := convertInput(ctx, planJSON)
	if err != nil {
		die(err)
	}

	// Apply resource attributes from OTEL_RESOURCE_ATTRIBUTES (overwrites existing keys).
	if resourceAttrs := parseResourceAttributes(os.Getenv("OTEL_RESOURCE_ATTRIBUTES")); len(resourceAttrs) > 0 {
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

func convertInput(ctx context.Context, raw []byte) (ptrace.Traces, error) {
	raw = bytes.TrimSpace(raw)

	opts := otlppgplan.ConvertOptions{
		ServiceName: defaultServiceName(),
		ExpandLoops: false,
	}

	var sess otlppgplan.SessionTraceContext
	if tp := os.Getenv("TRACEPARENT"); tp != "" && useTraceContext() {
		if tid, sid, ok := parseTraceparent(tp); ok {
			copy(sess.TraceID[:], tid)
			copy(sess.ParentSpanID[:], sid)
		}
	}

	// Direct JSON (EXPLAIN or array).
	if len(raw) > 0 && (raw[0] == '{' || raw[0] == '[') {
		tr, _, err := otlppgplan.ConvertWithSessionContext(ctx, raw, opts, &sess)
		return tr, err
	}

	// Otherwise attempt to extract multiple plan JSON blocks from auto_explain logs.
	blocks := extractJSONBlocks(raw)
	if len(blocks) == 0 {
		return ptrace.Traces{}, fmt.Errorf("no JSON plan found in input")
	}

	out := ptrace.NewTraces()
	for _, b := range blocks {
		tr, _, err := otlppgplan.ConvertWithSessionContext(ctx, b, opts, &sess)
		if err != nil {
			return ptrace.Traces{}, err
		}
		out = appendTraces(out, tr)
	}
	return out, nil
}

func die(err error) {
	_, _ = io.WriteString(os.Stderr, err.Error()+"\n")
	os.Exit(1)
}

func defaultServiceName() string {
	if s := os.Getenv("OTEL_SERVICE_NAME"); s != "" {
		return s
	}
	return "otlppgplan"
}

func useTraceContext() bool {
	if p := os.Getenv("OTEL_PROPAGATORS"); p != "" && !strings.Contains(p, "tracecontext") {
		return false
	}
	return true
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

func extractJSONBlocks(data []byte) [][]byte {
	var blocks [][]byte
	s := string(data)
	for {
		idx := strings.IndexByte(s, '{')
		if idx == -1 {
			break
		}
		end := findMatchingBrace(s, idx)
		if end == -1 {
			break
		}
		blocks = append(blocks, []byte(s[idx:end+1]))
		if end+1 >= len(s) {
			break
		}
		s = s[end+1:]
	}
	return blocks
}

func findMatchingBrace(s string, start int) int {
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func appendTraces(dst, src ptrace.Traces) ptrace.Traces {
	src.ResourceSpans().MoveAndAppendTo(dst.ResourceSpans())
	return dst
}
