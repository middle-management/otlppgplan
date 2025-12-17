package main

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/middle-management/otlppgplan"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	ctx := context.Background()
	planJSON, err := io.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}
	traces, err := otlppgplan.ConvertExplainJSONToTraces(ctx, planJSON, otlppgplan.ConvertOptions{
		ExpandLoops: false,
	})
	if err != nil {
		panic(err)
	}

	m := &ptrace.JSONMarshaler{}
	b, err := m.MarshalTraces(traces)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(os.Stdout, bytes.NewReader(b))
	if err != nil {
		panic(err)
	}
}
