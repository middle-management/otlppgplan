package otlppgplan

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// snapshotDir is where snapshot files are stored
const snapshotDir = "testdata/__snapshots__"

// Test constants for deterministic output
const (
	fixedBaseTimeUnix  = 1700000000 // 2023-11-15 00:00:00 UTC
	fixedTraceIDHex    = "0102030405060708090a0b0c0d0e0f10"
	fixedRootSpanIDHex = "0102030405060708"
	fixedSpanIDCounter = 100 // Start counter at 100 to avoid collisions
)

var (
	fixedBaseTime   = time.Unix(fixedBaseTimeUnix, 0)
	fixedTraceID    = fixedTraceIDHex
	fixedRootSpanID = fixedRootSpanIDHex
)

// TestConvertExplainJSONFiles tests parsing of all JSON files in testdata/examples/
// and generates snapshot output for verification.
//
// The test uses deterministic options to ensure reproducible snapshots:
// - BaseTime: Fixed timestamp for all spans
// - TraceID: Fixed trace ID
// - RootSpanID: Fixed root span ID
// - SpanIDCounter: Sequential counter for child span IDs
//
// Snapshots are stored in testdata/__snapshots__/ and automatically
// compared on each test run. To update snapshots after intentional changes,
// run: SNAPSHOT_UPDATE=1 go test -v
func TestConvertExplainJSONFiles(t *testing.T) {
	examplesDir := "testdata/examples"

	// Create snapshot directory if it doesn't exist
	err := os.MkdirAll(snapshotDir, 0755)
	if err != nil {
		t.Fatalf("failed to create snapshot directory: %v", err)
	}

	// Find all JSON files in the examples directory
	files, err := os.ReadDir(examplesDir)
	if err != nil {
		t.Fatalf("failed to read examples directory: %v", err)
	}

	if len(files) == 0 {
		t.Skip("no test files found in testdata/examples/")
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) != ".json" {
			continue
		}

		filePath := filepath.Join(examplesDir, file.Name())
		t.Run(file.Name(), func(t *testing.T) {
			// Read the JSON file
			content, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("failed to read test file %s: %v", file.Name(), err)
			}

			// Use deterministic counter for child span IDs
			counter := fixedSpanIDCounter

			// Convert to traces with deterministic options
			ctx := context.Background()
			opts := ConvertOptions{
				DBName:          "testdb",
				Statement:       "SELECT * FROM test",
				Operation:       "SELECT",
				PeerAddress:     "localhost",
				PeerPort:        5432,
				IncludePlanJSON: false,
				BaseTime:        &fixedBaseTime,
				TraceID:         &fixedTraceID,
				RootSpanID:      &fixedRootSpanID,
				SpanIDCounter:   &counter,
			}

			traces, err := ConvertExplainJSONToTraces(ctx, content, opts)
			if err != nil {
				t.Fatalf("failed to convert JSON to traces for %s: %v", file.Name(), err)
			}

			// Verify basic trace structure
			if traces.ResourceSpans().Len() == 0 {
				t.Fatal("should have resource spans")
			}
			if traces.ResourceSpans().At(0).ScopeSpans().Len() == 0 {
				t.Fatal("should have scope spans")
			}
			if traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().Len() == 0 {
				t.Fatal("should have spans")
			}

			// Marshal to JSON for snapshot comparison
			m := &ptrace.JSONMarshaler{}
			jsonBytes, err := m.MarshalTraces(traces)
			if err != nil {
				t.Fatalf("failed to marshal traces to JSON: %v", err)
			}

			// Generate snapshot file name
			snapshotName := strings.TrimSuffix(file.Name(), ".json") + ".snap.json"
			snapshotPath := filepath.Join(snapshotDir, snapshotName)

			// Check if snapshot file exists
			if _, err := os.Stat(snapshotPath); err == nil {
				// Snapshot exists, compare with it
				expectedContent, err := os.ReadFile(snapshotPath)
				if err != nil {
					t.Fatalf("failed to read snapshot file %s: %v", snapshotName, err)
				}

				// Compare the JSON output
				if string(jsonBytes) != string(expectedContent) {
					// Update the snapshot if SNAPSHOT_UPDATE environment variable is set
					if os.Getenv("SNAPSHOT_UPDATE") == "1" {
						err := os.WriteFile(snapshotPath, jsonBytes, 0644)
						if err != nil {
							t.Fatalf("failed to update snapshot file %s: %v", snapshotName, err)
						}
						t.Logf("Updated snapshot file: %s", snapshotName)
					} else {
						// Show the difference
						t.Errorf("Snapshot mismatch for %s\n", file.Name())
						t.Errorf("Expected snapshot file: %s\n", snapshotPath)
						t.Errorf("To update snapshots, run: SNAPSHOT_UPDATE=1 go test -v\n")
					}
				}
			} else {
				// No snapshot exists, create one
				err := os.WriteFile(snapshotPath, jsonBytes, 0644)
				if err != nil {
					t.Fatalf("failed to create snapshot file %s: %v", snapshotName, err)
				}
				t.Logf("Created new snapshot file: %s", snapshotName)
			}
		})
	}
}
