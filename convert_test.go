package otlppgplan

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// snapshotDir is where snapshot files are stored
const snapshotDir = "testdata/__snapshots__"

// Test constants for deterministic output
var (
	fixedBaseTime = time.Unix(1700000000, 0) // 2023-11-15 00:00:00 UTC
)

// TestConvertExplainJSONFiles tests parsing of all JSON files in testdata/examples/
// and generates snapshot output for verification.
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
				IDGenerator:     &DeterministicIDGenerator{},
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

type DeterministicIDGenerator struct {
	i int
}

func (d *DeterministicIDGenerator) NewTraceID() pcommon.TraceID {
	d.i++
	var tid [16]byte
	binary.LittleEndian.PutUint64(tid[:8], uint64(d.i))
	binary.LittleEndian.PutUint64(tid[8:], uint64(d.i))
	return pcommon.TraceID(tid)
}

// deterministicSpanID generates a span ID from a counter for testing
func (d *DeterministicIDGenerator) NewSpanID() pcommon.SpanID {
	var sid [8]byte
	binary.LittleEndian.PutUint64(sid[:], uint64(d.i))
	return pcommon.SpanID(sid)
}
