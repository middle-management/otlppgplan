package pgplanconnector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
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
