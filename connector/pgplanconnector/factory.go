package pgplanconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const (
	typeStr   = "pgplan"
	stability = component.StabilityLevelAlpha
)

// NewFactory creates a factory for the pgplan connector
func NewFactory() connector.Factory {
	return connector.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		connector.WithLogsToTraces(createLogsToTracesConnector, stability),
	)
}

// createLogsToTracesConnector creates a new logs-to-traces connector instance
func createLogsToTracesConnector(
	_ context.Context,
	set connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (connector.Logs, error) {
	config := cfg.(*Config)

	return &logsToTracesConnector{
		config:       config,
		logger:       set.Logger,
		nextConsumer: nextConsumer,
	}, nil
}
