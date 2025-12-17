package pgplanconnector

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

// Config defines the configuration for the pgplan connector
type Config struct {
	// Source configuration for extracting EXPLAIN JSON
	Source SourceConfig `mapstructure:"source"`

	// Conversion options passed to otlppgplan
	Conversion ConversionConfig `mapstructure:"conversion"`

	// Error handling behavior
	OnError string `mapstructure:"on_error"` // "drop" or "log"
}

// SourceConfig defines where to extract EXPLAIN JSON from
type SourceConfig struct {
	// Type of source: "body" or "attribute"
	Type string `mapstructure:"type"`

	// AttributeKey is the attribute key if Type is "attribute"
	AttributeKey string `mapstructure:"attribute_key"`
}

// ConversionConfig defines options for converting EXPLAIN JSON to traces
type ConversionConfig struct {
	// ServiceName for generated traces
	ServiceName string `mapstructure:"service_name"`

	// DBNameAttribute is the attribute key containing database name (optional)
	DBNameAttribute string `mapstructure:"db_name_attribute"`

	// IncludePlanJSON determines whether to include raw plan JSON in traces
	IncludePlanJSON bool `mapstructure:"include_plan_json"`

	// ExpandLoops determines whether to expand loop iterations into separate spans
	ExpandLoops bool `mapstructure:"expand_loops"`
}

// createDefaultConfig creates the default configuration
func createDefaultConfig() component.Config {
	return &Config{
		Source: SourceConfig{
			Type: "body",
		},
		Conversion: ConversionConfig{
			ServiceName:     "postgresql",
			IncludePlanJSON: false,
			ExpandLoops:     false,
		},
		OnError: "drop",
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Source.Type != "body" && c.Source.Type != "attribute" {
		return fmt.Errorf("source.type must be 'body' or 'attribute', got '%s'", c.Source.Type)
	}

	if c.Source.Type == "attribute" && c.Source.AttributeKey == "" {
		return fmt.Errorf("source.attribute_key is required when source.type is 'attribute'")
	}

	if c.OnError != "drop" && c.OnError != "log" {
		return fmt.Errorf("on_error must be 'drop' or 'log', got '%s'", c.OnError)
	}

	return nil
}
