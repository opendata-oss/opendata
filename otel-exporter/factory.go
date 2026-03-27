package opendataexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	typeStr = "opendata"
)

// NewFactory creates a factory for the OpenData exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		StorageBackend: StorageBackendConfig{
			Type: "memory",
		},
		DataPathPrefix:  "ingest/otel/metrics/data",
		ManifestPath:    "ingest/otel/metrics/manifest",
		FlushIntervalMs: 100,
		FlushSizeBytes:  64 * 1024 * 1024,
		Compression:     "zstd",
	}
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	oCfg := cfg.(*Config)
	exp, err := newExporter(oCfg, set)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exp.pushMetrics,
		exporterhelper.WithRetry(oCfg.BackOffConfig),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}
