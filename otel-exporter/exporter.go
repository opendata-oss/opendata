package opendataexporter

import (
	"context"
	"fmt"

	bindings "github.com/opendata-oss/opendata/ingest-bindings/go/opendata_ingest_bindings"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type opendataExporter struct {
	ingestor *bindings.Ingestor
	metadata []byte
}

func newExporter(cfg *Config, set exporter.Settings) (*opendataExporter, error) {
	backend, err := toStorageBackend(cfg.StorageBackend)
	if err != nil {
		return nil, fmt.Errorf("invalid storage backend: %w", err)
	}

	compression, err := toCompression(cfg.Compression)
	if err != nil {
		return nil, fmt.Errorf("invalid compression: %w", err)
	}

	ingestorCfg := bindings.IngestorConfig{
		StorageBackend:  backend,
		DataPathPrefix:  cfg.DataPathPrefix,
		ManifestPath:    cfg.ManifestPath,
		FlushIntervalMs: cfg.FlushIntervalMs,
		FlushSizeBytes:  cfg.FlushSizeBytes,
		MaxBufferedInputs: 1000,
		Compression:     compression,
	}

	ingestor, err := bindings.NewIngestor(ingestorCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create ingestor: %w", err)
	}

	return &opendataExporter{
		ingestor: ingestor,
		metadata: encodeMetadata(),
	}, nil
}

func (e *opendataExporter) pushMetrics(_ context.Context, md pmetric.Metrics) error {
	marshaler := &pmetric.ProtoMarshaler{}
	payload, err := marshaler.MarshalMetrics(md)
	if err != nil {
		return fmt.Errorf("failed to marshal metrics to OTLP protobuf: %w", err)
	}

	return e.ingestor.Ingest(payload, e.metadata)
}

func (e *opendataExporter) shutdown(_ context.Context) error {
	return e.ingestor.Close()
}

func toStorageBackend(cfg StorageBackendConfig) (bindings.StorageBackend, error) {
	switch cfg.Type {
	case "memory":
		return bindings.StorageBackendInMemory{}, nil
	case "local":
		if cfg.Local == nil {
			return nil, fmt.Errorf("local storage config required when type is 'local'")
		}
		return bindings.StorageBackendLocal{Path: cfg.Local.Path}, nil
	case "aws":
		if cfg.Aws == nil {
			return nil, fmt.Errorf("aws storage config required when type is 'aws'")
		}
		return bindings.StorageBackendAws{
			Bucket: cfg.Aws.Bucket,
			Region: cfg.Aws.Region,
		}, nil
	default:
		return nil, fmt.Errorf("unknown storage type: %s", cfg.Type)
	}
}

func toCompression(compression string) (bindings.Compression, error) {
	switch compression {
	case "none", "":
		return bindings.CompressionNone, nil
	case "zstd":
		return bindings.CompressionZstd, nil
	default:
		return 0, fmt.Errorf("unknown compression: %s", compression)
	}
}
