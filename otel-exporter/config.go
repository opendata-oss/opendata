package opendataexporter

import (
	"go.opentelemetry.io/collector/config/configretry"
)

// Config defines the configuration for the OpenData metrics exporter.
type Config struct {
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	// StorageBackend configures where data is persisted.
	StorageBackend StorageBackendConfig `mapstructure:"storage"`

	// DataPathPrefix is the object storage prefix for data batch objects.
	DataPathPrefix string `mapstructure:"data_path_prefix"`

	// ManifestPath is the path to the queue manifest in object storage.
	ManifestPath string `mapstructure:"manifest_path"`

	// FlushIntervalMs is the time interval in milliseconds that triggers a batch flush.
	FlushIntervalMs uint64 `mapstructure:"flush_interval_ms"`

	// FlushSizeBytes is the batch size in bytes that triggers a flush.
	FlushSizeBytes uint64 `mapstructure:"flush_size_bytes"`

	// Compression configures the batch compression codec. Either "none" or "zstd".
	Compression string `mapstructure:"compression"`
}

// StorageBackendConfig selects and configures the object storage backend.
type StorageBackendConfig struct {
	// Type is the storage backend type: "memory", "local", or "aws".
	Type string `mapstructure:"type"`

	// Local is the configuration for local filesystem storage.
	Local *LocalStorageConfig `mapstructure:"local,omitempty"`

	// Aws is the configuration for AWS S3 storage.
	Aws *AwsStorageConfig `mapstructure:"aws,omitempty"`
}

// LocalStorageConfig configures the local filesystem storage backend.
type LocalStorageConfig struct {
	Path string `mapstructure:"path"`
}

// AwsStorageConfig configures the AWS S3 storage backend.
type AwsStorageConfig struct {
	Bucket string `mapstructure:"bucket"`
	Region string `mapstructure:"region"`
}
