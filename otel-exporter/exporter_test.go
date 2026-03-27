package opendataexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func testConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.StorageBackend = StorageBackendConfig{Type: "memory"}
	cfg.FlushIntervalMs = 50
	cfg.Compression = "none"
	return cfg
}

func testMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test.metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(42.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	return md
}

func TestNewExporter(t *testing.T) {
	cfg := testConfig()
	set := exportertest.NewNopSettings(exportertest.NopType)
	exp, err := newExporter(cfg, set)
	require.NoError(t, err)
	require.NotNil(t, exp)
	assert.NotNil(t, exp.ingestor)
	assert.Equal(t, 4, len(exp.metadata))

	err = exp.shutdown(context.Background())
	assert.NoError(t, err)
}

func TestPushMetrics(t *testing.T) {
	cfg := testConfig()
	set := exportertest.NewNopSettings(exportertest.NopType)
	exp, err := newExporter(cfg, set)
	require.NoError(t, err)

	md := testMetrics()
	err = exp.pushMetrics(context.Background(), md)
	assert.NoError(t, err)

	err = exp.shutdown(context.Background())
	assert.NoError(t, err)
}

func TestToStorageBackendMemory(t *testing.T) {
	backend, err := toStorageBackend(StorageBackendConfig{Type: "memory"})
	require.NoError(t, err)
	assert.NotNil(t, backend)
}

func TestToStorageBackendLocal(t *testing.T) {
	backend, err := toStorageBackend(StorageBackendConfig{
		Type:  "local",
		Local: &LocalStorageConfig{Path: "/tmp/test"},
	})
	require.NoError(t, err)
	assert.NotNil(t, backend)
}

func TestToStorageBackendAws(t *testing.T) {
	backend, err := toStorageBackend(StorageBackendConfig{
		Type: "aws",
		Aws:  &AwsStorageConfig{Bucket: "test-bucket", Region: "us-west-2"},
	})
	require.NoError(t, err)
	assert.NotNil(t, backend)
}

func TestToStorageBackendUnknown(t *testing.T) {
	_, err := toStorageBackend(StorageBackendConfig{Type: "unknown"})
	assert.Error(t, err)
}

func TestToCompressionValues(t *testing.T) {
	tests := []struct {
		input string
	}{
		{"none"},
		{""},
		{"zstd"},
	}
	for _, tt := range tests {
		c, err := toCompression(tt.input)
		require.NoError(t, err)
		_ = c
	}
}

func TestToCompressionUnknown(t *testing.T) {
	_, err := toCompression("gzip")
	assert.Error(t, err)
}
