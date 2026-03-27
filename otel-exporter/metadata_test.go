package opendataexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeMetadata(t *testing.T) {
	meta := encodeMetadata()
	assert.Equal(t, 4, len(meta))
	assert.Equal(t, byte(metadataVersion), meta[0])
	assert.Equal(t, byte(signalTypeMetrics), meta[1])
	assert.Equal(t, byte(payloadEncodingOTLPProto), meta[2])
	assert.Equal(t, byte(0), meta[3])
}
