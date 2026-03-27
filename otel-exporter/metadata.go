package opendataexporter

// Metadata version and field values for the ingest metadata contract.
const (
	metadataVersion = 1

	signalTypeMetrics = 1
	signalTypeLogs    = 2
	signalTypeTraces  = 3

	payloadEncodingOTLPProto = 1
)

// encodeMetadata returns the 4-byte metadata payload for a metrics ingest call.
func encodeMetadata() []byte {
	return []byte{metadataVersion, signalTypeMetrics, payloadEncodingOTLPProto, 0}
}
