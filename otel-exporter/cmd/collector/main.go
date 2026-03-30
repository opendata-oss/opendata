package main

import (
	"log"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
	opendataexporter "github.com/opendata-oss/opendata/otel-exporter"
)

func main() {
	info := component.BuildInfo{
		Command:     "opendata-collector",
		Description: "OpenData OTel Collector with Prometheus receiver",
		Version:     "0.1.0",
	}

	params := otelcol.CollectorSettings{
		BuildInfo: info,
		Factories: components,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{
					fileprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
	}

	cmd := otelcol.NewCommand(params)
	if err := cmd.Execute(); err != nil {
		log.Fatalf("collector failed: %v", err)
	}
}

func components() (otelcol.Factories, error) {
	promFactory := prometheusreceiver.NewFactory()
	odFactory := opendataexporter.NewFactory()

	return otelcol.Factories{
		Receivers: map[component.Type]receiver.Factory{
			promFactory.Type(): promFactory,
		},
		Exporters: map[component.Type]exporter.Factory{
			odFactory.Type(): odFactory,
		},
		Telemetry: otelconftelemetry.NewFactory(),
	}, nil
}
