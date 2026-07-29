package oteltrace

import (
	"net/url"
	"path"
	"strings"
)

const (
	envSDKDisabled         = "OTEL_SDK_DISABLED"
	envEndpoint            = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envTracesEndpoint      = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	envMetricsEndpoint     = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
	envProtocol            = "OTEL_EXPORTER_OTLP_PROTOCOL"
	envTracesProtocol      = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
	envMetricsProtocol     = "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"
	protocolHTTPProtobuf   = "http/protobuf"
	defaultOTLPTracesPath  = "/v1/traces"
	defaultOTLPMetricsPath = "/v1/metrics"
)

type configState string

const (
	configDisabled  configState = "disabled"
	configReady     configState = "ready"
	configMalformed configState = "misconfigured"
)

type environmentConfig struct {
	Enabled         bool
	State           configState
	Protocol        string
	Endpoint        string // compatibility alias for TracesEndpoint
	TracesEnabled   bool
	MetricsEnabled  bool
	TracesEndpoint  string
	MetricsEndpoint string
}

// parseEnvironment keeps native export opt-in. The generic standard endpoint
// enables both traces and metrics; signal-specific endpoints independently
// enable only their signal and take precedence. Without any endpoint, no SDK,
// exporter, worker, or network client is created. Values are never returned in
// diagnostics.
func parseEnvironment(getenv func(string) string) environmentConfig {
	if getenv == nil || strings.EqualFold(strings.TrimSpace(getenv(envSDKDisabled)), "true") {
		return environmentConfig{State: configDisabled}
	}

	genericEndpoint := strings.TrimSpace(getenv(envEndpoint))
	traceEndpoint := strings.TrimSpace(getenv(envTracesEndpoint))
	metricEndpoint := strings.TrimSpace(getenv(envMetricsEndpoint))
	traceSpecific := traceEndpoint != ""
	metricSpecific := metricEndpoint != ""
	tracesEnabled := traceSpecific || genericEndpoint != ""
	metricsEnabled := metricSpecific || genericEndpoint != ""
	if !tracesEnabled && !metricsEnabled {
		return environmentConfig{State: configDisabled}
	}
	if traceEndpoint == "" {
		traceEndpoint = genericEndpoint
	}
	if metricEndpoint == "" {
		metricEndpoint = genericEndpoint
	}

	genericProtocol := strings.TrimSpace(getenv(envProtocol))
	traceProtocol := strings.TrimSpace(getenv(envTracesProtocol))
	metricProtocol := strings.TrimSpace(getenv(envMetricsProtocol))
	if traceProtocol == "" {
		traceProtocol = genericProtocol
	}
	if metricProtocol == "" {
		metricProtocol = genericProtocol
	}
	if traceProtocol == "" {
		traceProtocol = protocolHTTPProtobuf
	}
	if metricProtocol == "" {
		metricProtocol = protocolHTTPProtobuf
	}
	if (tracesEnabled && !strings.EqualFold(traceProtocol, protocolHTTPProtobuf)) ||
		(metricsEnabled && !strings.EqualFold(metricProtocol, protocolHTTPProtobuf)) {
		return environmentConfig{State: configMalformed, Protocol: protocolHTTPProtobuf}
	}

	var ok bool
	if tracesEnabled {
		traceEndpoint, ok = normalizeSignalEndpoint(traceEndpoint, traceSpecific, defaultOTLPTracesPath)
		if !ok {
			return environmentConfig{State: configMalformed, Protocol: protocolHTTPProtobuf}
		}
	}
	if metricsEnabled {
		metricEndpoint, ok = normalizeSignalEndpoint(metricEndpoint, metricSpecific, defaultOTLPMetricsPath)
		if !ok {
			return environmentConfig{State: configMalformed, Protocol: protocolHTTPProtobuf}
		}
	}
	return environmentConfig{
		Enabled: true, State: configReady, Protocol: protocolHTTPProtobuf,
		Endpoint: traceEndpoint, TracesEnabled: tracesEnabled, MetricsEnabled: metricsEnabled,
		TracesEndpoint: traceEndpoint, MetricsEndpoint: metricEndpoint,
	}
}

func normalizeSignalEndpoint(raw string, signalSpecific bool, defaultPath string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false
	}
	if !signalSpecific {
		parsed.Path = path.Join(parsed.Path, defaultPath)
		if !strings.HasPrefix(parsed.Path, "/") {
			parsed.Path = "/" + parsed.Path
		}
	} else if parsed.Path == "" {
		parsed.Path = "/"
	}
	parsed.RawPath = ""
	return parsed.String(), true
}
