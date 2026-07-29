package oteltrace

import (
	"net/url"
	"path"
	"strings"
)

const (
	envSDKDisabled        = "OTEL_SDK_DISABLED"
	envEndpoint           = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envTracesEndpoint     = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	envProtocol           = "OTEL_EXPORTER_OTLP_PROTOCOL"
	envTracesProtocol     = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
	protocolHTTPProtobuf  = "http/protobuf"
	defaultOTLPTracesPath = "/v1/traces"
)

type configState string

const (
	configDisabled  configState = "disabled"
	configReady     configState = "ready"
	configMalformed configState = "misconfigured"
)

type environmentConfig struct {
	Enabled  bool
	State    configState
	Endpoint string
	Protocol string
}

// parseEnvironment keeps native export opt-in. The standard OTLP endpoint
// variables enable it; without either endpoint, no SDK, exporter, worker, or
// network client is created. Values are never returned in diagnostics.
func parseEnvironment(getenv func(string) string) environmentConfig {
	if getenv == nil || strings.EqualFold(strings.TrimSpace(getenv(envSDKDisabled)), "true") {
		return environmentConfig{State: configDisabled}
	}

	rawEndpoint := strings.TrimSpace(getenv(envTracesEndpoint))
	signalSpecific := rawEndpoint != ""
	if rawEndpoint == "" {
		rawEndpoint = strings.TrimSpace(getenv(envEndpoint))
	}
	if rawEndpoint == "" {
		return environmentConfig{State: configDisabled}
	}

	protocol := strings.TrimSpace(getenv(envTracesProtocol))
	if protocol == "" {
		protocol = strings.TrimSpace(getenv(envProtocol))
	}
	if protocol == "" {
		protocol = protocolHTTPProtobuf
	}
	if !strings.EqualFold(protocol, protocolHTTPProtobuf) {
		return environmentConfig{State: configMalformed, Protocol: protocolHTTPProtobuf}
	}

	parsed, err := url.Parse(rawEndpoint)
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return environmentConfig{State: configMalformed, Protocol: protocolHTTPProtobuf}
	}
	if !signalSpecific {
		parsed.Path = path.Join(parsed.Path, defaultOTLPTracesPath)
		if !strings.HasPrefix(parsed.Path, "/") {
			parsed.Path = "/" + parsed.Path
		}
	} else if parsed.Path == "" {
		// Per-signal endpoint variables are used as-is; the OTel specification
		// defines an absent path as the root path.
		parsed.Path = "/"
	}
	parsed.RawPath = ""
	return environmentConfig{
		Enabled: true, State: configReady, Endpoint: parsed.String(), Protocol: protocolHTTPProtobuf,
	}
}
