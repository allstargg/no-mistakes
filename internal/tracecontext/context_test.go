package tracecontext

import (
	"strings"
	"testing"
)

const (
	testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	testTracestate  = "tracewake=prototype,vendor=opaque"
)

func TestParseAcceptsW3CTraceContext(t *testing.T) {
	result := Parse(testTraceparent, testTracestate)
	if len(result.Diagnostics) != 0 {
		t.Fatalf("Parse diagnostics = %v, want none", result.Diagnostics)
	}
	if result.Context == nil {
		t.Fatal("Parse context = nil, want valid context")
	}
	if result.Context.Traceparent != testTraceparent {
		t.Fatalf("Traceparent = %q, want %q", result.Context.Traceparent, testTraceparent)
	}
	if result.Context.Tracestate != testTracestate {
		t.Fatalf("Tracestate = %q, want %q", result.Context.Tracestate, testTracestate)
	}
}

func TestParseAcceptsW3CTracestateOptionalWhitespace(t *testing.T) {
	state := "tracewake = prototype \t, vendor=opaque"
	result := Parse(testTraceparent, state)
	if len(result.Diagnostics) != 0 || result.Context == nil || result.Context.Tracestate != state {
		t.Fatalf("Parse optional whitespace = %#v, want accepted state", result)
	}
}

func TestParseRejectsInvalidOrUnsupportedTraceparent(t *testing.T) {
	tests := []struct {
		name   string
		parent string
	}{
		{name: "malformed", parent: "not-a-traceparent"},
		{name: "uppercase", parent: "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"},
		{name: "zero trace id", parent: "00-00000000000000000000000000000000-00f067aa0ba902b7-01"},
		{name: "zero parent id", parent: "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"},
		{name: "forbidden version", parent: "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
		{name: "unsupported future version", parent: "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra"},
		{name: "oversized", parent: strings.Repeat("a", MaxTraceparentBytes+1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Parse(tt.parent, testTracestate)
			if result.Context != nil {
				t.Fatalf("Parse context = %#v, want nil", result.Context)
			}
			if len(result.Diagnostics) != 1 {
				t.Fatalf("Parse diagnostics = %v, want one bounded reason", result.Diagnostics)
			}
			if got := result.Diagnostics[0].String(); got == "" || len(got) > 96 || strings.Contains(got, tt.parent) {
				t.Fatalf("diagnostic is unsafe or unbounded: %q", got)
			}
		})
	}
}

func TestParseKeepsValidParentAndDropsInvalidTracestate(t *testing.T) {
	tests := []struct {
		name  string
		state string
	}{
		{name: "oversized", state: strings.Repeat("a", MaxTracestateBytes+1)},
		{name: "too many members", state: strings.Repeat("a=b,", MaxTracestateMembers) + "z=v"},
		{name: "duplicate key", state: "tracewake=one,tracewake=two"},
		{name: "invalid key", state: "Tracewake=value"},
		{name: "newline", state: "tracewake=value\nsecret=bad"},
		{name: "trailing space", state: "tracewake=value "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Parse(testTraceparent, tt.state)
			if result.Context == nil || result.Context.Traceparent != testTraceparent {
				t.Fatalf("valid parent was lost: %#v", result.Context)
			}
			if result.Context.Tracestate != "" {
				t.Fatalf("invalid tracestate persisted: %q", result.Context.Tracestate)
			}
			if len(result.Diagnostics) != 1 {
				t.Fatalf("Parse diagnostics = %v, want one bounded reason", result.Diagnostics)
			}
		})
	}
}

func TestFromEnvironmentUsesOnlyTraceContextAndRejectsBaggage(t *testing.T) {
	values := map[string]string{
		EnvTraceparent: testTraceparent,
		EnvTracestate:  testTracestate,
		EnvBaggage:     "authorization=secret",
		"UNRELATED":    "must-not-be-carried",
	}
	result := FromEnvironment(func(key string) string { return values[key] })
	if result.Context == nil || result.Context.Traceparent != testTraceparent || result.Context.Tracestate != testTracestate {
		t.Fatalf("FromEnvironment context = %#v, want approved trace context", result.Context)
	}
	if len(result.Diagnostics) != 1 || result.Diagnostics[0] != DiagnosticBaggageUnsupported {
		t.Fatalf("FromEnvironment diagnostics = %v, want baggage rejection", result.Diagnostics)
	}
}

func TestFromEnvironmentAbsentIsIndependent(t *testing.T) {
	result := FromEnvironment(func(string) string { return "" })
	if result.Context != nil || len(result.Diagnostics) != 0 {
		t.Fatalf("absent environment = %#v, want no parent and no diagnostic", result)
	}
}
