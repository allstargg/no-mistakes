package cli

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestParseSkipPushOptions(t *testing.T) {
	got, err := parseSkipPushOptions([]string{
		"ci.skip",
		"no-mistakes.skip=test,lint",
	})
	if err != nil {
		t.Fatalf("parseSkipPushOptions() error = %v", err)
	}
	want := []types.StepName{types.StepTest, types.StepLint}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseSkipPushOptions() = %v, want %v", got, want)
	}
}

func TestParseSkipPushOptionsRejectsUnknownStep(t *testing.T) {
	_, err := parseSkipPushOptions([]string{"no-mistakes.skip=test,deploy"})
	if err == nil {
		t.Fatal("expected unknown step to fail")
	}
}

func TestNormalizeNotifyGatePathResolvesLegacyDotGate(t *testing.T) {
	bare := filepath.Join(t.TempDir(), "repo123.git")
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatal(err)
	}
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(bare); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(oldDir); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	}()
	t.Setenv("PWD", ".")

	got, err := normalizeNotifyGatePath(".")
	if err != nil {
		t.Fatalf("normalizeNotifyGatePath: %v", err)
	}
	if got == "." || !filepath.IsAbs(got) {
		t.Fatalf("normalizeNotifyGatePath(.) = %q, want absolute path", got)
	}
	want, err := filepath.EvalSymlinks(bare)
	if err != nil {
		want = bare
	}
	gotResolved, err := filepath.EvalSymlinks(got)
	if err != nil {
		gotResolved = got
	}
	if gotResolved != want {
		t.Fatalf("normalizeNotifyGatePath(.) = %q (resolved %q), want %q", got, gotResolved, want)
	}
}

func TestFormatSkipPushOptions(t *testing.T) {
	got := formatSkipPushOptions([]types.StepName{types.StepTest, types.StepLint})
	want := []string{"no-mistakes.skip=test,lint"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("formatSkipPushOptions() = %v, want %v", got, want)
	}
}

func TestIntentPushOptionRoundTrip(t *testing.T) {
	// Multi-line, comma- and colon-bearing intent must survive the
	// line-oriented push-option transport intact.
	intent := "add retry to the uploader\n\nwhy: flaky network, commas, colons: ok"
	opt := formatIntentPushOption(intent)
	if opt == "" {
		t.Fatal("formatIntentPushOption returned empty for a non-empty intent")
	}
	got, err := parseIntentPushOptions([]string{"no-mistakes.skip=test", opt})
	if err != nil {
		t.Fatalf("parseIntentPushOptions() error = %v", err)
	}
	if got != intent {
		t.Fatalf("round-trip mismatch:\n got %q\nwant %q", got, intent)
	}
}

func TestFormatIntentPushOptionEmpty(t *testing.T) {
	if got := formatIntentPushOption("   "); got != "" {
		t.Fatalf("formatIntentPushOption(blank) = %q, want empty", got)
	}
}

func TestParseIntentPushOptionsNone(t *testing.T) {
	got, err := parseIntentPushOptions([]string{"no-mistakes.skip=test", "ci.skip"})
	if err != nil {
		t.Fatalf("parseIntentPushOptions() error = %v", err)
	}
	if got != "" {
		t.Fatalf("parseIntentPushOptions(no intent) = %q, want empty", got)
	}
}

func TestTraceContextPushOptionsRoundTrip(t *testing.T) {
	want := &tracecontext.Context{
		Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Tracestate:  "tracewake=prototype,vendor=opaque",
	}
	options := append([]string{"no-mistakes.skip=test", "unrelated=value"}, formatTraceContextPushOptions(want)...)
	result := parseTraceContextPushOptions(options)
	if len(result.Diagnostics) != 0 {
		t.Fatalf("parse diagnostics = %v, want none", result.Diagnostics)
	}
	if !reflect.DeepEqual(result.Context, want) {
		t.Fatalf("parsed context = %#v, want %#v", result.Context, want)
	}
}

func TestTraceContextPushOptionsIgnoreInvalidUnsupportedAndOversizedValues(t *testing.T) {
	tests := []struct {
		name    string
		options []string
	}{
		{name: "invalid", options: []string{"no-mistakes.traceparent=invalid"}},
		{name: "unsupported baggage", options: []string{"no-mistakes.baggage=authorization=secret"}},
		{name: "duplicate", options: []string{
			"no-mistakes.traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"no-mistakes.traceparent=00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
		}},
		{name: "oversized", options: []string{"no-mistakes.traceparent=" + strings.Repeat("a", tracecontext.MaxTraceparentBytes+1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseTraceContextPushOptions(tt.options)
			if result.Context != nil {
				t.Fatalf("invalid context was accepted: %#v", result.Context)
			}
			if len(result.Diagnostics) == 0 {
				t.Fatal("invalid context should produce a bounded diagnostic")
			}
			for _, diagnostic := range result.Diagnostics {
				if len(diagnostic.String()) > 96 || strings.Contains(diagnostic.String(), "secret") {
					t.Fatalf("unsafe diagnostic: %q", diagnostic)
				}
			}
		})
	}
}

func TestTraceContextPushOptionsDoNotInventGenericMetadataCarrier(t *testing.T) {
	result := parseTraceContextPushOptions([]string{
		"no-mistakes.metadata=arbitrary",
		"no-mistakes.trace-id=0123456789abcdef",
	})
	if result.Context != nil || len(result.Diagnostics) != 0 {
		t.Fatalf("generic metadata affected trace context: %#v", result)
	}
}
