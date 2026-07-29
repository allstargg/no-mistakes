//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// TestNativeOTLPTraceJourney drives the production HTTP/protobuf exporter from
// incoming TW-38 context through real run, step, gate, CI-failure, CI-green,
// and terminal durable facts. It then removes the collector and proves a second
// pipeline still completes.
func TestNativeOTLPTraceJourney(t *testing.T) {
	receiver := newOTLPTestReceiver(t)
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", receiver.server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "")
	for _, key := range []string{
		"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
		"OTEL_EXPORTER_OTLP_COMPRESSION", "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
		"OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_KEY", "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")

	h := NewHarness(t, SetupOpts{Agent: "claude", Scenario: axiScenario(t)})
	// The production CI monitor polls every 30 seconds. This journey needs a
	// failure, a later green observation, and terminal reconciliation, not a
	// real-time poll delay, so use the product's bounded timeout path to keep the
	// full E2E suite within its existing global deadline.
	globalConfigPath := filepath.Join(h.NMHome, "config.yaml")
	globalConfig, err := os.ReadFile(globalConfigPath)
	if err != nil {
		t.Fatalf("read global config: %v", err)
	}
	if err := os.WriteFile(globalConfigPath, append(globalConfig, []byte("ci_timeout: 1s\n")...), 0o644); err != nil {
		t.Fatalf("set short CI timeout: %v", err)
	}
	ctx := context.Background()
	parentURL := "https://github.com/tracewake/no-mistakes.git"
	forkURL := "https://github.com/tracewake-fork/no-mistakes.git"
	forkDir := filepath.Join(filepath.Dir(h.UpstreamDir), "otel-fork.git")
	if err := os.MkdirAll(forkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := h.runGit(ctx, forkDir, "init", "--bare", "--initial-branch=main"); err != nil {
		t.Fatalf("init fork: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "push", forkDir, "main"); err != nil {
		t.Fatalf("seed fork: %v\n%s", err, out)
	}
	configureGitURLRewrite(t, h, parentURL, h.UpstreamDir)
	configureGitURLRewrite(t, h, forkURL, forkDir)
	if out, err := h.runGit(ctx, h.WorkDir, "remote", "set-url", "origin", parentURL); err != nil {
		t.Fatalf("set origin: %v\n%s", err, out)
	}

	root := filepath.Dir(h.AgentLog)
	t.Setenv("FAKEAGENT_GH_MODE", "fork-pr")
	t.Setenv("FAKEAGENT_GH_PARENT", "tracewake/no-mistakes")
	t.Setenv("FAKEAGENT_GH_STATE_FILE", filepath.Join(root, "otel-gh-state-count"))
	t.Setenv("FAKEAGENT_GH_CHECKS_FILE", filepath.Join(root, "otel-gh-check-count"))
	t.Setenv("FAKEAGENT_GH_CHECKS_FAIL_FIRST", "1")
	if out, err := h.Run("init", "--fork-url", forkURL); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	capClient, err := ipc.Dial(paths.WithRoot(h.NMHome).Socket())
	if err != nil {
		t.Fatalf("dial initial capabilities: %v", err)
	}
	initialCaps, err := capClient.Capabilities()
	_ = capClient.Close()
	if err != nil || initialCaps.NativeOTLPTraces == nil || !initialCaps.NativeOTLPTraces.Enabled || initialCaps.NativeOTLPTraces.State != "ready" {
		t.Fatalf("native OTLP was not enabled by standard environment: caps=%#v err=%v", initialCaps, err)
	}

	const (
		branch      = "feature/tw39-otel"
		traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		tracestate  = "tracewake=tw39"
	)
	h.CommitChange(branch, "otel.txt", "native OTLP journey\n", "add OTLP journey")
	pushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := h.runGit(pushCtx, h.WorkDir, "push",
		"-o", "no-mistakes.traceparent="+traceparent,
		"-o", "no-mistakes.tracestate="+tracestate,
		"no-mistakes", branch); err != nil {
		t.Fatalf("push: %v\n%s", err, out)
	}

	run := h.WaitForRunRunning(branch, 30*time.Second)
	waitForStepGate(t, h, run.ID, types.StepReview)
	h.Respond(run.ID, types.StepReview, types.ActionApprove)
	waitForStepGate(t, h, run.ID, types.StepCI)
	h.Respond(run.ID, types.StepCI, types.ActionFix)
	completed := h.WaitForRun(branch, 120*time.Second)
	if completed.Status != types.RunCompleted {
		t.Fatalf("run status = %s, error=%v", completed.Status, completed.Error)
	}

	spans := receiver.waitForRun(t, completed.ID, 10*time.Second)
	assertOTLPJourneyShape(t, spans, completed, traceparent, tracestate)

	// Collector absence is a telemetry-health condition, never pipeline state.
	receiver.close()
	const outageBranch = "feature/tw39-collector-outage"
	h.CommitChange(outageBranch, "outage.txt", "collector is down\n", "test collector outage")
	outageCtx, outageCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer outageCancel()
	if out, err := h.runGit(outageCtx, h.WorkDir, "push", "no-mistakes", outageBranch); err != nil {
		t.Fatalf("outage push: %v\n%s", err, out)
	}
	outageRunning := h.WaitForRunRunning(outageBranch, 30*time.Second)
	waitForStepGate(t, h, outageRunning.ID, types.StepReview)
	h.Respond(outageRunning.ID, types.StepReview, types.ActionApprove)
	outageRun := h.WaitForRun(outageBranch, 120*time.Second)
	if outageRun.Status != types.RunCompleted {
		t.Fatalf("collector outage changed pipeline status = %s, error=%v", outageRun.Status, outageRun.Error)
	}

	socket := paths.WithRoot(h.NMHome).Socket()
	client, err := ipc.Dial(socket)
	if err != nil {
		t.Fatalf("dial capabilities: %v", err)
	}
	defer client.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		caps, err := client.Capabilities()
		if err == nil && caps.NativeOTLPTraces != nil && caps.NativeOTLPTraces.State == "degraded" {
			if !caps.NativeOTLPTraces.Enabled || caps.NativeOTLPTraces.ContentCapture {
				t.Fatalf("native OTLP health = %#v", caps.NativeOTLPTraces)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("native OTLP health did not degrade after outage: caps=%#v err=%v", caps, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForStepGate(t *testing.T, h *Harness, runID string, stepName types.StepName) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		run := h.RunInfo(runID)
		for _, step := range run.Steps {
			parked := step.Status == types.StepStatusAwaitingApproval || step.Status == types.StepStatusFixReview
			if run.AwaitingAgent && step.StepName == stepName && parked {
				return
			}
		}
		if run.Status == types.RunCompleted || run.Status == types.RunFailed || run.Status == types.RunCancelled {
			h.dumpDebugState()
			t.Fatalf("%s gate did not park before run became %s", stepName, run.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.dumpDebugState()
	t.Fatalf("%s gate did not park", stepName)
}

type otlpTestReceiver struct {
	server *httptest.Server
	mu     sync.Mutex
	spans  []*tracepb.Span
	closed bool
}

func newOTLPTestReceiver(t *testing.T) *otlpTestReceiver {
	t.Helper()
	r := &otlpTestReceiver{}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/v1/traces" || req.Method != http.MethodPost {
			http.NotFound(w, req)
			return
		}
		body, err := io.ReadAll(io.LimitReader(req.Body, 2<<20))
		if err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		var export collectortracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &export); err != nil {
			http.Error(w, "protobuf failed", http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		for _, resourceSpans := range export.ResourceSpans {
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				r.spans = append(r.spans, scopeSpans.Spans...)
			}
		}
		r.mu.Unlock()
		payload, _ := proto.Marshal(&collectortracepb.ExportTraceServiceResponse{})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(r.close)
	return r
}

func (r *otlpTestReceiver) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()
	r.server.Close()
}

func (r *otlpTestReceiver) waitForRun(t *testing.T, runID string, timeout time.Duration) []*tracepb.Span {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		spans := append([]*tracepb.Span(nil), r.spans...)
		r.mu.Unlock()
		var traceID []byte
		for _, span := range spans {
			if span.Name == "tracewake.no_mistakes.run" && otlpStringAttribute(span.Attributes, "tracewake.no_mistakes.run.id") == runID {
				traceID = span.TraceId
				break
			}
		}
		var matched []*tracepb.Span
		for _, span := range spans {
			if len(traceID) != 0 && bytes.Equal(span.TraceId, traceID) {
				matched = append(matched, span)
			}
		}
		if hasOTLPSpan(matched, "tracewake.no_mistakes.run") && hasOTLPSpan(matched, "tracewake.firstmate.human_gate.wait") {
			return matched
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for OTLP spans for run %s", runID)
	return nil
}

func assertOTLPJourneyShape(t *testing.T, spans []*tracepb.Span, run *ipc.RunInfo, traceparent, tracestate string) {
	t.Helper()
	runSpan := findOTLPSpan(t, spans, "tracewake.no_mistakes.run", "", "")
	review := findOTLPSpan(t, spans, "tracewake.no_mistakes.step", "tracewake.no_mistakes.step.name", "review")
	gate := findOTLPSpan(t, spans, "tracewake.firstmate.human_gate.wait", "", "")
	ci := findSuccessfulCISpan(t, spans)
	failure := findFailedCISpan(t, spans)

	if got := hex.EncodeToString(runSpan.TraceId); got != strings.Split(traceparent, "-")[1] {
		t.Fatalf("run trace id = %s, want incoming trace", got)
	}
	if got := hex.EncodeToString(runSpan.ParentSpanId); got != strings.Split(traceparent, "-")[2] {
		t.Fatalf("run parent = %s, want incoming parent", got)
	}
	if runSpan.TraceState != tracestate {
		t.Fatalf("run tracestate = %q, want %q", runSpan.TraceState, tracestate)
	}
	if hex.EncodeToString(review.ParentSpanId) != hex.EncodeToString(runSpan.SpanId) {
		t.Fatal("review is not a child of run")
	}
	if hex.EncodeToString(gate.ParentSpanId) != hex.EncodeToString(review.SpanId) {
		t.Fatal("gate is not a child of review")
	}
	if hex.EncodeToString(failure.ParentSpanId) != hex.EncodeToString(ci.SpanId) {
		t.Fatal("CI failure is not a child of CI step")
	}
	if runSpan.StartTimeUnixNano != uint64(run.CreatedAt)*uint64(time.Second) || runSpan.EndTimeUnixNano != uint64(run.UpdatedAt)*uint64(time.Second) {
		t.Fatalf("run source times = %d..%d, want %d..%d", runSpan.StartTimeUnixNano, runSpan.EndTimeUnixNano, run.CreatedAt, run.UpdatedAt)
	}
	if failure.Status == nil || failure.Status.Code != tracepb.Status_STATUS_CODE_ERROR || failure.Status.Message != "" {
		t.Fatalf("CI failure status = %#v", failure.Status)
	}
	green := false
	for _, event := range ci.Events {
		if event.Name == "tracewake.ci.green" && otlpStringAttribute(event.Attributes, "tracewake.no_mistakes.ci.state") == "success" {
			green = true
		}
	}
	if !green {
		t.Fatalf("CI step events = %#v, want registered green event", ci.Events)
	}

	allowed := map[string]bool{
		"tracewake.no_mistakes.run.id":           true,
		"tracewake.outcome":                      true,
		"tracewake.no_mistakes.phase":            true,
		"tracewake.no_mistakes.step.name":        true,
		"tracewake.no_mistakes.failure.category": true,
		"tracewake.wait.kind":                    true,
		"tracewake.no_mistakes.gate.kind":        true,
	}
	for _, span := range spans {
		for _, attr := range span.Attributes {
			if !allowed[attr.Key] {
				t.Fatalf("unregistered OTLP span attribute %q", attr.Key)
			}
		}
		text := span.String()
		for _, forbidden := range []string{"prompt", "response", "diff", "file contents", "raw command", "token:secret", "github.com/tracewake"} {
			if strings.Contains(strings.ToLower(text), forbidden) {
				t.Fatalf("OTLP span leaked %q: %s", forbidden, text)
			}
		}
	}
}

func findSuccessfulCISpan(t *testing.T, spans []*tracepb.Span) *tracepb.Span {
	t.Helper()
	for _, span := range spans {
		if span.Name == "tracewake.no_mistakes.step" && otlpStringAttribute(span.Attributes, "tracewake.no_mistakes.step.name") == "ci" &&
			otlpStringAttribute(span.Attributes, "tracewake.outcome") == "success" {
			return span
		}
	}
	t.Fatal("successful CI span not found")
	return nil
}

func findFailedCISpan(t *testing.T, spans []*tracepb.Span) *tracepb.Span {
	t.Helper()
	for _, span := range spans {
		if span.Name == "tracewake.no_mistakes.step" && otlpStringAttribute(span.Attributes, "tracewake.no_mistakes.step.name") == "ci" &&
			otlpStringAttribute(span.Attributes, "tracewake.no_mistakes.failure.category") == "ci_failed" {
			return span
		}
	}
	t.Fatal("failed CI observation span not found")
	return nil
}

func findOTLPSpan(t *testing.T, spans []*tracepb.Span, name, attrKey, attrValue string) *tracepb.Span {
	t.Helper()
	for _, span := range spans {
		if span.Name == name && (attrKey == "" || otlpStringAttribute(span.Attributes, attrKey) == attrValue) {
			return span
		}
	}
	t.Fatalf("span %q %s=%q not found", name, attrKey, attrValue)
	return nil
}

func hasOTLPSpan(spans []*tracepb.Span, name string) bool {
	for _, span := range spans {
		if span.Name == name {
			return true
		}
	}
	return false
}

func otlpStringAttribute(attrs []*commonpb.KeyValue, key string) string {
	for _, attr := range attrs {
		if attr.Key == key && attr.Value != nil {
			return attr.Value.GetStringValue()
		}
	}
	return ""
}
