package oteltrace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestEmitSnapshotBuildsRegisteredParentChildTraceFromSourceFacts(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	start := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	completed := start.Add(9 * time.Second)
	stepStart := start.Add(time.Second)
	stepEnd := start.Add(8 * time.Second)
	gateStart := start.Add(3 * time.Second)
	gateEnd := start.Add(5 * time.Second)
	waitMS := int64(2000)
	runID := "01K1A2B3C4D5E6F7G8H9J0K1M2"
	reviewID := "01K1A2B3C4D5E6F7G8H9J0K1M3"

	snapshot := Snapshot{
		Run: &db.Run{
			ID: runID, Status: types.RunCompleted,
			Traceparent: ptr(testTraceparent), Tracestate: ptr("tracewake=tw39"),
			CreatedAt: start.Unix(), UpdatedAt: completed.Unix(),
		},
		Steps: []*db.StepResult{{
			ID: reviewID, RunID: runID, StepName: types.StepReview,
			Status: types.StepStatusCompleted, StartedAt: unixPtr(stepStart), CompletedAt: unixPtr(stepEnd),
		}},
		Events: []*db.MetadataEvent{
			{
				EventID: "gate-enter", Type: db.EventTypeGateEntered, SourceTimestamp: gateStart,
				Gate: &db.GateEventMetadata{GateID: "gate-1", Phase: "entered", Step: "review", Class: "approval"},
			},
			{
				EventID: "gate-exit", Type: db.EventTypeGateExited, SourceTimestamp: gateEnd,
				Gate: &db.GateEventMetadata{GateID: "gate-1", Phase: "exited", Step: "review", Class: "approval", Outcome: "approved", WaitDurationMS: &waitMS},
			},
		},
	}

	runtime.EmitSnapshot(snapshot)
	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("span count = %d, want run + step + gate; spans=%v", len(spans), spanNames(spans))
	}
	run := findSpan(t, spans, spanNameRun)
	step := findSpan(t, spans, spanNameStep)
	gate := findSpan(t, spans, spanNameGate)

	if got := run.SpanContext.TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("run trace id = %s, want incoming trace id", got)
	}
	if got := run.Parent.SpanID().String(); got != "00f067aa0ba902b7" || !run.Parent.IsRemote() {
		t.Fatalf("run parent = %s remote=%v, want incoming remote parent", got, run.Parent.IsRemote())
	}
	if got := run.Parent.TraceState().String(); got != "tracewake=tw39" {
		t.Fatalf("run parent tracestate = %q, want preserved W3C tracestate", got)
	}
	if step.Parent.SpanID() != run.SpanContext.SpanID() {
		t.Fatalf("step parent = %s, want run %s", step.Parent.SpanID(), run.SpanContext.SpanID())
	}
	if gate.Parent.SpanID() != step.SpanContext.SpanID() {
		t.Fatalf("gate parent = %s, want review step %s", gate.Parent.SpanID(), step.SpanContext.SpanID())
	}
	assertTimes(t, run, start, completed)
	assertTimes(t, step, stepStart, stepEnd)
	assertTimes(t, gate, gateStart, gateEnd)
	assertAttribute(t, run.Attributes, attrRunID, runID)
	assertAttribute(t, run.Attributes, attrOutcome, outcomeSuccess)
	assertAttribute(t, run.Attributes, attrPhase, phaseComplete)
	assertAttribute(t, step.Attributes, attrStepName, "review")
	assertAttribute(t, gate.Attributes, attrWaitKind, "gate")
	assertAttribute(t, gate.Attributes, attrGateKind, "approval")
	if run.Status.Code != codes.Unset || step.Status.Code != codes.Unset || gate.Status.Code != codes.Unset {
		t.Fatalf("successful statuses = run %v step %v gate %v", run.Status.Code, step.Status.Code, gate.Status.Code)
	}
	if len(gate.Events) != 2 || gate.Events[0].Name != eventDecisionRequested || gate.Events[1].Name != eventDecisionResolved {
		t.Fatalf("gate events = %#v", gate.Events)
	}
}

func TestEmitSnapshotMapsCIAndFailureWithoutRawErrorContent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	start := time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC)
	end := start.Add(4 * time.Second)
	rawSecret := "https://token:secret@example.invalid/private raw failure output"
	runID := "01K1A2B3C4D5E6F7G8H9J0K1M4"
	snapshot := Snapshot{
		Run: &db.Run{ID: runID, Status: types.RunFailed, Error: &rawSecret, CreatedAt: start.Unix(), UpdatedAt: end.Unix()},
		Steps: []*db.StepResult{{
			ID: "ci-step", RunID: runID, StepName: types.StepCI, Status: types.StepStatusFailed,
			Error: &rawSecret, StartedAt: unixPtr(start.Add(time.Second)), CompletedAt: unixPtr(end),
		}},
		Events: []*db.MetadataEvent{{
			EventID: "ci-green", Type: db.EventTypeCIGreen, SourceTimestamp: start.Add(2 * time.Second),
			CI: &db.CIEventMetadata{State: "green", Outcome: "passed"},
		}},
	}

	runtime.EmitSnapshot(snapshot)
	spans := exporter.GetSpans()
	run := findSpan(t, spans, spanNameRun)
	step := findSpan(t, spans, spanNameStep)
	if run.Status.Code != codes.Error || run.Status.Description != "" {
		t.Fatalf("run status = %#v, want content-free error", run.Status)
	}
	if step.Status.Code != codes.Error || step.Status.Description != "" {
		t.Fatalf("step status = %#v, want content-free error", step.Status)
	}
	assertAttribute(t, run.Attributes, attrOutcome, outcomeFailed)
	assertAttribute(t, step.Attributes, attrFailureCategory, failureCIFailed)
	if len(step.Events) != 1 || step.Events[0].Name != eventCIGreen || !step.Events[0].Time.Equal(start.Add(2*time.Second)) {
		t.Fatalf("CI events = %#v", step.Events)
	}
	assertAttribute(t, step.Events[0].Attributes, attrCIState, ciSuccess)

	for _, span := range spans {
		text := fmt.Sprint(span.Name, span.Status.Description, span.Attributes, span.Events, span.Resource.Attributes())
		for _, forbidden := range []string{"token", "secret", "example.invalid", "private", "raw failure output"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("span %s leaked forbidden content %q: %s", span.Name, forbidden, text)
			}
		}
		for _, kv := range span.Attributes {
			if !approvedSpanAttribute(kv.Key) {
				t.Fatalf("span %s used unregistered attribute %q", span.Name, kv.Key)
			}
		}
	}
}

func TestEmitSnapshotRepresentsTransientCIFailureAsRegisteredFailureSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	start := time.Date(2026, 7, 29, 11, 30, 0, 0, time.UTC)
	runID := "01K1A2B3C4D5E6F7G8H9J0K1M5"
	ciStepID := "01K1A2B3C4D5E6F7G8H9J0K1M6"
	failureAt := start.Add(2 * time.Second)
	runtime.EmitSnapshot(Snapshot{
		Run: &db.Run{ID: runID, Status: types.RunCompleted, CreatedAt: start.Unix(), UpdatedAt: start.Add(5 * time.Second).Unix()},
		Steps: []*db.StepResult{{
			ID: ciStepID, RunID: runID, StepName: types.StepCI, Status: types.StepStatusCompleted,
			StartedAt: unixPtr(start.Add(time.Second)), CompletedAt: unixPtr(start.Add(4 * time.Second)),
		}},
		Events: []*db.MetadataEvent{{
			EventID: "ci-failure-event", Type: db.EventTypeCIFailure, SourceTimestamp: failureAt,
			CI: &db.CIEventMetadata{State: "failure", Outcome: "checks"},
		}},
	})

	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("span count = %d, want run + CI step + CI failure; spans=%v", len(spans), spanNames(spans))
	}
	var ciStep, failure *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name != spanNameStep {
			continue
		}
		if spans[i].Status.Code == codes.Error {
			failure = &spans[i]
		} else {
			ciStep = &spans[i]
		}
	}
	if ciStep == nil || failure == nil {
		t.Fatalf("CI spans = %#v", spans)
	}
	if failure.Parent.SpanID() != ciStep.SpanContext.SpanID() {
		t.Fatalf("CI failure parent = %s, want CI step %s", failure.Parent.SpanID(), ciStep.SpanContext.SpanID())
	}
	assertTimes(t, *failure, failureAt, failureAt)
	assertAttribute(t, failure.Attributes, attrFailureCategory, failureCIFailed)
	if failure.Status.Description != "" {
		t.Fatalf("CI failure status description leaked content: %q", failure.Status.Description)
	}
}

func TestEmitSnapshotUsesIndependentDeterministicIdentityWithoutValidParent(t *testing.T) {
	start := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	bad := "not-a-traceparent"
	snapshot := Snapshot{Run: &db.Run{ID: "run-root", Status: types.RunCompleted, Traceparent: &bad, CreatedAt: start.Unix(), UpdatedAt: start.Add(time.Second).Unix()}}

	first := emitWithFreshRuntime(t, snapshot)
	second := emitWithFreshRuntime(t, snapshot)
	if first.Parent.IsValid() || second.Parent.IsValid() {
		t.Fatalf("malformed persisted carrier minted parentage: first=%v second=%v", first.Parent, second.Parent)
	}
	if first.SpanContext.TraceID() != second.SpanContext.TraceID() || first.SpanContext.SpanID() != second.SpanContext.SpanID() {
		t.Fatalf("restart identity changed: first=%v second=%v", first.SpanContext, second.SpanContext)
	}
}

func TestEmitSnapshotHonorsValidUnsampledParent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	unsampled := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
	at := time.Date(2026, 7, 29, 12, 30, 0, 0, time.UTC)

	runtime.EmitSnapshot(Snapshot{Run: &db.Run{
		ID: "run-unsampled", Status: types.RunCompleted, Traceparent: &unsampled,
		CreatedAt: at.Unix(), UpdatedAt: at.Add(time.Second).Unix(),
	}})
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("unsampled W3C parent produced %d exported spans", len(spans))
	}
}

func TestEmitSnapshotMapsCancelledRunWithoutErrorContent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	at := time.Date(2026, 7, 29, 12, 45, 0, 0, time.UTC)
	raw := "cancelled: secret reason"

	runtime.EmitSnapshot(Snapshot{Run: &db.Run{
		ID: "run-cancelled", Status: types.RunCancelled, Error: &raw,
		CreatedAt: at.Unix(), UpdatedAt: at.Add(time.Second).Unix(),
	}})
	span := findSpan(t, exporter.GetSpans(), spanNameRun)
	assertAttribute(t, span.Attributes, attrOutcome, outcomeCancelled)
	if span.Status.Code != codes.Error || span.Status.Description != "" {
		t.Fatalf("cancelled status = %#v, want content-free error", span.Status)
	}
	if strings.Contains(fmt.Sprint(span.Attributes, span.Status), "secret reason") {
		t.Fatal("cancelled run leaked raw reason")
	}
}

func TestEnvironmentConfigurationIsOptInAndRejectsMalformedValues(t *testing.T) {
	tests := []struct {
		name         string
		env          map[string]string
		want         configState
		wantEndpoint string
	}{
		{name: "absent is disabled", want: configDisabled},
		{name: "sdk disabled", env: map[string]string{"OTEL_SDK_DISABLED": "true", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318"}, want: configDisabled},
		{name: "generic http endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318/base"}, want: configReady, wantEndpoint: "http://127.0.0.1:4318/base/v1/traces"},
		{name: "signal endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://collector.invalid/custom"}, want: configReady, wantEndpoint: "https://collector.invalid/custom"},
		{name: "signal root endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://collector.invalid"}, want: configReady, wantEndpoint: "https://collector.invalid/"},
		{name: "malformed endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "://bad"}, want: configMalformed},
		{name: "userinfo prohibited", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://token:secret@collector.invalid"}, want: configMalformed},
		{name: "query prohibited", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.invalid?token=secret"}, want: configMalformed},
		{name: "grpc not silently reinterpreted", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4317", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, want: configMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseEnvironment(func(key string) string { return tt.env[key] })
			if cfg.State != tt.want {
				t.Fatalf("state = %q, want %q", cfg.State, tt.want)
			}
			if tt.wantEndpoint != "" && cfg.Endpoint != tt.wantEndpoint {
				t.Fatalf("endpoint = %q, want %q", cfg.Endpoint, tt.wantEndpoint)
			}
			if tt.want == configDisabled && cfg.Enabled {
				t.Fatal("disabled configuration enabled the SDK")
			}
			if tt.want == configMalformed && cfg.Endpoint != "" {
				t.Fatalf("malformed configuration retained endpoint %q", cfg.Endpoint)
			}
		})
	}
}

func TestEnvironmentConfigurationFeatureDetectsTraceAndMetricSignals(t *testing.T) {
	tests := []struct {
		name        string
		env         map[string]string
		want        configState
		wantTraces  bool
		wantMetrics bool
		tracePath   string
		metricPath  string
	}{
		{name: "generic endpoint enables both", env: map[string]string{envEndpoint: "http://127.0.0.1:4318/base"}, want: configReady, wantTraces: true, wantMetrics: true, tracePath: "/base/v1/traces", metricPath: "/base/v1/metrics"},
		{name: "trace endpoint enables spans only", env: map[string]string{envTracesEndpoint: "http://127.0.0.1:4318/custom-traces"}, want: configReady, wantTraces: true, tracePath: "/custom-traces"},
		{name: "metric endpoint enables metrics only", env: map[string]string{envMetricsEndpoint: "http://127.0.0.1:4318/custom-metrics"}, want: configReady, wantMetrics: true, metricPath: "/custom-metrics"},
		{name: "malformed metric config fails closed", env: map[string]string{envTracesEndpoint: "http://127.0.0.1:4318/v1/traces", envMetricsEndpoint: "://secret"}, want: configMalformed},
		{name: "metric grpc is rejected", env: map[string]string{envMetricsEndpoint: "http://127.0.0.1:4318", envMetricsProtocol: "grpc"}, want: configMalformed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := parseEnvironment(func(key string) string { return tc.env[key] })
			if cfg.State != tc.want || cfg.TracesEnabled != tc.wantTraces || cfg.MetricsEnabled != tc.wantMetrics {
				t.Fatalf("config = %#v, want state=%s traces=%v metrics=%v", cfg, tc.want, tc.wantTraces, tc.wantMetrics)
			}
			if tc.tracePath != "" && !strings.HasSuffix(cfg.TracesEndpoint, tc.tracePath) {
				t.Fatalf("trace endpoint = %q, want suffix %q", cfg.TracesEndpoint, tc.tracePath)
			}
			if tc.metricPath != "" && !strings.HasSuffix(cfg.MetricsEndpoint, tc.metricPath) {
				t.Fatalf("metric endpoint = %q, want suffix %q", cfg.MetricsEndpoint, tc.metricPath)
			}
		})
	}
}

func TestHealthDoesNotRecoverWhileEitherSignalRemainsDegraded(t *testing.T) {
	runtime := &Runtime{}
	runtime.state.Store(healthIndexReady)
	runtime.traceDegraded.Store(true)
	runtime.updateExportHealth()
	if runtime.Health().State != healthDegraded {
		t.Fatalf("trace failure state = %q, want degraded", runtime.Health().State)
	}
	runtime.metricDegraded.Store(true)
	runtime.traceDegraded.Store(false)
	runtime.updateExportHealth()
	if runtime.Health().State != healthDegraded {
		t.Fatalf("metric failure was masked by trace recovery: %q", runtime.Health().State)
	}
	runtime.metricDegraded.Store(false)
	runtime.updateExportHealth()
	if runtime.Health().State != healthReady {
		t.Fatalf("both-signal recovery state = %q, want ready", runtime.Health().State)
	}
}

func TestBatchingIsNonBlockingUnderBackpressureAndShutdownIsBounded(t *testing.T) {
	exporter := newBlockingExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{
		QueueSize: 2, BatchSize: 1, BatchTimeout: time.Millisecond, ExportTimeout: 20 * time.Millisecond,
	})

	start := time.Now()
	for i := 0; i < 500; i++ {
		at := time.Unix(int64(i+1), 0).UTC()
		runtime.EmitSnapshot(Snapshot{Run: &db.Run{
			ID: fmt.Sprintf("run-%d", i), Status: types.RunCompleted,
			CreatedAt: at.Unix(), UpdatedAt: at.Add(time.Second).Unix(),
		}})
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("emission blocked under exporter pressure for %v", elapsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	shutdownStart := time.Now()
	_ = runtime.Shutdown(ctx)
	if elapsed := time.Since(shutdownStart); elapsed > 150*time.Millisecond {
		t.Fatalf("shutdown exceeded bound: %v", elapsed)
	}
}

func TestNotifyRunConcurrentWithShutdownIsRaceFree(t *testing.T) {
	database, err := db.Open(t.TempDir() + "/state.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	runtime := newRuntimeWithExporter(tracetest.NewInMemoryExporter(), processorConfig{BatchTimeout: time.Millisecond})
	runtime.database = database
	runtime.startProjector()

	var callers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		callers.Add(1)
		go func(worker int) {
			defer callers.Done()
			for n := 0; n < 1000; n++ {
				runtime.NotifyRun(fmt.Sprintf("run-%d-%d", worker, n))
			}
		}(worker)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	shutdownErr := runtime.Shutdown(ctx)
	cancel()
	callers.Wait()
	if shutdownErr != nil {
		t.Fatalf("shutdown: %v", shutdownErr)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectorUsesTerminalHookAndSuppressesDuplicateTerminalNotifications(t *testing.T) {
	database, runtime, exporter := projectorFixture(t)
	repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", &tracecontext.Context{Traceparent: testTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.StartStepWithEvent(context.Background(), run.ID, step.ID, 0); err != nil {
		t.Fatal(err)
	}
	if err := database.EnterRunGateWithEvent(context.Background(), run.ID, types.StepReview, db.GateClassApproval); err != nil {
		t.Fatal(err)
	}
	if err := database.ExitRunGateWithEvent(context.Background(), run.ID, 10, db.GateOutcomeApproved); err != nil {
		t.Fatal(err)
	}
	if err := database.CompleteStep(step.ID, 0, 10, ""); err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunCompleted); err != nil {
		t.Fatal(err)
	}
	waitForSpanCount(t, exporter, 3)

	// A retry of the same terminal write can wake projection again, but the
	// process-local bounded identity set must not duplicate terminal spans.
	if err := database.UpdateRunStatus(run.ID, types.RunCompleted); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := len(exporter.GetSpans()); got != 3 {
		t.Fatalf("span count after duplicate notification = %d, want 3", got)
	}
	if runtime.Health().State != healthReady {
		t.Fatalf("runtime health = %#v", runtime.Health())
	}
}

func TestRecoveryProjectsCrashedRunFromDurableFacts(t *testing.T) {
	database, _, exporter := projectorFixture(t)
	repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "feature", "head", "base")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	step, err := database.InsertStepResult(run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.StartStepWithEvent(context.Background(), run.ID, step.ID, 0); err != nil {
		t.Fatal(err)
	}
	if count, err := database.RecoverStaleRuns("daemon crashed during execution"); err != nil || count != 1 {
		t.Fatalf("RecoverStaleRuns = (%d, %v), want (1, nil)", count, err)
	}
	waitForSpanCount(t, exporter, 2)
	stepSpan := findSpan(t, exporter.GetSpans(), spanNameStep)
	assertAttribute(t, stepSpan.Attributes, attrFailureCategory, failureCrashed)
	if stepSpan.Status.Code != codes.Error {
		t.Fatalf("recovered step status = %v, want error", stepSpan.Status.Code)
	}
}

func TestDisabledAndMalformedProductionConfigurationStayInert(t *testing.T) {
	for _, key := range []string{envSDKDisabled, envEndpoint, envTracesEndpoint, envProtocol, envTracesProtocol} {
		t.Setenv(key, "")
	}
	disabled := NewFromEnvironment(nil, "test")
	if got := disabled.Health(); got.Enabled || got.State != healthDisabled || got.ContentCapture {
		t.Fatalf("disabled health = %#v", got)
	}
	if disabled.provider != nil || disabled.notify != nil {
		t.Fatal("absent endpoint created SDK or projection worker")
	}

	t.Setenv(envEndpoint, "://secret malformed")
	malformed := NewFromEnvironment(nil, "test")
	if got := malformed.Health(); got.Enabled || got.State != healthMisconfigured || got.ContentCapture {
		t.Fatalf("malformed health = %#v", got)
	}
	if malformed.provider != nil || malformed.notify != nil {
		t.Fatal("malformed endpoint created SDK or projection worker")
	}
}

func TestProductionHTTPExporterProjectsTerminalRunWithoutExplicitFlush(t *testing.T) {
	received := make(chan *collectortracepb.ExportTraceServiceRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.Path != defaultOTLPTracesPath {
			http.NotFound(w, req)
			return
		}
		body, err := io.ReadAll(io.LimitReader(req.Body, maxOTLPRequestBytes+1))
		if err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		var export collectortracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &export); err != nil {
			http.Error(w, "decode failed", http.StatusBadRequest)
			return
		}
		select {
		case received <- &export:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Setenv(envSDKDisabled, "false")
	t.Setenv(envEndpoint, server.URL)
	t.Setenv(envTracesEndpoint, "")
	t.Setenv(envMetricsEndpoint, "")
	t.Setenv(envProtocol, protocolHTTPProtobuf)
	t.Setenv(envTracesProtocol, "")
	t.Setenv(envMetricsProtocol, "")
	for _, key := range []string{
		"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
		"OTEL_EXPORTER_OTLP_COMPRESSION", "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
		"OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_KEY", "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
	} {
		t.Setenv(key, "")
	}
	database, err := db.Open(t.TempDir() + "/state.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	runtime := NewFromEnvironment(database, "test")
	database.SetRunTerminalHook(runtime.NotifyRun)
	t.Cleanup(func() {
		database.SetRunTerminalHook(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Shutdown(ctx)
		_ = database.Close()
	})
	repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "feature", "head", "base")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(run.ID, types.RunCompleted); err != nil {
		t.Fatal(err)
	}

	select {
	case export := <-received:
		var found bool
		for _, resourceSpans := range export.ResourceSpans {
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				for _, span := range scopeSpans.Spans {
					if span.Name == spanNameRun {
						found = true
					}
				}
			}
		}
		if !found {
			t.Fatalf("OTLP request did not contain %q", spanNameRun)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("production HTTP exporter did not receive terminal projection")
	}
}

func TestExporterFailureOnlyDegradesHealth(t *testing.T) {
	exporter := &failingExporter{err: errors.New("collector unavailable at secret endpoint")}
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	at := time.Now().UTC()

	// Exporter failure must not be returned through the pipeline-facing seam.
	runtime.EmitSnapshot(Snapshot{Run: &db.Run{ID: "outage", Status: types.RunCompleted, CreatedAt: at.Unix(), UpdatedAt: at.Add(time.Second).Unix()}})
	if got := runtime.Health(); got.State != healthDegraded || got.Enabled != true || got.ContentCapture {
		t.Fatalf("health = %#v, want enabled degraded metadata-only", got)
	}
}

func projectorFixture(t *testing.T) (*db.DB, *Runtime, *tracetest.InMemoryExporter) {
	t.Helper()
	database, err := db.Open(t.TempDir() + "/state.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{BatchTimeout: time.Millisecond})
	runtime.database = database
	runtime.startProjector()
	database.SetRunTerminalHook(runtime.NotifyRun)
	t.Cleanup(func() {
		database.SetRunTerminalHook(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Shutdown(ctx)
		_ = database.Close()
	})
	return database, runtime, exporter
}

func waitForSpanCount(t *testing.T, exporter *tracetest.InMemoryExporter, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(exporter.GetSpans()) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("span count = %d, want >= %d", len(exporter.GetSpans()), want)
}

func emitWithFreshRuntime(t *testing.T, snapshot Snapshot) tracetest.SpanStub {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	runtime.EmitSnapshot(snapshot)
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("span count = %d, want 1", len(spans))
	}
	// InMemoryExporter clears itself during shutdown, so copy before cleanup.
	span := spans[0]
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	return span
}

func findSpan(t *testing.T, spans tracetest.SpanStubs, name string) tracetest.SpanStub {
	t.Helper()
	for _, span := range spans {
		if span.Name == name {
			return span
		}
	}
	t.Fatalf("span %q not found in %v", name, spanNames(spans))
	return tracetest.SpanStub{}
}

func spanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, span := range spans {
		out = append(out, span.Name)
	}
	return out
}

func assertTimes(t *testing.T, span tracetest.SpanStub, start, end time.Time) {
	t.Helper()
	if !span.StartTime.Equal(start) || !span.EndTime.Equal(end) {
		t.Fatalf("%s times = %s..%s, want %s..%s", span.Name, span.StartTime, span.EndTime, start, end)
	}
}

func assertAttribute(t *testing.T, attrs []attribute.KeyValue, key attribute.Key, want any) {
	t.Helper()
	for _, kv := range attrs {
		if kv.Key == key {
			if fmt.Sprint(kv.Value.AsInterface()) != fmt.Sprint(want) {
				t.Fatalf("attribute %s = %v, want %v", key, kv.Value.AsInterface(), want)
			}
			return
		}
	}
	t.Fatalf("attribute %s missing from %v", key, attrs)
}

func unixPtr(value time.Time) *int64 {
	unix := value.Unix()
	return &unix
}

func ptr(value string) *string { return &value }

func approvedSpanAttribute(key attribute.Key) bool {
	switch key {
	case attrRunID, attrOutcome, attrPhase, attrStepName, attrFailureCategory, attrWaitKind, attrGateKind:
		return true
	default:
		return false
	}
}

type failingExporter struct {
	err error
}

func (e *failingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return e.err }
func (e *failingExporter) Shutdown(context.Context) error                             { return nil }

type blockingExporter struct {
	started chan struct{}
	once    sync.Once
}

func newBlockingExporter() *blockingExporter { return &blockingExporter{started: make(chan struct{})} }
func (e *blockingExporter) ExportSpans(ctx context.Context, _ []sdktrace.ReadOnlySpan) error {
	e.once.Do(func() { close(e.started) })
	<-ctx.Done()
	return ctx.Err()
}
func (e *blockingExporter) Shutdown(context.Context) error { return nil }

var _ = tracecontext.Context{}
