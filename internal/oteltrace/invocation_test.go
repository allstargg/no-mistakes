package oteltrace

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestEmitSnapshotProjectsCompletedInvocationsUnderAuthoritativeStep(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	start := time.Date(2026, 7, 29, 15, 0, 0, 0, time.UTC)
	runID := "run-invocation-parent"
	stepID := "step-review"
	provider := "OpenAI"
	deltaInput, deltaOutput := 150, 25
	runtime.EmitSnapshot(Snapshot{
		Run: &db.Run{ID: runID, Status: types.RunCompleted, CreatedAt: start.Unix(), UpdatedAt: start.Add(10 * time.Second).Unix()},
		Steps: []*db.StepResult{{
			ID: stepID, RunID: runID, StepName: types.StepReview, Status: types.StepStatusCompleted,
			StartedAt: unixPtr(start.Add(time.Second)), CompletedAt: unixPtr(start.Add(9 * time.Second)),
		}},
		Invocations: []db.AgentInvocation{{
			ID: "inv-1", RunID: runID, StepName: "review", Agent: "codex", Model: "gpt-5.6-sol", ModelProvider: &provider,
			SessionMode: db.InvocationModeResumed, StartedAt: start.Add(2 * time.Second).Unix(), CompletedAt: start.Add(8 * time.Second).Unix(),
			DurationMS: 6000, ExitStatus: "ok", DeltaInputTokens: &deltaInput, DeltaOutputTokens: &deltaOutput,
		}},
	})

	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("span count = %d, want run + step + invocation; spans=%v", len(spans), spanNames(spans))
	}
	invocation := findSpan(t, spans, spanNameInvocation)
	step := findSpan(t, spans, spanNameStep)
	if invocation.Parent.SpanID() != step.SpanContext.SpanID() {
		t.Fatalf("invocation parent = %s, want step %s", invocation.Parent.SpanID(), step.SpanContext.SpanID())
	}
	assertTimes(t, invocation, start.Add(2*time.Second), start.Add(8*time.Second))
	assertAttribute(t, invocation.Attributes, attrInvocationID, "inv-1")
	assertAttribute(t, invocation.Attributes, attrOperationName, "invoke_agent")
	assertAttribute(t, invocation.Attributes, attrStepName, "review")
	assertAttribute(t, invocation.Attributes, attrOutcome, outcomeSuccess)
	assertAttribute(t, invocation.Attributes, attrSessionMode, "resumed")
	assertAttribute(t, invocation.Attributes, attrHarnessFamily, "codex")
	assertAttribute(t, invocation.Attributes, attrProviderName, "openai")
	assertAttribute(t, invocation.Attributes, attrModelFamily, "gpt")
	assertOnlyApprovedInvocationAttributes(t, invocation.Attributes)
}

func TestEmitSnapshotInvocationUnknownsStayAbsentAndFailuresAreBounded(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporter(exporter, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	at := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)
	runtime.EmitSnapshot(Snapshot{
		Run: &db.Run{ID: "run-unknown", Status: types.RunFailed, CreatedAt: at.Unix(), UpdatedAt: at.Add(2 * time.Second).Unix()},
		Invocations: []db.AgentInvocation{{
			ID: "inv-unknown", RunID: "run-unknown", StepName: "provider supplied raw step", Agent: "acp:arbitrary-provider-and-model",
			Model: "secret exact model value", SessionMode: "provider-special", StartedAt: at.Unix(), CompletedAt: at.Add(time.Second).Unix(),
			DurationMS: 1000, ExitStatus: "error", FailureCategory: "raw error with token:secret",
		}},
	})

	invocation := findSpan(t, exporter.GetSpans(), spanNameInvocation)
	assertAttribute(t, invocation.Attributes, attrOutcome, outcomeFailed)
	assertAttribute(t, invocation.Attributes, attrStepName, "other")
	assertAttribute(t, invocation.Attributes, attrHarnessFamily, "other")
	assertAttribute(t, invocation.Attributes, attrFailureCategory, failureToolError)
	for _, forbidden := range []attribute.Key{attrProviderName, attrModelFamily, attrSessionMode} {
		if hasAttribute(invocation.Attributes, forbidden) {
			t.Fatalf("unknown %s became an invocation attribute: %v", forbidden, invocation.Attributes)
		}
	}
	text := fmt.Sprint(invocation.Attributes, invocation.Status)
	for _, forbidden := range []string{"secret exact", "arbitrary-provider", "token:secret", "provider-special", "raw error"} {
		if contains(text, forbidden) {
			t.Fatalf("invocation span leaked %q: %s", forbidden, text)
		}
	}
	assertOnlyApprovedInvocationAttributes(t, invocation.Attributes)
}

func TestInvocationMetricProjectionUsesPerInvocationDeltasAndExplicitCoverage(t *testing.T) {
	reader := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(deltaTemporality))
	exporter := tracetest.NewInMemoryExporter()
	runtime := newRuntimeWithExporterAndMetricReader(exporter, reader, processorConfig{Synchronous: true})
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	at := time.Date(2026, 7, 29, 17, 0, 0, 0, time.UTC)
	provider := "openai"
	firstInput, firstOutput := 100, 20
	resumedInput, resumedOutput := 150, 30
	waitMS := int64(1200)
	roundtrips, tools, zero := 4, 3, 0
	invocations := []db.AgentInvocation{
		{
			ID: "fresh", RunID: "metric-run", StepName: "review", Agent: "codex", Model: "gpt-5.6-sol", ModelProvider: &provider,
			SessionMode: db.InvocationModeStarted, StartedAt: at.Unix(), CompletedAt: at.Add(2 * time.Second).Unix(), DurationMS: 2000,
			ExitStatus: "ok", DeltaInputTokens: &firstInput, DeltaOutputTokens: &firstOutput,
			SubprocessWaitMS: &waitMS, ModelRoundtrips: &roundtrips, ToolCalls: &tools,
			ToolWaitCalls: &zero, ToolTestLintCalls: &zero, ToolEditCalls: &zero, ToolReadCalls: &zero, ToolGitCalls: &zero, ToolOtherCalls: &tools,
		},
		{
			ID: "resumed", RunID: "metric-run", StepName: "review", Agent: "codex", Model: "gpt-5.6-sol", ModelProvider: &provider,
			SessionMode: db.InvocationModeResumed, StartedAt: at.Add(3 * time.Second).Unix(), CompletedAt: at.Add(6 * time.Second).Unix(), DurationMS: 3000,
			ExitStatus: "ok", DeltaInputTokens: &resumedInput, DeltaOutputTokens: &resumedOutput,
			SubprocessWaitMS: &waitMS, ModelRoundtrips: &roundtrips, ToolCalls: &tools,
			ToolWaitCalls: &zero, ToolTestLintCalls: &zero, ToolEditCalls: &zero, ToolReadCalls: &zero, ToolGitCalls: &zero, ToolOtherCalls: &tools,
		},
		{
			ID: "unknown", RunID: "metric-run", StepName: "test", Agent: "copilot", SessionMode: db.InvocationModeCold,
			StartedAt: at.Add(7 * time.Second).Unix(), CompletedAt: at.Add(8 * time.Second).Unix(), DurationMS: 1000, ExitStatus: "ok",
		},
	}
	runtime.EmitSnapshot(Snapshot{
		Run:         &db.Run{ID: "metric-run", Status: types.RunCompleted, CreatedAt: at.Unix(), UpdatedAt: at.Add(9 * time.Second).Unix()},
		Invocations: invocations, MetricInvocations: invocations,
	})

	metrics := collectMetricData(t, reader)
	inputPoints := intHistogramPoints(t, metrics, metricGenAIClientTokenUsage, attrTokenType, "input")
	outputPoints := intHistogramPoints(t, metrics, metricGenAIClientTokenUsage, attrTokenType, "output")
	assertHistogramValues(t, inputPoints, []int64{100, 150})
	assertHistogramValues(t, outputPoints, []int64{20, 30})
	assertMetricAttributesApproved(t, metrics)
	if metricHasAttribute(metrics, attrInvocationID) || metricHasAttribute(metrics, attrRunID) {
		t.Fatal("metric dimensions contained durable identities")
	}
	if got := gaugeValue(t, metrics, metricTelemetryCoverage, map[attribute.Key]string{
		attrCapability: "token", attrCoverage: "unavailable", attrHarnessFamily: "other",
	}); got != 1 {
		t.Fatalf("unavailable token coverage = %d, want 1", got)
	}
	if got := gaugeValue(t, metrics, metricTelemetryCoverage, map[attribute.Key]string{
		attrCapability: "tool_activity", attrCoverage: "reported", attrHarnessFamily: "codex",
	}); got != 2 {
		t.Fatalf("reported activity coverage = %d, want 2", got)
	}
	if got := floatHistogramCount(t, metrics, metricSubprocessWaitDuration); got != 2 {
		t.Fatalf("subprocess wait histogram count = %d, want 2", got)
	}
}

func TestProjectorRestartReprojectsStableSpansWithoutDuplicateMetricAccounting(t *testing.T) {
	database, err := db.Open(t.TempDir() + "/projection.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repo, err := database.InsertRepo(t.TempDir(), "https://example.invalid/repo.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "feature", "head", "base")
	if err != nil {
		t.Fatal(err)
	}
	input, output := 100, 20
	invocation := db.AgentInvocation{
		RunID: run.ID, StepName: "review", Round: 1, Purpose: "review", Agent: "codex",
		SessionMode: db.InvocationModeStarted, StartedAt: 10, CompletedAt: 11, DurationMS: 1000,
		ExitStatus: "ok", DeltaInputTokens: &input, DeltaOutputTokens: &output,
	}
	if err := database.InsertAgentInvocationWithEvent(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}

	firstTrace := tracetest.NewInMemoryExporter()
	firstReader := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(deltaTemporality))
	first := newRuntimeWithExporterAndMetricReader(firstTrace, firstReader, processorConfig{Synchronous: true})
	first.database = database
	first.startProjector()
	database.SetRunTerminalHook(first.NotifyRun)
	if err := database.UpdateRunStatus(run.ID, types.RunCompleted); err != nil {
		t.Fatal(err)
	}
	waitForSpanCount(t, firstTrace, 2)
	firstMetrics := collectMetricData(t, firstReader)
	if !metricExists(firstMetrics, metricGenAIClientTokenUsage) {
		t.Fatal("first projection did not account token metrics")
	}
	firstInvocation := findSpan(t, firstTrace.GetSpans(), spanNameInvocation)
	database.SetRunTerminalHook(nil)
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	secondTrace := tracetest.NewInMemoryExporter()
	secondReader := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(deltaTemporality))
	second := newRuntimeWithExporterAndMetricReader(secondTrace, secondReader, processorConfig{Synchronous: true})
	second.database = database
	second.startProjector() // bounded startup replay finds the terminal run
	defer second.Shutdown(context.Background())
	waitForSpanCount(t, secondTrace, 2)
	secondInvocation := findSpan(t, secondTrace.GetSpans(), spanNameInvocation)
	if firstInvocation.SpanContext.TraceID() != secondInvocation.SpanContext.TraceID() ||
		firstInvocation.SpanContext.SpanID() != secondInvocation.SpanContext.SpanID() {
		t.Fatalf("restart changed invocation identity: first=%v second=%v", firstInvocation.SpanContext, secondInvocation.SpanContext)
	}
	secondMetrics := collectMetricData(t, secondReader)
	if metricExists(secondMetrics, metricGenAIClientTokenUsage) || metricExists(secondMetrics, metricInvocationDuration) {
		t.Fatal("restart reprojection duplicated additive invocation metrics")
	}
	if !metricExists(secondMetrics, metricTelemetryCoverage) {
		t.Fatal("restart replay did not rebuild current coverage from durable invocations")
	}
}

func TestCoverageStoreIsIdempotentAndMemoryBounded(t *testing.T) {
	store := &coverageStore{counts: map[coverageKey]int64{}, invocations: map[string][]coverageKey{}}
	key := coverageKey{capability: "token", coverage: "reported", harness: "codex"}
	store.setInvocation("duplicate", []coverageKey{key})
	store.setInvocation("duplicate", []coverageKey{key})
	for i := 1; i < coverageInvocationLimit+1; i++ {
		store.setInvocation(fmt.Sprintf("inv-%d", i), []coverageKey{key})
	}
	if len(store.invocations) != coverageInvocationLimit || len(store.order) != coverageInvocationLimit {
		t.Fatalf("coverage identities = %d/%d, want bounded %d", len(store.invocations), len(store.order), coverageInvocationLimit)
	}
	if got := store.counts[key]; got != coverageInvocationLimit {
		t.Fatalf("coverage count = %d, want %d", got, coverageInvocationLimit)
	}
	if _, retained := store.invocations["duplicate"]; retained {
		t.Fatal("coverage store did not evict its oldest identity")
	}
}

func TestNormalizeInvocationMetricDimensionsIsStrictlyBounded(t *testing.T) {
	providers := []string{"openai", "ANTHROPIC", "azure.ai.openai", "provider/customer/secret", ""}
	models := []string{"gpt-5.6-sol", "claude-opus-4-7", "o3", "customer-model-123", ""}
	agents := []string{"codex", "claude", "pi", "opencode", "copilot", "acp:customer-secret", ""}
	for _, provider := range providers {
		if got := normalizeProvider(provider); !oneOfString(got, "openai", "anthropic", "azure.ai.openai", "other") {
			t.Fatalf("normalizeProvider(%q) = %q", provider, got)
		}
	}
	for _, model := range models {
		if got := normalizeModelFamily(model); !oneOfString(got, "gpt", "claude", "other") {
			t.Fatalf("normalizeModelFamily(%q) = %q", model, got)
		}
	}
	for _, agentName := range agents {
		if got := normalizeHarnessFamily(agentName); !oneOfString(got, "codex", "claude", "pi", "opencode", "other") {
			t.Fatalf("normalizeHarnessFamily(%q) = %q", agentName, got)
		}
	}
}

func assertOnlyApprovedInvocationAttributes(t *testing.T, attrs []attribute.KeyValue) {
	t.Helper()
	for _, kv := range attrs {
		if !approvedInvocationSpanAttribute(kv.Key) {
			t.Fatalf("unapproved invocation span attribute %q", kv.Key)
		}
	}
}

func hasAttribute(attrs []attribute.KeyValue, key attribute.Key) bool {
	for _, kv := range attrs {
		if kv.Key == key {
			return true
		}
	}
	return false
}

func contains(value, substring string) bool {
	for i := 0; i+len(substring) <= len(value); i++ {
		if value[i:i+len(substring)] == substring {
			return true
		}
	}
	return false
}

func deltaTemporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.DeltaTemporality
}

func collectMetricData(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var out metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &out); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return out
}

func intHistogramPoints(t *testing.T, metrics metricdata.ResourceMetrics, name string, key attribute.Key, value string) []metricdata.HistogramDataPoint[int64] {
	t.Helper()
	for _, scope := range metrics.ScopeMetrics {
		for _, candidate := range scope.Metrics {
			if candidate.Name != name {
				continue
			}
			histogram, ok := candidate.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Fatalf("metric %s data = %T, want int histogram", name, candidate.Data)
			}
			var points []metricdata.HistogramDataPoint[int64]
			for _, point := range histogram.DataPoints {
				if attributeString(point.Attributes, key) == value {
					points = append(points, point)
				}
			}
			return points
		}
	}
	t.Fatalf("metric %s not found", name)
	return nil
}

func assertHistogramValues(t *testing.T, points []metricdata.HistogramDataPoint[int64], want []int64) {
	t.Helper()
	var count uint64
	var sum int64
	var minimum, maximum int64
	for i, point := range points {
		pointMin, minDefined := point.Min.Value()
		pointMax, maxDefined := point.Max.Value()
		if !minDefined || !maxDefined {
			t.Fatalf("histogram point = %#v, want defined extrema", point)
		}
		if i == 0 || pointMin < minimum {
			minimum = pointMin
		}
		if i == 0 || pointMax > maximum {
			maximum = pointMax
		}
		count += point.Count
		sum += point.Sum
	}
	var wantSum int64
	wantMin, wantMax := want[0], want[0]
	for _, value := range want {
		wantSum += value
		if value < wantMin {
			wantMin = value
		}
		if value > wantMax {
			wantMax = value
		}
	}
	if count != uint64(len(want)) || sum != wantSum || minimum != wantMin || maximum != wantMax {
		t.Fatalf("histogram summary = count %d sum %d min %d max %d, want values %v", count, sum, minimum, maximum, want)
	}
}

func attributeString(set attribute.Set, key attribute.Key) string {
	value, ok := set.Value(key)
	if !ok {
		return ""
	}
	return value.AsString()
}

func gaugeValue(t *testing.T, metrics metricdata.ResourceMetrics, name string, dimensions map[attribute.Key]string) int64 {
	t.Helper()
	for _, scope := range metrics.ScopeMetrics {
		for _, candidate := range scope.Metrics {
			if candidate.Name != name {
				continue
			}
			gauge, ok := candidate.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("metric %s data = %T, want int gauge", name, candidate.Data)
			}
			for _, point := range gauge.DataPoints {
				matched := true
				for key, want := range dimensions {
					if attributeString(point.Attributes, key) != want {
						matched = false
					}
				}
				if matched {
					return point.Value
				}
			}
		}
	}
	t.Fatalf("gauge %s dimensions %v not found", name, dimensions)
	return 0
}

func floatHistogramCount(t *testing.T, metrics metricdata.ResourceMetrics, name string) uint64 {
	t.Helper()
	for _, scope := range metrics.ScopeMetrics {
		for _, candidate := range scope.Metrics {
			if candidate.Name != name {
				continue
			}
			histogram, ok := candidate.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("metric %s data = %T, want float histogram", name, candidate.Data)
			}
			var count uint64
			for _, point := range histogram.DataPoints {
				count += point.Count
			}
			return count
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func metricExists(metrics metricdata.ResourceMetrics, name string) bool {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return true
			}
		}
	}
	return false
}

func metricHasAttribute(metrics metricdata.ResourceMetrics, key attribute.Key) bool {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			switch data := metric.Data.(type) {
			case metricdata.Histogram[int64]:
				for _, point := range data.DataPoints {
					if _, ok := point.Attributes.Value(key); ok {
						return true
					}
				}
			case metricdata.Histogram[float64]:
				for _, point := range data.DataPoints {
					if _, ok := point.Attributes.Value(key); ok {
						return true
					}
				}
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					if _, ok := point.Attributes.Value(key); ok {
						return true
					}
				}
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					if _, ok := point.Attributes.Value(key); ok {
						return true
					}
				}
			}
		}
	}
	return false
}

func assertMetricAttributesApproved(t *testing.T, metrics metricdata.ResourceMetrics) {
	t.Helper()
	for _, scope := range metrics.ScopeMetrics {
		for _, candidate := range scope.Metrics {
			visit := func(set attribute.Set) {
				for _, kv := range set.ToSlice() {
					if !approvedMetricAttribute(kv.Key) {
						t.Fatalf("metric %s used unapproved dimension %q", candidate.Name, kv.Key)
					}
				}
			}
			switch data := candidate.Data.(type) {
			case metricdata.Histogram[int64]:
				for _, point := range data.DataPoints {
					visit(point.Attributes)
				}
			case metricdata.Histogram[float64]:
				for _, point := range data.DataPoints {
					visit(point.Attributes)
				}
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					visit(point.Attributes)
				}
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					visit(point.Attributes)
				}
			}
		}
	}
}

func oneOfString(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
