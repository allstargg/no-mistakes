package oteltrace

import (
	"context"
	"sync"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// The largest approved instrument has at most 16 providers * 6 model families
// * 6 harness families * 3 outcomes = 1,728 series per collection. Keep the
// SDK cap just above that closed theoretical maximum so valid registry values
// never spill into an unapproved overflow dimension.
const (
	metricCardinalityLimit  = 2048
	coverageInvocationLimit = 4096
)

var (
	tokenBoundaries             = []float64{1, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}
	operationDurationBoundaries = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
	waitDurationBoundaries      = []float64{1, 5, 15, 60, 300, 900, 1800, 3600, 7200, 14400, 43200, 86400}
)

type invocationInstruments struct {
	tokens             otelmetric.Int64Histogram
	operationDuration  otelmetric.Float64Histogram
	invocationDuration otelmetric.Float64Histogram
	fallbacks          otelmetric.Int64Counter
	subprocessWait     otelmetric.Float64Histogram
	coverage           otelmetric.Int64ObservableGauge
	coverageReg        otelmetric.Registration
}

type coverageKey struct {
	capability string
	coverage   string
	harness    string
}

type coverageStore struct {
	mu          sync.Mutex
	counts      map[coverageKey]int64
	invocations map[string][]coverageKey
	order       []string
}

// initMetrics installs the fixed Tracewake registry instruments on reader.
// Production passes a bounded periodic reader; tests pass a manual reader.
func (r *Runtime) initMetrics(reader sdkmetric.Reader, res *resource.Resource, serviceVersion string) error {
	if r == nil || reader == nil {
		return nil
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
		sdkmetric.WithCardinalityLimit(metricCardinalityLimit),
		sdkmetric.WithView(metricHistogramView(metricGenAIClientTokenUsage, tokenBoundaries)),
		sdkmetric.WithView(metricHistogramView(metricGenAIClientOperation, operationDurationBoundaries)),
		sdkmetric.WithView(metricHistogramView(metricInvocationDuration, operationDurationBoundaries)),
		sdkmetric.WithView(metricHistogramView(metricSubprocessWaitDuration, waitDurationBoundaries)),
	)
	meter := provider.Meter(instrumentationName,
		otelmetric.WithInstrumentationVersion(serviceVersion),
		otelmetric.WithSchemaURL(semconv.SchemaURL),
	)
	var instruments invocationInstruments
	var err error
	if instruments.tokens, err = meter.Int64Histogram(metricGenAIClientTokenUsage,
		otelmetric.WithUnit("{token}"), otelmetric.WithDescription("Reported per-invocation token usage.")); err != nil {
		_ = provider.Shutdown(context.Background())
		return err
	}
	if instruments.operationDuration, err = meter.Float64Histogram(metricGenAIClientOperation,
		otelmetric.WithUnit("s"), otelmetric.WithDescription("GenAI invocation duration.")); err != nil {
		_ = provider.Shutdown(context.Background())
		return err
	}
	if instruments.invocationDuration, err = meter.Float64Histogram(metricInvocationDuration,
		otelmetric.WithUnit("s"), otelmetric.WithDescription("No-mistakes agent invocation duration.")); err != nil {
		_ = provider.Shutdown(context.Background())
		return err
	}
	if instruments.fallbacks, err = meter.Int64Counter(metricSessionFallbacks,
		otelmetric.WithUnit("{fallback}"), otelmetric.WithDescription("Resume-session fallbacks.")); err != nil {
		_ = provider.Shutdown(context.Background())
		return err
	}
	if instruments.subprocessWait, err = meter.Float64Histogram(metricSubprocessWaitDuration,
		otelmetric.WithUnit("s"), otelmetric.WithDescription("Tool subprocess wait duration.")); err != nil {
		_ = provider.Shutdown(context.Background())
		return err
	}
	if instruments.coverage, err = meter.Int64ObservableGauge(metricTelemetryCoverage,
		otelmetric.WithUnit("1"), otelmetric.WithDescription("Projected invocation coverage by bounded capability.")); err != nil {
		_ = provider.Shutdown(context.Background())
		return err
	}
	store := &coverageStore{
		counts:      make(map[coverageKey]int64, 64),
		invocations: make(map[string][]coverageKey, 256),
		order:       make([]string, 0, 256),
	}
	instruments.coverageReg, err = meter.RegisterCallback(func(_ context.Context, observer otelmetric.Observer) error {
		store.mu.Lock()
		defer store.mu.Unlock()
		for key, value := range store.counts {
			observer.ObserveInt64(instruments.coverage, value, otelmetric.WithAttributes(
				attrCapability.String(key.capability),
				attrCoverage.String(key.coverage),
				attrHarnessFamily.String(key.harness),
			))
		}
		return nil
	}, instruments.coverage)
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return err
	}
	r.metricProvider = provider
	r.metrics = &instruments
	r.coverage = store
	return nil
}

func metricHistogramView(name string, boundaries []float64) sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{Name: name},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: boundaries}},
	)
}

func deltaMetricTemporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.DeltaTemporality
}

func (r *Runtime) emitInvocationMetrics(invocations []db.AgentInvocation) {
	if r == nil || r.metrics == nil || len(invocations) == 0 {
		return
	}
	ctx := context.Background()
	for i := range invocations {
		inv := &invocations[i]
		if inv.ID == "" || inv.DurationMS < 0 {
			continue
		}
		r.recordInvocationMetrics(ctx, inv)
	}
}

func (r *Runtime) recordInvocationMetrics(ctx context.Context, inv *db.AgentInvocation) {
	harness := normalizeHarnessFamily(inv.Agent)
	provider := "other"
	if inv.ModelProvider != nil {
		provider = normalizeProvider(*inv.ModelProvider)
	}
	model := normalizeModelFamily(inv.Model)
	operationAttrs := []attribute.KeyValue{
		attrProviderName.String(provider),
		attrOperationName.String("invoke_agent"),
		attrModelFamily.String(model),
		attrHarnessFamily.String(harness),
	}
	r.metrics.operationDuration.Record(ctx, float64(inv.DurationMS)/1000,
		otelmetric.WithAttributes(append(operationAttrs, attrOutcome.String(invocationOutcome(inv.ExitStatus)))...),
	)

	invocationAttrs := []attribute.KeyValue{
		attrStepName.String(registryStepName(types.StepName(inv.StepName))),
		attrHarnessFamily.String(harness),
		attrModelFamily.String(model),
	}
	if category := invocationFailureCategory(inv.ExitStatus, inv.FailureCategory); category != "" {
		invocationAttrs = append(invocationAttrs, attrFailureCategory.String(category))
	}
	r.metrics.invocationDuration.Record(ctx, float64(inv.DurationMS)/1000, otelmetric.WithAttributes(invocationAttrs...))

	if inv.DeltaInputTokens != nil && *inv.DeltaInputTokens >= 0 {
		attrs := appendMetricAttribute(operationAttrs, attrTokenType.String("input"))
		r.metrics.tokens.Record(ctx, int64(*inv.DeltaInputTokens), otelmetric.WithAttributes(attrs...))
	}
	if inv.DeltaOutputTokens != nil && *inv.DeltaOutputTokens >= 0 {
		attrs := appendMetricAttribute(operationAttrs, attrTokenType.String("output"))
		r.metrics.tokens.Record(ctx, int64(*inv.DeltaOutputTokens), otelmetric.WithAttributes(attrs...))
	}
	if inv.SessionMode == db.InvocationModeFallback {
		r.metrics.fallbacks.Add(ctx, 1, otelmetric.WithAttributes(
			attrSessionMode.String("fallback"), attrHarnessFamily.String(harness),
		))
	}
	if inv.SubprocessWaitMS != nil && *inv.SubprocessWaitMS >= 0 {
		r.metrics.subprocessWait.Record(ctx, float64(*inv.SubprocessWaitMS)/1000,
			otelmetric.WithAttributes(attrStepName.String(registryStepName(types.StepName(inv.StepName)))),
		)
	}
}

func (r *Runtime) recordInvocationCoverage(invocations []db.AgentInvocation) {
	if r == nil || r.coverage == nil {
		return
	}
	for i := range invocations {
		inv := &invocations[i]
		if inv.ID == "" {
			continue
		}
		harness := normalizeHarnessFamily(inv.Agent)
		entries := []coverageKey{
			{capability: "token", coverage: tokenCoverage(inv), harness: harness},
			{capability: "model_time", coverage: pointerCoverage(inv.SubprocessWaitMS != nil, false), harness: harness},
			{capability: "tool_activity", coverage: activityCoverage(inv), harness: harness},
			{capability: "provider_identity", coverage: pointerCoverage(inv.ModelProvider != nil && *inv.ModelProvider != "", false), harness: harness},
		}
		r.coverage.setInvocation(inv.ID, entries)
	}
}

func appendMetricAttribute(attrs []attribute.KeyValue, value attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, len(attrs), len(attrs)+1)
	copy(out, attrs)
	return append(out, value)
}

func (s *coverageStore) setInvocation(id string, entries []coverageKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.invocations[id]; exists {
		return
	}
	if len(s.order) == coverageInvocationLimit {
		evicted := s.order[0]
		s.order = s.order[1:]
		for _, key := range s.invocations[evicted] {
			s.counts[key]--
			if s.counts[key] == 0 {
				delete(s.counts, key)
			}
		}
		delete(s.invocations, evicted)
	}
	s.invocations[id] = append([]coverageKey(nil), entries...)
	s.order = append(s.order, id)
	for _, key := range entries {
		s.counts[key]++
	}
}

func tokenCoverage(inv *db.AgentInvocation) string {
	input := inv.DeltaInputTokens != nil
	output := inv.DeltaOutputTokens != nil
	return pointerCoverage(input && output, input || output)
}

func activityCoverage(inv *db.AgentInvocation) string {
	known := []bool{
		inv.ModelRoundtrips != nil, inv.ToolCalls != nil, inv.ToolWaitCalls != nil,
		inv.ToolTestLintCalls != nil, inv.ToolEditCalls != nil, inv.ToolReadCalls != nil,
		inv.ToolGitCalls != nil, inv.ToolOtherCalls != nil, inv.SubprocessWaitMS != nil,
	}
	all, any := true, false
	for _, value := range known {
		all = all && value
		any = any || value
	}
	return pointerCoverage(all, any)
}

func pointerCoverage(reported, partial bool) string {
	if reported {
		return "reported"
	}
	if partial {
		return "partial"
	}
	return "unavailable"
}

func approvedMetricAttribute(key attribute.Key) bool {
	switch key {
	case attrProviderName, attrOperationName, attrTokenType, attrModelFamily, attrHarnessFamily,
		attrOutcome, attrStepName, attrFailureCategory, attrSessionMode, attrCapability, attrCoverage:
		return true
	default:
		return false
	}
}
