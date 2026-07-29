// Package oteltrace exports a bounded metadata-only reconstruction of durable
// no-mistakes lifecycle facts. It is deliberately separate from the existing
// anonymous product telemetry package.
package oteltrace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/kunchenguid/no-mistakes/internal/oteltrace"

	defaultQueueSize     = 256
	defaultBatchSize     = 64
	defaultBatchTimeout  = 250 * time.Millisecond
	defaultExportTimeout = time.Second
	defaultInitTimeout   = 500 * time.Millisecond
	defaultRetryElapsed  = time.Second
	maxOTLPRequestBytes  = 1 << 20
)

type healthState string

const (
	healthDisabled      healthState = "disabled"
	healthReady         healthState = "ready"
	healthDegraded      healthState = "degraded"
	healthMisconfigured healthState = "misconfigured"
	healthStopped       healthState = "stopped"
)

// Health is the bounded native-OTLP status exposed through daemon capability
// discovery. It never includes the endpoint, headers, errors, or credentials.
type Health struct {
	Enabled                bool
	State                  healthState
	Protocol               string
	QueueCapacity          int
	MetricCardinalityLimit int
	InvocationSpans        bool
	GenAIMetrics           bool
	ExactUsage             bool
	SessionFallbackMetrics bool
	ActivityMetrics        bool
	CoverageMetrics        bool
	DurableMetricDedupe    bool
	ContentCapture         bool
}

type processorConfig struct {
	Synchronous   bool
	QueueSize     int
	BatchSize     int
	BatchTimeout  time.Duration
	ExportTimeout time.Duration
}

// Runtime owns the optional SDK provider. Production uses a non-blocking batch
// processor; tests may use the synchronous processor to inspect exact spans.
type Runtime struct {
	provider       *sdktrace.TracerProvider
	tracer         trace.Tracer
	metricProvider *sdkmetric.MeterProvider
	metrics        *invocationInstruments
	coverage       *coverageStore
	otelResource   *resource.Resource
	enabled        bool
	tracesEnabled  bool
	metricsEnabled bool
	protocol       string
	queue          int
	state          atomic.Uint32
	traceDegraded  atomic.Bool
	metricDegraded atomic.Bool
	stopOnce       sync.Once

	database *db.DB
	notify   chan string
	stop     chan struct{}
	done     chan struct{}
}

var (
	healthStates = [...]healthState{
		healthDisabled,
		healthReady,
		healthDegraded,
		healthMisconfigured,
		healthStopped,
	}
	installErrorHandler sync.Once
)

const (
	healthIndexDisabled uint32 = iota
	healthIndexReady
	healthIndexDegraded
	healthIndexMisconfigured
	healthIndexStopped
)

// NewFromEnvironment creates native OTLP export only when a standard OTLP
// endpoint variable is present and valid. Trace and metric signals initialize
// independently under one bounded health surface. Configuration and collector
// failure are diagnostics only and never returned into daemon or pipeline work.
func NewFromEnvironment(database *db.DB, serviceVersion string) *Runtime {
	cfg := parseEnvironment(os.Getenv)
	switch cfg.State {
	case configDisabled:
		return disabledRuntime(healthIndexDisabled, cfg.Protocol)
	case configMalformed:
		return disabledRuntime(healthIndexMisconfigured, cfg.Protocol)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultInitTimeout)
	defer cancel()
	var r *Runtime
	traceFailed, metricFailed := false, false
	if cfg.TracesEnabled {
		exporter, err := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpointURL(cfg.TracesEndpoint),
			otlptracehttp.WithTimeout(defaultExportTimeout),
			otlptracehttp.WithMaxRequestSize(maxOTLPRequestBytes),
			otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
				Enabled: true, InitialInterval: 100 * time.Millisecond,
				MaxInterval: 500 * time.Millisecond, MaxElapsedTime: defaultRetryElapsed,
			}),
		)
		if err != nil {
			traceFailed = true
		} else {
			r = newRuntimeWithExporterAndVersion(exporter, processorConfig{}, serviceVersion)
			r.tracesEnabled = true
		}
	}
	if r == nil {
		r = &Runtime{protocol: protocolHTTPProtobuf, otelResource: runtimeResource(serviceVersion)}
		r.state.Store(healthIndexReady)
	}
	if cfg.MetricsEnabled {
		exporter, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpointURL(cfg.MetricsEndpoint),
			otlpmetrichttp.WithTimeout(defaultExportTimeout),
			otlpmetrichttp.WithMaxRequestSize(maxOTLPRequestBytes),
			otlpmetrichttp.WithTemporalitySelector(deltaMetricTemporality),
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				Enabled: true, InitialInterval: 100 * time.Millisecond,
				MaxInterval: 500 * time.Millisecond, MaxElapsedTime: defaultRetryElapsed,
			}),
		)
		if err != nil {
			metricFailed = true
		} else {
			wrapped := &healthMetricExporter{inner: exporter, runtime: r}
			reader := sdkmetric.NewPeriodicReader(wrapped,
				sdkmetric.WithInterval(defaultBatchTimeout),
				sdkmetric.WithTimeout(defaultExportTimeout),
			)
			if err := r.initMetrics(reader, r.otelResource, serviceVersion); err != nil {
				metricFailed = true
			} else {
				r.metricsEnabled = true
			}
		}
	}
	r.enabled = r.tracesEnabled || r.metricsEnabled
	r.protocol = cfg.Protocol
	r.traceDegraded.Store(traceFailed)
	r.metricDegraded.Store(metricFailed)
	r.updateExportHealth()
	if !r.enabled {
		return disabledRuntime(healthIndexDegraded, cfg.Protocol)
	}
	installNativeOTLPErrorHandler()
	r.database = database
	if database != nil {
		r.startProjector()
	}
	return r
}

func disabledRuntime(state uint32, protocol string) *Runtime {
	r := &Runtime{protocol: protocol}
	r.state.Store(state)
	return r
}

func newRuntimeWithExporter(exporter sdktrace.SpanExporter, cfg processorConfig) *Runtime {
	return newRuntimeWithExporterAndVersion(exporter, cfg, "test")
}

func newRuntimeWithExporterAndMetricReader(exporter sdktrace.SpanExporter, reader sdkmetric.Reader, cfg processorConfig) *Runtime {
	r := newRuntimeWithExporterAndVersion(exporter, cfg, "test")
	if err := r.initMetrics(reader, r.otelResource, "test"); err != nil {
		r.state.Store(healthIndexDegraded)
	} else {
		r.metricsEnabled = true
	}
	return r
}

func newRuntimeWithExporterAndVersion(exporter sdktrace.SpanExporter, cfg processorConfig, serviceVersion string) *Runtime {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > cfg.QueueSize {
		cfg.BatchSize = min(defaultBatchSize, cfg.QueueSize)
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = defaultBatchTimeout
	}
	if cfg.ExportTimeout <= 0 {
		cfg.ExportTimeout = defaultExportTimeout
	}

	r := &Runtime{enabled: true, tracesEnabled: true, protocol: protocolHTTPProtobuf, queue: cfg.QueueSize}
	r.state.Store(healthIndexReady)
	wrapped := &healthExporter{inner: exporter, runtime: r}
	res := runtimeResource(serviceVersion)
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithIDGenerator(deterministicIDGenerator{}),
		sdktrace.WithSpanLimits(sdktrace.SpanLimits{
			AttributeValueLengthLimit:   256,
			AttributeCountLimit:         8,
			EventCountLimit:             16,
			LinkCountLimit:              0,
			AttributePerEventCountLimit: 4,
			AttributePerLinkCountLimit:  0,
		}),
	}
	if cfg.Synchronous {
		options = append(options, sdktrace.WithSyncer(wrapped))
	} else {
		options = append(options, sdktrace.WithBatcher(wrapped,
			sdktrace.WithMaxQueueSize(cfg.QueueSize),
			sdktrace.WithMaxExportBatchSize(cfg.BatchSize),
			sdktrace.WithBatchTimeout(cfg.BatchTimeout),
			sdktrace.WithExportTimeout(cfg.ExportTimeout),
		))
	}
	r.otelResource = res
	r.provider = sdktrace.NewTracerProvider(options...)
	r.tracer = r.provider.Tracer(instrumentationName,
		trace.WithInstrumentationVersion(serviceVersion),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	installNativeOTLPErrorHandler()
	return r
}

func runtimeResource(serviceVersion string) *resource.Resource {
	return resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName("no-mistakes"),
		semconv.ServiceVersion(serviceVersion),
		semconv.ServiceInstanceID(newInstanceID()),
		semconv.DeploymentEnvironmentName("local"),
		semconv.OSTypeKey.String(runtime.GOOS),
		semconv.ProcessRuntimeName("go"),
	)
}

func installNativeOTLPErrorHandler() {
	// The SDK otherwise reports exporter failures through a process-global raw
	// error callback. Replace that once with a bounded value-free diagnostic.
	installErrorHandler.Do(func() {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
			slog.Warn("native OTLP export skipped", "reason", "export_failed")
		}))
	})
}

// Health returns a fixed-shape, value-free status snapshot.
func (r *Runtime) Health() Health {
	if r == nil {
		return Health{State: healthDisabled}
	}
	index := r.state.Load()
	state := healthDegraded
	if int(index) < len(healthStates) {
		state = healthStates[index]
	}
	metricLimit := 0
	if r.metricsEnabled {
		metricLimit = metricCardinalityLimit
	}
	return Health{
		Enabled: r.enabled, State: state, Protocol: r.protocol,
		QueueCapacity: r.queue, MetricCardinalityLimit: metricLimit,
		InvocationSpans: r.tracesEnabled, GenAIMetrics: r.metricsEnabled,
		ExactUsage: r.metricsEnabled, SessionFallbackMetrics: r.metricsEnabled,
		ActivityMetrics: r.metricsEnabled, CoverageMetrics: r.metricsEnabled,
		DurableMetricDedupe: r.metricsEnabled, ContentCapture: false,
	}
}

// ForceFlush is test and shutdown support. Pipeline work never calls it.
func (r *Runtime) ForceFlush(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var errs []error
	if r.provider != nil {
		errs = append(errs, r.provider.ForceFlush(ctx))
	}
	if r.metricProvider != nil {
		errs = append(errs, r.metricProvider.ForceFlush(ctx))
	}
	return errors.Join(errs...)
}

// Shutdown stops the projection worker and bounds SDK flush/export by ctx.
// Its result is diagnostics only and must never become a daemon exit failure.
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var err error
	r.stopOnce.Do(func() {
		if r.stop != nil {
			close(r.stop)
			select {
			case <-r.done:
			case <-ctx.Done():
			}
		}
		var errs []error
		if r.metricProvider != nil {
			errs = append(errs, r.metricProvider.Shutdown(ctx))
		}
		if r.provider != nil {
			errs = append(errs, r.provider.Shutdown(ctx))
		}
		err = errors.Join(errs...)
		r.state.Store(healthIndexStopped)
	})
	return err
}

type healthExporter struct {
	inner   sdktrace.SpanExporter
	runtime *Runtime
}

func (e *healthExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.inner.ExportSpans(ctx, spans)
	e.runtime.traceDegraded.Store(err != nil)
	e.runtime.updateExportHealth()
	return err
}

func (e *healthExporter) Shutdown(ctx context.Context) error {
	err := e.inner.Shutdown(ctx)
	e.runtime.traceDegraded.Store(err != nil)
	e.runtime.updateExportHealth()
	return err
}

type healthMetricExporter struct {
	inner   sdkmetric.Exporter
	runtime *Runtime
}

func (e *healthMetricExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return e.inner.Temporality(kind)
}

func (e *healthMetricExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return e.inner.Aggregation(kind)
}

func (e *healthMetricExporter) Export(ctx context.Context, metrics *metricdata.ResourceMetrics) error {
	err := e.inner.Export(ctx, metrics)
	e.runtime.metricDegraded.Store(err != nil)
	e.runtime.updateExportHealth()
	return err
}

func (e *healthMetricExporter) ForceFlush(ctx context.Context) error {
	err := e.inner.ForceFlush(ctx)
	e.runtime.metricDegraded.Store(err != nil)
	e.runtime.updateExportHealth()
	return err
}

func (e *healthMetricExporter) Shutdown(ctx context.Context) error {
	err := e.inner.Shutdown(ctx)
	e.runtime.metricDegraded.Store(err != nil)
	e.runtime.updateExportHealth()
	return err
}

func (r *Runtime) updateExportHealth() {
	if r == nil || r.state.Load() == healthIndexStopped {
		return
	}
	if r.traceDegraded.Load() || r.metricDegraded.Load() {
		r.state.Store(healthIndexDegraded)
		return
	}
	r.state.Store(healthIndexReady)
}

func newInstanceID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(value[:])
}
