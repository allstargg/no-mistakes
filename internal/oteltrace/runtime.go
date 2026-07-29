// Package oteltrace exports a bounded metadata-only reconstruction of durable
// no-mistakes lifecycle facts. It is deliberately separate from the existing
// anonymous product telemetry package.
package oteltrace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
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
	Enabled        bool
	State          healthState
	Protocol       string
	QueueCapacity  int
	ContentCapture bool
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
	provider *sdktrace.TracerProvider
	tracer   trace.Tracer
	enabled  bool
	protocol string
	queue    int
	state    atomic.Uint32
	stopOnce sync.Once

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
// endpoint variable is present and valid. Configuration and collector failure
// are represented in Health and never returned into daemon or pipeline work.
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
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(cfg.Endpoint),
		otlptracehttp.WithTimeout(defaultExportTimeout),
		otlptracehttp.WithMaxRequestSize(maxOTLPRequestBytes),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled: true, InitialInterval: 100 * time.Millisecond,
			MaxInterval: 500 * time.Millisecond, MaxElapsedTime: defaultRetryElapsed,
		}),
	)
	if err != nil {
		return disabledRuntime(healthIndexDegraded, cfg.Protocol)
	}
	r := newRuntimeWithExporterAndVersion(exporter, processorConfig{}, serviceVersion)
	r.protocol = cfg.Protocol
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

	r := &Runtime{enabled: true, protocol: protocolHTTPProtobuf, queue: cfg.QueueSize}
	r.state.Store(healthIndexReady)
	wrapped := &healthExporter{inner: exporter, runtime: r}
	res := resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName("no-mistakes"),
		semconv.ServiceVersion(serviceVersion),
		semconv.ServiceInstanceID(newInstanceID()),
		semconv.DeploymentEnvironmentName("local"),
		semconv.OSTypeKey.String(runtime.GOOS),
		semconv.ProcessRuntimeName("go"),
	)
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
	r.provider = sdktrace.NewTracerProvider(options...)
	r.tracer = r.provider.Tracer(instrumentationName,
		trace.WithInstrumentationVersion(serviceVersion),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	// The SDK otherwise reports exporter failures through a process-global raw
	// error callback. Replace that once with a bounded value-free diagnostic.
	installErrorHandler.Do(func() {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
			slog.Warn("native OTLP trace export skipped", "reason", "export_failed")
		}))
	})
	return r
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
	return Health{
		Enabled: r.enabled, State: state, Protocol: r.protocol,
		QueueCapacity: r.queue, ContentCapture: false,
	}
}

// ForceFlush is test and shutdown support. Pipeline work never calls it.
func (r *Runtime) ForceFlush(ctx context.Context) error {
	if r == nil || r.provider == nil {
		return nil
	}
	return r.provider.ForceFlush(ctx)
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
		if r.provider != nil {
			err = r.provider.Shutdown(ctx)
		}
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
	if err != nil {
		e.runtime.state.Store(healthIndexDegraded)
	} else {
		e.runtime.state.CompareAndSwap(healthIndexDegraded, healthIndexReady)
	}
	return err
}

func (e *healthExporter) Shutdown(ctx context.Context) error {
	err := e.inner.Shutdown(ctx)
	if err != nil {
		e.runtime.state.Store(healthIndexDegraded)
	}
	return err
}

func newInstanceID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(value[:])
}
