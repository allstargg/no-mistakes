package oteltrace

import (
	"context"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
	"github.com/kunchenguid/no-mistakes/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Snapshot is a bounded, metadata-only view of one terminal run. All fields
// come from the existing durable run, step, and TW-34 lifecycle owners. The
// exporter accepts no prompt, response, log, diff, file, command, URL, or raw
// error argument.
type Snapshot struct {
	Run    *db.Run
	Steps  []*db.StepResult
	Events []*db.MetadataEvent
}

// EmitSnapshot reconstructs ended logical lifecycle spans from source facts.
// No SDK span object is kept alive while a pipeline runs or across a daemon
// restart. A recovered run is therefore represented truthfully as a completed
// logical operation whose end is the durable recovery transition, rather than
// pretending that a process-local span survived the crash.
func (r *Runtime) EmitSnapshot(snapshot Snapshot) {
	if r == nil || !r.enabled || r.tracer == nil || snapshot.Run == nil || !terminalRun(snapshot.Run.Status) {
		return
	}
	r.emitSnapshot(snapshot)
}

func (r *Runtime) emitSnapshot(snapshot Snapshot) {
	run := snapshot.Run
	start := unixSourceTime(run.CreatedAt)
	end := unixSourceTime(run.UpdatedAt)
	if end.Before(start) {
		end = start
	}

	parent := incomingParent(run)
	runCtx, runSpan := r.tracer.Start(
		withSpanIdentity(parent, "run:"+run.ID),
		spanNameRun,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(start),
		trace.WithAttributes(runAttributes(run, snapshot.Steps)...),
	)
	setRunStatus(runSpan, run.Status)

	stepContexts := make(map[string]context.Context, len(snapshot.Steps))
	var ciContext context.Context
	for _, step := range snapshot.Steps {
		if !terminalStep(step) || step.StartedAt == nil || step.CompletedAt == nil {
			continue
		}
		stepStart := unixSourceTime(*step.StartedAt)
		stepEnd := unixSourceTime(*step.CompletedAt)
		if stepEnd.Before(stepStart) {
			stepEnd = stepStart
		}
		stepCtx, span := r.tracer.Start(
			withSpanIdentity(runCtx, "step:"+run.ID+":"+step.ID),
			spanNameStep,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithTimestamp(stepStart),
			trace.WithAttributes(stepAttributes(run, step)...),
		)
		setStepStatus(span, step.Status)
		stepContexts[string(step.StepName)] = stepCtx
		if step.StepName == types.StepCI {
			ciContext = stepCtx
			addCIEvents(span, snapshot.Events)
		}
		span.End(trace.WithTimestamp(stepEnd))
	}
	if ciContext == nil {
		addCIEvents(runSpan, snapshot.Events)
	}
	emitCIFailureSpans(r, runCtx, ciContext, run.ID, snapshot.Events)

	emitGateSpans(r, runCtx, stepContexts, snapshot.Events)
	runSpan.End(trace.WithTimestamp(end))
}

func incomingParent(run *db.Run) context.Context {
	base := context.Background()
	if run == nil || run.Traceparent == nil {
		return base
	}
	tracestate := ""
	if run.Tracestate != nil {
		tracestate = *run.Tracestate
	}
	validated := tracecontext.Parse(*run.Traceparent, tracestate)
	if validated.Context == nil {
		return base
	}
	carrier := propagation.MapCarrier{"traceparent": validated.Context.Traceparent}
	if validated.Context.Tracestate != "" {
		carrier["tracestate"] = validated.Context.Tracestate
	}
	extracted := propagation.TraceContext{}.Extract(base, carrier)
	if !trace.SpanContextFromContext(extracted).IsValid() {
		return base
	}
	return extracted
}

func runAttributes(run *db.Run, steps []*db.StepResult) []attribute.KeyValue {
	return []attribute.KeyValue{
		attrRunID.String(run.ID),
		attrOutcome.String(runOutcome(run.Status)),
		attrPhase.String(runPhase(run.Status, steps)),
	}
}

func stepAttributes(run *db.Run, step *db.StepResult) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attrStepName.String(registryStepName(step.StepName)),
		attrRunID.String(run.ID),
		attrOutcome.String(stepOutcome(step.Status)),
	}
	if category := stepFailureCategory(run, step); category != "" {
		attrs = append(attrs, attrFailureCategory.String(category))
	}
	return attrs
}

func emitGateSpans(r *Runtime, runCtx context.Context, stepContexts map[string]context.Context, events []*db.MetadataEvent) {
	entered := make(map[string]*db.MetadataEvent)
	for _, event := range events {
		if event == nil || event.Gate == nil {
			continue
		}
		if event.Type == db.EventTypeGateEntered {
			entered[event.Gate.GateID] = event
			continue
		}
		if event.Type != db.EventTypeGateExited {
			continue
		}
		startEvent := entered[event.Gate.GateID]
		if startEvent == nil || startEvent.Gate == nil {
			continue
		}
		parent := runCtx
		if stepCtx := stepContexts[startEvent.Gate.Step]; stepCtx != nil {
			parent = stepCtx
		}
		start := startEvent.SourceTimestamp.UTC()
		end := event.SourceTimestamp.UTC()
		if end.Before(start) {
			end = start
		}
		kind := registryGateKind(startEvent.Gate.Class)
		_, span := r.tracer.Start(
			withSpanIdentity(parent, "gate:"+event.Gate.GateID),
			spanNameGate,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithTimestamp(start),
			trace.WithAttributes(attrWaitKind.String("gate"), attrGateKind.String(kind)),
		)
		span.AddEvent(eventDecisionRequested,
			trace.WithTimestamp(start),
			trace.WithAttributes(attrGateKind.String(kind)),
		)
		span.AddEvent(eventDecisionResolved,
			trace.WithTimestamp(end),
			trace.WithAttributes(attrGateKind.String(kind)),
		)
		if gateFailed(event.Gate.Outcome) {
			span.SetStatus(codes.Error, "")
		}
		span.End(trace.WithTimestamp(end))
	}
}

func emitCIFailureSpans(r *Runtime, runCtx, ciCtx context.Context, runID string, events []*db.MetadataEvent) {
	parent := ciCtx
	if parent == nil {
		parent = runCtx
	}
	for _, event := range events {
		if event == nil || event.EventID == "" || event.Type != db.EventTypeCIFailure || event.CI == nil {
			continue
		}
		at := event.SourceTimestamp.UTC()
		_, span := r.tracer.Start(
			withSpanIdentity(parent, "ci-failure:"+event.EventID),
			spanNameStep,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithTimestamp(at),
			trace.WithAttributes(
				attrStepName.String("ci"),
				attrRunID.String(runID),
				attrOutcome.String(outcomeFailed),
				attrFailureCategory.String(failureCIFailed),
			),
		)
		span.SetStatus(codes.Error, "")
		span.End(trace.WithTimestamp(at))
	}
}

func addCIEvents(span trace.Span, events []*db.MetadataEvent) {
	for _, event := range events {
		if event == nil || event.Type != db.EventTypeCIGreen || event.CI == nil {
			continue
		}
		span.AddEvent(eventCIGreen,
			trace.WithTimestamp(event.SourceTimestamp.UTC()),
			trace.WithAttributes(attrCIState.String(ciSuccess)),
		)
	}
}

func terminalRun(status types.RunStatus) bool {
	return status == types.RunCompleted || status == types.RunFailed || status == types.RunCancelled
}

func terminalStep(step *db.StepResult) bool {
	if step == nil {
		return false
	}
	return step.Status == types.StepStatusCompleted || step.Status == types.StepStatusSkipped || step.Status == types.StepStatusFailed
}

func unixSourceTime(value int64) time.Time { return time.Unix(value, 0).UTC() }

func runOutcome(status types.RunStatus) string {
	switch status {
	case types.RunCompleted:
		return outcomeSuccess
	case types.RunCancelled:
		return outcomeCancelled
	default:
		return outcomeFailed
	}
}

func stepOutcome(status types.StepStatus) string {
	if status == types.StepStatusFailed {
		return outcomeFailed
	}
	return outcomeSuccess
}

func runPhase(status types.RunStatus, steps []*db.StepResult) string {
	if status == types.RunCompleted {
		return phaseComplete
	}
	for _, step := range steps {
		if step != nil && step.Status == types.StepStatusFailed {
			return registryPhase(step.StepName)
		}
	}
	return phaseOther
}

func registryStepName(step types.StepName) string {
	switch step {
	case types.StepIntent, types.StepReview, types.StepTest, types.StepLint, types.StepPR, types.StepCI:
		return string(step)
	case types.StepDocument:
		return "docs"
	default:
		return "other"
	}
}

func registryPhase(step types.StepName) string { return registryStepName(step) }

func registryGateKind(class string) string {
	if class == "approval" || class == "fix_review" {
		return class
	}
	return "other"
}

func stepFailureCategory(run *db.Run, step *db.StepResult) string {
	if step.Status != types.StepStatusFailed {
		return ""
	}
	if run != nil && run.Error != nil && *run.Error == "daemon crashed during execution" {
		return failureCrashed
	}
	if step.StepName == types.StepCI {
		return failureCIFailed
	}
	return failureToolError
}

func setRunStatus(span trace.Span, status types.RunStatus) {
	if status == types.RunFailed || status == types.RunCancelled {
		span.SetStatus(codes.Error, "")
	}
}

func setStepStatus(span trace.Span, status types.StepStatus) {
	if status == types.StepStatusFailed {
		span.SetStatus(codes.Error, "")
	}
}

func gateFailed(outcome string) bool {
	switch outcome {
	case "aborted", "cancelled", "failed":
		return true
	default:
		return false
	}
}
