package oteltrace

import "go.opentelemetry.io/otel/attribute"

// These names are the no-mistakes subset of Tracewake's TW-9 registry. Keep
// this package metadata-only: adding attributes requires a registry change and
// a privacy review, not a caller-provided map.
const (
	spanNameRun  = "tracewake.no_mistakes.run"
	spanNameStep = "tracewake.no_mistakes.step"
	spanNameGate = "tracewake.firstmate.human_gate.wait"

	eventDecisionRequested = "tracewake.decision.requested"
	eventDecisionResolved  = "tracewake.decision.resolved"
	eventCIGreen           = "tracewake.ci.green"

	outcomeSuccess   = "success"
	outcomeFailed    = "failed"
	outcomeCancelled = "cancelled"

	phaseComplete = "complete"
	phaseOther    = "other"

	failureToolError = "tool_error"
	failureCIFailed  = "ci_failed"
	failureCrashed   = "crashed"

	ciSuccess = "success"
)

var (
	attrRunID           = attribute.Key("tracewake.no_mistakes.run.id")
	attrOutcome         = attribute.Key("tracewake.outcome")
	attrPhase           = attribute.Key("tracewake.no_mistakes.phase")
	attrStepName        = attribute.Key("tracewake.no_mistakes.step.name")
	attrFailureCategory = attribute.Key("tracewake.no_mistakes.failure.category")
	attrWaitKind        = attribute.Key("tracewake.wait.kind")
	attrGateKind        = attribute.Key("tracewake.no_mistakes.gate.kind")
	attrCIState         = attribute.Key("tracewake.no_mistakes.ci.state")
)
