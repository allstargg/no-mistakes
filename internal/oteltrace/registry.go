package oteltrace

import "go.opentelemetry.io/otel/attribute"

// These names are the no-mistakes subset of Tracewake's TW-9 registry. Keep
// this package metadata-only: adding attributes requires a registry change and
// a privacy review, not a caller-provided map.
const (
	spanNameRun        = "tracewake.no_mistakes.run"
	spanNameStep       = "tracewake.no_mistakes.step"
	spanNameGate       = "tracewake.firstmate.human_gate.wait"
	spanNameInvocation = "tracewake.agent.invoke"

	metricGenAIClientTokenUsage  = "gen_ai.client.token.usage"
	metricGenAIClientOperation   = "gen_ai.client.operation.duration"
	metricInvocationDuration     = "tracewake.no_mistakes.invocation.duration"
	metricSessionFallbacks       = "tracewake.no_mistakes.session.fallbacks"
	metricSubprocessWaitDuration = "tracewake.no_mistakes.subprocess_wait.duration"
	metricTelemetryCoverage      = "tracewake.telemetry.coverage"

	eventDecisionRequested = "tracewake.decision.requested"
	eventDecisionResolved  = "tracewake.decision.resolved"
	eventCIGreen           = "tracewake.ci.green"

	outcomeSuccess   = "success"
	outcomeFailed    = "failed"
	outcomeCancelled = "cancelled"

	phaseComplete = "complete"
	phaseOther    = "other"

	failureToolError   = "tool_error"
	failureCIFailed    = "ci_failed"
	failureCrashed     = "crashed"
	failureSpawnFailed = "spawn_failed"
	failureCancelled   = "cancelled"

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

	attrInvocationID  = attribute.Key("tracewake.no_mistakes.invocation.id")
	attrOperationName = attribute.Key("gen_ai.operation.name")
	attrProviderName  = attribute.Key("gen_ai.provider.name")
	attrTokenType     = attribute.Key("gen_ai.token.type")
	attrHarnessFamily = attribute.Key("tracewake.harness.family")
	attrModelFamily   = attribute.Key("tracewake.model.family")
	attrSessionMode   = attribute.Key("tracewake.no_mistakes.session.mode")
	attrCapability    = attribute.Key("tracewake.capability")
	attrCoverage      = attribute.Key("tracewake.coverage")
)
