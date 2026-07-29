package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/agent"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// perfRecordingAgent decorates the step agent to persist one local
// agent_invocations row per invocation: identity, purpose, session mode,
// timing, exit status, and token usage. Recording is local-only and
// best-effort: a failed insert never fails the invocation, and no
// per-invocation record leaves the machine.
type perfRecordingAgent struct {
	inner    agent.Agent
	db       *db.DB
	runID    string
	stepName types.StepName
	// round returns the 1-based round the current invocation belongs to.
	round func() int
}

func (a *perfRecordingAgent) Name() string { return a.inner.Name() }

func (a *perfRecordingAgent) Close() error { return a.inner.Close() }

// SupportsSessionResume forwards the wrapped adapter's session capability.
func (a *perfRecordingAgent) SupportsSessionResume() bool {
	return agent.SupportsSessionResume(a.inner)
}

func (a *perfRecordingAgent) SupportsSessionProvider(provider string) bool {
	return agent.SupportsSessionProvider(a.inner, provider)
}

func (a *perfRecordingAgent) Run(ctx context.Context, opts agent.RunOpts) (*agent.Result, error) {
	attempts := 0
	previous := opts.OnAttempt
	opts.OnAttempt = func(attempt agent.Attempt) {
		if previous != nil {
			previous(attempt)
		}
		attempts++
		attemptOpts := opts
		attemptOpts.Session = attempt.Session
		attemptOpts.SessionFallback = attempt.SessionFallback
		a.record(ctx, attemptOpts, attempt.Agent, attempt.Result, attempt.Err, attempt.StartedAt, attempt.CompletedAt)
	}
	start := time.Now()
	result, err := a.inner.Run(ctx, opts)
	if attempts == 0 {
		a.record(ctx, opts, a.inner.Name(), result, err, start, time.Now())
	}
	return result, err
}

func (a *perfRecordingAgent) record(ctx context.Context, opts agent.RunOpts, agentName string, result *agent.Result, runErr error, startedAt, completedAt time.Time) {
	if a.db == nil {
		return
	}

	purpose := opts.Purpose
	if purpose == "" {
		purpose = string(a.stepName)
	}

	sessionKey := invocationSessionKey(opts, result)
	inv := db.AgentInvocation{
		RunID:       a.runID,
		StepName:    string(a.stepName),
		Round:       a.round(),
		Purpose:     purpose,
		Agent:       agentName,
		SessionMode: invocationSessionMode(opts),
		SessionKey:  sessionKey,
		StartedAt:   startedAt.Unix(),
		CompletedAt: completedAt.Unix(),
		DurationMS:  completedAt.Sub(startedAt).Milliseconds(),
		ExitStatus:  "ok",
	}
	if opts.SessionFallback && opts.SessionFallbackReason != "" {
		reason := opts.SessionFallbackReason
		inv.FallbackReason = &reason
	}
	if opts.Workload != nil {
		files, lines := opts.Workload.Files, opts.Workload.Lines
		inv.WorkloadFiles = &files
		inv.WorkloadLines = &lines
	}
	a.recordResult(&inv, sessionKey, result)
	if runErr != nil {
		if ctx.Err() != nil || errors.Is(runErr, context.Canceled) {
			inv.ExitStatus = "cancelled"
			inv.FailureCategory = "cancelled"
		} else {
			inv.ExitStatus = "error"
			inv.FailureCategory = classifyInvocationFailure(runErr)
		}
	}

	// The invocation already finished (including a cancelled one, recorded with
	// ExitStatus "cancelled"), so its durable record and coupled event must not
	// be aborted by the same cancellation that ended it. Detach the write from
	// caller cancellation while keeping context values.
	if dbErr := a.db.InsertAgentInvocationWithEvent(context.WithoutCancel(ctx), inv); dbErr != nil {
		slog.Warn("failed to record agent invocation", "step", a.stepName, "error", dbErr)
	}
}

// recordResult folds a successful (or partially successful) result's identity,
// usage, per-round token deltas, and bounded activity metrics into inv. Every
// field the adapter did not report is left nil so it is stored as unknown
// rather than a fabricated zero.
func (a *perfRecordingAgent) recordResult(inv *db.AgentInvocation, sessionKey string, result *agent.Result) {
	if result == nil {
		return
	}
	inv.Model = result.Model
	if result.ModelProvider != "" {
		provider := result.ModelProvider
		inv.ModelProvider = &provider
	}
	inv.InputTokens = result.Usage.InputTokens
	inv.OutputTokens = result.Usage.OutputTokens
	inv.CacheReadTokens = result.Usage.CacheReadTokens

	inputReported, outputReported, cacheReadReported := usageComponentCoverage(result)
	if result.CacheCreationReported {
		cacheCreation := result.Usage.CacheCreationTokens
		inv.CacheCreationTokens = &cacheCreation
	}

	// Per-invocation deltas are derived only when the durable predecessor proves
	// continuity. A cumulative resumed observation with no predecessor, a
	// provider/adapter change, an unknown predecessor component, source-time
	// rollback, or counter reset remains unknown rather than being attributed to
	// the current invocation. Fresh/cold and non-cumulative reports are already
	// per-invocation and use the exact reported counters.
	if result.UsageReported {
		if !result.SessionUsageCumulative || inv.SessionMode != db.InvocationModeResumed {
			setKnownUsageDeltas(inv, result, inputReported, outputReported, cacheReadReported)
		} else if previous, err := a.db.LatestSessionUsageObservation(a.runID, sessionKey); err == nil &&
			continuousUsagePredecessor(previous, inv) {
			setCumulativeUsageDeltas(inv, result, previous, inputReported, outputReported, cacheReadReported)
		}
	}
	if inv.DeltaInputTokens != nil && inv.DeltaCacheReadTokens != nil {
		fresh := agent.FreshInputTokens(*inv.DeltaInputTokens, *inv.DeltaCacheReadTokens)
		inv.FreshInputTokens = &fresh
	}

	if result.Metrics != nil {
		m := result.Metrics
		// Reasoning is nullable independently from ordinary token usage. The
		// adapter flag distinguishes a genuine zero from an absent field.
		if result.ReasoningTokensReported {
			reasoning := result.Usage.ReasoningTokens
			inv.ReasoningTokens = &reasoning
		}
		roundtrips := m.ModelRoundtrips
		inv.ModelRoundtrips = &roundtrips
		toolCalls := m.ToolCalls
		inv.ToolCalls = &toolCalls
		wait := m.ToolCategories.Wait
		testLint := m.ToolCategories.TestLint
		edit := m.ToolCategories.Edit
		read := m.ToolCategories.Read
		git := m.ToolCategories.Git
		other := m.ToolCategories.Other
		inv.ToolWaitCalls = &wait
		inv.ToolTestLintCalls = &testLint
		inv.ToolEditCalls = &edit
		inv.ToolReadCalls = &read
		inv.ToolGitCalls = &git
		inv.ToolOtherCalls = &other
		subprocessWait := m.SubprocessWaitMS
		inv.SubprocessWaitMS = &subprocessWait
	}

	if count, ok := countOutputFindings(result.Output); ok {
		inv.FindingCount = &count
	}
}

func usageComponentCoverage(result *agent.Result) (input, output, cacheRead bool) {
	if result == nil || !result.UsageReported {
		return false, false, false
	}
	input = result.InputTokensReported
	output = result.OutputTokensReported
	cacheRead = result.CacheReadTokensReported
	if !input && !output && !cacheRead {
		// Compatibility for adapters/tests written before component coverage was
		// explicit. All current partial adapters set at least one flag.
		return true, true, true
	}
	return input, output, cacheRead
}

func setKnownUsageDeltas(inv *db.AgentInvocation, result *agent.Result, input, output, cacheRead bool) {
	if input {
		value := result.Usage.InputTokens
		inv.DeltaInputTokens = &value
	}
	if output {
		value := result.Usage.OutputTokens
		inv.DeltaOutputTokens = &value
	}
	if cacheRead {
		value := result.Usage.CacheReadTokens
		inv.DeltaCacheReadTokens = &value
	}
}

func continuousUsagePredecessor(previous *db.AgentInvocation, current *db.AgentInvocation) bool {
	if previous == nil || current == nil || previous.Agent != current.Agent ||
		previous.StartedAt > current.StartedAt || previous.CompletedAt > current.StartedAt {
		return false
	}
	if (previous.ModelProvider == nil) != (current.ModelProvider == nil) {
		return false
	}
	return previous.ModelProvider == nil || strings.EqualFold(strings.TrimSpace(*previous.ModelProvider), strings.TrimSpace(*current.ModelProvider))
}

func setCumulativeUsageDeltas(inv *db.AgentInvocation, result *agent.Result, previous *db.AgentInvocation, input, output, cacheRead bool) {
	if input && previous.DeltaInputTokens != nil && result.Usage.InputTokens >= previous.InputTokens {
		value := result.Usage.InputTokens - previous.InputTokens
		inv.DeltaInputTokens = &value
	}
	if output && previous.DeltaOutputTokens != nil && result.Usage.OutputTokens >= previous.OutputTokens {
		value := result.Usage.OutputTokens - previous.OutputTokens
		inv.DeltaOutputTokens = &value
	}
	if cacheRead && previous.DeltaCacheReadTokens != nil && result.Usage.CacheReadTokens >= previous.CacheReadTokens {
		value := result.Usage.CacheReadTokens - previous.CacheReadTokens
		inv.DeltaCacheReadTokens = &value
	}
}

// countOutputFindings returns the number of findings in a structured output
// payload and whether the payload was findings-shaped at all (had a "findings"
// key). It never retains any finding content - only the count.
func countOutputFindings(output json.RawMessage) (int, bool) {
	if len(output) == 0 {
		return 0, false
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(output, &envelope); err != nil {
		return 0, false
	}
	raw, ok := envelope["findings"]
	if !ok {
		return 0, false
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return 0, false
	}
	return len(items), true
}

func invocationSessionMode(opts agent.RunOpts) string {
	switch {
	case opts.SessionFallback:
		return db.InvocationModeFallback
	case opts.Session == nil:
		return db.InvocationModeCold
	case opts.Session.ID != "":
		return db.InvocationModeResumed
	default:
		return db.InvocationModeStarted
	}
}

// invocationSessionKey fingerprints the session identity so reuse is
// auditable without storing the raw resumable id in the telemetry table.
func invocationSessionKey(opts agent.RunOpts, result *agent.Result) string {
	id := ""
	if result != nil && result.SessionID != "" {
		id = result.SessionID
	} else if opts.Session != nil && opts.Session.ID != "" {
		id = opts.Session.ID
	}
	if id == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:8])
}

// classifyInvocationFailure buckets an invocation error into a
// low-cardinality category. Only the category is stored - never the error
// text, which can embed agent output.
func classifyInvocationFailure(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "parse events") || strings.Contains(msg, "output parse"):
		return "parse"
	case strings.Contains(msg, "exited"):
		return "exit"
	case strings.Contains(msg, "start"):
		return "spawn"
	default:
		return "other"
	}
}

// classifyFallbackReason buckets the error that failed a session resume into a
// low-cardinality reason (see db.FallbackReason*). Like classifyInvocationFailure
// it stores only the category, never the error text.
func classifyFallbackReason(err error) string {
	if err == nil {
		return db.FallbackReasonOther
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "unexpected argument") || strings.Contains(msg, "unrecognized") ||
		strings.Contains(msg, "unknown flag") || strings.Contains(msg, "unexpected flag"):
		return db.FallbackReasonUnsupported
	case strings.Contains(msg, "parse events") || strings.Contains(msg, "output parse"):
		return db.FallbackReasonParse
	case strings.Contains(msg, "exited"):
		return db.FallbackReasonExit
	case strings.Contains(msg, "start"):
		return db.FallbackReasonSpawn
	case strings.Contains(msg, "temporarily") || strings.Contains(msg, "capacity") ||
		strings.Contains(msg, "rate limit") || strings.Contains(msg, "overloaded") ||
		strings.Contains(msg, "timeout"):
		return db.FallbackReasonTransient
	default:
		return db.FallbackReasonOther
	}
}
