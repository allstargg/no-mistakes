package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Agent invocation session modes recorded for local performance telemetry.
const (
	InvocationModeCold     = "cold"     // no durable session involved
	InvocationModeStarted  = "started"  // began a new durable session
	InvocationModeResumed  = "resumed"  // resumed an existing durable session
	InvocationModeFallback = "fallback" // fresh session after a failed resume
)

// Fallback reasons classify why a resume failed and forced a fresh-session
// retry. They are low-cardinality and content-free (never the error text, which
// can embed agent output), so a silent-fallback regression is both countable
// and diagnosable from telemetry alone.
const (
	FallbackReasonTransient   = "transient"   // retryable provider/transport error
	FallbackReasonParse       = "parse"       // could not parse the resumed output
	FallbackReasonExit        = "exit"        // resumed process exited non-zero
	FallbackReasonSpawn       = "spawn"       // resumed process failed to start
	FallbackReasonUnsupported = "unsupported" // adapter rejected a resume flag
	FallbackReasonOther       = "other"       // anything else
)

// AgentInvocation is one agent process invocation's local performance
// evidence. It stores identity, timing, session mode, bounded activity counts,
// and token usage only - never prompts, model outputs, diffs, raw command
// arguments, or credentials - and it stays local: no per-invocation record is
// ever sent to remote telemetry.
//
// Fields typed as pointers are nullable: a nil value means the datum was not
// reported for this invocation and is recorded as unknown, never a fabricated
// zero. Pre-existing rows created before these columns existed read back as
// nil, so they too report unknown honestly.
type AgentInvocation struct {
	ID       string
	RunID    string
	StepName string
	Round    int
	// Purpose is the pipeline duty served: review, review-fix,
	// test-evidence, housekeeping, document, lint, pr, intent, or a
	// step-derived default.
	Purpose string
	Agent   string
	Model   string
	// ModelProvider is the provider that served the model (openai, anthropic,
	// ...). Nil when the adapter cannot report it.
	ModelProvider *string
	// SessionMode is one of the InvocationMode constants.
	SessionMode string
	// SessionKey is a privacy-safe fingerprint (truncated SHA-256) of the
	// adapter-native session identity, so session reuse is auditable without
	// storing the raw resumable identity in a second place.
	SessionKey string
	// FallbackReason classifies why a fallback invocation happened (one of the
	// FallbackReason constants). Nil unless SessionMode is fallback.
	FallbackReason  *string
	StartedAt       int64
	CompletedAt     int64
	DurationMS      int64
	ExitStatus      string // ok | error | cancelled
	FailureCategory string // parse | exit | spawn | cancelled | other ("" when ok)
	InputTokens     int
	OutputTokens    int
	CacheReadTokens int
	// CacheCreationTokens is the provider's cache-creation cost. Nil when the
	// provider does not surface it (codex), distinguishing "not reported" from a
	// genuine zero.
	CacheCreationTokens *int
	// FreshInputTokens is InputTokens minus CacheReadTokens: the non-cached
	// portion of this invocation's input. Nil when no usage was reported.
	FreshInputTokens *int
	// ReasoningTokens is the model's hidden-reasoning output tokens, when the
	// provider reports them. Nil when not reported.
	ReasoningTokens *int
	// SubprocessWaitMS is the wall-clock this invocation spent inside tool
	// subprocesses; DurationMS minus it is model/reasoning time. Nil when the
	// adapter reported no activity metrics.
	SubprocessWaitMS *int64
	// Delta* are the per-round token amounts for resumed durable sessions whose
	// raw counters are cumulative: current cumulative minus the same session's
	// prior cumulative. For cold/started/fallback rows they equal the raw
	// counters. Nil when no usage was reported.
	DeltaInputTokens     *int
	DeltaOutputTokens    *int
	DeltaCacheReadTokens *int
	// ModelRoundtrips is the count of model-authored items (messages + tool
	// calls) - a live-stream proxy for productive model round-trips. Nil when
	// the adapter reported no activity metrics.
	ModelRoundtrips *int
	// ToolCalls is the count of whole tool invocations. Nil when unknown.
	ToolCalls *int
	// Tool*Calls is the bounded per-category sub-command histogram. Because a
	// compound command counts once per sub-command, their sum can exceed
	// ToolCalls. Nil when the adapter reported no activity metrics.
	ToolWaitCalls     *int
	ToolTestLintCalls *int
	ToolEditCalls     *int
	ToolReadCalls     *int
	ToolGitCalls      *int
	ToolOtherCalls    *int
	// WorkloadFiles and WorkloadLines record the bounded size of the change this
	// invocation worked over. Nil for invocations with no meaningful workload
	// (or steps that do not supply it).
	WorkloadFiles *int
	WorkloadLines *int
	// FindingCount is the number of findings in this invocation's structured
	// output. Nil when the output is not findings-shaped.
	FindingCount *int
}

// agentInvocationColumns is the canonical column order shared by insert and
// select so the placeholder list and scan destinations cannot drift apart.
const agentInvocationColumns = `id, run_id, step_name, round, purpose, agent, model, model_provider,
	session_mode, session_key, fallback_reason,
	started_at, completed_at, duration_ms, subprocess_wait_ms, exit_status, failure_category,
	input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
	fresh_input_tokens, reasoning_tokens,
	delta_input_tokens, delta_output_tokens, delta_cache_read_tokens,
	model_roundtrips, tool_calls,
	tool_wait_calls, tool_test_lint_calls, tool_edit_calls, tool_read_calls, tool_git_calls, tool_other_calls,
	workload_files, workload_lines, finding_count`

// agentInvocationInsertPlaceholders has one '?' per agentInvocationColumns entry.
const agentInvocationInsertPlaceholders = `?, ?, ?, ?, ?, ?, ?, ?,
	?, ?, ?,
	?, ?, ?, ?, ?, ?,
	?, ?, ?, ?,
	?, ?,
	?, ?, ?,
	?, ?,
	?, ?, ?, ?, ?, ?,
	?, ?, ?`

// InsertAgentInvocation records one completed agent invocation. Nil pointer
// fields are stored as SQL NULL (database/sql dereferences non-nil pointers).
func (d *DB) InsertAgentInvocation(inv AgentInvocation) (*AgentInvocation, error) {
	inv.ID = newID()
	if err := insertAgentInvocationExec(context.Background(), d.sql, inv); err != nil {
		return nil, err
	}
	return &inv, nil
}

// insertAgentInvocationExec writes one already-identified invocation through
// exec, so the plain InsertAgentInvocation and the event-coupled
// InsertAgentInvocationWithEvent persist an identical row. The caller assigns
// inv.ID before calling.
func insertAgentInvocationExec(ctx context.Context, exec sqlExecutor, inv AgentInvocation) error {
	_, err := exec.ExecContext(ctx,
		`INSERT INTO agent_invocations (`+agentInvocationColumns+`)
		 VALUES (`+agentInvocationInsertPlaceholders+`)`,
		inv.ID, inv.RunID, inv.StepName, inv.Round, inv.Purpose, inv.Agent, inv.Model, inv.ModelProvider,
		inv.SessionMode, inv.SessionKey, inv.FallbackReason,
		inv.StartedAt, inv.CompletedAt, inv.DurationMS, inv.SubprocessWaitMS, inv.ExitStatus, inv.FailureCategory,
		inv.InputTokens, inv.OutputTokens, inv.CacheReadTokens, inv.CacheCreationTokens,
		inv.FreshInputTokens, inv.ReasoningTokens,
		inv.DeltaInputTokens, inv.DeltaOutputTokens, inv.DeltaCacheReadTokens,
		inv.ModelRoundtrips, inv.ToolCalls,
		inv.ToolWaitCalls, inv.ToolTestLintCalls, inv.ToolEditCalls, inv.ToolReadCalls, inv.ToolGitCalls, inv.ToolOtherCalls,
		inv.WorkloadFiles, inv.WorkloadLines, inv.FindingCount,
	)
	if err != nil {
		return fmt.Errorf("insert agent invocation: %w", err)
	}
	return nil
}

// GetAgentInvocationsByRun returns a run's invocations in execution order.
func (d *DB) GetAgentInvocationsByRun(runID string) ([]AgentInvocation, error) {
	return d.getAgentInvocationsByRun(context.Background(), runID, 0)
}

// GetAgentInvocationsByRunContext is the bounded cancellable projection read.
func (d *DB) GetAgentInvocationsByRunContext(ctx context.Context, runID string, limit int) ([]AgentInvocation, error) {
	if !validBoundedID(runID) || limit <= 0 || limit > MaxOTLPMetricProjectionBatch {
		return nil, fmt.Errorf("get agent invocations: invalid projection read")
	}
	return d.getAgentInvocationsByRun(ctx, runID, limit)
}

func (d *DB) getAgentInvocationsByRun(ctx context.Context, runID string, limit int) ([]AgentInvocation, error) {
	query := `SELECT ` + agentInvocationColumns + ` FROM agent_invocations WHERE run_id = ? ORDER BY started_at, id`
	args := []any{runID}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := d.sql.QueryContext(ctx, query, args...)

	if err != nil {
		return nil, fmt.Errorf("get agent invocations: %w", err)
	}
	defer rows.Close()

	var invocations []AgentInvocation
	for rows.Next() {
		inv, err := scanAgentInvocation(rows)
		if err != nil {
			return nil, err
		}
		invocations = append(invocations, inv)
	}
	return invocations, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanAgentInvocation(row scanner) (AgentInvocation, error) {
	var inv AgentInvocation
	if err := row.Scan(
		&inv.ID, &inv.RunID, &inv.StepName, &inv.Round, &inv.Purpose, &inv.Agent, &inv.Model, &inv.ModelProvider,
		&inv.SessionMode, &inv.SessionKey, &inv.FallbackReason,
		&inv.StartedAt, &inv.CompletedAt, &inv.DurationMS, &inv.SubprocessWaitMS, &inv.ExitStatus, &inv.FailureCategory,
		&inv.InputTokens, &inv.OutputTokens, &inv.CacheReadTokens, &inv.CacheCreationTokens,
		&inv.FreshInputTokens, &inv.ReasoningTokens,
		&inv.DeltaInputTokens, &inv.DeltaOutputTokens, &inv.DeltaCacheReadTokens,
		&inv.ModelRoundtrips, &inv.ToolCalls,
		&inv.ToolWaitCalls, &inv.ToolTestLintCalls, &inv.ToolEditCalls, &inv.ToolReadCalls, &inv.ToolGitCalls, &inv.ToolOtherCalls,
		&inv.WorkloadFiles, &inv.WorkloadLines, &inv.FindingCount,
	); err != nil {
		return AgentInvocation{}, fmt.Errorf("scan agent invocation: %w", err)
	}
	return inv, nil
}

// LatestSessionUsageObservation returns the immediately preceding durable row
// for a non-empty session fingerprint. Projection delta logic must inspect the
// whole observation, not only its raw counters: adapter/provider continuity,
// source ordering, and per-component coverage decide whether subtraction is
// truthful. Insertion order is authoritative when source timestamps have only
// second precision; a source timestamp that moves backwards is still detected
// by the caller and fails closed.
func (d *DB) LatestSessionUsageObservation(runID, sessionKey string) (*AgentInvocation, error) {
	if sessionKey == "" {
		return nil, nil
	}
	row := d.sql.QueryRow(
		`SELECT `+agentInvocationColumns+` FROM agent_invocations
		 WHERE run_id = ? AND session_key = ?
		 ORDER BY rowid DESC LIMIT 1`,
		runID, sessionKey,
	)
	inv, err := scanAgentInvocation(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &inv, nil
}

const MaxOTLPMetricProjectionBatch = 1000

// ClaimOTLPMetricInvocations durably claims a bounded set of invocation
// measurements before they are submitted to the process-local asynchronous
// metric SDK. A durable identity is returned only on its first claim, so daemon
// restart and terminal-run reprojection do not double-account additive metrics.
// The checkpoint means submitted-to-SDK, not collector delivery: a crash after
// claim can lose optional telemetry, which is preferable to inventing an
// exactly-once broker on the pipeline path.
func (d *DB) ClaimOTLPMetricInvocations(ctx context.Context, invocationIDs []string) ([]string, error) {
	if len(invocationIDs) == 0 {
		return nil, nil
	}
	if len(invocationIDs) > MaxOTLPMetricProjectionBatch {
		return nil, fmt.Errorf("claim OTLP metric invocations: batch exceeds %d", MaxOTLPMetricProjectionBatch)
	}
	for _, id := range invocationIDs {
		if !validBoundedID(id) {
			return nil, fmt.Errorf("claim OTLP metric invocations: invalid identity")
		}
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("claim OTLP metric invocations: begin: %w", err)
	}
	defer tx.Rollback()
	claimed := make([]string, 0, len(invocationIDs))
	seen := make(map[string]struct{}, len(invocationIDs))
	for _, id := range invocationIDs {
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		result, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO otlp_metric_projections (invocation_id, claimed_at) VALUES (?, ?)`,
			id, time.Now().UTC().UnixMilli(),
		)
		if err != nil {
			return nil, fmt.Errorf("claim OTLP metric invocation: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("claim OTLP metric invocation: rows affected: %w", err)
		}
		if rows == 1 {
			claimed = append(claimed, id)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim OTLP metric invocations: commit: %w", err)
	}
	return claimed, nil
}

// AgentInvocationAggregate summarizes invocations for one purpose, powering
// the read-only performance report. Nullable sums preserve unknown when no row
// reported that metric. MetricsRows reports activity-metric coverage.
type AgentInvocationAggregate struct {
	Purpose             string
	Count               int
	TotalDurationMS     int64
	AvgDurationMS       int64
	SubprocessWaitMS    *int64
	Cold                int
	Started             int
	Resumed             int
	Fallback            int
	Errors              int
	InputTokens         int64
	OutputTokens        int64
	CacheReadTokens     int64
	CacheCreationTokens *int64
	FreshInputTokens    *int64
	ReasoningTokens     *int64
	ModelRoundtrips     *int64
	ToolCalls           *int64
	ToolWaitCalls       *int64
	ToolTestLintCalls   *int64
	ToolEditCalls       *int64
	ToolReadCalls       *int64
	ToolGitCalls        *int64
	ToolOtherCalls      *int64
	// MetricsRows counts invocations in the group whose adapter reported
	// activity metrics (model_roundtrips is non-NULL).
	MetricsRows int
}

// AgentInvocationAggregates returns per-purpose aggregates across all runs,
// largest total duration first.
func (d *DB) AgentInvocationAggregates() ([]AgentInvocationAggregate, error) {
	rows, err := d.sql.Query(`
		SELECT purpose,
		       COUNT(*),
		       COALESCE(SUM(duration_ms), 0),
		       CASE WHEN COUNT(subprocess_wait_ms) = COUNT(*) THEN SUM(subprocess_wait_ms) END,
		       COALESCE(SUM(CASE WHEN session_mode = 'cold' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN session_mode = 'started' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN session_mode = 'resumed' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN session_mode = 'fallback' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN exit_status != 'ok' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(input_tokens), 0),
		       COALESCE(SUM(output_tokens), 0),
		       COALESCE(SUM(cache_read_tokens), 0),
		       CASE WHEN COUNT(cache_creation_tokens) = COUNT(*) THEN SUM(cache_creation_tokens) END,
		       CASE WHEN COUNT(fresh_input_tokens) = COUNT(*) THEN SUM(fresh_input_tokens) END,
		       CASE WHEN COUNT(reasoning_tokens) = COUNT(*) THEN SUM(reasoning_tokens) END,
		       CASE WHEN COUNT(model_roundtrips) = COUNT(*) THEN SUM(model_roundtrips) END,
		       CASE WHEN COUNT(tool_calls) = COUNT(*) THEN SUM(tool_calls) END,
		       CASE WHEN COUNT(tool_wait_calls) = COUNT(*) THEN SUM(tool_wait_calls) END,
		       CASE WHEN COUNT(tool_test_lint_calls) = COUNT(*) THEN SUM(tool_test_lint_calls) END,
		       CASE WHEN COUNT(tool_edit_calls) = COUNT(*) THEN SUM(tool_edit_calls) END,
		       CASE WHEN COUNT(tool_read_calls) = COUNT(*) THEN SUM(tool_read_calls) END,
		       CASE WHEN COUNT(tool_git_calls) = COUNT(*) THEN SUM(tool_git_calls) END,
		       CASE WHEN COUNT(tool_other_calls) = COUNT(*) THEN SUM(tool_other_calls) END,
		       COALESCE(SUM(CASE WHEN model_roundtrips IS NOT NULL THEN 1 ELSE 0 END), 0)
		FROM agent_invocations
		GROUP BY purpose
		ORDER BY SUM(duration_ms) DESC`)
	if err != nil {
		return nil, fmt.Errorf("agent invocation aggregates: %w", err)
	}
	defer rows.Close()

	var aggregates []AgentInvocationAggregate
	for rows.Next() {
		var a AgentInvocationAggregate
		if err := rows.Scan(
			&a.Purpose, &a.Count, &a.TotalDurationMS, &a.SubprocessWaitMS,
			&a.Cold, &a.Started, &a.Resumed, &a.Fallback, &a.Errors,
			&a.InputTokens, &a.OutputTokens, &a.CacheReadTokens, &a.CacheCreationTokens,
			&a.FreshInputTokens, &a.ReasoningTokens, &a.ModelRoundtrips, &a.ToolCalls,
			&a.ToolWaitCalls, &a.ToolTestLintCalls, &a.ToolEditCalls, &a.ToolReadCalls, &a.ToolGitCalls, &a.ToolOtherCalls,
			&a.MetricsRows,
		); err != nil {
			return nil, fmt.Errorf("scan agent invocation aggregate: %w", err)
		}
		if a.Count > 0 {
			a.AvgDurationMS = a.TotalDurationMS / int64(a.Count)
		}
		aggregates = append(aggregates, a)
	}
	return aggregates, rows.Err()
}

// RunInvocationSummary is the low-cardinality per-run rollup used for the
// bounded terminal remote summary (counts only - no ids, paths, or models).
type RunInvocationSummary struct {
	Count           int
	Resumed         int
	Fallback        int
	TotalDurationMS int64
}

// AgentInvocationSummaryForRun returns the run's invocation rollup.
func (d *DB) AgentInvocationSummaryForRun(runID string) (RunInvocationSummary, error) {
	var s RunInvocationSummary
	err := d.sql.QueryRow(`
		SELECT COUNT(*),
		       COALESCE(SUM(CASE WHEN session_mode = 'resumed' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN session_mode = 'fallback' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(duration_ms), 0)
		FROM agent_invocations WHERE run_id = ?`, runID).
		Scan(&s.Count, &s.Resumed, &s.Fallback, &s.TotalDurationMS)
	if err != nil {
		return RunInvocationSummary{}, fmt.Errorf("agent invocation summary: %w", err)
	}
	return s, nil
}
