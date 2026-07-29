package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// TW-34 lifecycle event types. Every type has a fixed family-specific metadata
// shape in the event_*_metadata tables. The event log intentionally has no
// generic payload or caller-defined attribute channel.
const (
	EventTypeInvocationStarted   MetadataEventType = "io.no_mistakes.invocation.started.v1"
	EventTypeInvocationCompleted MetadataEventType = "io.no_mistakes.invocation.completed.v1"
	EventTypeGateEntered         MetadataEventType = "io.no_mistakes.gate.entered.v1"
	EventTypeGateExited          MetadataEventType = "io.no_mistakes.gate.exited.v1"
	EventTypeCIRunning           MetadataEventType = "io.no_mistakes.ci.running.v1"
	EventTypeCIGreen             MetadataEventType = "io.no_mistakes.ci.green.v1"
	EventTypeCIFailure           MetadataEventType = "io.no_mistakes.ci.failure.v1"
	EventTypeCIMergeWait         MetadataEventType = "io.no_mistakes.ci.merge_wait.v1"
	EventTypeCITerminal          MetadataEventType = "io.no_mistakes.ci.terminal.v1"
	EventTypePRCreated           MetadataEventType = "io.no_mistakes.pr.created.v1"
	EventTypePROpened            MetadataEventType = "io.no_mistakes.pr.opened.v1"
	EventTypePRChecksWait        MetadataEventType = "io.no_mistakes.pr.checks_wait.v1"
	EventTypePRReviewWait        MetadataEventType = "io.no_mistakes.pr.review_wait.v1"
	EventTypePRMerged            MetadataEventType = "io.no_mistakes.pr.merged.v1"
	EventTypePRClosed            MetadataEventType = "io.no_mistakes.pr.closed.v1"
)

const (
	schemaInvocationStarted   MetadataPayloadSchema = "io.no_mistakes.invocation.started.v1"
	schemaInvocationCompleted MetadataPayloadSchema = "io.no_mistakes.invocation.completed.v1"
	schemaGateEntered         MetadataPayloadSchema = "io.no_mistakes.gate.entered.v1"
	schemaGateExited          MetadataPayloadSchema = "io.no_mistakes.gate.exited.v1"
	schemaCIRunning           MetadataPayloadSchema = "io.no_mistakes.ci.running.v1"
	schemaCIGreen             MetadataPayloadSchema = "io.no_mistakes.ci.green.v1"
	schemaCIFailure           MetadataPayloadSchema = "io.no_mistakes.ci.failure.v1"
	schemaCIMergeWait         MetadataPayloadSchema = "io.no_mistakes.ci.merge_wait.v1"
	schemaCITerminal          MetadataPayloadSchema = "io.no_mistakes.ci.terminal.v1"
	schemaPRCreated           MetadataPayloadSchema = "io.no_mistakes.pr.created.v1"
	schemaPROpened            MetadataPayloadSchema = "io.no_mistakes.pr.opened.v1"
	schemaPRChecksWait        MetadataPayloadSchema = "io.no_mistakes.pr.checks_wait.v1"
	schemaPRReviewWait        MetadataPayloadSchema = "io.no_mistakes.pr.review_wait.v1"
	schemaPRMerged            MetadataPayloadSchema = "io.no_mistakes.pr.merged.v1"
	schemaPRClosed            MetadataPayloadSchema = "io.no_mistakes.pr.closed.v1"
)

// InvocationUsageMetadata contains only exact token counters reported by the
// adapter. A nil field means unknown. No caller converts an unreported datum to
// zero when constructing this shape.
type InvocationUsageMetadata struct {
	InputTokens          *int
	OutputTokens         *int
	CacheReadTokens      *int
	CacheCreationTokens  *int
	FreshInputTokens     *int
	ReasoningTokens      *int
	DeltaInputTokens     *int
	DeltaOutputTokens    *int
	DeltaCacheReadTokens *int
}

// InvocationActivityMetadata is a fixed, bounded-cardinality activity summary.
// It contains counts only, never tool names, commands, prompts, output, or text.
type InvocationActivityMetadata struct {
	ModelRoundtrips   *int
	ToolCalls         *int
	ToolWaitCalls     *int
	ToolTestLintCalls *int
	ToolEditCalls     *int
	ToolReadCalls     *int
	ToolGitCalls      *int
	ToolOtherCalls    *int
	WorkloadFiles     *int
	WorkloadLines     *int
	FindingCount      *int
}

// InvocationEventMetadata is the typed metadata for an invocation start or
// completion. InvocationID correlates the pair and is the same ID as the
// durable agent_invocations row.
type InvocationEventMetadata struct {
	InvocationID    string
	Phase           string
	Step            string
	Purpose         string
	SessionMode     string
	Outcome         string
	FailureCategory string
	DurationMS      *int64
	Usage           *InvocationUsageMetadata
	Activity        *InvocationActivityMetadata
}

// GateEventMetadata correlates a gate enter/exit pair. Step is the gate
// identity within the pipeline; GateID distinguishes repeated waits at that
// step. Class is approval or fix_review.
type GateEventMetadata struct {
	GateID         string
	Phase          string
	Step           string
	Class          string
	Outcome        string
	WaitDurationMS *int64
}

// CIEventMetadata uses only the CI monitor's bounded authoritative vocabulary.
type CIEventMetadata struct {
	State   string
	Outcome string
}

// PREventMetadata uses only normalized lifecycle and wait states already owned
// by the PR step and CI monitor.
type PREventMetadata struct {
	State string
}

func validateLifecycleMetadata(input MetadataEventInput) error {
	families := 0
	if input.invocation != nil {
		families++
	}
	if input.gate != nil {
		families++
	}
	if input.ci != nil {
		families++
	}
	if input.pr != nil {
		families++
	}
	if families > 1 {
		return ErrInvalidMetadataEvent
	}

	switch input.Type {
	case EventTypeInvocationStarted:
		if !validInvocationMetadata(input.invocation, "started") {
			return ErrInvalidMetadataEvent
		}
	case EventTypeInvocationCompleted:
		if !validInvocationMetadata(input.invocation, "completed") {
			return ErrInvalidMetadataEvent
		}
	case EventTypeGateEntered:
		if !validGateMetadata(input.gate, "entered") {
			return ErrInvalidMetadataEvent
		}
	case EventTypeGateExited:
		if !validGateMetadata(input.gate, "exited") {
			return ErrInvalidMetadataEvent
		}
	case EventTypeCIRunning:
		if !validCIMetadata(input.ci, "running") {
			return ErrInvalidMetadataEvent
		}
	case EventTypeCIGreen:
		if !validCIMetadata(input.ci, "green") {
			return ErrInvalidMetadataEvent
		}
	case EventTypeCIFailure:
		if !validCIMetadata(input.ci, "failure") {
			return ErrInvalidMetadataEvent
		}
	case EventTypeCIMergeWait:
		if !validCIMetadata(input.ci, "merge_wait") {
			return ErrInvalidMetadataEvent
		}
	case EventTypeCITerminal:
		if !validCIMetadata(input.ci, "terminal") {
			return ErrInvalidMetadataEvent
		}
	case EventTypePRCreated:
		if !validPRMetadata(input.pr, "created") {
			return ErrInvalidMetadataEvent
		}
	case EventTypePROpened:
		if !validPRMetadata(input.pr, "open") {
			return ErrInvalidMetadataEvent
		}
	case EventTypePRChecksWait:
		if !validPRMetadata(input.pr, "checks_wait") {
			return ErrInvalidMetadataEvent
		}
	case EventTypePRReviewWait:
		if !validPRMetadata(input.pr, "review_wait") {
			return ErrInvalidMetadataEvent
		}
	case EventTypePRMerged:
		if !validPRMetadata(input.pr, "merged") {
			return ErrInvalidMetadataEvent
		}
	case EventTypePRClosed:
		if !validPRMetadata(input.pr, "closed") {
			return ErrInvalidMetadataEvent
		}
	default:
		if families != 0 {
			return ErrInvalidMetadataEvent
		}
	}
	return nil
}

func validInvocationMetadata(m *InvocationEventMetadata, phase string) bool {
	if m == nil || m.Phase != phase || !validBoundedID(m.InvocationID) ||
		!oneOf(m.Step, "intent", "rebase", "review", "test", "document", "lint", "push", "pr", "ci", "unknown") ||
		!oneOf(m.Purpose, "intent", "intent-fix", "rebase", "rebase-fix", "review", "review-fix", "test", "test-evidence", "test-fix", "document", "document-fix", "housekeeping", "lint", "lint-fix", "push", "pr", "ci", "ci-fix", "other") ||
		!oneOf(m.SessionMode, "cold", "started", "resumed", "fallback", "other") {
		return false
	}
	if phase == "started" {
		return m.Outcome == "" && m.FailureCategory == "" && m.DurationMS == nil && m.Usage == nil && m.Activity == nil
	}
	if !oneOf(m.Outcome, "ok", "error", "cancelled", "unknown") || m.DurationMS == nil || *m.DurationMS < 0 {
		return false
	}
	if m.FailureCategory != "" && !oneOf(m.FailureCategory, "parse", "exit", "spawn", "cancelled", "other") {
		return false
	}
	return validInvocationUsage(m.Usage) && validInvocationActivity(m.Activity)
}

func validInvocationUsage(usage *InvocationUsageMetadata) bool {
	if usage == nil {
		return true
	}
	return nonNegativeInts(
		usage.InputTokens, usage.OutputTokens, usage.CacheReadTokens,
		usage.CacheCreationTokens, usage.FreshInputTokens, usage.ReasoningTokens,
		usage.DeltaInputTokens, usage.DeltaOutputTokens, usage.DeltaCacheReadTokens,
	)
}

func validInvocationActivity(activity *InvocationActivityMetadata) bool {
	if activity == nil {
		return true
	}
	return nonNegativeInts(
		activity.ModelRoundtrips, activity.ToolCalls, activity.ToolWaitCalls,
		activity.ToolTestLintCalls, activity.ToolEditCalls, activity.ToolReadCalls,
		activity.ToolGitCalls, activity.ToolOtherCalls, activity.WorkloadFiles,
		activity.WorkloadLines, activity.FindingCount,
	)
}

func validGateMetadata(m *GateEventMetadata, phase string) bool {
	if m == nil || m.Phase != phase || !validBoundedID(m.GateID) ||
		!oneOf(m.Step, "intent", "rebase", "review", "test", "document", "lint", "push", "pr", "ci", "unknown") ||
		!oneOf(m.Class, "approval", "fix_review", "unknown") {
		return false
	}
	if phase == "entered" {
		return m.Outcome == "" && m.WaitDurationMS == nil
	}
	return oneOf(m.Outcome, "approved", "fix_requested", "skipped", "aborted", "reconciled", "cancelled", "terminal", "failed", "unknown") &&
		m.WaitDurationMS != nil && *m.WaitDurationMS >= 0
}

func validCIMetadata(m *CIEventMetadata, state string) bool {
	if m == nil || m.State != state {
		return false
	}
	switch state {
	case "running":
		return oneOf(m.Outcome, "checks", "unknown")
	case "green", "merge_wait":
		return oneOf(m.Outcome, "passed", "no_checks")
	case "failure":
		return oneOf(m.Outcome, "checks", "merge_conflict", "checks_and_merge_conflict")
	case "terminal":
		return oneOf(m.Outcome, "merged", "closed")
	default:
		return false
	}
}

func validPRMetadata(m *PREventMetadata, state string) bool {
	return m != nil && m.State == state
}

func validBoundedID(value string) bool {
	return value != "" && len(value) <= 128 && value == strings.TrimSpace(value) && !strings.ContainsAny(value, "\r\n\t ")
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func nonNegativeInts(values ...*int) bool {
	for _, value := range values {
		if value != nil && *value < 0 {
			return false
		}
	}
	return true
}

func insertLifecycleMetadata(ctx context.Context, exec sqlExecutor, eventID string, input MetadataEventInput) error {
	switch {
	case input.invocation != nil:
		m := input.invocation
		var usage InvocationUsageMetadata
		if m.Usage != nil {
			usage = *m.Usage
		}
		var activity InvocationActivityMetadata
		if m.Activity != nil {
			activity = *m.Activity
		}
		_, err := exec.ExecContext(ctx, `INSERT INTO event_invocation_metadata
			(event_id, invocation_id, phase, step_name, purpose, session_mode, outcome, failure_category, duration_ms,
			 input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, fresh_input_tokens, reasoning_tokens,
			 delta_input_tokens, delta_output_tokens, delta_cache_read_tokens,
			 model_roundtrips, tool_calls, tool_wait_calls, tool_test_lint_calls, tool_edit_calls, tool_read_calls,
			 tool_git_calls, tool_other_calls, workload_files, workload_lines, finding_count)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			eventID, m.InvocationID, m.Phase, m.Step, m.Purpose, m.SessionMode, eventNullableString(m.Outcome), eventNullableString(m.FailureCategory), m.DurationMS,
			usage.InputTokens, usage.OutputTokens, usage.CacheReadTokens, usage.CacheCreationTokens, usage.FreshInputTokens, usage.ReasoningTokens,
			usage.DeltaInputTokens, usage.DeltaOutputTokens, usage.DeltaCacheReadTokens,
			activity.ModelRoundtrips, activity.ToolCalls, activity.ToolWaitCalls, activity.ToolTestLintCalls, activity.ToolEditCalls, activity.ToolReadCalls,
			activity.ToolGitCalls, activity.ToolOtherCalls, activity.WorkloadFiles, activity.WorkloadLines, activity.FindingCount,
		)
		return err
	case input.gate != nil:
		m := input.gate
		_, err := exec.ExecContext(ctx, `INSERT INTO event_gate_metadata
			(event_id, gate_id, phase, step_name, gate_class, outcome, wait_duration_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			eventID, m.GateID, m.Phase, m.Step, m.Class, eventNullableString(m.Outcome), m.WaitDurationMS)
		return err
	case input.ci != nil:
		m := input.ci
		_, err := exec.ExecContext(ctx, `INSERT INTO event_ci_metadata (event_id, state, outcome) VALUES (?, ?, ?)`,
			eventID, m.State, eventNullableString(m.Outcome))
		return err
	case input.pr != nil:
		m := input.pr
		_, err := exec.ExecContext(ctx, `INSERT INTO event_pr_metadata (event_id, state) VALUES (?, ?)`, eventID, m.State)
		return err
	default:
		return nil
	}
}

func eventNullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (d *DB) loadLifecycleMetadata(ctx context.Context, events []*MetadataEvent) error {
	if len(events) == 0 {
		return nil
	}
	byID := make(map[string]*MetadataEvent, len(events))
	ids := make([]any, 0, len(events))
	placeholders := make([]string, 0, len(events))
	families := map[string]bool{}
	for _, event := range events {
		byID[event.EventID] = event
		ids = append(ids, event.EventID)
		placeholders = append(placeholders, "?")
		switch event.Type {
		case EventTypeInvocationStarted, EventTypeInvocationCompleted:
			families["invocation"] = true
		case EventTypeGateEntered, EventTypeGateExited:
			families["gate"] = true
		case EventTypeCIRunning, EventTypeCIGreen, EventTypeCIFailure, EventTypeCIMergeWait, EventTypeCITerminal:
			families["ci"] = true
		case EventTypePRCreated, EventTypePROpened, EventTypePRChecksWait, EventTypePRReviewWait, EventTypePRMerged, EventTypePRClosed:
			families["pr"] = true
		}
	}
	in := strings.Join(placeholders, ",")
	if families["invocation"] {
		if err := d.loadInvocationMetadata(ctx, byID, in, ids); err != nil {
			return err
		}
	}
	if families["gate"] {
		if err := d.loadGateMetadata(ctx, byID, in, ids); err != nil {
			return err
		}
	}
	if families["ci"] {
		if err := d.loadCIMetadata(ctx, byID, in, ids); err != nil {
			return err
		}
	}
	if families["pr"] {
		if err := d.loadPRMetadata(ctx, byID, in, ids); err != nil {
			return err
		}
	}
	for _, event := range events {
		switch event.Type {
		case EventTypeInvocationStarted, EventTypeInvocationCompleted:
			if event.Invocation == nil {
				return ErrInvalidMetadataEventRecord
			}
		case EventTypeGateEntered, EventTypeGateExited:
			if event.Gate == nil {
				return ErrInvalidMetadataEventRecord
			}
		case EventTypeCIRunning, EventTypeCIGreen, EventTypeCIFailure, EventTypeCIMergeWait, EventTypeCITerminal:
			if event.CI == nil {
				return ErrInvalidMetadataEventRecord
			}
		case EventTypePRCreated, EventTypePROpened, EventTypePRChecksWait, EventTypePRReviewWait, EventTypePRMerged, EventTypePRClosed:
			if event.PR == nil {
				return ErrInvalidMetadataEventRecord
			}
		}
	}
	return nil
}

func (d *DB) loadInvocationMetadata(ctx context.Context, byID map[string]*MetadataEvent, in string, ids []any) error {
	rows, err := d.sql.QueryContext(ctx, `SELECT event_id, invocation_id, phase, step_name, purpose, session_mode,
		outcome, failure_category, duration_ms,
		input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, fresh_input_tokens, reasoning_tokens,
		delta_input_tokens, delta_output_tokens, delta_cache_read_tokens,
		model_roundtrips, tool_calls, tool_wait_calls, tool_test_lint_calls, tool_edit_calls, tool_read_calls,
		tool_git_calls, tool_other_calls, workload_files, workload_lines, finding_count
		FROM event_invocation_metadata WHERE event_id IN (`+in+`)`, ids...)
	if err != nil {
		return fmt.Errorf("read invocation event metadata: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		m := &InvocationEventMetadata{}
		var outcome, failure sql.NullString
		var duration sql.NullInt64
		var usage InvocationUsageMetadata
		var activity InvocationActivityMetadata
		if err := rows.Scan(&eventID, &m.InvocationID, &m.Phase, &m.Step, &m.Purpose, &m.SessionMode,
			&outcome, &failure, &duration,
			&usage.InputTokens, &usage.OutputTokens, &usage.CacheReadTokens, &usage.CacheCreationTokens, &usage.FreshInputTokens, &usage.ReasoningTokens,
			&usage.DeltaInputTokens, &usage.DeltaOutputTokens, &usage.DeltaCacheReadTokens,
			&activity.ModelRoundtrips, &activity.ToolCalls, &activity.ToolWaitCalls, &activity.ToolTestLintCalls, &activity.ToolEditCalls, &activity.ToolReadCalls,
			&activity.ToolGitCalls, &activity.ToolOtherCalls, &activity.WorkloadFiles, &activity.WorkloadLines, &activity.FindingCount); err != nil {
			return fmt.Errorf("read invocation event metadata: scan: %w", err)
		}
		m.Outcome = outcome.String
		m.FailureCategory = failure.String
		if duration.Valid {
			value := duration.Int64
			m.DurationMS = &value
		}
		if invocationUsagePresent(&usage) {
			m.Usage = &usage
		}
		if invocationActivityPresent(&activity) {
			m.Activity = &activity
		}
		event := byID[eventID]
		if event == nil || !validInvocationMetadata(m, m.Phase) {
			return ErrInvalidMetadataEventRecord
		}
		event.Invocation = m
	}
	return rows.Err()
}

func (d *DB) loadGateMetadata(ctx context.Context, byID map[string]*MetadataEvent, in string, ids []any) error {
	rows, err := d.sql.QueryContext(ctx, `SELECT event_id, gate_id, phase, step_name, gate_class, outcome, wait_duration_ms
		FROM event_gate_metadata WHERE event_id IN (`+in+`)`, ids...)
	if err != nil {
		return fmt.Errorf("read gate event metadata: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		m := &GateEventMetadata{}
		var outcome sql.NullString
		var wait sql.NullInt64
		if err := rows.Scan(&eventID, &m.GateID, &m.Phase, &m.Step, &m.Class, &outcome, &wait); err != nil {
			return fmt.Errorf("read gate event metadata: scan: %w", err)
		}
		m.Outcome = outcome.String
		if wait.Valid {
			value := wait.Int64
			m.WaitDurationMS = &value
		}
		event := byID[eventID]
		if event == nil || !validGateMetadata(m, m.Phase) {
			return ErrInvalidMetadataEventRecord
		}
		event.Gate = m
	}
	return rows.Err()
}

func (d *DB) loadCIMetadata(ctx context.Context, byID map[string]*MetadataEvent, in string, ids []any) error {
	rows, err := d.sql.QueryContext(ctx, `SELECT event_id, state, outcome FROM event_ci_metadata WHERE event_id IN (`+in+`)`, ids...)
	if err != nil {
		return fmt.Errorf("read CI event metadata: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		m := &CIEventMetadata{}
		var outcome sql.NullString
		if err := rows.Scan(&eventID, &m.State, &outcome); err != nil {
			return fmt.Errorf("read CI event metadata: scan: %w", err)
		}
		m.Outcome = outcome.String
		event := byID[eventID]
		if event == nil || !validCIMetadata(m, m.State) {
			return ErrInvalidMetadataEventRecord
		}
		event.CI = m
	}
	return rows.Err()
}

func (d *DB) loadPRMetadata(ctx context.Context, byID map[string]*MetadataEvent, in string, ids []any) error {
	rows, err := d.sql.QueryContext(ctx, `SELECT event_id, state FROM event_pr_metadata WHERE event_id IN (`+in+`)`, ids...)
	if err != nil {
		return fmt.Errorf("read PR event metadata: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		m := &PREventMetadata{}
		if err := rows.Scan(&eventID, &m.State); err != nil {
			return fmt.Errorf("read PR event metadata: scan: %w", err)
		}
		event := byID[eventID]
		if event == nil || !validPRMetadata(m, m.State) {
			return ErrInvalidMetadataEventRecord
		}
		event.PR = m
	}
	return rows.Err()
}

func invocationUsagePresent(u *InvocationUsageMetadata) bool {
	return u != nil && (u.InputTokens != nil || u.OutputTokens != nil || u.CacheReadTokens != nil || u.CacheCreationTokens != nil ||
		u.FreshInputTokens != nil || u.ReasoningTokens != nil || u.DeltaInputTokens != nil || u.DeltaOutputTokens != nil || u.DeltaCacheReadTokens != nil)
}

func invocationActivityPresent(a *InvocationActivityMetadata) bool {
	return a != nil && (a.ModelRoundtrips != nil || a.ToolCalls != nil || a.ToolWaitCalls != nil || a.ToolTestLintCalls != nil ||
		a.ToolEditCalls != nil || a.ToolReadCalls != nil || a.ToolGitCalls != nil || a.ToolOtherCalls != nil ||
		a.WorkloadFiles != nil || a.WorkloadLines != nil || a.FindingCount != nil)
}
