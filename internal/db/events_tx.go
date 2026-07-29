package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
)

// sqlExecutor is the subset of *sql.DB and *sql.Tx that the metadata-event
// append and every event-coupled state write use. Sharing it lets one helper
// run either standalone on the pooled connection or inside an open
// transaction, which is the whole mechanism behind CommitWithEvent.
type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// These are the original TW-36 transaction-boundary event identifiers. Run
// and step remain production events. The gate.awaiting_agent and
// invocation.recorded identifiers remain readable compatibility fixtures;
// TW-34 production paths use the typed gate enter/exit and invocation
// started/completed events in event_lifecycle.go.
const (
	// EventTypeRunCreated is emitted with the run row's creation.
	EventTypeRunCreated MetadataEventType = "io.no_mistakes.run.created.v1"
	// EventTypeStepStarted is emitted when a step transitions to running.
	EventTypeStepStarted MetadataEventType = "io.no_mistakes.step.started.v1"
	// EventTypeGateAwaitingAgent is retained for pre-TW-34 compatibility.
	EventTypeGateAwaitingAgent MetadataEventType = "io.no_mistakes.gate.awaiting_agent.v1"
	// EventTypeInvocationRecorded is retained for pre-TW-34 compatibility.
	EventTypeInvocationRecorded MetadataEventType = "io.no_mistakes.invocation.recorded.v1"

	schemaRunCreated         MetadataPayloadSchema = "io.no_mistakes.run.created.v1"
	schemaStepStarted        MetadataPayloadSchema = "io.no_mistakes.step.started.v1"
	schemaGateAwaitingAgent  MetadataPayloadSchema = "io.no_mistakes.gate.awaiting_agent.v1"
	schemaInvocationRecorded MetadataPayloadSchema = "io.no_mistakes.invocation.recorded.v1"
)

// familyEventInput builds the bounded, content-free MetadataEventInput for one
// in-scope mutation. SourceTimestamp is the mutation moment; the versioned
// type and schema carry the classification and the run link carries TW-38
// trace correlation. It never accepts caller content.
func familyEventInput(t MetadataEventType, schema MetadataPayloadSchema, runID string) MetadataEventInput {
	return MetadataEventInput{
		SourceTimestamp: time.Now().UTC(),
		Type:            t,
		PayloadSchema:   schema,
		PayloadVersion:  1,
		RunID:           runID,
	}
}

// CommitWithEvent runs a state mutation and appends the metadata event that
// describes it inside one SQLite transaction. Either both commit or neither
// does, which is the TW-36 invariant: a committed in-scope state change can
// never lack its event, and a recorded event can never claim a state change
// that rolled back. It is the single-event compatibility wrapper around
// CommitWithEvents.
//
// Scope: the mutate closure MUST perform all of its writes through the *sql.Tx
// it is handed and never touch the surrounding *sql.DB. The pool is capped at
// one connection (SetMaxOpenConns(1)), so a stray d.sql call inside mutate
// would deadlock against the transaction that already holds that connection.
//
// Ordering: the mutation runs first, then the event append, so the event's
// run/trace linkage reads the post-mutation row - including a run created by
// the same transaction. Input is validated before BEGIN, so a malformed event
// fails without ever opening a transaction or touching state.
//
// Errors and rollback: any mutation or append error returns without commit and
// the deferred Rollback discards the whole transaction; the returned error is
// the underlying cause so callers keep their existing failure handling. A
// commit error is reported and the transaction is likewise discarded.
//
// Retry: none. The single-writer pool serializes transactions and the shared
// 5s busy_timeout absorbs brief contention, so there is no retry loop to add;
// a caller that must proceed regardless of event persistence is out of scope
// for coupling and uses AppendMetadataEventBestEffort instead.
func (d *DB) CommitWithEvent(ctx context.Context, input MetadataEventInput, mutate func(*sql.Tx) error) (*MetadataEvent, error) {
	events, err := d.CommitWithEvents(ctx, []MetadataEventInput{input}, mutate)
	if err != nil {
		return nil, err
	}
	return events[0], nil
}

// CommitWithEvents extends the TW-36 boundary to a fixed set of events for one
// authoritative mutation. TW-34 uses it when one durable source record carries
// more than one lifecycle fact, such as an invocation's source start and end
// timestamps or a terminal PR observation that also terminates CI. Every input
// is validated before BEGIN. The mutation runs once, all events and their typed
// metadata append in order, and then one commit makes the entire set durable.
func (d *DB) CommitWithEvents(ctx context.Context, inputs []MetadataEventInput, mutate func(*sql.Tx) error) ([]*MetadataEvent, error) {
	if len(inputs) == 0 {
		return nil, ErrInvalidMetadataEvent
	}
	for _, input := range inputs {
		if err := validateMetadataEventInput(input); err != nil {
			return nil, err
		}
	}
	recordedAt := time.Now().UTC()

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("commit with events: begin: %w", err)
	}
	defer tx.Rollback()

	if err := mutate(tx); err != nil {
		return nil, err
	}
	events := make([]*MetadataEvent, 0, len(inputs))
	for _, input := range inputs {
		event, err := appendMetadataEventTx(ctx, tx, input, recordedAt)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit with events: commit: %w", err)
	}
	for _, event := range events {
		d.fireEventAppended(event.Sequence)
	}
	return events, nil
}

// InsertRunWithEvent is the run-family coupled mutation: it creates the run row
// and appends the run-created event in one transaction. traceCtx must already
// be validated (daemon ingress does this); the append re-reads and revalidates
// the persisted carrier, so an invalid carrier fails closed and no run is
// created. Returns the constructed run only when the transaction commits.
func (d *DB) InsertRunWithEvent(ctx context.Context, repoID, branch, headSHA, baseSHA string, traceCtx *tracecontext.Context) (*Run, error) {
	r := newRunRecord(repoID, branch, headSHA, baseSHA, traceCtx)
	input := familyEventInput(EventTypeRunCreated, schemaRunCreated, r.ID)
	if _, err := d.CommitWithEvent(ctx, input, func(tx *sql.Tx) error {
		return insertRunExec(ctx, tx, r)
	}); err != nil {
		return nil, err
	}
	return r, nil
}

// StartStepWithEvent is the step-family coupled mutation: it marks the step
// running and appends the step-started event in one transaction. runID links
// the event; stepID is the step_results row to start.
func (d *DB) StartStepWithEvent(ctx context.Context, runID, stepID string, autoFixLimit int) error {
	input := familyEventInput(EventTypeStepStarted, schemaStepStarted, runID)
	_, err := d.CommitWithEvent(ctx, input, func(tx *sql.Tx) error {
		return startStepExec(ctx, tx, stepID, autoFixLimit)
	})
	return err
}

// SetRunAwaitingAgentWithEvent is the gate-family coupled mutation: it stamps
// the awaiting-agent gate marker and appends the gate event in one
// transaction.
func (d *DB) SetRunAwaitingAgentWithEvent(ctx context.Context, runID string) error {
	input := familyEventInput(EventTypeGateAwaitingAgent, schemaGateAwaitingAgent, runID)
	_, err := d.CommitWithEvent(ctx, input, func(tx *sql.Tx) error {
		return setRunAwaitingAgentExec(ctx, tx, runID)
	})
	return err
}

// InsertAgentInvocationWithEvent is the invocation-family coupled mutation. A
// completed local invocation row authoritatively carries both source moments,
// so its transaction emits an ordered started/completed pair linked by the
// row's stable invocation ID. The start fact becomes visible only after the row
// completes; no-mistakes does not invent a separate in-progress invocation
// state. The call site remains best-effort, so a recording failure never fails
// pipeline execution.
func (d *DB) InsertAgentInvocationWithEvent(ctx context.Context, inv AgentInvocation) error {
	inv.ID = newID()
	started := invocationEventInput(inv, true)
	completed := invocationEventInput(inv, false)
	_, err := d.CommitWithEvents(ctx, []MetadataEventInput{started, completed}, func(tx *sql.Tx) error {
		return insertAgentInvocationExec(ctx, tx, inv)
	})
	return err
}

func invocationEventInput(inv AgentInvocation, started bool) MetadataEventInput {
	metadata := &InvocationEventMetadata{
		InvocationID: inv.ID,
		Step:         normalizeLifecycleStep(inv.StepName),
		Purpose:      normalizeInvocationPurpose(inv.Purpose),
		SessionMode:  normalizeInvocationSessionMode(inv.SessionMode),
	}
	eventType := EventTypeInvocationStarted
	schema := schemaInvocationStarted
	source := time.Unix(inv.StartedAt, 0).UTC()
	if started {
		metadata.Phase = "started"
	} else {
		eventType = EventTypeInvocationCompleted
		schema = schemaInvocationCompleted
		source = time.Unix(inv.CompletedAt, 0).UTC()
		metadata.Phase = "completed"
		duration := inv.DurationMS
		metadata.DurationMS = &duration
		metadata.Outcome = normalizeInvocationOutcome(inv.ExitStatus)
		metadata.FailureCategory = normalizeInvocationFailure(inv.FailureCategory)
		metadata.Usage = invocationUsageMetadata(inv)
		metadata.Activity = invocationActivityMetadata(inv)
	}
	input := familyEventInput(eventType, schema, inv.RunID)
	input.SourceTimestamp = source
	input.invocation = metadata
	return input
}

func normalizeLifecycleStep(step string) string {
	switch step {
	case "intent", "rebase", "review", "test", "document", "lint", "push", "pr", "ci":
		return step
	default:
		return "unknown"
	}
}

func normalizeInvocationPurpose(purpose string) string {
	switch purpose {
	case "intent", "intent-fix", "rebase", "rebase-fix", "review", "review-fix", "test", "test-evidence", "test-fix", "document", "document-fix", "housekeeping", "lint", "lint-fix", "push", "pr", "ci", "ci-fix":
		return purpose
	default:
		return "other"
	}
}

func normalizeInvocationSessionMode(mode string) string {
	switch mode {
	case InvocationModeCold, InvocationModeStarted, InvocationModeResumed, InvocationModeFallback:
		return mode
	default:
		return "other"
	}
}

func normalizeInvocationOutcome(outcome string) string {
	switch outcome {
	case "ok", "error", "cancelled":
		return outcome
	default:
		return "unknown"
	}
}

func normalizeInvocationFailure(category string) string {
	switch category {
	case "":
		return ""
	case "parse", "exit", "spawn", "cancelled", "other":
		return category
	default:
		return "other"
	}
}

func invocationUsageMetadata(inv AgentInvocation) *InvocationUsageMetadata {
	reported := inv.FreshInputTokens != nil || inv.CacheCreationTokens != nil || inv.ReasoningTokens != nil ||
		inv.DeltaInputTokens != nil || inv.DeltaOutputTokens != nil || inv.DeltaCacheReadTokens != nil
	if !reported {
		return nil
	}
	var input, output, cacheRead *int
	if inv.DeltaInputTokens != nil {
		value := inv.InputTokens
		input = &value
	}
	if inv.DeltaOutputTokens != nil {
		value := inv.OutputTokens
		output = &value
	}
	if inv.DeltaCacheReadTokens != nil {
		value := inv.CacheReadTokens
		cacheRead = &value
	}
	return &InvocationUsageMetadata{
		InputTokens:          input,
		OutputTokens:         output,
		CacheReadTokens:      cacheRead,
		CacheCreationTokens:  inv.CacheCreationTokens,
		FreshInputTokens:     inv.FreshInputTokens,
		ReasoningTokens:      inv.ReasoningTokens,
		DeltaInputTokens:     inv.DeltaInputTokens,
		DeltaOutputTokens:    inv.DeltaOutputTokens,
		DeltaCacheReadTokens: inv.DeltaCacheReadTokens,
	}
}

func invocationActivityMetadata(inv AgentInvocation) *InvocationActivityMetadata {
	metadata := &InvocationActivityMetadata{
		ModelRoundtrips:   inv.ModelRoundtrips,
		ToolCalls:         inv.ToolCalls,
		ToolWaitCalls:     inv.ToolWaitCalls,
		ToolTestLintCalls: inv.ToolTestLintCalls,
		ToolEditCalls:     inv.ToolEditCalls,
		ToolReadCalls:     inv.ToolReadCalls,
		ToolGitCalls:      inv.ToolGitCalls,
		ToolOtherCalls:    inv.ToolOtherCalls,
		WorkloadFiles:     inv.WorkloadFiles,
		WorkloadLines:     inv.WorkloadLines,
		FindingCount:      inv.FindingCount,
	}
	if !invocationActivityPresent(metadata) {
		return nil
	}
	return metadata
}
