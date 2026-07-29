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

// TW-36 couples exactly one authoritative mutation per state family (run,
// step, gate, invocation) with a single metadata event that describes it. The
// set is intentionally minimal: it proves the transaction boundary across all
// four families without inventing lifecycle coverage, which TW-34 owns. Each
// identifier is a bounded, content-free, source-owned versioned type; the
// payload schema matches because the event carries no payload.
const (
	// EventTypeRunCreated is emitted with the run row's creation.
	EventTypeRunCreated MetadataEventType = "io.no_mistakes.run.created.v1"
	// EventTypeStepStarted is emitted when a step transitions to running.
	EventTypeStepStarted MetadataEventType = "io.no_mistakes.step.started.v1"
	// EventTypeGateAwaitingAgent is emitted when a run parks at an approval
	// gate awaiting the driving agent.
	EventTypeGateAwaitingAgent MetadataEventType = "io.no_mistakes.gate.awaiting_agent.v1"
	// EventTypeInvocationRecorded is emitted with one agent invocation's local
	// performance row.
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
// that rolled back.
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
	if err := validateMetadataEventInput(input); err != nil {
		return nil, err
	}
	recordedAt := time.Now().UTC()

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("commit with event: begin: %w", err)
	}
	defer tx.Rollback()

	if err := mutate(tx); err != nil {
		return nil, err
	}
	event, err := appendMetadataEventTx(ctx, tx, input, recordedAt)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit with event: commit: %w", err)
	}
	return event, nil
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

// InsertAgentInvocationWithEvent is the invocation-family coupled mutation: it
// records one invocation row and appends the invocation event in one
// transaction. The invocation itself remains best-effort at the call site (a
// failure is logged and the pipeline continues); coupling only guarantees that
// when it IS recorded, its event is recorded with it, and never one without
// the other.
func (d *DB) InsertAgentInvocationWithEvent(ctx context.Context, inv AgentInvocation) error {
	inv.ID = newID()
	input := familyEventInput(EventTypeInvocationRecorded, schemaInvocationRecorded, inv.RunID)
	_, err := d.CommitWithEvent(ctx, input, func(tx *sql.Tx) error {
		return insertAgentInvocationExec(ctx, tx, inv)
	})
	return err
}
