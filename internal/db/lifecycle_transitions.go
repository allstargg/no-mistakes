package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/types"
)

// GateClass is the bounded class of a persisted approval wait.
type GateClass string

const (
	GateClassApproval  GateClass = "approval"
	GateClassFixReview GateClass = "fix_review"
	GateClassUnknown   GateClass = "unknown"
)

// GateOutcome is the bounded result of leaving a persisted gate wait.
type GateOutcome string

const (
	GateOutcomeApproved     GateOutcome = "approved"
	GateOutcomeFixRequested GateOutcome = "fix_requested"
	GateOutcomeSkipped      GateOutcome = "skipped"
	GateOutcomeAborted      GateOutcome = "aborted"
	GateOutcomeReconciled   GateOutcome = "reconciled"
	GateOutcomeCancelled    GateOutcome = "cancelled"
	GateOutcomeTerminal     GateOutcome = "terminal"
	GateOutcomeFailed       GateOutcome = "failed"
	GateOutcomeUnknown      GateOutcome = "unknown"
)

// CIState and CIOutcome mirror the existing CI monitor vocabulary. Outcomes
// are deliberately an allowlist and never contain check names or provider
// errors.
type CIState string
type CIOutcome string

const (
	CIStateRunning  CIState = "running"
	CIStateGreen    CIState = "green"
	CIStateFailure  CIState = "failure"
	CIStateTerminal CIState = "terminal"

	CIOutcomeChecks                 CIOutcome = "checks"
	CIOutcomePassed                 CIOutcome = "passed"
	CIOutcomeNoChecks               CIOutcome = "no_checks"
	CIOutcomeMergeConflict          CIOutcome = "merge_conflict"
	CIOutcomeChecksAndMergeConflict CIOutcome = "checks_and_merge_conflict"
	CIOutcomeMerged                 CIOutcome = "merged"
	CIOutcomeClosed                 CIOutcome = "closed"
	CIOutcomeUnknown                CIOutcome = "unknown"
)

// CIObservation is one normalized source observation from the CI monitor.
type CIObservation struct {
	State       CIState
	Outcome     CIOutcome
	ReviewReady bool
}

// PRState is the normalized durable lifecycle state already owned by runs.
type PRState string

const (
	PRStateOpen   PRState = "open"
	PRStateMerged PRState = "merged"
	PRStateClosed PRState = "closed"
)

// EnterRunGateWithEvent sets the authoritative awaiting-agent marker and its
// restart-safe gate identity, then appends gate.entered in the same transaction.
// The WHERE transition guard makes an identical retry a no-op with no duplicate
// event.
func (d *DB) EnterRunGateWithEvent(ctx context.Context, runID string, step types.StepName, class GateClass) error {
	gateID := newID()
	ts := now()
	metadata := &GateEventMetadata{
		GateID: gateID,
		Phase:  "entered",
		Step:   normalizeLifecycleStep(string(step)),
		Class:  normalizeGateClass(class),
	}
	input := lifecycleInput(EventTypeGateEntered, schemaGateEntered, runID, time.Unix(ts, 0).UTC())
	input.gate = metadata
	if err := validateMetadataEventInput(input); err != nil {
		return err
	}

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("enter run gate: begin: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE runs SET awaiting_agent_since = ?, awaiting_agent_gate_id = ?, awaiting_agent_step = ?, awaiting_agent_class = ?, updated_at = ?
		WHERE id = ? AND awaiting_agent_since IS NULL`, ts, gateID, metadata.Step, metadata.Class, ts, runID)
	if err != nil {
		return fmt.Errorf("enter run gate: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("enter run gate: rows affected: %w", err)
	}
	if changed == 0 {
		return tx.Commit()
	}
	event, err := appendMetadataEventTx(ctx, tx, input, time.Now().UTC())
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("enter run gate: commit: %w", err)
	}
	d.fireEventAppended(event.Sequence)
	return nil
}

// ExitRunGateWithEvent clears one active marker, accumulates its measured wait,
// and appends gate.exited atomically. A retry after the marker is already clear
// is a no-op, including after daemon restart.
func (d *DB) ExitRunGateWithEvent(ctx context.Context, runID string, waitMS int64, outcome GateOutcome) error {
	if waitMS < 0 {
		waitMS = 0
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("exit run gate: begin: %w", err)
	}
	defer tx.Rollback()

	var since sql.NullInt64
	var gateID, step, class sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT awaiting_agent_since, awaiting_agent_gate_id, awaiting_agent_step, awaiting_agent_class FROM runs WHERE id = ?`, runID).
		Scan(&since, &gateID, &step, &class); err != nil {
		if err == sql.ErrNoRows {
			return tx.Commit()
		}
		return fmt.Errorf("exit run gate: read marker: %w", err)
	}
	if !since.Valid {
		return tx.Commit()
	}
	identity := gateID.String
	if identity == "" {
		identity = legacyGateID(runID, since.Int64)
	}
	detail := &GateEventMetadata{
		GateID:         identity,
		Phase:          "exited",
		Step:           normalizeLifecycleStep(step.String),
		Class:          normalizeGateClass(GateClass(class.String)),
		Outcome:        normalizeGateOutcome(outcome),
		WaitDurationMS: &waitMS,
	}
	input := lifecycleInput(EventTypeGateExited, schemaGateExited, runID, time.Now().UTC())
	input.gate = detail
	if err := validateMetadataEventInput(input); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET awaiting_agent_since = NULL, awaiting_agent_gate_id = NULL, awaiting_agent_step = NULL, awaiting_agent_class = NULL,
		parked_ms = COALESCE(parked_ms, 0) + ?, updated_at = ? WHERE id = ? AND awaiting_agent_since IS NOT NULL`, waitMS, now(), runID); err != nil {
		return fmt.Errorf("exit run gate: %w", err)
	}
	event, err := appendMetadataEventTx(ctx, tx, input, time.Now().UTC())
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("exit run gate: commit: %w", err)
	}
	d.fireEventAppended(event.Sequence)
	return nil
}

// RecordRunPROpenWithEvent records the PR URL/open state after the provider
// mutation succeeds and emits either pr.created or pr.opened. The URL remains
// only in run state; event metadata never copies it. Repeating the same durable
// observation emits nothing.
func (d *DB) RecordRunPROpenWithEvent(ctx context.Context, runID, prURL string, created bool) error {
	ts := now()
	typ, schema, state := EventTypePROpened, schemaPROpened, "open"
	if created {
		typ, schema, state = EventTypePRCreated, schemaPRCreated, "created"
	}
	input := lifecycleInput(typ, schema, runID, time.Unix(ts, 0).UTC())
	input.pr = &PREventMetadata{State: state}
	if err := validateMetadataEventInput(input); err != nil {
		return err
	}

	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("record run PR open: begin: %w", err)
	}
	defer tx.Rollback()
	var currentURL, currentState sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT pr_url, pr_state FROM runs WHERE id = ?`, runID).Scan(&currentURL, &currentState); err != nil {
		if err == sql.ErrNoRows {
			return tx.Commit()
		}
		return fmt.Errorf("record run PR open: read current state: %w", err)
	}
	if terminalPRState(currentState.String) {
		if currentURL.String != prURL {
			if _, err := tx.ExecContext(ctx, `UPDATE runs SET pr_url = ?, updated_at = ? WHERE id = ?`, prURL, ts, runID); err != nil {
				return fmt.Errorf("record run PR open: preserve terminal URL: %w", err)
			}
		}
		return tx.Commit()
	}
	if currentURL.String == prURL && currentState.String == string(PRStateOpen) {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET pr_url = ?, pr_state = 'open', pr_state_observed_at = ?,
		pr_activity = CASE WHEN pr_activity IN ('checks_wait', 'review_wait') THEN pr_activity ELSE 'open' END,
		updated_at = ? WHERE id = ?`, prURL, ts, ts, runID); err != nil {
		return fmt.Errorf("record run PR open: %w", err)
	}
	event, err := appendMetadataEventTx(ctx, tx, input, time.Now().UTC())
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("record run PR open: commit: %w", err)
	}
	d.fireEventAppended(event.Sequence)
	return nil
}

// UpdateRunCIWithEvents persists a normalized CI state, readiness, and the PR's
// checks/review wait activity with the events that describe only changed facts.
// Consecutive identical observations and replay after restart are no-ops.
func (d *DB) UpdateRunCIWithEvents(ctx context.Context, runID string, observation CIObservation) error {
	if !validCIObservation(observation) {
		return ErrInvalidMetadataEvent
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("update run CI lifecycle: begin: %w", err)
	}
	defer tx.Rollback()

	var currentState, currentOutcome, currentActivity, prState sql.NullString
	var readyAt sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT ci_state, ci_outcome, ci_ready_at, pr_activity, pr_state FROM runs WHERE id = ?`, runID).
		Scan(&currentState, &currentOutcome, &readyAt, &currentActivity, &prState); err != nil {
		if err == sql.ErrNoRows {
			return tx.Commit()
		}
		return fmt.Errorf("update run CI lifecycle: read current state: %w", err)
	}
	if terminalPRState(prState.String) {
		return tx.Commit()
	}
	ts := now()
	source := time.Unix(ts, 0).UTC()
	inputs := make([]MetadataEventInput, 0, 3)
	stateChanged := currentState.String != string(observation.State) || currentOutcome.String != string(observation.Outcome)
	if stateChanged {
		typ, schema := ciEventIdentity(observation.State)
		input := lifecycleInput(typ, schema, runID, source)
		input.ci = &CIEventMetadata{State: string(observation.State), Outcome: string(observation.Outcome)}
		inputs = append(inputs, input)
	}
	readyChanged := readyAt.Valid != observation.ReviewReady
	if observation.ReviewReady && readyChanged {
		input := lifecycleInput(EventTypeCIMergeWait, schemaCIMergeWait, runID, source)
		input.ci = &CIEventMetadata{State: "merge_wait", Outcome: string(observation.Outcome)}
		inputs = append(inputs, input)
	}
	desiredActivity := "checks_wait"
	activityType, activitySchema := EventTypePRChecksWait, schemaPRChecksWait
	if observation.ReviewReady {
		desiredActivity = "review_wait"
		activityType, activitySchema = EventTypePRReviewWait, schemaPRReviewWait
	}
	activityChanged := currentActivity.String != desiredActivity
	if activityChanged {
		input := lifecycleInput(activityType, activitySchema, runID, source)
		input.pr = &PREventMetadata{State: desiredActivity}
		inputs = append(inputs, input)
	}
	if len(inputs) == 0 {
		return tx.Commit()
	}
	for _, input := range inputs {
		if err := validateMetadataEventInput(input); err != nil {
			return err
		}
	}
	var nextReadyAt any
	if observation.ReviewReady {
		if readyAt.Valid {
			nextReadyAt = readyAt.Int64
		} else {
			nextReadyAt = ts
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET ci_state = ?, ci_outcome = ?, ci_ready_at = ?, pr_activity = ?, updated_at = ? WHERE id = ?`,
		observation.State, observation.Outcome, nextReadyAt, desiredActivity, ts, runID); err != nil {
		return fmt.Errorf("update run CI lifecycle: %w", err)
	}
	events, err := appendLifecycleInputs(ctx, tx, inputs)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("update run CI lifecycle: commit: %w", err)
	}
	d.fireLifecycleEvents(events)
	return nil
}

// UpdateRunPRStateWithEvent persists a monotonic normalized PR observation.
// A merged or closed transition also finalizes the active run and CI step and
// emits PR/CI terminal facts in that same transaction. If a CI approval gate
// is active, its terminal exit and wait duration join the same commit.
func (d *DB) UpdateRunPRStateWithEvent(ctx context.Context, runID string, observed PRState) error {
	if observed != PRStateOpen && observed != PRStateMerged && observed != PRStateClosed {
		return ErrInvalidMetadataEvent
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("update run PR lifecycle: begin: %w", err)
	}
	defer tx.Rollback()

	var current sql.NullString
	var awaitingSince sql.NullInt64
	var gateID, gateStep, gateClass sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT pr_state, awaiting_agent_since, awaiting_agent_gate_id, awaiting_agent_step, awaiting_agent_class FROM runs WHERE id = ?`, runID).
		Scan(&current, &awaitingSince, &gateID, &gateStep, &gateClass); err != nil {
		if err == sql.ErrNoRows {
			return tx.Commit()
		}
		return fmt.Errorf("update run PR lifecycle: read current state: %w", err)
	}
	next := PRState(monotonicPRState(current.String, string(observed)))
	if next == PRState(current.String) {
		return tx.Commit()
	}
	ts := now()
	source := time.Unix(ts, 0).UTC()
	inputs := make([]MetadataEventInput, 0, 3)
	prType, prSchema, prMetadataState := EventTypePROpened, schemaPROpened, "open"
	if next == PRStateMerged {
		prType, prSchema, prMetadataState = EventTypePRMerged, schemaPRMerged, "merged"
	} else if next == PRStateClosed {
		prType, prSchema, prMetadataState = EventTypePRClosed, schemaPRClosed, "closed"
	}
	prInput := lifecycleInput(prType, prSchema, runID, source)
	prInput.pr = &PREventMetadata{State: prMetadataState}
	inputs = append(inputs, prInput)

	if next == PRStateMerged || next == PRStateClosed {
		ciOutcome := CIOutcomeMerged
		if next == PRStateClosed {
			ciOutcome = CIOutcomeClosed
		}
		ciInput := lifecycleInput(EventTypeCITerminal, schemaCITerminal, runID, source)
		ciInput.ci = &CIEventMetadata{State: "terminal", Outcome: string(ciOutcome)}
		inputs = append(inputs, ciInput)
		if awaitingSince.Valid {
			waitMS := int64(0)
			if ts > awaitingSince.Int64 {
				waitMS = (ts - awaitingSince.Int64) * 1000
			}
			identity := gateID.String
			if identity == "" {
				identity = legacyGateID(runID, awaitingSince.Int64)
			}
			gateInput := lifecycleInput(EventTypeGateExited, schemaGateExited, runID, source)
			gateInput.gate = &GateEventMetadata{
				GateID: identity, Phase: "exited", Step: normalizeLifecycleStep(gateStep.String),
				Class: normalizeGateClass(GateClass(gateClass.String)), Outcome: string(GateOutcomeTerminal), WaitDurationMS: &waitMS,
			}
			inputs = append(inputs, gateInput)
		}
	}
	for _, input := range inputs {
		if err := validateMetadataEventInput(input); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET pr_state = ?, pr_state_observed_at = ?,
		pr_activity = ?, ci_state = CASE WHEN ? IN ('merged', 'closed') THEN 'terminal' ELSE ci_state END,
		ci_outcome = CASE WHEN ? IN ('merged', 'closed') THEN ? ELSE ci_outcome END,
		ci_ready_at = CASE WHEN ? IN ('merged', 'closed') THEN NULL ELSE ci_ready_at END, updated_at = ? WHERE id = ?`,
		next, ts, next, next, next, next, next, ts, runID); err != nil {
		return fmt.Errorf("update run PR lifecycle: %w", err)
	}
	if terminalPRState(string(next)) {
		if err := finalizeTerminalPRRun(tx, runID, ts); err != nil {
			return fmt.Errorf("update run PR lifecycle: %w", err)
		}
	}
	events, err := appendLifecycleInputs(ctx, tx, inputs)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("update run PR lifecycle: commit: %w", err)
	}
	d.fireLifecycleEvents(events)
	if terminalPRState(string(next)) {
		d.fireRunTerminal(runID)
	}
	return nil
}

func lifecycleInput(eventType MetadataEventType, schema MetadataPayloadSchema, runID string, source time.Time) MetadataEventInput {
	return MetadataEventInput{SourceTimestamp: source, Type: eventType, PayloadSchema: schema, PayloadVersion: 1, RunID: runID}
}

func appendLifecycleInputs(ctx context.Context, tx *sql.Tx, inputs []MetadataEventInput) ([]*MetadataEvent, error) {
	recordedAt := time.Now().UTC()
	events := make([]*MetadataEvent, 0, len(inputs))
	for _, input := range inputs {
		event, err := appendMetadataEventTx(ctx, tx, input, recordedAt)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (d *DB) fireLifecycleEvents(events []*MetadataEvent) {
	for _, event := range events {
		d.fireEventAppended(event.Sequence)
	}
}

func ciEventIdentity(state CIState) (MetadataEventType, MetadataPayloadSchema) {
	switch state {
	case CIStateGreen:
		return EventTypeCIGreen, schemaCIGreen
	case CIStateFailure:
		return EventTypeCIFailure, schemaCIFailure
	default:
		return EventTypeCIRunning, schemaCIRunning
	}
}

func validCIObservation(observation CIObservation) bool {
	switch observation.State {
	case CIStateRunning:
		return observation.Outcome == CIOutcomeChecks || observation.Outcome == CIOutcomeUnknown
	case CIStateGreen:
		return observation.Outcome == CIOutcomePassed || observation.Outcome == CIOutcomeNoChecks
	case CIStateFailure:
		return observation.Outcome == CIOutcomeChecks || observation.Outcome == CIOutcomeMergeConflict || observation.Outcome == CIOutcomeChecksAndMergeConflict
	default:
		return false
	}
}

func normalizeGateClass(class GateClass) string {
	if class == GateClassApproval || class == GateClassFixReview {
		return string(class)
	}
	return string(GateClassUnknown)
}

func normalizeGateOutcome(outcome GateOutcome) string {
	switch outcome {
	case GateOutcomeApproved, GateOutcomeFixRequested, GateOutcomeSkipped, GateOutcomeAborted, GateOutcomeReconciled,
		GateOutcomeCancelled, GateOutcomeTerminal, GateOutcomeFailed:
		return string(outcome)
	default:
		return string(GateOutcomeUnknown)
	}
}

func legacyGateID(runID string, since int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", runID, since)))
	return hex.EncodeToString(sum[:8])
}
