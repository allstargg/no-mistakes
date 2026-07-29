package db

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestInvocationLifecycleEventsPreserveTimestampsUsageAndUnknowns(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", &tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	input, output, cacheRead, reasoning := 17, 5, 11, 3
	inv := minimalInvocation(run.ID)
	inv.ID = "ignored-caller-id"
	inv.StartedAt = 100
	inv.CompletedAt = 104
	inv.DurationMS = 4000
	inv.SessionMode = InvocationModeResumed
	inv.FreshInputTokens = &input
	inv.DeltaInputTokens = &input
	inv.DeltaOutputTokens = &output
	inv.DeltaCacheReadTokens = &cacheRead
	inv.ReasoningTokens = &reasoning
	inv.InputTokens = 28
	inv.OutputTokens = output
	inv.CacheReadTokens = cacheRead
	modelTurns, tools, edits := 4, 6, 2
	inv.ModelRoundtrips = &modelTurns
	inv.ToolCalls = &tools
	inv.ToolEditCalls = &edits

	if err := d.InsertAgentInvocationWithEvent(ctx, inv); err != nil {
		t.Fatalf("InsertAgentInvocationWithEvent: %v", err)
	}
	events := allEvents(t, d)
	if len(events) != 2 || events[0].Type != EventTypeInvocationStarted || events[1].Type != EventTypeInvocationCompleted {
		t.Fatalf("invocation events = %#v, want started then completed", events)
	}
	started, completed := events[0], events[1]
	if got := started.SourceTimestamp.Unix(); got != inv.StartedAt {
		t.Fatalf("start source timestamp = %d, want %d", got, inv.StartedAt)
	}
	if got := completed.SourceTimestamp.Unix(); got != inv.CompletedAt {
		t.Fatalf("completion source timestamp = %d, want %d", got, inv.CompletedAt)
	}
	if started.Invocation == nil || completed.Invocation == nil || started.Invocation.InvocationID == "" || started.Invocation.InvocationID != completed.Invocation.InvocationID {
		t.Fatalf("invocation identity did not correlate start/end: start=%#v end=%#v", started.Invocation, completed.Invocation)
	}
	if started.Invocation.Usage != nil || started.Invocation.Activity != nil || started.Invocation.DurationMS != nil {
		t.Fatalf("start event fabricated completion facts: %#v", started.Invocation)
	}
	if completed.Invocation.Usage == nil || completed.Invocation.Usage.InputTokens == nil || *completed.Invocation.Usage.InputTokens != 28 || completed.Invocation.Usage.ReasoningTokens == nil || *completed.Invocation.Usage.ReasoningTokens != reasoning {
		t.Fatalf("completed usage = %#v", completed.Invocation.Usage)
	}
	if completed.Invocation.Activity == nil || completed.Invocation.Activity.ToolCalls == nil || *completed.Invocation.Activity.ToolCalls != tools || completed.Invocation.Activity.ToolEditCalls == nil || *completed.Invocation.Activity.ToolEditCalls != edits {
		t.Fatalf("completed activity = %#v", completed.Invocation.Activity)
	}

	unknown := minimalInvocation(run.ID)
	unknown.StartedAt = 200
	unknown.CompletedAt = 201
	if err := d.InsertAgentInvocationWithEvent(ctx, unknown); err != nil {
		t.Fatal(err)
	}
	events = allEvents(t, d)
	unknownEnd := events[len(events)-1]
	if unknownEnd.Invocation == nil || unknownEnd.Invocation.Usage != nil || unknownEnd.Invocation.Activity != nil {
		t.Fatalf("unreported usage/activity must remain unknown, got %#v", unknownEnd.Invocation)
	}
}

func TestGateLifecycleEventsCorrelateIdentityDurationAndDedupeAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "gate.sqlite")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := d.InsertRepo("/tmp/tw34-gate", "https://example.com/tw34.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.EnterRunGateWithEvent(ctx, run.ID, types.StepReview, GateClassApproval); err != nil {
		t.Fatal(err)
	}
	if err := d.EnterRunGateWithEvent(ctx, run.ID, types.StepReview, GateClassApproval); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.ExitRunGateWithEvent(ctx, run.ID, 2750, GateOutcomeApproved); err != nil {
		t.Fatal(err)
	}
	if err := d.ExitRunGateWithEvent(ctx, run.ID, 9999, GateOutcomeApproved); err != nil {
		t.Fatal(err)
	}
	events := allEvents(t, d)
	if len(events) != 2 || events[0].Type != EventTypeGateEntered || events[1].Type != EventTypeGateExited {
		t.Fatalf("gate events = %#v, want one enter and one exit", events)
	}
	enter, exit := events[0].Gate, events[1].Gate
	if enter == nil || exit == nil || enter.GateID == "" || enter.GateID != exit.GateID || enter.Step != string(types.StepReview) || enter.Class != string(GateClassApproval) {
		t.Fatalf("gate identity/class mismatch: enter=%#v exit=%#v", enter, exit)
	}
	if exit.WaitDurationMS == nil || *exit.WaitDurationMS != 2750 || exit.Outcome != string(GateOutcomeApproved) {
		t.Fatalf("gate exit = %#v", exit)
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.AwaitingAgentSince != nil || got.ParkedMS != 2750 {
		t.Fatalf("run gate state = awaiting %v parked %d", got.AwaitingAgentSince, got.ParkedMS)
	}
}

func TestCIAndPRLifecycleEventsAreTransitionBasedAndTerminalAtomic(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", &tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := d.RecordRunPROpenWithEvent(ctx, run.ID, "https://github.com/example/repo/pull/42", true); err != nil {
		t.Fatal(err)
	}
	// Replaying the same durable observations must not duplicate facts.
	if err := d.RecordRunPROpenWithEvent(ctx, run.ID, "https://github.com/example/repo/pull/42", true); err != nil {
		t.Fatal(err)
	}
	for _, observation := range []CIObservation{
		{State: CIStateRunning, Outcome: CIOutcomeChecks},
		{State: CIStateRunning, Outcome: CIOutcomeChecks},
		{State: CIStateFailure, Outcome: CIOutcomeChecks},
		{State: CIStateRunning, Outcome: CIOutcomeChecks},
		{State: CIStateGreen, Outcome: CIOutcomePassed},
		{State: CIStateGreen, Outcome: CIOutcomePassed, ReviewReady: true},
		{State: CIStateGreen, Outcome: CIOutcomePassed, ReviewReady: true},
	} {
		if err := d.UpdateRunCIWithEvents(ctx, run.ID, observation); err != nil {
			t.Fatalf("UpdateRunCIWithEvents(%#v): %v", observation, err)
		}
	}
	if err := d.UpdateRunPRStateWithEvent(ctx, run.ID, PRStateMerged); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunPRStateWithEvent(ctx, run.ID, PRStateMerged); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunPRStateWithEvent(ctx, run.ID, PRStateOpen); err != nil {
		t.Fatal(err)
	}

	events := allEvents(t, d)
	want := []MetadataEventType{
		EventTypePRCreated,
		EventTypeCIRunning, EventTypePRChecksWait,
		EventTypeCIFailure,
		EventTypeCIRunning,
		EventTypeCIGreen,
		EventTypeCIMergeWait, EventTypePRReviewWait,
		EventTypePRMerged, EventTypeCITerminal,
	}
	if len(events) != len(want) {
		t.Fatalf("event count = %d, want %d\nevents=%#v", len(events), len(want), events)
	}
	for i, typ := range want {
		if events[i].Type != typ {
			t.Fatalf("event[%d] type = %s, want %s", i, events[i].Type, typ)
		}
		if events[i].TraceContext == nil || events[i].TraceContext.Traceparent != validTraceparent {
			t.Fatalf("event[%d] lost trace linkage: %#v", i, events[i].TraceContext)
		}
	}
	if got := events[len(events)-1].CI; got == nil || got.State != string(CIStateTerminal) || got.Outcome != string(CIOutcomeMerged) {
		t.Fatalf("terminal CI metadata = %#v", got)
	}
	persisted, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != types.RunCompleted || persisted.PRState == nil || *persisted.PRState != string(PRStateMerged) {
		t.Fatalf("terminal run = status %s PR %#v", persisted.Status, persisted.PRState)
	}
}

func TestLifecycleMetadataFailureRollsBackSourceMutation(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.sql.Exec(`CREATE TRIGGER reject_gate_metadata BEFORE INSERT ON event_gate_metadata BEGIN SELECT RAISE(ABORT, 'reject lifecycle metadata'); END`); err != nil {
		t.Fatal(err)
	}
	if err := d.EnterRunGateWithEvent(ctx, run.ID, types.StepReview, GateClassApproval); err == nil {
		t.Fatal("expected lifecycle metadata fault")
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.AwaitingAgentSince != nil {
		t.Fatal("gate source mutation survived event metadata failure")
	}
	if events := allEvents(t, d); len(events) != 0 {
		t.Fatalf("rolled-back transition leaked events: %#v", events)
	}
}

func TestLifecycleEventsNormalizeUntrustedCategoriesWithoutLeakingRawValues(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", nil)
	if err != nil {
		t.Fatal(err)
	}
	inv := minimalInvocation(run.ID)
	inv.StepName = "https://user:secret@example.com/private-step"
	inv.Purpose = "raw arbitrary purpose with spaces"
	inv.SessionMode = "provider-special-mode"
	inv.ExitStatus = "fatal: prompt contents"
	inv.FailureCategory = "raw provider error"
	if err := d.InsertAgentInvocationWithEvent(ctx, inv); err != nil {
		t.Fatal(err)
	}
	events := allEvents(t, d)
	completed := events[len(events)-1].Invocation
	if completed == nil || completed.Step != "unknown" || completed.Purpose != "other" || completed.SessionMode != "other" || completed.Outcome != "unknown" || completed.FailureCategory != "other" {
		t.Fatalf("normalized invocation metadata = %#v", completed)
	}
	for _, raw := range []string{"secret", "private-step", inv.Purpose, inv.SessionMode, inv.ExitStatus, inv.FailureCategory} {
		if eventContainsLifecycleValue(events, raw) {
			t.Fatalf("event metadata leaked raw value %q", raw)
		}
	}
	if err := d.UpdateRunCIWithEvents(ctx, run.ID, CIObservation{State: CIState("provider-state"), Outcome: CIOutcome("raw error")}); !errors.Is(err, ErrInvalidMetadataEvent) {
		t.Fatalf("invalid CI vocabulary error = %v", err)
	}
}

func TestTerminalPRTransitionAtomicallyExitsActiveGate(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunStatus(run.ID, types.RunRunning); err != nil {
		t.Fatal(err)
	}
	if err := d.RecordRunPROpenWithEvent(ctx, run.ID, "https://github.com/example/repo/pull/42", true); err != nil {
		t.Fatal(err)
	}
	if err := d.EnterRunGateWithEvent(ctx, run.ID, types.StepCI, GateClassApproval); err != nil {
		t.Fatal(err)
	}
	if err := d.UpdateRunPRStateWithEvent(ctx, run.ID, PRStateClosed); err != nil {
		t.Fatal(err)
	}
	events := allEvents(t, d)
	if len(events) != 5 {
		t.Fatalf("terminal gate journey events = %#v", events)
	}
	if events[2].Type != EventTypePRClosed || events[3].Type != EventTypeCITerminal || events[4].Type != EventTypeGateExited {
		t.Fatalf("terminal ordering = %s, %s, %s", events[2].Type, events[3].Type, events[4].Type)
	}
	if events[4].Gate == nil || events[4].Gate.Outcome != string(GateOutcomeTerminal) || events[4].Gate.WaitDurationMS == nil {
		t.Fatalf("terminal gate exit metadata = %#v", events[4].Gate)
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != types.RunCompleted || got.AwaitingAgentSince != nil || got.AwaitingAgentGateID != nil {
		t.Fatalf("terminal gate state = %#v", got)
	}
}

func TestConcurrentIdenticalCIObservationsEmitOneTransitionSet(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", nil)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 24
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- d.UpdateRunCIWithEvents(ctx, run.ID, CIObservation{State: CIStateRunning, Outcome: CIOutcomeChecks})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	events := allEvents(t, d)
	if len(events) != 2 || events[0].Type != EventTypeCIRunning || events[1].Type != EventTypePRChecksWait {
		t.Fatalf("concurrent duplicate observations emitted %#v", events)
	}
}

func TestOpenMigratesTypedLifecycleMetadataIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lifecycle-migration.sqlite")
	for attempt := 0; attempt < 2; attempt++ {
		d, err := Open(path)
		if err != nil {
			t.Fatalf("Open attempt %d: %v", attempt+1, err)
		}
		for _, table := range []string{"event_invocation_metadata", "event_gate_metadata", "event_ci_metadata", "event_pr_metadata"} {
			var name string
			if err := d.sql.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name); err != nil || name != table {
				t.Fatalf("typed lifecycle table %s missing: name=%q err=%v", table, name, err)
			}
		}
		for _, column := range []string{"pr_activity", "ci_state", "ci_outcome", "awaiting_agent_gate_id", "awaiting_agent_step", "awaiting_agent_class"} {
			if !hasColumn(t, d, "runs", column) {
				t.Fatalf("runs.%s missing", column)
			}
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func eventContainsLifecycleValue(events []*MetadataEvent, value string) bool {
	for _, event := range events {
		if event.Invocation != nil {
			joined := strings.Join([]string{event.Invocation.InvocationID, event.Invocation.Step, event.Invocation.Purpose, event.Invocation.SessionMode, event.Invocation.Outcome, event.Invocation.FailureCategory}, "|")
			if strings.Contains(joined, value) {
				return true
			}
		}
	}
	return false
}

func TestCommitWithEventsRollsBackAllEventsAndMutation(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", nil)
	if err != nil {
		t.Fatal(err)
	}
	inputs := []MetadataEventInput{
		familyEventInput(EventTypeGateAwaitingAgent, schemaGateAwaitingAgent, run.ID),
		{}, // invalid second event must prevent the mutation from running
	}
	mutated := false
	_, err = d.CommitWithEvents(ctx, inputs, func(tx *sql.Tx) error {
		mutated = true
		return nil
	})
	if !errors.Is(err, ErrInvalidMetadataEvent) {
		t.Fatalf("CommitWithEvents error = %v", err)
	}
	if mutated {
		t.Fatal("mutation ran before every event was validated")
	}
}
