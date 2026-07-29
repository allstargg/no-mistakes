package db

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const validTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func openEventTxDB(t *testing.T) (*DB, *Repo) {
	t.Helper()
	d := openTestDB(t)
	repo, err := d.InsertRepo("/tmp/tw36", "https://example.com/tw36.git", "main")
	if err != nil {
		t.Fatalf("insert repo: %v", err)
	}
	return d, repo
}

// corruptRunTrace stores a non-NULL but invalid W3C parent on a run so the
// linked metadata-event append fails deterministically after the state
// mutation has run inside the transaction. This is the fault used to prove
// rollback of an already-applied state change.
func corruptRunTrace(t *testing.T, d *DB, runID string) {
	t.Helper()
	if _, err := d.sql.Exec(`UPDATE runs SET traceparent = ? WHERE id = ?`, "not-a-valid-traceparent", runID); err != nil {
		t.Fatalf("corrupt run trace: %v", err)
	}
}

func allEvents(t *testing.T, d *DB) []*MetadataEvent {
	t.Helper()
	events, err := d.ReadMetadataEvents(context.Background(), 0, MaxMetadataEventReadBatch)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	return events
}

func minimalInvocation(runID string) AgentInvocation {
	return AgentInvocation{
		RunID:       runID,
		StepName:    "review",
		Round:       1,
		Purpose:     "review",
		Agent:       "codex",
		SessionMode: InvocationModeCold,
		StartedAt:   1,
		CompletedAt: 2,
		DurationMS:  1000,
		ExitStatus:  "ok",
	}
}

// TestUncoupledStateAndEventCanDiverge reproduces the pre-TW-36 counterfactual:
// a state write and a separately attempted metadata event are two independent
// transactions, so a deterministic fault on the event leaves committed state
// with no event describing it. This is exactly the integrity gap CommitWithEvent
// closes; the companion assertion shows the coupled path cannot diverge.
func TestUncoupledStateAndEventCanDiverge(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	corruptRunTrace(t, d, run.ID)

	// Uncoupled: the state write commits on its own connection...
	if err := d.SetRunAwaitingAgent(run.ID); err != nil {
		t.Fatalf("plain state write: %v", err)
	}
	// ...and the separate event append fails on the corrupt trace.
	if _, err := d.AppendMetadataEvent(ctx, familyEventInput(EventTypeGateAwaitingAgent, schemaGateAwaitingAgent, run.ID)); err == nil {
		t.Fatal("expected the separate event append to fail on corrupt trace")
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.AwaitingAgentSince == nil {
		t.Fatal("counterfactual not reproduced: state did not commit")
	}
	if n := len(allEvents(t, d)); n != 0 {
		t.Fatalf("counterfactual not reproduced: event count = %d, want 0", n)
	}
	// State committed with no event describing it: the divergence TW-36 fixes.

	// Coupled: the same fault on a fresh run rolls state and event back together.
	run2, err := d.InsertRunWithTraceContext(repo.ID, "feature2", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	corruptRunTrace(t, d, run2.ID)
	if err := d.SetRunAwaitingAgentWithEvent(ctx, run2.ID); err == nil {
		t.Fatal("expected coupled write to fail on corrupt trace")
	}
	got2, err := d.GetRun(run2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got2.AwaitingAgentSince != nil {
		t.Fatal("coupled write leaked state after event failure")
	}
	if n := len(allEvents(t, d)); n != 0 {
		t.Fatalf("coupled write leaked an event after rollback: count = %d", n)
	}
}

// TestCommitWithEvent_CommitsStateAndEventTogether proves the happy path: a
// state mutation and its event both survive a storage reopen with matching
// linkage.
func TestCommitWithEvent_CommitsStateAndEventTogether(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := d.InsertRepo("/tmp/tw36", "https://example.com/tw36.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent, Tracestate: "tracewake=prototype"})
	if err != nil {
		t.Fatal(err)
	}
	event, err := d.CommitWithEvent(ctx,
		familyEventInput(EventTypeGateAwaitingAgent, schemaGateAwaitingAgent, run.ID),
		func(tx *sql.Tx) error { return setRunAwaitingAgentExec(ctx, tx, run.ID) })
	if err != nil {
		t.Fatalf("CommitWithEvent: %v", err)
	}
	if event.Sequence <= 0 || event.EventID == "" {
		t.Fatalf("event identity = seq %d id %q", event.Sequence, event.EventID)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.AwaitingAgentSince == nil {
		t.Fatal("state missing after reopen")
	}
	events := allEvents(t, reopened)
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("event missing/mismatched after reopen: %#v", events)
	}
	if events[0].RunID == nil || *events[0].RunID != run.ID {
		t.Fatalf("event run linkage = %v", events[0].RunID)
	}
	if events[0].TraceContext == nil || events[0].TraceContext.Traceparent != validTraceparent {
		t.Fatalf("event trace linkage = %#v", events[0].TraceContext)
	}
}

// TestCommitWithEvent_MutationErrorWritesNoEvent proves the "no event claims
// state that did not commit" direction: when the mutation fails, the whole
// transaction rolls back and no event is written.
func TestCommitWithEvent_MutationErrorWritesNoEvent(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", nil)
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("mutation failed")
	sentinel := "sentinel-should-roll-back"
	_, err = d.CommitWithEvent(ctx,
		familyEventInput(EventTypeGateAwaitingAgent, schemaGateAwaitingAgent, run.ID),
		func(tx *sql.Tx) error {
			// Apply a real state write, then fail: proves the write is discarded.
			if _, e := tx.ExecContext(ctx, `UPDATE runs SET branch = ? WHERE id = ?`, sentinel, run.ID); e != nil {
				return e
			}
			return boom
		})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Branch == sentinel {
		t.Fatal("state mutation was not rolled back after mutation error")
	}
	if n := len(allEvents(t, d)); n != 0 {
		t.Fatalf("event count = %d, want 0", n)
	}
}

// TestCommitWithEvent_EventFailureRollsBackState proves the "no committed state
// lacks its event" direction: when the event append fails after the mutation
// has already applied inside the transaction, the mutation is rolled back too.
func TestCommitWithEvent_EventFailureRollsBackState(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	corruptRunTrace(t, d, run.ID)

	_, err = d.CommitWithEvent(ctx,
		familyEventInput(EventTypeGateAwaitingAgent, schemaGateAwaitingAgent, run.ID),
		func(tx *sql.Tx) error { return setRunAwaitingAgentExec(ctx, tx, run.ID) })
	if !errors.Is(err, ErrInvalidMetadataEventTraceContext) {
		t.Fatalf("error = %v, want ErrInvalidMetadataEventTraceContext", err)
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.AwaitingAgentSince != nil {
		t.Fatal("state mutation survived a failed event append")
	}
	if n := len(allEvents(t, d)); n != 0 {
		t.Fatalf("event count = %d, want 0", n)
	}
}

// TestCommitWithEvent_InvalidEventDoesNotRunMutation proves the event input is
// validated before BEGIN, so a malformed event never opens a transaction or
// touches state.
func TestCommitWithEvent_InvalidEventDoesNotRunMutation(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", nil)
	if err != nil {
		t.Fatal(err)
	}
	mutated := false
	bad := MetadataEventInput{RunID: run.ID} // zero timestamp/type: invalid
	_, err = d.CommitWithEvent(ctx, bad, func(tx *sql.Tx) error {
		mutated = true
		return nil
	})
	if !errors.Is(err, ErrInvalidMetadataEvent) {
		t.Fatalf("error = %v, want ErrInvalidMetadataEvent", err)
	}
	if mutated {
		t.Fatal("mutation ran for an invalid event input")
	}
	if n := len(allEvents(t, d)); n != 0 {
		t.Fatalf("event count = %d, want 0", n)
	}
}

// TestInsertRunWithEvent_AtomicAndFaultRollback covers the run family: the
// happy path commits run row and run-created event together, and an invalid
// trace carrier fails the append so no run is created.
func TestInsertRunWithEvent_AtomicAndFaultRollback(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)

	run, err := d.InsertRunWithEvent(ctx, repo.ID, "feature", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatalf("InsertRunWithEvent: %v", err)
	}
	got, err := d.GetRun(run.ID)
	if err != nil || got == nil {
		t.Fatalf("run not created: %v", err)
	}
	events := allEvents(t, d)
	if len(events) != 1 || events[0].Type != EventTypeRunCreated {
		t.Fatalf("events = %#v, want one run.created", events)
	}
	if events[0].RunID == nil || *events[0].RunID != run.ID {
		t.Fatalf("event run linkage = %v", events[0].RunID)
	}

	// Fault: an invalid parent carrier fails the coupled append, so the run is
	// not created and no event is written.
	faulted, err := d.InsertRunWithEvent(ctx, repo.ID, "feature2", "head", "base",
		&tracecontext.Context{Traceparent: "bogus-parent"})
	if err == nil {
		t.Fatal("expected InsertRunWithEvent to fail on invalid trace")
	}
	if faulted != nil {
		t.Fatalf("run returned despite rollback: %#v", faulted)
	}
	if n := len(allEvents(t, d)); n != 1 {
		t.Fatalf("event count = %d, want 1 (no event from the faulted insert)", n)
	}
	runs, err := d.GetRunsByRepo(repo.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("run count = %d, want 1 (faulted run rolled back)", len(runs))
	}
}

// TestStartStepWithEvent_AtomicAndFaultRollback covers the step family.
func TestStartStepWithEvent_AtomicAndFaultRollback(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	step, err := d.InsertStepResult(run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}

	if err := d.StartStepWithEvent(ctx, run.ID, step.ID, 3); err != nil {
		t.Fatalf("StartStepWithEvent: %v", err)
	}
	gotStep, err := d.GetStepResult(step.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotStep.Status != types.StepStatusRunning {
		t.Fatalf("step status = %q, want running", gotStep.Status)
	}
	events := allEvents(t, d)
	if len(events) != 1 || events[0].Type != EventTypeStepStarted {
		t.Fatalf("events = %#v, want one step.started", events)
	}

	// Fault: corrupt the run trace so the append fails; the step start rolls back.
	step2, err := d.InsertStepResult(run.ID, types.StepTest)
	if err != nil {
		t.Fatal(err)
	}
	corruptRunTrace(t, d, run.ID)
	if err := d.StartStepWithEvent(ctx, run.ID, step2.ID, 0); err == nil {
		t.Fatal("expected StartStepWithEvent to fail on corrupt trace")
	}
	gotStep2, err := d.GetStepResult(step2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotStep2.Status != types.StepStatusPending {
		t.Fatalf("faulted step status = %q, want pending (rolled back)", gotStep2.Status)
	}
	if n := len(allEvents(t, d)); n != 1 {
		t.Fatalf("event count = %d, want 1", n)
	}
}

// TestSetRunAwaitingAgentWithEvent_Atomic covers the gate family happy path;
// its fault rollback is exercised by TestUncoupledStateAndEventCanDiverge.
func TestSetRunAwaitingAgentWithEvent_Atomic(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SetRunAwaitingAgentWithEvent(ctx, run.ID); err != nil {
		t.Fatalf("SetRunAwaitingAgentWithEvent: %v", err)
	}
	got, err := d.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.AwaitingAgentSince == nil {
		t.Fatal("awaiting marker not set")
	}
	events := allEvents(t, d)
	if len(events) != 1 || events[0].Type != EventTypeGateAwaitingAgent {
		t.Fatalf("events = %#v, want one gate.awaiting_agent", events)
	}
}

// TestInsertAgentInvocationWithEvent_AtomicAndFKFaultRollback covers the
// invocation family: the happy path commits row and event together, and an
// invocation for a non-existent run fails the mutation (FK) so no event lands.
func TestInsertAgentInvocationWithEvent_AtomicAndFKFaultRollback(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	run, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.InsertAgentInvocationWithEvent(ctx, minimalInvocation(run.ID)); err != nil {
		t.Fatalf("InsertAgentInvocationWithEvent: %v", err)
	}
	invs, err := d.GetAgentInvocationsByRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(invs) != 1 {
		t.Fatalf("invocation count = %d, want 1", len(invs))
	}
	events := allEvents(t, d)
	if len(events) != 2 || events[0].Type != EventTypeInvocationStarted || events[1].Type != EventTypeInvocationCompleted {
		t.Fatalf("events = %#v, want invocation started/completed pair", events)
	}

	// Fault: a foreign-key violation inside the mutation rolls the whole thing
	// back, so no event is written for an invocation that never persisted.
	if err := d.InsertAgentInvocationWithEvent(ctx, minimalInvocation("nonexistent-run")); err == nil {
		t.Fatal("expected FK violation for a non-existent run")
	}
	if n := len(allEvents(t, d)); n != 2 {
		t.Fatalf("event count = %d, want 2 (no event from the faulted insert)", n)
	}
}

// TestCoupledMutationsRemainMutuallyConsistentAfterReopen runs a representative
// end-to-end-shaped batch of coupled mutations across all four families, then
// reopens storage and proves every committed state row has its event and every
// event maps to committed state (criterion 9 at the storage layer).
func TestCoupledMutationsRemainMutuallyConsistentAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journey.sqlite")
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := d.InsertRepo("/tmp/tw36", "https://example.com/tw36.git", "main")
	if err != nil {
		t.Fatal(err)
	}

	run, err := d.InsertRunWithEvent(ctx, repo.ID, "feature", "head", "base",
		&tracecontext.Context{Traceparent: validTraceparent})
	if err != nil {
		t.Fatal(err)
	}
	step, err := d.InsertStepResult(run.ID, types.StepReview)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.StartStepWithEvent(ctx, run.ID, step.ID, 0); err != nil {
		t.Fatal(err)
	}
	if err := d.SetRunAwaitingAgentWithEvent(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if err := d.InsertAgentInvocationWithEvent(ctx, minimalInvocation(run.ID)); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	events := allEvents(t, reopened)

	wantTypes := map[MetadataEventType]bool{
		EventTypeRunCreated:          true,
		EventTypeStepStarted:         true,
		EventTypeGateAwaitingAgent:   true,
		EventTypeInvocationStarted:   true,
		EventTypeInvocationCompleted: true,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("event count = %d, want %d", len(events), len(wantTypes))
	}
	seen := map[MetadataEventType]bool{}
	for _, e := range events {
		seen[e.Type] = true
		// Every event maps to committed state: the linked run exists.
		if e.RunID == nil {
			t.Fatalf("event %s has no run linkage", e.Type)
		}
		if r, err := reopened.GetRun(*e.RunID); err != nil || r == nil {
			t.Fatalf("event %s claims run %v that did not commit", e.Type, e.RunID)
		}
	}
	for want := range wantTypes {
		if !seen[want] {
			t.Fatalf("missing event %s for committed state", want)
		}
	}

	// Every committed state family has its event.
	gotRun, _ := reopened.GetRun(run.ID)
	if gotRun == nil || gotRun.AwaitingAgentSince == nil {
		t.Fatal("committed gate state missing after reopen")
	}
	gotStep, _ := reopened.GetStepResult(step.ID)
	if gotStep == nil || gotStep.Status != types.StepStatusRunning {
		t.Fatal("committed step state missing after reopen")
	}
	if invs, _ := reopened.GetAgentInvocationsByRun(run.ID); len(invs) != 1 {
		t.Fatal("committed invocation state missing after reopen")
	}
}

// TestCommitWithEvent_ConcurrentCoupledMutationsSerialize proves the coupled
// path is race-free and that the single-writer pool serializes transactions:
// concurrent run creations all commit with unique, monotonic event sequences,
// each linked to a committed run. Run under -race.
func TestCommitWithEvent_ConcurrentCoupledMutationsSerialize(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	const n = 40

	var wg sync.WaitGroup
	runIDs := make(chan string, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, err := d.InsertRunWithEvent(ctx, repo.ID, "feature", "head", "base",
				&tracecontext.Context{Traceparent: validTraceparent})
			if err != nil {
				errs <- err
				return
			}
			runIDs <- run.ID
		}()
	}
	wg.Wait()
	close(errs)
	close(runIDs)
	for err := range errs {
		t.Fatalf("concurrent coupled mutation: %v", err)
	}
	committed := map[string]bool{}
	for id := range runIDs {
		committed[id] = true
	}
	if len(committed) != n {
		t.Fatalf("committed runs = %d, want %d", len(committed), n)
	}

	events := allEvents(t, d)
	if len(events) != n {
		t.Fatalf("event count = %d, want %d", len(events), n)
	}
	ids := map[string]bool{}
	seqs := make([]int64, 0, n)
	for _, e := range events {
		if ids[e.EventID] {
			t.Fatalf("duplicate event id %q", e.EventID)
		}
		ids[e.EventID] = true
		seqs = append(seqs, e.Sequence)
		if e.RunID == nil || !committed[*e.RunID] {
			t.Fatalf("event %s not linked to a committed run", e.EventID)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("sequences not unique/monotonic: %v", seqs)
		}
	}
}

// TestInsertRunWithEvent_MatchesPlainInsertShape defends against parallel
// ownership drift: the event-coupled run insert persists the same row shape as
// the plain insert because both go through insertRunExec.
func TestInsertRunWithEvent_MatchesPlainInsertShape(t *testing.T) {
	ctx := context.Background()
	d, repo := openEventTxDB(t)
	trace := &tracecontext.Context{Traceparent: validTraceparent, Tracestate: "tracewake=prototype"}

	plain, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", trace)
	if err != nil {
		t.Fatal(err)
	}
	coupled, err := d.InsertRunWithEvent(ctx, repo.ID, "feature", "head", "base", trace)
	if err != nil {
		t.Fatal(err)
	}
	gotPlain, _ := d.GetRun(plain.ID)
	gotCoupled, _ := d.GetRun(coupled.ID)

	if gotPlain.Status != gotCoupled.Status ||
		gotPlain.Branch != gotCoupled.Branch ||
		gotPlain.HeadSHA != gotCoupled.HeadSHA ||
		gotPlain.BaseSHA != gotCoupled.BaseSHA ||
		deref(gotPlain.SubmittedHeadSHA) != deref(gotCoupled.SubmittedHeadSHA) ||
		deref(gotPlain.Traceparent) != deref(gotCoupled.Traceparent) ||
		deref(gotPlain.Tracestate) != deref(gotCoupled.Tracestate) ||
		deref(gotPlain.PRState) != deref(gotCoupled.PRState) {
		t.Fatalf("coupled insert diverged from plain insert:\nplain=%#v\ncoupled=%#v", gotPlain, gotCoupled)
	}
	// The coupled insert additionally wrote exactly one event; the plain did not.
	if n := len(allEvents(t, d)); n != 1 {
		t.Fatalf("event count = %d, want 1 (only the coupled insert emits)", n)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
