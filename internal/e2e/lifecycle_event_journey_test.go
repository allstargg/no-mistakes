//go:build e2e

package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// TestLifecycleEventJourney is the TW-34 prototype journey. A real gated push
// runs an agent invocation, parks and exits a review gate, creates a PR, sees CI
// running then green/review-ready, and finally observes merge. After the
// temporary daemon restarts, TW-37 replays every typed fact with TW-38 trace
// correlation and no duplicate identity or sequence.
func TestLifecycleEventJourney(t *testing.T) {
	h := NewHarness(t, SetupOpts{Agent: "claude", Scenario: axiScenario(t)})
	ctx := context.Background()
	parentURL := "https://github.com/tracewake/no-mistakes.git"
	forkURL := "https://github.com/tracewake-fork/no-mistakes.git"
	forkDir := filepath.Join(filepath.Dir(h.UpstreamDir), "lifecycle-fork.git")
	if err := os.MkdirAll(forkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := h.runGit(ctx, forkDir, "init", "--bare", "--initial-branch=main"); err != nil {
		t.Fatalf("init fork: %v\n%s", err, out)
	}
	if out, err := h.runGit(ctx, h.WorkDir, "push", forkDir, "main"); err != nil {
		t.Fatalf("seed fork: %v\n%s", err, out)
	}
	configureGitURLRewrite(t, h, parentURL, h.UpstreamDir)
	configureGitURLRewrite(t, h, forkURL, forkDir)
	if out, err := h.runGit(ctx, h.WorkDir, "remote", "set-url", "origin", parentURL); err != nil {
		t.Fatalf("set origin: %v\n%s", err, out)
	}

	root := filepath.Dir(h.AgentLog)
	t.Setenv("FAKEAGENT_GH_MODE", "fork-pr")
	t.Setenv("FAKEAGENT_GH_PARENT", "tracewake/no-mistakes")
	t.Setenv("FAKEAGENT_GH_STATE_FILE", filepath.Join(root, "gh-state-count"))
	t.Setenv("FAKEAGENT_GH_CHECKS_FILE", filepath.Join(root, "gh-check-count"))
	if out, err := h.Run("init", "--fork-url", forkURL); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}

	const (
		branch      = "feature/tw34-lifecycle"
		traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		tracestate  = "tracewake=tw34"
	)
	h.CommitChange(branch, "lifecycle.txt", "typed lifecycle events\n", "add lifecycle journey")
	pushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := h.runGit(pushCtx, h.WorkDir, "push",
		"-o", "no-mistakes.traceparent="+traceparent,
		"-o", "no-mistakes.tracestate="+tracestate,
		"no-mistakes", branch); err != nil {
		t.Fatalf("push: %v\n%s", err, out)
	}

	run := h.WaitForRunRunning(branch, 30*time.Second)
	deadline := time.Now().Add(45 * time.Second)
	for {
		run = h.RunInfo(run.ID)
		reviewParked := false
		for _, step := range run.Steps {
			if step.StepName == types.StepReview && step.Status == types.StepStatusAwaitingApproval {
				reviewParked = true
				break
			}
		}
		if run.AwaitingAgent && reviewParked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("review gate did not park: %#v", run)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Leave a measurable wait in the gate duration without making the test slow.
	time.Sleep(20 * time.Millisecond)
	h.Respond(run.ID, types.StepReview, types.ActionApprove)

	completed := h.WaitForRun(branch, 120*time.Second)
	if completed.Status != types.RunCompleted {
		t.Fatalf("run status = %s, error=%v", completed.Status, completed.Error)
	}
	if completed.PRURL == nil {
		t.Fatal("journey did not create a PR")
	}

	// Restart only the harness-owned temporary daemon, then replay from durable
	// storage through the public TW-37 subscription.
	if out, err := h.Run("daemon", "restart"); err != nil {
		t.Fatalf("restart temporary daemon: %v\n%s", err, out)
	}
	socket := paths.WithRoot(h.NMHome).Socket()
	var frames <-chan ipc.EventStreamFrame
	var stop func()
	var err error
	deadline = time.Now().Add(20 * time.Second)
	for {
		frames, stop, err = ipc.SubscribeEvents(socket, &ipc.SubscribeEventsParams{
			Version: ipc.SubscribeEventsVersion,
			Filter:  &ipc.EventFilter{RunIDs: []string{run.ID}},
		})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("subscribe after restart: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	events, _ := drainEventFrames(frames, 3*time.Second, 20*time.Second)
	stop()
	assertMetadataOnlyAndOrdered(t, events)

	byType := make(map[string][]ipc.MetadataEventInfo)
	seenSequence := make(map[int64]bool)
	seenID := make(map[string]bool)
	for _, event := range events {
		if seenSequence[event.Sequence] || seenID[event.EventID] {
			t.Fatalf("duplicate replay fact: sequence=%d id=%s", event.Sequence, event.EventID)
		}
		seenSequence[event.Sequence] = true
		seenID[event.EventID] = true
		if event.TraceContext == nil || event.TraceContext.Traceparent != traceparent || event.TraceContext.Tracestate != tracestate {
			t.Fatalf("event %s trace = %#v", event.Type, event.TraceContext)
		}
		byType[event.Type] = append(byType[event.Type], event)
	}
	for _, typ := range []db.MetadataEventType{
		db.EventTypeInvocationStarted, db.EventTypeInvocationCompleted,
		db.EventTypeGateEntered, db.EventTypeGateExited,
		db.EventTypePRCreated, db.EventTypePRChecksWait, db.EventTypePRReviewWait, db.EventTypePRMerged,
		db.EventTypeCIRunning, db.EventTypeCIGreen, db.EventTypeCIMergeWait, db.EventTypeCITerminal,
	} {
		if len(byType[string(typ)]) == 0 {
			t.Fatalf("replay missing %s; types=%v", typ, eventTypeKeys(byType))
		}
	}

	starts := make(map[string]bool)
	for _, event := range byType[string(db.EventTypeInvocationStarted)] {
		if event.Invocation == nil {
			t.Fatalf("invocation start missing typed metadata: %#v", event)
		}
		starts[event.Invocation.InvocationID] = true
	}
	for _, event := range byType[string(db.EventTypeInvocationCompleted)] {
		if event.Invocation == nil || !starts[event.Invocation.InvocationID] {
			t.Fatalf("invocation completion lacks correlated start: %#v", event.Invocation)
		}
	}
	enter := byType[string(db.EventTypeGateEntered)][0].Gate
	exit := byType[string(db.EventTypeGateExited)][0].Gate
	if enter == nil || exit == nil || enter.GateID != exit.GateID || exit.WaitDurationMS == nil {
		t.Fatalf("gate correlation/duration = enter %#v exit %#v", enter, exit)
	}
}

func eventTypeKeys(events map[string][]ipc.MetadataEventInfo) []string {
	keys := make([]string, 0, len(events))
	for key := range events {
		keys = append(keys, key)
	}
	return keys
}
