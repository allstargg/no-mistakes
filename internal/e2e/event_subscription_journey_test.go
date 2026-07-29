//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// TestEventSubscriptionJourney is the TW-37 prototype end-to-end: create events
// across multiple runs, subscribe globally without any known run ID, disconnect,
// create more events, restart the daemon, resume from the previously returned
// cursor, and receive each retained event exactly once in sequence.
func TestEventSubscriptionJourney(t *testing.T) {
	h := NewHarness(t, SetupOpts{Agent: "claude"})
	if out, err := h.Run("init"); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}
	socket := paths.WithRoot(h.NMHome).Socket()

	// Feature discovery: the daemon advertises the capability so a consumer can
	// select global mode instead of falling back to per-run subscription.
	if !daemonSupportsGlobalEvents(t, socket) {
		t.Fatal("daemon did not advertise the subscribe_events capability")
	}

	// Two completed runs on distinct branches produce durable metadata events.
	runA := completeRun(t, h, "feature/events-a", "a.txt")
	runB := completeRun(t, h, "feature/events-b", "b.txt")

	// Subscribe globally from the beginning and replay the retained backlog.
	frames, cancel, err := ipc.SubscribeEvents(socket, &ipc.SubscribeEventsParams{Version: ipc.SubscribeEventsVersion})
	if err != nil {
		t.Fatalf("subscribe events: %v", err)
	}
	backlog, cursor := drainEventFrames(frames, 3*time.Second, 20*time.Second)
	cancel()

	if cursor == "" {
		t.Fatal("no cursor returned from the initial subscription")
	}
	runsInBacklog := runIDsOf(backlog)
	if !runsInBacklog[runA.ID] || !runsInBacklog[runB.ID] {
		t.Fatalf("backlog missing run events: have runs %v, want %s and %s", keys(runsInBacklog), runA.ID, runB.ID)
	}
	assertMetadataOnlyAndOrdered(t, backlog)
	cursorSeq, err := ipc.DecodeEventCursor(cursor)
	if err != nil {
		t.Fatalf("decode returned cursor %q: %v", cursor, err)
	}

	// Disconnected: create more events on a third run.
	runC := completeRun(t, h, "feature/events-c", "c.txt")

	// Restart the daemon; durable replay must survive it. (This is the
	// harness's own temporary daemon, not the shared service.)
	if out, err := h.Run("daemon", "restart"); err != nil {
		t.Fatalf("daemon restart: %v\n%s", err, out)
	}

	// Resume from the previously returned cursor after the restart.
	var resumeFrames <-chan ipc.EventStreamFrame
	var resumeCancel func()
	deadline := time.Now().Add(20 * time.Second)
	for {
		resumeFrames, resumeCancel, err = ipc.SubscribeEvents(socket, &ipc.SubscribeEventsParams{
			Version: ipc.SubscribeEventsVersion,
			Cursor:  cursor,
		})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("resume subscribe after restart: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	resumed, resumeCursor := drainEventFrames(resumeFrames, 3*time.Second, 20*time.Second)
	resumeCancel()

	// Every resumed event is strictly after the cursor: no already-consumed
	// event from run A or B is redelivered (exactly once from the client view).
	runsResumed := runIDsOf(resumed)
	if !runsResumed[runC.ID] {
		t.Fatalf("resume did not deliver run C events: have runs %v, want %s", keys(runsResumed), runC.ID)
	}
	if runsResumed[runA.ID] || runsResumed[runB.ID] {
		t.Fatalf("resume redelivered already-consumed events: runs %v", keys(runsResumed))
	}
	for _, event := range resumed {
		if event.Sequence <= cursorSeq {
			t.Fatalf("resumed event seq %d is not after cursor seq %d", event.Sequence, cursorSeq)
		}
	}
	assertMetadataOnlyAndOrdered(t, resumed)

	// No sequence is delivered in both phases: exactly once across the journey.
	seen := make(map[int64]bool, len(backlog))
	for _, event := range backlog {
		seen[event.Sequence] = true
	}
	for _, event := range resumed {
		if seen[event.Sequence] {
			t.Fatalf("sequence %d delivered in both phases (not exactly once)", event.Sequence)
		}
	}
	if resumeCursor != "" {
		if seq, err := ipc.DecodeEventCursor(resumeCursor); err != nil || seq < cursorSeq {
			t.Fatalf("resume cursor %q regressed below %d", resumeCursor, cursorSeq)
		}
	}
}

func daemonSupportsGlobalEvents(t *testing.T, socket string) bool {
	t.Helper()
	client, err := ipc.Dial(socket)
	if err != nil {
		t.Fatalf("dial daemon: %v", err)
	}
	defer client.Close()
	caps, err := client.Capabilities()
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	return caps.Supports(ipc.CapabilitySubscribeEvents, ipc.SubscribeEventsVersion)
}

// completeRun commits and pushes a one-file change on branch and waits for the
// resulting pipeline run to finish, returning the run.
func completeRun(t *testing.T, h *Harness, branch, file string) *ipc.RunInfo {
	t.Helper()
	h.CommitChange(branch, file, "content for "+branch+"\n", "add "+file)
	h.PushToGate(branch)
	run := h.WaitForRun(branch, 60*time.Second)
	if run.Status != types.RunCompleted {
		t.Fatalf("run for %s status = %q, want completed (error=%v)", branch, run.Status, run.Error)
	}
	return run
}

// drainEventFrames reads frames until the stream is quiet for quiet, the channel
// closes, or max elapses. It returns the event frames and the highest cursor
// seen across all frames (events and checkpoints).
func drainEventFrames(frames <-chan ipc.EventStreamFrame, quiet, max time.Duration) ([]ipc.MetadataEventInfo, string) {
	var events []ipc.MetadataEventInfo
	cursor := ""
	cursorSeq := int64(-1)
	hardDeadline := time.After(max)
	quietTimer := time.NewTimer(quiet)
	defer quietTimer.Stop()
	for {
		select {
		case frame, ok := <-frames:
			if !ok {
				return events, cursor
			}
			if frame.Cursor != "" {
				if seq, err := ipc.DecodeEventCursor(frame.Cursor); err == nil && seq > cursorSeq {
					cursorSeq = seq
					cursor = frame.Cursor
				}
			}
			if frame.Kind == ipc.EventStreamFrameEvent && frame.Event != nil {
				events = append(events, *frame.Event)
			}
			if !quietTimer.Stop() {
				select {
				case <-quietTimer.C:
				default:
				}
			}
			quietTimer.Reset(quiet)
		case <-quietTimer.C:
			return events, cursor
		case <-hardDeadline:
			return events, cursor
		}
	}
}

func runIDsOf(events []ipc.MetadataEventInfo) map[string]bool {
	out := make(map[string]bool)
	for _, event := range events {
		if event.RunID != nil {
			out[*event.RunID] = true
		}
	}
	return out
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// assertMetadataOnlyAndOrdered verifies the content-exclusion classification
// and strict sequence ordering the stream guarantees.
func assertMetadataOnlyAndOrdered(t *testing.T, events []ipc.MetadataEventInfo) {
	t.Helper()
	prev := int64(-1)
	for i, event := range events {
		if event.ContentClass != "metadata" {
			t.Fatalf("event[%d] content_class = %q, want metadata", i, event.ContentClass)
		}
		if !strings.HasPrefix(event.Type, "io.no_mistakes.") {
			t.Fatalf("event[%d] type = %q, want io.no_mistakes.* namespace", i, event.Type)
		}
		if event.Sequence <= prev {
			t.Fatalf("events not strictly ascending at %d: %d after %d", i, event.Sequence, prev)
		}
		prev = event.Sequence
	}
}
