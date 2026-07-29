//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

// TestTraceContextJourney exercises the public Git boundary end to end: a W3C
// parent enters through narrowly named push options, crosses the managed hook
// and typed daemon IPC, survives run persistence, and is returned by get_run
// after the pipeline completes successfully.
func TestTraceContextJourney(t *testing.T) {
	h := NewHarness(t, SetupOpts{Agent: "claude"})
	if out, err := h.Run("init"); err != nil {
		t.Fatalf("init: %v\n%s", err, out)
	}

	const (
		branch      = "feature/trace-context"
		traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		tracestate  = "tracewake=prototype"
	)
	h.CommitChange(branch, "trace.txt", "trace context journey\n", "test trace context journey")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := h.runGit(ctx, h.WorkDir,
		"push",
		"-o", "no-mistakes.traceparent="+traceparent,
		"-o", "no-mistakes.tracestate="+tracestate,
		"no-mistakes", branch,
	)
	if err != nil {
		t.Fatalf("push with trace context: %v\n%s", err, out)
	}

	run := h.WaitForRun(branch, 45*time.Second)
	if run.Status != types.RunCompleted {
		t.Fatalf("pipeline status = %q, want completed: error=%v", run.Status, run.Error)
	}
	persisted := h.RunInfo(run.ID)
	if persisted.TraceContext == nil {
		t.Fatal("get_run omitted persisted trace context")
	}
	if persisted.TraceContext.Traceparent != traceparent || persisted.TraceContext.Tracestate != tracestate {
		t.Fatalf("get_run trace context = %#v, want traceparent=%q tracestate=%q", persisted.TraceContext, traceparent, tracestate)
	}

	// TW-33 provides the typed durable seam but deliberately does not emit
	// lifecycle coverage (TW-34). Append one representative event explicitly,
	// reopen storage, and prove sequence plus run/trace linkage survive.
	store, err := db.Open(paths.WithRoot(h.NMHome).DB())
	if err != nil {
		t.Fatalf("open event store: %v", err)
	}
	event, err := store.AppendMetadataEvent(context.Background(), db.MetadataEventInput{
		SourceTimestamp: time.Now().UTC(),
		Type:            db.MetadataEventType("io.no_mistakes.run.completed.v1"),
		PayloadSchema:   db.MetadataPayloadSchema("io.no_mistakes.run.v1"),
		PayloadVersion:  1,
		RunID:           run.ID,
	})
	if err != nil {
		_ = store.Close()
		t.Fatalf("append representative metadata event: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close event store: %v", err)
	}

	reopened, err := db.OpenReadOnly(paths.WithRoot(h.NMHome).DB())
	if err != nil {
		t.Fatalf("reopen event store read-only: %v", err)
	}
	defer reopened.Close()
	events, err := reopened.ReadMetadataEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("read metadata events after reopen: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("metadata events after reopen = %d, want 1", len(events))
	}
	got := events[0]
	if got.EventID != event.EventID || got.Sequence != event.Sequence {
		t.Fatalf("metadata event identity = %#v, want %#v", got, event)
	}
	if got.RunID == nil || *got.RunID != run.ID {
		t.Fatalf("metadata event run linkage = %v, want %q", got.RunID, run.ID)
	}
	if got.TraceContext == nil || got.TraceContext.Traceparent != traceparent || got.TraceContext.Tracestate != tracestate {
		t.Fatalf("metadata event trace linkage = %#v", got.TraceContext)
	}
}
