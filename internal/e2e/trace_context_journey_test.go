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

	// TW-36 couples in-scope run/step/gate/invocation mutations with a metadata
	// event in one transaction, so a real completed run leaves durable events.
	// Reopen storage and prove the pipeline's own events survived and stay
	// mutually consistent with committed state and the run's TW-38 trace.
	reopened, err := db.OpenReadOnly(paths.WithRoot(h.NMHome).DB())
	if err != nil {
		t.Fatalf("reopen event store read-only: %v", err)
	}
	defer reopened.Close()
	events, err := reopened.ReadMetadataEvents(context.Background(), 0, db.MaxMetadataEventReadBatch)
	if err != nil {
		t.Fatalf("read metadata events after reopen: %v", err)
	}
	// A completed run creates itself and starts at least one step, so it emits
	// more than a single event through the coupled transaction boundary.
	if len(events) < 2 {
		t.Fatalf("pipeline metadata events after reopen = %d, want >= 2", len(events))
	}
	runCreated := 0
	for _, got := range events {
		// Every event maps to committed state and carries this run's trace.
		if got.RunID == nil || *got.RunID != run.ID {
			t.Fatalf("metadata event %s run linkage = %v, want %q", got.Type, got.RunID, run.ID)
		}
		if r, err := reopened.GetRun(*got.RunID); err != nil || r == nil {
			t.Fatalf("metadata event %s claims run %q that did not commit: %v", got.Type, run.ID, err)
		}
		if got.TraceContext == nil || got.TraceContext.Traceparent != traceparent || got.TraceContext.Tracestate != tracestate {
			t.Fatalf("metadata event %s trace linkage = %#v", got.Type, got.TraceContext)
		}
		if got.Type == db.EventTypeRunCreated {
			runCreated++
		}
	}
	if runCreated != 1 {
		t.Fatalf("run.created events = %d, want exactly 1", runCreated)
	}
}
