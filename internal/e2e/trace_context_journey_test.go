//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

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
}
