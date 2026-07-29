package cli

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/paths"
	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
)

func TestRerunTransfersIncomingTraceContextThroughTypedIPC(t *testing.T) {
	nmHome := makeSocketSafeTempDir(t)
	t.Setenv("NM_HOME", nmHome)
	t.Setenv(tracecontext.EnvTraceparent, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	t.Setenv(tracecontext.EnvTracestate, "tracewake=prototype")
	p := paths.WithRoot(nmHome)
	if err := p.EnsureDirs(); err != nil {
		t.Fatal(err)
	}

	workDir := filepath.Join(t.TempDir(), "work")
	if err := exec.Command("git", "init", "--initial-branch=feature/trace", workDir).Run(); err != nil {
		t.Fatalf("git init: %v", err)
	}
	for _, args := range [][]string{
		{"-C", workDir, "config", "user.email", "trace@example.com"},
		{"-C", workDir, "config", "user.name", "Trace Test"},
		{"-C", workDir, "commit", "--allow-empty", "-m", "initial"},
	} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if resolved, err := filepath.EvalSymlinks(workDir); err == nil {
		workDir = resolved
	}
	t.Chdir(workDir)
	database, err := db.Open(p.DB())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.InsertRepo(workDir, "https://example.com/owner/repo.git", "main"); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	received := make(chan ipc.RerunParams, 1)
	srv := ipc.NewServer()
	srv.Handle(ipc.MethodHealth, func(context.Context, json.RawMessage) (interface{}, error) {
		return &ipc.HealthResult{Status: "ok"}, nil
	})
	srv.Handle(ipc.MethodRerun, func(_ context.Context, raw json.RawMessage) (interface{}, error) {
		var params ipc.RerunParams
		if err := json.Unmarshal(raw, &params); err != nil {
			return nil, err
		}
		received <- params
		return &ipc.RerunResult{RunID: "run-1"}, nil
	})
	if err := srv.Listen(p.Socket()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.ServeReady() }()
	t.Cleanup(srv.Close)

	cmd := newRerunCmd()
	if err := cmd.RunE(cmd, nil); err != nil {
		t.Fatalf("rerun: %v", err)
	}
	params := <-received
	if params.TraceContext == nil {
		t.Fatal("rerun IPC omitted trace_context")
	}
	if params.TraceContext.Traceparent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" || params.TraceContext.Tracestate != "tracewake=prototype" {
		t.Fatalf("rerun trace_context = %#v", params.TraceContext)
	}
}
