package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
	"github.com/kunchenguid/no-mistakes/internal/pipeline"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

func TestMetadataEventCleanupFailureDoesNotBlockPipelineAndLogsSafeDiagnostic(t *testing.T) {
	oldCleanup := cleanupMetadataEvents
	cleanupMetadataEvents = func(context.Context, *db.DB, time.Duration, time.Time, int) (int64, error) {
		return 0, errors.New("https://user:secret@example.com/private")
	}
	defer func() { cleanupMetadataEvents = oldCleanup }()

	oldLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(oldLogger)

	step := &mockPassStep{name: types.StepReview}
	p, database := startTestDaemonWithSteps(t, func() []pipeline.Step { return []pipeline.Step{step} })
	_, headSHA := setupTestGitRepo(t, p, database, "event-cleanup-failure-repo")

	client, err := ipc.Dial(p.Socket())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var result ipc.PushReceivedResult
	if err := client.Call(ipc.MethodPushReceived, &ipc.PushReceivedParams{
		Gate: p.RepoDir("event-cleanup-failure-repo"),
		Ref:  "refs/heads/main",
		Old:  "0000000000000000000000000000000000000000",
		New:  headSHA,
	}, &result); err != nil {
		t.Fatalf("cleanup failure blocked run creation: %v", err)
	}
	run := waitForRunTerminalState(t, database, result.RunID)
	if run.Status != types.RunCompleted {
		t.Fatalf("run status = %q, want completed", run.Status)
	}
	if step.execCnt.Load() != 1 {
		t.Fatalf("step executions = %d, want 1", step.execCnt.Load())
	}

	got := logs.String()
	if !strings.Contains(got, string(db.MetadataEventDiagnosticCleanupFailed)) {
		t.Fatalf("cleanup diagnostic log = %q", got)
	}
	for _, secret := range []string{"secret", "private", "example.com"} {
		if strings.Contains(got, secret) {
			t.Fatalf("cleanup diagnostic leaked %q: %s", secret, got)
		}
	}
}
