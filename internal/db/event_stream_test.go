package db

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The retention watermark and the append hook are the two storage primitives
// TW-37's global subscription is built on: the watermark decides typed cursor
// expiry after retention removed history, and the hook is the O(1) wake-up that
// keeps a slow subscriber from ever delaying a database write.

func TestPurgedThroughSequenceStartsAtZeroAndAdvancesWithRetention(t *testing.T) {
	database := openTestDB(t)
	ctx := context.Background()

	if watermark, err := database.PurgedThroughSequence(ctx); err != nil || watermark != 0 {
		t.Fatalf("initial watermark = %d, err = %v, want 0", watermark, err)
	}

	repo, err := database.InsertRepo("/work/watermark", "https://example.com/watermark.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := database.InsertRun(repo.ID, "terminal", "head", "base")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(terminal.ID, "completed"); err != nil {
		t.Fatal(err)
	}

	reference := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	old := reference.Add(-48 * time.Hour)
	var seqs []int64
	for i := 0; i < 4; i++ {
		event, err := database.appendMetadataEventAt(ctx, metadataEventInput(terminal.ID, old.Add(time.Duration(i)*time.Second)), old)
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, event.Sequence)
	}

	// Delete the two oldest eligible events; the watermark must equal the
	// largest deleted sequence exactly, never the largest remaining one.
	deleted, err := database.CleanupMetadataEvents(ctx, 24*time.Hour, reference, 2)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	watermark, err := database.PurgedThroughSequence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if watermark != seqs[1] {
		t.Fatalf("watermark = %d, want %d (second-oldest deleted sequence)", watermark, seqs[1])
	}

	// A no-op cleanup (nothing eligible) never regresses the watermark.
	future := reference.Add(-72 * time.Hour) // cutoff older than every event
	if deleted, err := database.CleanupMetadataEvents(ctx, 24*time.Hour, future, 10); err != nil || deleted != 0 {
		t.Fatalf("expected no deletions, got deleted=%d err=%v", deleted, err)
	}
	if again, err := database.PurgedThroughSequence(ctx); err != nil || again != watermark {
		t.Fatalf("watermark regressed to %d (err %v), want stable %d", again, err, watermark)
	}
}

func TestPurgedThroughSequenceSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	path := dir + "/state.sqlite"

	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-72 * time.Hour)
	var last int64
	for i := 0; i < 3; i++ {
		event, err := database.appendMetadataEventAt(ctx, metadataEventInput("", old), old)
		if err != nil {
			t.Fatal(err)
		}
		last = event.Sequence
	}
	if _, err := database.CleanupMetadataEvents(ctx, time.Hour, time.Now().UTC(), 10); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	watermark, err := reopened.PurgedThroughSequence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if watermark != last {
		t.Fatalf("watermark after reopen = %d, want %d", watermark, last)
	}
}

func TestLatestEventSequence(t *testing.T) {
	database := openTestDB(t)
	ctx := context.Background()
	if latest, err := database.LatestEventSequence(ctx); err != nil || latest != 0 {
		t.Fatalf("empty-log latest = %d, err = %v, want 0", latest, err)
	}
	source := time.Now().UTC()
	var want int64
	for i := 0; i < 3; i++ {
		event, err := database.appendMetadataEventAt(ctx, metadataEventInput("", source), source)
		if err != nil {
			t.Fatal(err)
		}
		want = event.Sequence
	}
	if latest, err := database.LatestEventSequence(ctx); err != nil || latest != want {
		t.Fatalf("latest = %d, err = %v, want %d", latest, err, want)
	}
}

func TestEventAppendedHookFiresWithCommittedSequence(t *testing.T) {
	database := openTestDB(t)
	ctx := context.Background()

	var mu sync.Mutex
	var got []int64
	database.SetEventAppendedHook(func(sequence int64) {
		mu.Lock()
		got = append(got, sequence)
		mu.Unlock()
	})

	source := time.Now().UTC()
	var want []int64
	for i := 0; i < 3; i++ {
		event, err := database.appendMetadataEventAt(ctx, metadataEventInput("", source), source)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, event.Sequence)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("hook fired %d times, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("hook sequence[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestEventAppendedHookFiresForCoupledTransaction(t *testing.T) {
	database := openTestDB(t)
	ctx := context.Background()
	repo, err := database.InsertRepo("/work/coupled", "https://example.com/coupled.git", "main")
	if err != nil {
		t.Fatal(err)
	}

	var fired atomic.Int64
	database.SetEventAppendedHook(func(int64) { fired.Add(1) })

	// InsertRunWithEvent commits run state and its event in one transaction.
	if _, err := database.InsertRunWithEvent(ctx, repo.ID, "branch", "head", "base", nil); err != nil {
		t.Fatal(err)
	}
	if fired.Load() != 1 {
		t.Fatalf("hook fired %d times for one coupled commit, want 1", fired.Load())
	}
}

func TestEventAppendedHookClearsWithNil(t *testing.T) {
	database := openTestDB(t)
	ctx := context.Background()
	var fired atomic.Int64
	database.SetEventAppendedHook(func(int64) { fired.Add(1) })
	database.SetEventAppendedHook(nil)
	source := time.Now().UTC()
	if _, err := database.appendMetadataEventAt(ctx, metadataEventInput("", source), source); err != nil {
		t.Fatal(err)
	}
	if fired.Load() != 0 {
		t.Fatalf("cleared hook fired %d times, want 0", fired.Load())
	}
}

// A slow subscriber's notification must never hold the database connection or
// lock: the event commits (and the connection is released) before the hook is
// called, so a stuck hook cannot delay another database operation.
func TestEventAppendedHookDoesNotHoldConnectionWhileBlocked(t *testing.T) {
	database := openTestDB(t)
	ctx := context.Background()
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var blockOnce sync.Once
	database.SetEventAppendedHook(func(int64) {
		blockOnce.Do(func() {
			entered <- struct{}{}
			<-release // simulate a stuck consumer inside the notification
		})
	})
	defer close(release)

	go func() {
		source := time.Now().UTC()
		_, _ = database.appendMetadataEventAt(ctx, metadataEventInput("", source), source)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("append never reached the notification hook")
	}

	// The hook is blocked, but the insert has already committed and released the
	// single pooled connection. A concurrent read must succeed promptly and see
	// the committed event, proving the blocked notification holds no lock.
	done := make(chan int64, 1)
	go func() {
		latest, err := database.LatestEventSequence(ctx)
		if err != nil {
			done <- -1
			return
		}
		done <- latest
	}()
	select {
	case latest := <-done:
		if latest != 1 {
			t.Fatalf("read while hook blocked returned latest=%d, want 1 (committed event)", latest)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("database read blocked while a stuck notification held the connection")
	}
}
