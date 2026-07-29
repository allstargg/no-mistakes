package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	testEventType     = MetadataEventType("io.no_mistakes.run.completed.v1")
	testPayloadSchema = MetadataPayloadSchema("io.no_mistakes.run.v1")
)

func metadataEventInput(runID string, source time.Time) MetadataEventInput {
	return MetadataEventInput{
		SourceTimestamp: source,
		Type:            testEventType,
		PayloadSchema:   testPayloadSchema,
		PayloadVersion:  1,
		RunID:           runID,
	}
}

func TestOpenMigratesEventLogIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`
		CREATE TABLE repos (id TEXT PRIMARY KEY, working_path TEXT NOT NULL UNIQUE, upstream_url TEXT NOT NULL, default_branch TEXT NOT NULL DEFAULT 'main', created_at INTEGER NOT NULL);
		CREATE TABLE runs (id TEXT PRIMARY KEY, repo_id TEXT NOT NULL, branch TEXT NOT NULL, head_sha TEXT NOT NULL, base_sha TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', pr_url TEXT, error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
		INSERT INTO repos VALUES ('repo-1', '/work/repo', 'https://example.com/repo.git', 'main', 1);
		INSERT INTO runs VALUES ('run-1', 'repo-1', 'feature', 'head', 'base', 'completed', NULL, NULL, 1, 1);
	`); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		database, err := Open(path)
		if err != nil {
			t.Fatalf("Open attempt %d: %v", attempt+1, err)
		}
		for _, column := range []string{
			"sequence", "event_id", "source_timestamp", "event_type",
			"payload_schema", "payload_version", "content_class", "run_id",
			"traceparent", "tracestate", "recorded_at",
		} {
			if !hasColumn(t, database, "event_log", column) {
				t.Fatalf("event_log.%s missing after migration", column)
			}
		}
		if attempt == 0 {
			event, err := database.AppendMetadataEvent(context.Background(), metadataEventInput("run-1", time.Now().UTC()))
			if err != nil {
				t.Fatalf("append event linked to migrated TW-38-compatible run: %v", err)
			}
			if event.TraceContext != nil {
				t.Fatalf("legacy run event gained trace context: %#v", event.TraceContext)
			}
		} else {
			events, err := database.ReadMetadataEvents(context.Background(), 0, 10)
			if err != nil || len(events) != 1 {
				t.Fatalf("events after idempotent reopen = %#v, %v", events, err)
			}
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMetadataEventAppendReadAndRestartPreservesRunTraceLinkage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := database.InsertRepo("/work/traced", "https://example.com/traced.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	traceCtx := &tracecontext.Context{
		Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Tracestate:  "tracewake=prototype",
	}
	run, err := database.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", traceCtx)
	if err != nil {
		t.Fatal(err)
	}
	source := time.Date(2026, time.July, 29, 12, 34, 56, 789123000, time.FixedZone("source", -4*60*60))
	appended, err := database.AppendMetadataEvent(context.Background(), metadataEventInput(run.ID, source))
	if err != nil {
		t.Fatalf("AppendMetadataEvent: %v", err)
	}
	if appended.Sequence <= 0 || appended.EventID == "" {
		t.Fatalf("event identity = sequence %d id %q", appended.Sequence, appended.EventID)
	}
	if appended.ContentClass != MetadataContentClass {
		t.Fatalf("content class = %q, want %q", appended.ContentClass, MetadataContentClass)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	readonly, err := OpenReadOnly(path)
	if err != nil {
		t.Fatalf("OpenReadOnly after restart: %v", err)
	}
	defer readonly.Close()
	events, err := readonly.ReadMetadataEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatalf("ReadMetadataEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	got := events[0]
	if got.EventID != appended.EventID || got.Sequence != appended.Sequence {
		t.Fatalf("event identity after reopen = %#v, want %#v", got, appended)
	}
	if got.RunID == nil || *got.RunID != run.ID {
		t.Fatalf("run linkage = %v, want %q", got.RunID, run.ID)
	}
	if got.TraceContext == nil || *got.TraceContext != *traceCtx {
		t.Fatalf("trace linkage = %#v, want %#v", got.TraceContext, traceCtx)
	}
	if !got.SourceTimestamp.Equal(source) {
		t.Fatalf("source timestamp = %s, want %s", got.SourceTimestamp, source)
	}
}

func TestMetadataEventSequenceRemainsMonotonicAcrossCleanupAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Millisecond)
	first, err := database.appendMetadataEventAt(context.Background(), metadataEventInput("", old), old)
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.appendMetadataEventAt(context.Background(), metadataEventInput("", old.Add(time.Second)), old)
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence <= first.Sequence {
		t.Fatalf("sequences = %d then %d, want strictly increasing", first.Sequence, second.Sequence)
	}
	next, err := database.ReadMetadataEvents(context.Background(), first.Sequence, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 || next[0].EventID != second.EventID {
		t.Fatalf("read after sequence %d = %#v, want second event", first.Sequence, next)
	}
	deleted, err := database.CleanupMetadataEvents(context.Background(), 24*time.Hour, time.Now().UTC(), MaxMetadataEventCleanupBatch)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	third, err := database.AppendMetadataEvent(context.Background(), metadataEventInput("", time.Now().UTC()))
	if err != nil {
		t.Fatal(err)
	}
	if third.Sequence <= second.Sequence {
		t.Fatalf("sequence after delete/restart = %d, want > %d", third.Sequence, second.Sequence)
	}
}

func TestMetadataEventIDsAndSequencesAreUniqueUnderConcurrentAppend(t *testing.T) {
	database := openTestDB(t)
	const count = 64
	events := make(chan *MetadataEvent, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			event, err := database.AppendMetadataEvent(context.Background(), metadataEventInput("", time.Now().UTC()))
			if err != nil {
				errs <- err
				return
			}
			events <- event
		}()
	}
	wg.Wait()
	close(errs)
	close(events)
	for err := range errs {
		t.Errorf("concurrent append: %v", err)
	}
	ids := make(map[string]struct{}, count)
	sequences := make([]int64, 0, count)
	for event := range events {
		if _, duplicate := ids[event.EventID]; duplicate {
			t.Fatalf("duplicate event ID %q", event.EventID)
		}
		ids[event.EventID] = struct{}{}
		sequences = append(sequences, event.Sequence)
	}
	if len(sequences) != count {
		t.Fatalf("appended = %d, want %d", len(sequences), count)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	for i := 1; i < len(sequences); i++ {
		if sequences[i] <= sequences[i-1] {
			t.Fatalf("sequences are not unique and monotonic: %v", sequences)
		}
	}
}

func TestMetadataEventClassificationIsMetadataOnlyAndHasNoGenericPayloadChannel(t *testing.T) {
	database := openTestDB(t)
	event, err := database.AppendMetadataEvent(context.Background(), metadataEventInput("", time.Now().UTC()))
	if err != nil {
		t.Fatal(err)
	}
	if event.ContentClass != MetadataContentClass {
		t.Fatalf("content class = %q", event.ContentClass)
	}

	inputType := reflect.TypeOf(MetadataEventInput{})
	for _, prohibited := range []string{"ContentClass", "Content", "Payload", "PayloadJSON", "Data", "Attributes"} {
		if _, found := inputType.FieldByName(prohibited); found {
			t.Fatalf("MetadataEventInput exposes prohibited generic channel %q", prohibited)
		}
	}
	if hasColumn(t, database, "event_log", "payload_json") || hasColumn(t, database, "event_log", "content") {
		t.Fatal("event_log exposes a generic content/payload column")
	}

	_, err = database.sql.Exec(`
		INSERT INTO event_log
			(event_id, source_timestamp, event_type, payload_schema, payload_version, content_class, recorded_at)
		VALUES ('duplicate-content-test', '2026-07-29T00:00:00Z', ?, ?, 1, 'content', 1)`,
		string(testEventType), string(testPayloadSchema),
	)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "check constraint") {
		t.Fatalf("content-class insert error = %v, want CHECK constraint", err)
	}
}

func TestMetadataEventSchemaEnforcesUniqueEventID(t *testing.T) {
	database := openTestDB(t)
	insert := func(source string) error {
		_, err := database.sql.Exec(`
			INSERT INTO event_log
				(event_id, source_timestamp, event_type, payload_schema, payload_version, content_class, recorded_at)
			VALUES ('same-event-id', ?, ?, ?, 1, 'metadata', 1)`,
			source, string(testEventType), string(testPayloadSchema),
		)
		return err
	}
	if err := insert("2026-07-29T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := insert("2026-07-29T00:00:01Z"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Fatalf("duplicate insert error = %v, want UNIQUE constraint", err)
	}
}

func TestMetadataEventValidationRejectsUnboundedOrMismatchedMetadata(t *testing.T) {
	database := openTestDB(t)
	tests := []MetadataEventInput{
		{},
		{SourceTimestamp: time.Now(), Type: MetadataEventType(strings.Repeat("x", MaxMetadataEventTypeBytes+1)), PayloadSchema: testPayloadSchema, PayloadVersion: 1},
		{SourceTimestamp: time.Now(), Type: "io.no_mistakes.run.completed.v1", PayloadSchema: "io.no_mistakes.run.v2", PayloadVersion: 1},
		{SourceTimestamp: time.Now(), Type: "io.tracewake.run.completed.v1", PayloadSchema: testPayloadSchema, PayloadVersion: 1},
	}
	for i, input := range tests {
		if _, err := database.AppendMetadataEvent(context.Background(), input); !errors.Is(err, ErrInvalidMetadataEvent) {
			t.Errorf("case %d error = %v, want ErrInvalidMetadataEvent", i, err)
		}
	}
	if _, err := database.ReadMetadataEvents(context.Background(), 0, MaxMetadataEventReadBatch+1); !errors.Is(err, ErrInvalidMetadataEventRead) {
		t.Fatalf("oversized read error = %v, want ErrInvalidMetadataEventRead", err)
	}
	if _, err := database.AppendMetadataEvent(context.Background(), metadataEventInput("missing-run", time.Now().UTC())); !errors.Is(err, ErrMetadataEventRunNotFound) {
		t.Fatalf("missing run linkage error = %v, want ErrMetadataEventRunNotFound", err)
	}
}

func TestMetadataEventRetentionIsBoundedDeterministicAndProtectsActiveRuns(t *testing.T) {
	database := openTestDB(t)
	repo, err := database.InsertRepo("/work/retention", "https://example.com/retention.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	active, err := database.InsertRun(repo.ID, "active", "head-a", "base")
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := database.InsertRun(repo.ID, "terminal", "head-b", "base")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpdateRunStatus(terminal.ID, types.RunCompleted); err != nil {
		t.Fatal(err)
	}

	reference := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
	old := reference.Add(-48 * time.Hour)
	activeEvent, err := database.appendMetadataEventAt(context.Background(), metadataEventInput(active.ID, old), old)
	if err != nil {
		t.Fatal(err)
	}
	var terminalEvents []*MetadataEvent
	for i := 0; i < 3; i++ {
		event, err := database.appendMetadataEventAt(context.Background(), metadataEventInput(terminal.ID, old.Add(time.Duration(i)*time.Second)), old)
		if err != nil {
			t.Fatal(err)
		}
		terminalEvents = append(terminalEvents, event)
	}
	newEvent, err := database.appendMetadataEventAt(context.Background(), metadataEventInput(terminal.ID, reference), reference)
	if err != nil {
		t.Fatal(err)
	}

	deleted, err := database.CleanupMetadataEvents(context.Background(), 24*time.Hour, reference, 2)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("first bounded cleanup deleted %d, want 2", deleted)
	}
	remaining, err := database.ReadMetadataEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	remainingIDs := make(map[string]bool, len(remaining))
	for _, event := range remaining {
		remainingIDs[event.EventID] = true
	}
	if !remainingIDs[activeEvent.EventID] {
		t.Fatal("cleanup deleted event linked to an active run")
	}
	if remainingIDs[terminalEvents[0].EventID] || remainingIDs[terminalEvents[1].EventID] {
		t.Fatalf("cleanup did not deterministically delete oldest eligible sequences: remaining=%v", remainingIDs)
	}
	if !remainingIDs[terminalEvents[2].EventID] || !remainingIDs[newEvent.EventID] {
		t.Fatalf("cleanup exceeded batch or age bound: remaining=%v", remainingIDs)
	}

	if err := database.UpdateRunStatus(active.ID, types.RunCompleted); err != nil {
		t.Fatal(err)
	}
	deleted, err = database.CleanupMetadataEvents(context.Background(), 24*time.Hour, reference, 10)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("second cleanup deleted %d, want active-old + terminal-old", deleted)
	}
	remaining, err = database.ReadMetadataEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 || remaining[0].EventID != newEvent.EventID {
		t.Fatalf("remaining events = %#v, want only new event", remaining)
	}
}

func TestMetadataEventCleanupIsBoundedWhileStorageIsBusy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	old := time.Now().UTC().Add(-48 * time.Hour)
	if _, err := database.appendMetadataEventAt(context.Background(), metadataEventInput("", old), old); err != nil {
		t.Fatal(err)
	}

	locker, err := sql.Open("sqlite", path+"?_pragma=journal_mode(wal)&_pragma=foreign_keys(on)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	if _, err := locker.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer locker.Exec("ROLLBACK")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = database.CleanupMetadataEvents(ctx, 24*time.Hour, time.Now().UTC(), 10)
	if err == nil {
		t.Fatal("cleanup unexpectedly succeeded while storage was write-locked")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cancelled cleanup blocked for %v", elapsed)
	}
}

func TestMetadataEventRunDeletionPreservesExistingRepoBehavior(t *testing.T) {
	database := openTestDB(t)
	repo, err := database.InsertRepo("/work/delete", "https://example.com/delete.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	run, err := database.InsertRun(repo.ID, "feature", "head", "base")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendMetadataEvent(context.Background(), metadataEventInput(run.ID, time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if err := database.DeleteRepo(repo.ID); err != nil {
		t.Fatalf("DeleteRepo with event linkage: %v", err)
	}
	events, err := database.ReadMetadataEvents(context.Background(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].RunID == nil || *events[0].RunID != run.ID {
		t.Fatalf("event after repo cascade = %#v, want immutable historical run linkage", events)
	}
}

func TestMetadataEventRejectsInvalidPersistedRunTraceContext(t *testing.T) {
	database := openTestDB(t)
	repo, _ := database.InsertRepo("/work/invalid-trace", "https://example.com/invalid.git", "main")
	run, _ := database.InsertRun(repo.ID, "feature", "head", "base")
	if _, err := database.sql.Exec(`UPDATE runs SET traceparent = ? WHERE id = ?`, "secret-invalid-trace-value", run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.AppendMetadataEvent(context.Background(), metadataEventInput(run.ID, time.Now().UTC())); !errors.Is(err, ErrInvalidMetadataEventTraceContext) {
		t.Fatalf("append error = %v, want ErrInvalidMetadataEventTraceContext", err)
	}
}

func TestAppendMetadataEventBestEffortProducesBoundedSafeDiagnostic(t *testing.T) {
	database := openTestDB(t)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	oldLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(oldLogger)

	input := metadataEventInput("", time.Now().UTC())
	input.RunID = "https://user:secret@example.com/private"
	event, diagnostic := database.AppendMetadataEventBestEffort(context.Background(), input)
	if event != nil || diagnostic != MetadataEventDiagnosticWriteFailed {
		t.Fatalf("best-effort result = event %#v diagnostic %q", event, diagnostic)
	}
	got := logs.String()
	if !strings.Contains(got, string(MetadataEventDiagnosticWriteFailed)) {
		t.Fatalf("diagnostic log = %q", got)
	}
	for _, secret := range []string{"secret", "private", input.RunID} {
		if strings.Contains(got, secret) {
			t.Fatalf("diagnostic leaked input %q: %s", secret, got)
		}
	}
}
