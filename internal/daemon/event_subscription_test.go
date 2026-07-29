package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
)

func openEventDB(t *testing.T) *db.DB {
	t.Helper()
	database, err := db.Open(filepath.Join(t.TempDir(), "events.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { database.Close() })
	return database
}

func appendEvent(t *testing.T, database *db.DB, runID string) *db.MetadataEvent {
	t.Helper()
	event, err := database.AppendMetadataEvent(context.Background(), db.MetadataEventInput{
		SourceTimestamp: time.Now().UTC(),
		Type:            "io.no_mistakes.test.v1",
		PayloadSchema:   "io.no_mistakes.test.v1",
		PayloadVersion:  1,
		RunID:           runID,
	})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	return event
}

func withFastEventStream(t *testing.T) {
	t.Helper()
	batch, buffer, grace, idle := eventStreamReadBatch, eventStreamHandoffBuffer, eventStreamSlowGrace, eventStreamIdleCheckpoint
	eventStreamIdleCheckpoint = 60 * time.Millisecond
	t.Cleanup(func() {
		eventStreamReadBatch, eventStreamHandoffBuffer, eventStreamSlowGrace, eventStreamIdleCheckpoint = batch, buffer, grace, idle
	})
}

type collectingSend struct {
	mu     sync.Mutex
	frames []ipc.EventStreamFrame
	gate   chan struct{} // when non-nil, every send blocks until this is closed
}

func (c *collectingSend) fn(v interface{}) error {
	if c.gate != nil {
		<-c.gate
	}
	frame, ok := v.(ipc.EventStreamFrame)
	if !ok {
		return fmt.Errorf("unexpected frame %T", v)
	}
	c.mu.Lock()
	c.frames = append(c.frames, frame)
	c.mu.Unlock()
	return nil
}

func (c *collectingSend) eventSequences() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var seqs []int64
	for _, f := range c.frames {
		if f.Kind == ipc.EventStreamFrameEvent && f.Event != nil {
			seqs = append(seqs, f.Event.Sequence)
		}
	}
	return seqs
}

func (c *collectingSend) latestCheckpoint() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	last := ""
	for _, f := range c.frames {
		if f.Kind == ipc.EventStreamFrameCheckpoint {
			last = f.Cursor
		}
	}
	return last
}

func (c *collectingSend) waitForEvents(t *testing.T, want int, timeout time.Duration) []int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if seqs := c.eventSequences(); len(seqs) >= want {
			return seqs
		}
		time.Sleep(5 * time.Millisecond)
	}
	return c.eventSequences()
}

// runStream starts streamGlobalEvents in a goroutine and returns the collector,
// a cancel function, and a channel carrying the terminal error.
func runStream(t *testing.T, database *db.DB, notifier *eventNotifier, filter *ipc.CompiledEventFilter, after int64, collector *collectingSend) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- streamGlobalEvents(ctx, database, notifier, filter, after, collector.fn)
	}()
	return cancel, errCh
}

func matchAll(t *testing.T) *ipc.CompiledEventFilter {
	t.Helper()
	filter, err := ipc.CompileEventFilter(nil)
	if err != nil {
		t.Fatal(err)
	}
	return filter
}

func TestEventNotifierWakesWaiters(t *testing.T) {
	notifier := newEventNotifier()
	waitCh := notifier.subscribe()
	select {
	case <-waitCh:
		t.Fatal("waiter woke before any notify")
	default:
	}
	notifier.notify(7)
	select {
	case <-waitCh:
	case <-time.After(time.Second):
		t.Fatal("waiter not woken by notify")
	}
	// A fresh subscribe after the notify is not pre-closed.
	next := notifier.subscribe()
	select {
	case <-next:
		t.Fatal("fresh waiter should block until the next notify")
	default:
	}
}

// A concurrent storm of notifies and subscribes must be race-free and never
// double-close a channel. Run under -race to exercise it.
func TestEventNotifierConcurrentIsRaceFree(t *testing.T) {
	notifier := newEventNotifier()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for seq := base; ; seq += 8 {
				select {
				case <-stop:
					return
				default:
				}
				notifier.notify(seq)
			}
		}(int64(i))
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-notifier.subscribe():
				}
			}
		}()
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestStreamReplaysBacklogInOrderExactlyOnce(t *testing.T) {
	withFastEventStream(t)
	database := openEventDB(t)
	notifier := newEventNotifier()
	database.SetEventAppendedHook(notifier.notify)

	var want []int64
	for i := 0; i < 5; i++ {
		want = append(want, appendEvent(t, database, "").Sequence)
	}

	collector := &collectingSend{}
	cancel, errCh := runStream(t, database, notifier, matchAll(t), 0, collector)
	defer func() { cancel(); <-errCh }()

	got := collector.waitForEvents(t, len(want), 3*time.Second)
	if len(got) != len(want) {
		t.Fatalf("received %d events, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event[%d] = %d, want %d (order/exactly-once broken)", i, got[i], want[i])
		}
	}
}

func TestStreamResumesFromCursor(t *testing.T) {
	withFastEventStream(t)
	database := openEventDB(t)
	notifier := newEventNotifier()
	database.SetEventAppendedHook(notifier.notify)

	var seqs []int64
	for i := 0; i < 5; i++ {
		seqs = append(seqs, appendEvent(t, database, "").Sequence)
	}

	// Resume after the third event: only the last two are delivered.
	collector := &collectingSend{}
	cancel, errCh := runStream(t, database, notifier, matchAll(t), seqs[2], collector)
	defer func() { cancel(); <-errCh }()

	got := collector.waitForEvents(t, 2, 3*time.Second)
	if len(got) != 2 || got[0] != seqs[3] || got[1] != seqs[4] {
		t.Fatalf("resume delivered %v, want [%d %d]", got, seqs[3], seqs[4])
	}
}

func TestStreamDeliversLiveEventsAfterCatchup(t *testing.T) {
	withFastEventStream(t)
	database := openEventDB(t)
	notifier := newEventNotifier()
	database.SetEventAppendedHook(notifier.notify)

	first := appendEvent(t, database, "").Sequence

	collector := &collectingSend{}
	cancel, errCh := runStream(t, database, notifier, matchAll(t), 0, collector)
	defer func() { cancel(); <-errCh }()

	if got := collector.waitForEvents(t, 1, 2*time.Second); len(got) != 1 || got[0] != first {
		t.Fatalf("catch-up delivered %v, want [%d]", got, first)
	}

	// Now append live; the notifier must wake the stream to deliver them.
	live := appendEvent(t, database, "").Sequence
	got := collector.waitForEvents(t, 2, 2*time.Second)
	if len(got) != 2 || got[1] != live {
		t.Fatalf("live delivery got %v, want second event %d", got, live)
	}
}

func TestStreamFilterExcludesAndCheckpointsPastFilteredTail(t *testing.T) {
	withFastEventStream(t)
	database := openEventDB(t)
	notifier := newEventNotifier()
	database.SetEventAppendedHook(notifier.notify)

	repo, err := database.InsertRepo("/work/filter", "https://example.com/filter.git", "main")
	if err != nil {
		t.Fatal(err)
	}
	wanted, err := database.InsertRun(repo.ID, "wanted", "head-a", "base")
	if err != nil {
		t.Fatal(err)
	}
	other, err := database.InsertRun(repo.ID, "other", "head-b", "base")
	if err != nil {
		t.Fatal(err)
	}

	match := appendEvent(t, database, wanted.ID).Sequence
	// Two trailing events the filter excludes; the stream must still advance a
	// checkpoint past them so a resume neither re-scans nor falsely expires.
	appendEvent(t, database, other.ID)
	lastExcluded := appendEvent(t, database, other.ID).Sequence

	filter, err := ipc.CompileEventFilter(&ipc.EventFilter{RunIDs: []string{wanted.ID}})
	if err != nil {
		t.Fatal(err)
	}
	collector := &collectingSend{}
	cancel, errCh := runStream(t, database, notifier, filter, 0, collector)
	defer func() { cancel(); <-errCh }()

	got := collector.waitForEvents(t, 1, 2*time.Second)
	if len(got) != 1 || got[0] != match {
		t.Fatalf("filtered stream delivered %v, want only [%d]", got, match)
	}
	// The persistable checkpoint advances past the excluded trailing events.
	deadline := time.Now().Add(2 * time.Second)
	want := ipc.EncodeEventCursor(lastExcluded)
	for time.Now().Before(deadline) {
		if collector.latestCheckpoint() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("checkpoint = %q, want %q (past filtered tail)", collector.latestCheckpoint(), want)
}

// The central safety guarantee: a stuck consumer is disconnected within the
// grace window, never blocks the append that produces events, and never blocks
// another subscriber.
func TestStreamSlowConsumerDisconnectsWithoutBlockingProducerOrOthers(t *testing.T) {
	database := openEventDB(t)
	notifier := newEventNotifier()
	database.SetEventAppendedHook(notifier.notify)

	// Tiny buffer + short grace so a wedged consumer disconnects quickly.
	batch, buffer, grace, idle := eventStreamReadBatch, eventStreamHandoffBuffer, eventStreamSlowGrace, eventStreamIdleCheckpoint
	eventStreamHandoffBuffer = 1
	eventStreamSlowGrace = 200 * time.Millisecond
	eventStreamIdleCheckpoint = 60 * time.Millisecond
	t.Cleanup(func() {
		eventStreamReadBatch, eventStreamHandoffBuffer, eventStreamSlowGrace, eventStreamIdleCheckpoint = batch, buffer, grace, idle
	})

	for i := 0; i < 5; i++ {
		appendEvent(t, database, "")
	}

	// Subscriber A wedges: every send blocks until we release the gate.
	stuck := &collectingSend{gate: make(chan struct{})}
	stuckCtx, stuckCancel := context.WithCancel(context.Background())
	defer stuckCancel()
	stuckErr := make(chan error, 1)
	go func() {
		stuckErr <- streamGlobalEvents(stuckCtx, database, notifier, matchAll(t), 0, stuck.fn)
	}()

	// While A is wedged, a producer append must return promptly - the stuck
	// consumer cannot hold the database write path.
	appendDone := make(chan int64, 1)
	go func() { appendDone <- appendEvent(t, database, "").Sequence }()
	select {
	case <-appendDone:
	case <-time.After(2 * time.Second):
		close(stuck.gate)
		t.Fatal("append blocked while a subscriber was stuck")
	}

	// A must disconnect itself for resync rather than buffering without bound.
	select {
	case err := <-stuckErr:
		if !errors.Is(err, errSlowEventConsumer) {
			t.Fatalf("stuck subscriber error = %v, want errSlowEventConsumer", err)
		}
	case <-time.After(3 * time.Second):
		close(stuck.gate)
		t.Fatal("stuck subscriber was not disconnected within the grace window")
	}
	close(stuck.gate) // release A's writer goroutine so it can exit cleanly

	// Subscriber B (draining) is unaffected and receives the full backlog.
	eventStreamHandoffBuffer = 256
	healthy := &collectingSend{}
	bCancel, bErr := runStream(t, database, notifier, matchAll(t), 0, healthy)
	defer func() { bCancel(); <-bErr }()
	got := healthy.waitForEvents(t, 6, 3*time.Second)
	if len(got) != 6 {
		t.Fatalf("healthy subscriber received %d events, want 6 (%v)", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("healthy subscriber out of order at %d: %v", i, got)
		}
	}
}

func TestResolveEventStart(t *testing.T) {
	database := openEventDB(t)
	ctx := context.Background()

	if seq, err := resolveEventStart(ctx, database, ""); err != nil || seq != 0 {
		t.Fatalf("empty cursor -> (%d, %v), want (0, nil)", seq, err)
	}
	if seq, err := resolveEventStart(ctx, database, ipc.EncodeEventCursor(9)); err != nil || seq != 9 {
		t.Fatalf("valid cursor -> (%d, %v), want (9, nil)", seq, err)
	}

	_, err := resolveEventStart(ctx, database, "not-a-cursor")
	var malformed *ipc.CodedError
	if !errors.As(err, &malformed) || malformed.Code != ipc.ErrCodeInvalidCursor {
		t.Fatalf("malformed cursor error = %v, want CodedError invalid cursor", err)
	}
}

func TestResolveEventStartExpiredBelowWatermark(t *testing.T) {
	database := openEventDB(t)
	ctx := context.Background()

	// Append three runless events, then clean them out by referencing a point
	// far enough in the future that their acceptance time is past retention, so
	// the watermark advances without needing a backdated recorded_at.
	var last int64
	for i := 0; i < 3; i++ {
		last = appendEvent(t, database, "").Sequence
	}
	futureReference := time.Now().UTC().Add(72 * time.Hour)
	if _, err := database.CleanupMetadataEvents(ctx, time.Hour, futureReference, 10); err != nil {
		t.Fatal(err)
	}
	watermark, err := database.PurgedThroughSequence(ctx)
	if err != nil || watermark != last {
		t.Fatalf("watermark = %d (err %v), want %d", watermark, err, last)
	}

	// A resume cursor strictly below the watermark is expired; at/above is not.
	_, err = resolveEventStart(ctx, database, ipc.EncodeEventCursor(watermark-1))
	var expired *ipc.CodedError
	if !errors.As(err, &expired) || expired.Code != ipc.ErrCodeCursorExpired {
		t.Fatalf("below-watermark cursor error = %v, want CodedError cursor expired", err)
	}
	if seq, err := resolveEventStart(ctx, database, ipc.EncodeEventCursor(watermark)); err != nil || seq != watermark {
		t.Fatalf("at-watermark cursor -> (%d, %v), want (%d, nil)", seq, err, watermark)
	}
}

func TestHandleCapabilitiesAdvertisesSubscribeEvents(t *testing.T) {
	result, err := handleCapabilities(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	caps, ok := result.(*ipc.CapabilitiesResult)
	if !ok {
		t.Fatalf("result type = %T, want *ipc.CapabilitiesResult", result)
	}
	if !caps.Supports(ipc.CapabilitySubscribeEvents, ipc.SubscribeEventsVersion) {
		t.Fatalf("capabilities = %#v, want subscribe_events v%d", caps.Capabilities, ipc.SubscribeEventsVersion)
	}
	if !caps.Supports(ipc.CapabilityNativeOTLPTraces, ipc.NativeOTLPTracesVersion) {
		t.Fatalf("capabilities = %#v, want native OTLP traces v%d", caps.Capabilities, ipc.NativeOTLPTracesVersion)
	}
	if caps.NativeOTLPTraces == nil || caps.NativeOTLPTraces.State != "disabled" || caps.NativeOTLPTraces.ContentCapture {
		t.Fatalf("native OTLP health = %#v, want disabled metadata-only", caps.NativeOTLPTraces)
	}
}

func TestHandleSubscribeEventsRejectsUnsupportedVersion(t *testing.T) {
	database := openEventDB(t)
	handler := handleSubscribeEvents(database, newEventNotifier())
	_, err := handler(context.Background(), []byte(`{"version":999}`))
	var coded *ipc.CodedError
	if !errors.As(err, &coded) || coded.Code != ipc.ErrCodeUnsupportedVersion {
		t.Fatalf("unsupported version error = %v, want CodedError unsupported version", err)
	}
}

func TestHandleSubscribeEventsRejectsInvalidFilter(t *testing.T) {
	database := openEventDB(t)
	handler := handleSubscribeEvents(database, newEventNotifier())
	_, err := handler(context.Background(), []byte(`{"version":1,"filter":{"run_ids":["bad id"]}}`))
	var coded *ipc.CodedError
	if !errors.As(err, &coded) || coded.Code != ipc.ErrCodeInvalidFilter {
		t.Fatalf("invalid filter error = %v, want CodedError invalid filter", err)
	}
}
