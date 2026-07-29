package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/ipc"
)

// This file implements the daemon side of TW-37's global metadata-event
// subscription: capability discovery plus a database-backed replay stream with
// a bounded live handoff. The durable event log is the source of truth, so
// replay survives daemon and client restarts; the notifier is only a wake-up.
//
// The overriding safety property is that a slow, disconnected, or maliciously
// idle subscriber can never block pipeline execution, a database write,
// retention, or another subscriber. Two things enforce it: the notifier's
// notify is O(1) and independent of any consumer, and the streamer materializes
// each batch (releasing the single database connection) before it ever blocks
// on a socket write. Backpressure is bounded - a consumer that cannot keep up
// is disconnected for resync rather than buffered without bound.

// Streaming tunables. They are package vars so tests can shrink the timing and
// buffering without waiting real seconds.
var (
	eventStreamReadBatch      = db.MaxMetadataEventReadBatch
	eventStreamHandoffBuffer  = 256
	eventStreamSlowGrace      = 30 * time.Second
	eventStreamIdleCheckpoint = 20 * time.Second
)

// errSlowEventConsumer disconnects a subscriber that cannot drain the bounded
// handoff within the grace window. Its backlog stays durable in the database,
// so it reconnects with its last cursor to resync exactly once.
var errSlowEventConsumer = errors.New("event subscriber too slow; disconnecting for resync")

// eventNotifier wakes global event streamers when a metadata event is appended.
// notify is O(1), holds its lock only for a channel swap, and never touches a
// consumer's send state, so a stuck subscriber cannot delay the append that
// fired it. A missed wake-up is harmless: the streamer also re-reads on an idle
// timer, so the notifier is an optimization, not a correctness dependency.
type eventNotifier struct {
	mu     sync.Mutex
	ch     chan struct{}
	latest int64
}

func newEventNotifier() *eventNotifier {
	return &eventNotifier{ch: make(chan struct{})}
}

// notify records the newest sequence and wakes every current waiter.
func (n *eventNotifier) notify(sequence int64) {
	n.mu.Lock()
	if sequence > n.latest {
		n.latest = sequence
	}
	waiters := n.ch
	n.ch = make(chan struct{})
	n.mu.Unlock()
	close(waiters)
}

// subscribe returns a channel closed by the next notify. A waiter must capture
// it before its catch-up read so a notify that races the read is not lost.
func (n *eventNotifier) subscribe() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ch
}

// handleCapabilities advertises the optional capabilities this daemon
// implements so a client can select global mode only when supported.
func handleCapabilities(_ context.Context, _ json.RawMessage) (interface{}, error) {
	return &ipc.CapabilitiesResult{Capabilities: []ipc.Capability{
		{Name: ipc.CapabilitySubscribeEvents, Versions: []int{ipc.SubscribeEventsVersion}},
	}}, nil
}

// handleSubscribeEvents validates the typed request and prepares the stream.
// Every rejection is a bounded typed error returned before any streaming, so an
// unknown version, invalid filter, malformed cursor, or expired cursor never
// touches pipeline work.
func handleSubscribeEvents(database *db.DB, notifier *eventNotifier) ipc.StreamHandlerFunc {
	return func(ctx context.Context, params json.RawMessage) (ipc.StreamFunc, error) {
		var p ipc.SubscribeEventsParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("invalid params: %w", err)
		}
		if p.Version != ipc.SubscribeEventsVersion {
			return nil, &ipc.CodedError{Code: ipc.ErrCodeUnsupportedVersion, Message: ipc.ErrEventVersionUnsupported.Error()}
		}
		filter, err := ipc.CompileEventFilter(p.Filter)
		if err != nil {
			return nil, &ipc.CodedError{Code: ipc.ErrCodeInvalidFilter, Message: ipc.ErrEventFilterInvalid.Error()}
		}
		afterSequence, err := resolveEventStart(ctx, database, p.Cursor)
		if err != nil {
			return nil, err
		}
		return func(send func(interface{}) error) error {
			return streamGlobalEvents(ctx, database, notifier, filter, afterSequence, send)
		}, nil
	}
}

// resolveEventStart turns a request cursor into the exclusive start sequence. An
// empty cursor is a fresh subscription that replays the retained backlog from
// the beginning; expiry is only meaningful for a resume that presents a cursor.
func resolveEventStart(ctx context.Context, database *db.DB, cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	sequence, err := ipc.DecodeEventCursor(cursor)
	if err != nil {
		return 0, &ipc.CodedError{Code: ipc.ErrCodeInvalidCursor, Message: ipc.ErrEventCursorMalformed.Error()}
	}
	// A resume cursor strictly below the retention watermark depends on events
	// that no longer exist, so answering it would silently skip history. Fail
	// with a typed expiry instead; the client resyncs from the beginning.
	watermark, err := database.PurgedThroughSequence(ctx)
	if err != nil {
		return 0, fmt.Errorf("read retention watermark: %w", err)
	}
	if sequence < watermark {
		return 0, &ipc.CodedError{Code: ipc.ErrCodeCursorExpired, Message: ipc.ErrEventCursorExpired.Error()}
	}
	return sequence, nil
}

// streamGlobalEvents replays durable events after afterSequence in ascending
// order, then tails new ones, sending each event exactly once with its cursor.
// It never holds the database connection while blocked on the client: each
// batch is materialized before any send, and the writer runs on its own
// goroutine behind a bounded handoff so backpressure disconnects a slow
// consumer instead of stalling the reader or growing without bound.
func streamGlobalEvents(
	ctx context.Context,
	database *db.DB,
	notifier *eventNotifier,
	filter *ipc.CompiledEventFilter,
	afterSequence int64,
	send func(interface{}) error,
) error {
	frames := make(chan ipc.EventStreamFrame, eventStreamHandoffBuffer)
	writerDone := make(chan struct{})
	var (
		writerMu  sync.Mutex
		writerErr error
	)
	go func() {
		defer close(writerDone)
		for frame := range frames {
			if err := send(frame); err != nil {
				writerMu.Lock()
				writerErr = err
				writerMu.Unlock()
				return
			}
		}
	}()
	readWriterErr := func() error {
		writerMu.Lock()
		defer writerMu.Unlock()
		return writerErr
	}
	defer close(frames)

	// enqueue hands one frame to the writer with bounded backpressure. The fast
	// path is a non-blocking send; only a full buffer arms the grace window,
	// after which a consumer that still cannot make room is disconnected.
	enqueue := func(frame ipc.EventStreamFrame) error {
		select {
		case frames <- frame:
			return nil
		default:
		}
		timer := time.NewTimer(eventStreamSlowGrace)
		defer timer.Stop()
		select {
		case frames <- frame:
			return nil
		case <-writerDone:
			if err := readWriterErr(); err != nil {
				return err
			}
			return nil
		case <-ctx.Done():
			return nil
		case <-timer.C:
			return errSlowEventConsumer
		}
	}

	lastSeq := afterSequence
	lastSentSeq := afterSequence
	for {
		waitCh := notifier.subscribe()

		for {
			events, err := database.ReadMetadataEvents(ctx, lastSeq, eventStreamReadBatch)
			if err != nil {
				if ctx.Err() != nil {
					return nil // client disconnected or daemon shutting down
				}
				return err
			}
			if len(events) == 0 {
				break
			}
			for _, event := range events {
				lastSeq = event.Sequence
				if !filter.Matches(event.RunID, string(event.Type)) {
					continue
				}
				if err := enqueue(eventFrame(event)); err != nil {
					return err
				}
				lastSentSeq = event.Sequence
			}
			if len(events) < eventStreamReadBatch {
				break
			}
		}
		// Advance the client's persistable cursor past trailing filtered events
		// so a later resume neither re-scans them nor falsely expires.
		if lastSeq > lastSentSeq {
			if err := enqueue(checkpointFrame(lastSeq)); err != nil {
				return err
			}
			lastSentSeq = lastSeq
		}

		idle := time.NewTimer(eventStreamIdleCheckpoint)
		select {
		case <-waitCh:
			idle.Stop()
		case <-idle.C:
			// Idle keepalive: re-emit the current cursor so a live-but-quiet
			// client persists progress and a dead one is noticed on send.
			if err := enqueue(checkpointFrame(lastSeq)); err != nil {
				return err
			}
			lastSentSeq = lastSeq
		case <-writerDone:
			idle.Stop()
			return readWriterErr()
		case <-ctx.Done():
			idle.Stop()
			return nil
		}
	}
}

func eventFrame(event *db.MetadataEvent) ipc.EventStreamFrame {
	return ipc.EventStreamFrame{
		Kind:   ipc.EventStreamFrameEvent,
		Cursor: ipc.EncodeEventCursor(event.Sequence),
		Event:  metadataEventInfo(event),
	}
}

func checkpointFrame(sequence int64) ipc.EventStreamFrame {
	return ipc.EventStreamFrame{
		Kind:   ipc.EventStreamFrameCheckpoint,
		Cursor: ipc.EncodeEventCursor(sequence),
	}
}

// metadataEventInfo projects a durable event onto its metadata-only wire form.
// It copies only classification and correlation fields; there is no content or
// payload to leak because the storage row has none.
func metadataEventInfo(event *db.MetadataEvent) *ipc.MetadataEventInfo {
	info := &ipc.MetadataEventInfo{
		Sequence:        event.Sequence,
		EventID:         event.EventID,
		Type:            string(event.Type),
		PayloadSchema:   string(event.PayloadSchema),
		PayloadVersion:  event.PayloadVersion,
		ContentClass:    event.ContentClass,
		SourceTimestamp: event.SourceTimestamp.UnixMilli(),
		RecordedAt:      event.RecordedAt.UnixMilli(),
		TraceContext:    event.TraceContext,
	}
	if event.RunID != nil {
		runID := *event.RunID
		info.RunID = &runID
	}
	return info
}
