package ipc_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/ipc"
)

func TestSubscribeEventsStreamsFrames(t *testing.T) {
	sock := socketPath(t)
	srv := startServer(t, sock)

	srv.HandleStream(ipc.MethodSubscribeEvents, func(_ context.Context, raw json.RawMessage) (ipc.StreamFunc, error) {
		var p ipc.SubscribeEventsParams
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
		if p.Version != ipc.SubscribeEventsVersion {
			return nil, &ipc.CodedError{Code: ipc.ErrCodeUnsupportedVersion, Message: ipc.ErrEventVersionUnsupported.Error()}
		}
		return func(send func(interface{}) error) error {
			run := "run-1"
			if err := send(ipc.EventStreamFrame{
				Kind:   ipc.EventStreamFrameEvent,
				Cursor: ipc.EncodeEventCursor(1),
				Event: &ipc.MetadataEventInfo{
					Sequence: 1, EventID: "e1", Type: "io.no_mistakes.run.created.v1",
					ContentClass: "metadata", RunID: &run,
				},
			}); err != nil {
				return err
			}
			return send(ipc.EventStreamFrame{Kind: ipc.EventStreamFrameCheckpoint, Cursor: ipc.EncodeEventCursor(3)})
		}, nil
	})

	frames, cancel, err := ipc.SubscribeEvents(sock, &ipc.SubscribeEventsParams{Version: ipc.SubscribeEventsVersion})
	if err != nil {
		t.Fatalf("subscribe events: %v", err)
	}
	defer cancel()

	var got []ipc.EventStreamFrame
	for frame := range frames {
		got = append(got, frame)
	}
	if len(got) != 2 {
		t.Fatalf("received %d frames, want 2", len(got))
	}
	if got[0].Kind != ipc.EventStreamFrameEvent || got[0].Event == nil || got[0].Event.Sequence != 1 {
		t.Fatalf("frame 0 = %#v, want event seq 1", got[0])
	}
	if got[1].Kind != ipc.EventStreamFrameCheckpoint || got[1].Cursor != ipc.EncodeEventCursor(3) {
		t.Fatalf("frame 1 = %#v, want checkpoint cursor 3", got[1])
	}
}

func TestSubscribeEventsMapsTypedSetupErrors(t *testing.T) {
	cases := map[string]struct {
		code int
		want error
	}{
		"unsupported version": {ipc.ErrCodeUnsupportedVersion, ipc.ErrEventVersionUnsupported},
		"invalid filter":      {ipc.ErrCodeInvalidFilter, ipc.ErrEventFilterInvalid},
		"malformed cursor":    {ipc.ErrCodeInvalidCursor, ipc.ErrEventCursorMalformed},
		"expired cursor":      {ipc.ErrCodeCursorExpired, ipc.ErrEventCursorExpired},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sock := socketPath(t)
			srv := startServer(t, sock)
			code, sentinel := tc.code, tc.want
			srv.HandleStream(ipc.MethodSubscribeEvents, func(_ context.Context, _ json.RawMessage) (ipc.StreamFunc, error) {
				return nil, &ipc.CodedError{Code: code, Message: sentinel.Error()}
			})
			_, _, err := ipc.SubscribeEvents(sock, &ipc.SubscribeEventsParams{Version: ipc.SubscribeEventsVersion})
			if !errors.Is(err, tc.want) {
				t.Fatalf("subscribe error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSubscribeEventsUnsupportedMethodFallsBack(t *testing.T) {
	sock := socketPath(t)
	_ = startServer(t, sock) // no subscribe_events handler registered

	_, _, err := ipc.SubscribeEvents(sock, &ipc.SubscribeEventsParams{Version: ipc.SubscribeEventsVersion})
	if !errors.Is(err, ipc.ErrEventCapabilityUnsupported) {
		t.Fatalf("subscribe against old daemon = %v, want ErrEventCapabilityUnsupported", err)
	}
	if !ipc.IsUnsupportedCapability(err) {
		t.Fatal("IsUnsupportedCapability should report the fallback signal")
	}
}

func TestCapabilitiesDiscoveryAndFallback(t *testing.T) {
	sock := socketPath(t)
	srv := startServer(t, sock)
	srv.Handle(ipc.MethodCapabilities, func(_ context.Context, _ json.RawMessage) (interface{}, error) {
		return &ipc.CapabilitiesResult{Capabilities: []ipc.Capability{
			{Name: ipc.CapabilitySubscribeEvents, Versions: []int{ipc.SubscribeEventsVersion}},
		}}, nil
	})

	client, err := ipc.Dial(sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	caps, err := client.Capabilities()
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if !caps.Supports(ipc.CapabilitySubscribeEvents, ipc.SubscribeEventsVersion) {
		t.Fatal("daemon should advertise subscribe_events v1")
	}
}

func TestCapabilitiesUnsupportedOnOldDaemon(t *testing.T) {
	sock := socketPath(t)
	_ = startServer(t, sock) // capabilities not registered

	client, err := ipc.Dial(sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	_, err = client.Capabilities()
	if !ipc.IsUnsupportedCapability(err) {
		t.Fatalf("capabilities against old daemon = %v, want unsupported", err)
	}
}

// A stuck client that stops reading gets its connection torn down when the
// caller cancels; the frame channel then closes without leaking the reader.
func TestSubscribeEventsCancelClosesChannel(t *testing.T) {
	sock := socketPath(t)
	srv := startServer(t, sock)
	srv.HandleStream(ipc.MethodSubscribeEvents, func(ctx context.Context, _ json.RawMessage) (ipc.StreamFunc, error) {
		return func(send func(interface{}) error) error {
			<-ctx.Done()
			return nil
		}, nil
	})

	frames, cancel, err := ipc.SubscribeEvents(sock, &ipc.SubscribeEventsParams{Version: ipc.SubscribeEventsVersion})
	if err != nil {
		t.Fatalf("subscribe events: %v", err)
	}
	cancel()
	select {
	case _, open := <-frames:
		if open {
			// Drain any buffered frame then expect close.
			for range frames {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("frame channel not closed after cancel")
	}
}
