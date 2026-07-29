package ipc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Capabilities queries the daemon's optional capability set. An older daemon
// that predates this method returns a method-not-found RPCError; callers use
// IsUnsupportedCapability to detect that and fall back to per-run mode.
func (c *Client) Capabilities() (*CapabilitiesResult, error) {
	var result CapabilitiesResult
	if err := c.Call(MethodCapabilities, &CapabilitiesParams{}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// IsUnsupportedCapability reports whether err means the daemon does not
// implement a queried capability (either the capabilities method itself is
// absent, or a typed unsupported sentinel was returned). Tracewake uses this to
// select per-run mode.
func IsUnsupportedCapability(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrEventCapabilityUnsupported) {
		return true
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		return rpcErr.Code == ErrMethodNotFound
	}
	return false
}

// SubscribeEvents opens a dedicated connection and starts a global metadata
// event stream. It returns a frame channel, a cancel function, and an error.
//
// The channel is closed when the stream ends (connection drop, cancel, or
// daemon shutdown); the caller reconnects with its last cursor to resume. A
// typed setup failure - an unsupported version, invalid filter, malformed
// cursor, or an expired cursor - is returned as a matching sentinel
// (ErrEventVersionUnsupported, ErrEventFilterInvalid, ErrEventCursorMalformed,
// ErrEventCursorExpired), and ErrEventCapabilityUnsupported when the daemon does
// not implement the method at all.
func SubscribeEvents(socketPath string, params *SubscribeEventsParams) (<-chan EventStreamFrame, func(), error) {
	conn, err := dialEndpoint(socketPath)
	if err != nil {
		return nil, nil, err
	}
	encoder := json.NewEncoder(conn)
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	req, err := NewRequest(MethodSubscribeEvents, params)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("marshal request: %w", err)
	}
	if err := encoder.Encode(req); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("send request: %w", err)
	}

	if !scanner.Scan() {
		conn.Close()
		if err := scanner.Err(); err != nil {
			return nil, nil, fmt.Errorf("read response: %w", err)
		}
		return nil, nil, fmt.Errorf("read response: connection closed")
	}
	var resp Response
	if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("parse response: %w", err)
	}
	if resp.Error != nil {
		conn.Close()
		return nil, nil, eventErrorFromRPC(resp.Error)
	}

	ch := make(chan EventStreamFrame, 64)
	done := make(chan struct{})
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			close(done)
			conn.Close()
		})
	}

	go func() {
		defer close(ch)
		for scanner.Scan() {
			var frame EventStreamFrame
			if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
				continue // skip a malformed frame rather than tearing down the stream
			}
			select {
			case ch <- frame:
			case <-done:
				return
			}
		}
	}()

	return ch, cancel, nil
}
