package ipc

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
)

// This file defines the TW-37 global metadata-event subscription surface: a
// typed, versioned, feature-detected IPC capability that streams only the
// durable TW-33 metadata event log. It is a bounded prototype replay API, not a
// general message broker: there is no content access, no arbitrary query, no
// acknowledgements, and no consumer groups. The per-run live subscription
// (MethodSubscribe) is unchanged and independent of everything here.

// Global-subscription method names and capability discovery constants.
const (
	// MethodCapabilities lets a client feature-detect optional daemon
	// capabilities and their supported versions before using them. An older
	// daemon returns method-not-found, which the client treats as "capability
	// absent" and falls back to per-run mode.
	MethodCapabilities = "capabilities"

	// MethodSubscribeEvents streams the durable metadata event log globally,
	// without any known run ID, from a monotonic cursor.
	MethodSubscribeEvents = "subscribe_events"

	// CapabilitySubscribeEvents is the discovery name for MethodSubscribeEvents.
	CapabilitySubscribeEvents = "subscribe_events"

	// SubscribeEventsVersion is the only global-subscription protocol version
	// this build implements. A client requests it explicitly; a daemon that
	// only knows a different version fails closed with a typed error.
	SubscribeEventsVersion = 1
)

// Server-defined JSON-RPC error codes for global event subscription. They sit
// in the JSON-RPC -32000..-32099 "server error" reserved range and are mapped
// to the typed sentinels below on the client so callers switch on error
// identity, not on message text.
const (
	ErrCodeUnsupportedVersion = -32010
	ErrCodeInvalidFilter      = -32011
	ErrCodeInvalidCursor      = -32012
	ErrCodeCursorExpired      = -32013
)

// Typed global-subscription errors. Callers use errors.Is against these; the
// daemon returns them (via CodedError) and the client maps response codes back
// to them, so the same identity works on both sides.
var (
	// ErrEventCapabilityUnsupported means the daemon does not implement global
	// event subscription at all (older build). Fall back to per-run mode.
	ErrEventCapabilityUnsupported = errors.New("global event subscription is not supported by this daemon")
	// ErrEventVersionUnsupported means the daemon does not implement the
	// requested protocol version.
	ErrEventVersionUnsupported = errors.New("global event subscription protocol version is not supported")
	// ErrEventFilterInvalid means the subscription filter violated a bound or
	// contained a malformed selector.
	ErrEventFilterInvalid = errors.New("event subscription filter is invalid")
	// ErrEventCursorMalformed means the resume cursor could not be parsed.
	ErrEventCursorMalformed = errors.New("event subscription cursor is malformed")
	// ErrEventCursorExpired means retention has removed history the cursor
	// depends on; the client must resync from the beginning.
	ErrEventCursorExpired = errors.New("event subscription cursor has expired; resync from the beginning")
)

// CodedError carries a JSON-RPC error code chosen by a handler so a typed
// outcome survives the transport instead of collapsing to a generic internal
// error. The server writes CodedError.Code onto the wire response.
type CodedError struct {
	Code    int
	Message string
}

func (e *CodedError) Error() string { return e.Message }

// RPCCode reports the JSON-RPC error code the server should send.
func (e *CodedError) RPCCode() int { return e.Code }

// codedError builds a CodedError whose message is a stable typed sentinel.
func codedError(code int, sentinel error) *CodedError {
	return &CodedError{Code: code, Message: sentinel.Error()}
}

// --- Capability discovery ---

// CapabilitiesParams has no fields but exists for method-signature consistency.
type CapabilitiesParams struct{}

// Capability advertises one optional daemon capability and the protocol
// versions it supports.
type Capability struct {
	Name     string `json:"name"`
	Versions []int  `json:"versions"`
}

// CapabilitiesResult lists the daemon's optional capabilities.
type CapabilitiesResult struct {
	Capabilities []Capability `json:"capabilities"`
}

// Supports reports whether the daemon advertises name at the given version.
func (r *CapabilitiesResult) Supports(name string, version int) bool {
	if r == nil {
		return false
	}
	for _, capability := range r.Capabilities {
		if capability.Name != name {
			continue
		}
		for _, v := range capability.Versions {
			if v == version {
				return true
			}
		}
	}
	return false
}

// --- Subscription parameters ---

// SubscribeEventsParams starts a global metadata-event stream. Version is
// mandatory so an unknown version fails closed. Cursor is empty for a fresh
// subscription that replays the retained backlog from the beginning, or a value
// previously returned by the stream to resume after it. Filter is an optional
// bounded selector.
type SubscribeEventsParams struct {
	Version int          `json:"version"`
	Cursor  string       `json:"cursor,omitempty"`
	Filter  *EventFilter `json:"filter,omitempty"`
}

// Bounds on a subscription filter. They keep the prototype from becoming an
// arbitrary query surface.
const (
	MaxEventFilterRunIDs   = 64
	MaxEventFilterTypes    = 64
	maxEventFilterRunIDLen = 128
	maxEventFilterTypeLen  = 256
)

var eventFilterTypePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,256}$`)

// EventFilter is the only selection the prototype accepts: exact run IDs and/or
// exact event types, each a bounded list. Empty lists match everything. It is
// deliberately not a query language.
type EventFilter struct {
	RunIDs []string `json:"run_ids,omitempty"`
	Types  []string `json:"types,omitempty"`
}

// CompiledEventFilter is a validated, allocation-light matcher. An empty
// CompiledEventFilter (from a nil EventFilter) matches every event.
type CompiledEventFilter struct {
	runIDs map[string]struct{}
	types  map[string]struct{}
}

// CompileEventFilter validates bounds and selector shape and returns a matcher.
// It returns ErrEventFilterInvalid for any oversized list or malformed selector
// so the daemon can answer with a typed error before streaming.
func CompileEventFilter(filter *EventFilter) (*CompiledEventFilter, error) {
	compiled := &CompiledEventFilter{}
	if filter == nil {
		return compiled, nil
	}
	if len(filter.RunIDs) > MaxEventFilterRunIDs || len(filter.Types) > MaxEventFilterTypes {
		return nil, ErrEventFilterInvalid
	}
	if len(filter.RunIDs) > 0 {
		compiled.runIDs = make(map[string]struct{}, len(filter.RunIDs))
		for _, runID := range filter.RunIDs {
			if runID == "" || len(runID) > maxEventFilterRunIDLen ||
				runID != strings.TrimSpace(runID) || strings.ContainsAny(runID, "\r\n\t ") {
				return nil, ErrEventFilterInvalid
			}
			compiled.runIDs[runID] = struct{}{}
		}
	}
	if len(filter.Types) > 0 {
		compiled.types = make(map[string]struct{}, len(filter.Types))
		for _, eventType := range filter.Types {
			if len(eventType) > maxEventFilterTypeLen || !eventFilterTypePattern.MatchString(eventType) {
				return nil, ErrEventFilterInvalid
			}
			compiled.types[eventType] = struct{}{}
		}
	}
	return compiled, nil
}

// Matches reports whether an event with the given run linkage and type passes
// the filter. A nil runID never matches a run-ID filter.
func (c *CompiledEventFilter) Matches(runID *string, eventType string) bool {
	if c == nil {
		return true
	}
	if len(c.runIDs) > 0 {
		if runID == nil {
			return false
		}
		if _, ok := c.runIDs[*runID]; !ok {
			return false
		}
	}
	if len(c.types) > 0 {
		if _, ok := c.types[eventType]; !ok {
			return false
		}
	}
	return true
}

// --- Stream frames ---

// MetadataEventInfo is the wire form of one durable metadata event. It carries
// only classification and correlation metadata: there is deliberately no
// payload, content, or free-form field, which is how content exclusion is
// enforced at the protocol boundary. Timestamps are unix milliseconds.
type MetadataEventInfo struct {
	Sequence        int64                 `json:"sequence"`
	EventID         string                `json:"event_id"`
	Type            string                `json:"type"`
	PayloadSchema   string                `json:"payload_schema"`
	PayloadVersion  int                   `json:"payload_version"`
	ContentClass    string                `json:"content_class"`
	SourceTimestamp int64                 `json:"source_timestamp_ms"`
	RecordedAt      int64                 `json:"recorded_at_ms"`
	RunID           *string               `json:"run_id,omitempty"`
	TraceContext    *tracecontext.Context `json:"trace_context,omitempty"`
}

// EventStreamFrameKind discriminates the two stream frame shapes.
type EventStreamFrameKind string

const (
	// EventStreamFrameEvent carries one metadata event plus its cursor.
	EventStreamFrameEvent EventStreamFrameKind = "event"
	// EventStreamFrameCheckpoint carries only a cursor. It lets a filtering or
	// idle client persist forward progress past events it did not receive, so a
	// later resume does not re-scan them and does not falsely expire.
	EventStreamFrameCheckpoint EventStreamFrameKind = "checkpoint"
)

// EventStreamFrame is one item in a global subscription stream. Cursor is the
// resume token reflecting every event up to and including this frame.
type EventStreamFrame struct {
	Kind   EventStreamFrameKind `json:"kind"`
	Cursor string               `json:"cursor"`
	Event  *MetadataEventInfo   `json:"event,omitempty"`
}

// --- Cursor ---

// The cursor is an opaque, versioned encoding of a monotonic sequence. The
// "nm1:" prefix is a format version so a future encoding change is detectable
// rather than silently misread; an unrecognized shape is a malformed cursor.
const eventCursorPrefix = "nm1:"

// EncodeEventCursor renders the resume token for a sequence.
func EncodeEventCursor(sequence int64) string {
	if sequence < 0 {
		sequence = 0
	}
	return eventCursorPrefix + strconv.FormatInt(sequence, 10)
}

// DecodeEventCursor parses a resume token into its sequence. It returns
// ErrEventCursorMalformed for any value it did not produce, including the empty
// string; callers treat an empty cursor as "fresh subscription" before calling
// this.
func DecodeEventCursor(cursor string) (int64, error) {
	rest, ok := strings.CutPrefix(cursor, eventCursorPrefix)
	if !ok {
		return 0, ErrEventCursorMalformed
	}
	sequence, err := strconv.ParseInt(rest, 10, 64)
	if err != nil || sequence < 0 {
		return 0, ErrEventCursorMalformed
	}
	return sequence, nil
}

// eventErrorFromRPC maps a wire error from a subscribe_events/capabilities call
// back to a typed sentinel so callers can switch on error identity.
func eventErrorFromRPC(rpcErr *RPCError) error {
	if rpcErr == nil {
		return nil
	}
	switch rpcErr.Code {
	case ErrMethodNotFound:
		return ErrEventCapabilityUnsupported
	case ErrCodeUnsupportedVersion:
		return ErrEventVersionUnsupported
	case ErrCodeInvalidFilter:
		return ErrEventFilterInvalid
	case ErrCodeInvalidCursor:
		return ErrEventCursorMalformed
	case ErrCodeCursorExpired:
		return ErrEventCursorExpired
	default:
		return rpcErr
	}
}
