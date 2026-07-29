package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
	"github.com/kunchenguid/no-mistakes/internal/types"
)

const (
	// MetadataContentClass is the only content classification accepted by the
	// prototype event log. Content-bearing events require a separately reviewed
	// policy and storage API.
	MetadataContentClass = "metadata"

	MaxMetadataEventTypeBytes     = 256
	MaxMetadataPayloadSchemaBytes = 256
	MaxMetadataEventReadBatch     = 1000
	MaxMetadataEventCleanupBatch  = 1000
	metadataEventCleanupBusyMS    = 250
)

var (
	ErrInvalidMetadataEvent             = errors.New("invalid metadata event")
	ErrMetadataEventRunNotFound         = errors.New("metadata event run not found")
	ErrInvalidMetadataEventTraceContext = errors.New("invalid metadata event trace context")
	ErrInvalidMetadataEventRead         = errors.New("invalid metadata event read")
	ErrInvalidMetadataEventRetention    = errors.New("invalid metadata event retention")
	ErrInvalidMetadataEventRecord       = errors.New("invalid persisted metadata event")

	metadataEventTypePattern     = regexp.MustCompile(`^io\.no_mistakes\.[a-z0-9_.]+\.v([1-9][0-9]*)$`)
	metadataPayloadSchemaPattern = regexp.MustCompile(`^io\.no_mistakes\.[a-z0-9_.-]+\.v([1-9][0-9]*)$`)
)

// MetadataEventType is a versioned no-mistakes event type. Append validates
// the bounded source-owned io.no_mistakes.*.vN namespace.
type MetadataEventType string

// MetadataPayloadSchema is the versioned schema identifier for a metadata
// event. The log stores the identifier and version, not an arbitrary payload.
type MetadataPayloadSchema string

// MetadataEventInput is the narrow append contract for prototype events. It
// intentionally has no payload, map, raw JSON, or caller-selected content
// class. Private family pointers let this package attach fixed typed lifecycle
// metadata without exposing a generic content dumping channel.
type MetadataEventInput struct {
	SourceTimestamp time.Time
	Type            MetadataEventType
	PayloadSchema   MetadataPayloadSchema
	PayloadVersion  int
	RunID           string

	// Family metadata is private so external callers cannot turn the event log
	// into a generic attribute channel. Only typed lifecycle constructors in
	// this package can populate one of these fixed metadata shapes.
	invocation *InvocationEventMetadata
	gate       *GateEventMetadata
	ci         *CIEventMetadata
	pr         *PREventMetadata
}

// MetadataEvent is one durable source event ordered by Sequence.
type MetadataEvent struct {
	Sequence        int64
	EventID         string
	SourceTimestamp time.Time
	Type            MetadataEventType
	PayloadSchema   MetadataPayloadSchema
	PayloadVersion  int
	ContentClass    string
	RunID           *string
	TraceContext    *tracecontext.Context
	RecordedAt      time.Time
	Invocation      *InvocationEventMetadata
	Gate            *GateEventMetadata
	CI              *CIEventMetadata
	PR              *PREventMetadata
}

// MetadataEventDiagnostic is a bounded, value-free failure reason safe for
// logs. It never contains caller input, a database error, or event metadata.
type MetadataEventDiagnostic string

const (
	MetadataEventDiagnosticWriteFailed   MetadataEventDiagnostic = "metadata_event_write_failed"
	MetadataEventDiagnosticCleanupFailed MetadataEventDiagnostic = "metadata_event_cleanup_failed"
)

// AppendMetadataEvent appends one validated metadata-only event on its own
// connection. A linked event copies the run's already-persisted TW-38 trace
// context so correlation survives daemon and storage restarts.
//
// This standalone form commits the event by itself. TW-36 in-scope mutations
// that must commit state and their describing event atomically use
// CommitWithEvent instead, which runs the same append inside the state
// transaction (see events_tx.go).
func (d *DB) AppendMetadataEvent(ctx context.Context, input MetadataEventInput) (*MetadataEvent, error) {
	return d.appendMetadataEventAt(ctx, input, time.Now().UTC())
}

func (d *DB) appendMetadataEventAt(ctx context.Context, input MetadataEventInput, recordedAt time.Time) (*MetadataEvent, error) {
	if err := validateMetadataEventInput(input); err != nil {
		return nil, err
	}
	tx, err := d.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("append metadata event: begin: %w", err)
	}
	defer tx.Rollback()
	event, err := appendMetadataEventTx(ctx, tx, input, recordedAt)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("append metadata event: commit: %w", err)
	}
	d.fireEventAppended(event.Sequence)
	return event, nil
}

// appendMetadataEventTx inserts one validated metadata event through exec,
// which is either the shared *sql.DB or an open *sql.Tx. Running it against a
// transaction is what lets CommitWithEvent commit a state mutation and its
// describing event as one unit. The caller is responsible for having already
// run validateMetadataEventInput on input.
func appendMetadataEventTx(ctx context.Context, exec sqlExecutor, input MetadataEventInput, recordedAt time.Time) (*MetadataEvent, error) {
	if recordedAt.IsZero() {
		return nil, ErrInvalidMetadataEvent
	}

	event := &MetadataEvent{
		EventID:         newID(),
		SourceTimestamp: input.SourceTimestamp.UTC(),
		Type:            input.Type,
		PayloadSchema:   input.PayloadSchema,
		PayloadVersion:  input.PayloadVersion,
		ContentClass:    MetadataContentClass,
		RecordedAt:      recordedAt.UTC(),
	}

	var runID any
	var traceparent any
	var tracestate any
	if input.RunID != "" {
		var storedTraceparent, storedTracestate sql.NullString
		err := exec.QueryRowContext(ctx,
			`SELECT traceparent, tracestate FROM runs WHERE id = ?`, input.RunID,
		).Scan(&storedTraceparent, &storedTracestate)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMetadataEventRunNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("append metadata event: read run linkage: %w", err)
		}
		if storedTracestate.Valid && !storedTraceparent.Valid {
			return nil, ErrInvalidMetadataEventTraceContext
		}
		if storedTraceparent.Valid {
			validated := tracecontext.Parse(storedTraceparent.String, storedTracestate.String)
			if validated.Context == nil || len(validated.Diagnostics) != 0 {
				return nil, ErrInvalidMetadataEventTraceContext
			}
			event.TraceContext = validated.Context
			traceparent = validated.Context.Traceparent
			if validated.Context.Tracestate != "" {
				tracestate = validated.Context.Tracestate
			}
		}
		linkedRunID := input.RunID
		event.RunID = &linkedRunID
		runID = linkedRunID
	}

	result, err := exec.ExecContext(ctx, `
		INSERT INTO event_log
			(event_id, source_timestamp, event_type, payload_schema, payload_version,
			 content_class, run_id, traceparent, tracestate, recorded_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.EventID,
		event.SourceTimestamp.Format(time.RFC3339Nano),
		string(event.Type),
		string(event.PayloadSchema),
		event.PayloadVersion,
		event.ContentClass,
		runID,
		traceparent,
		tracestate,
		event.RecordedAt.UnixMilli(),
	)
	if err != nil {
		return nil, fmt.Errorf("append metadata event: %w", err)
	}
	sequence, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("append metadata event: read sequence: %w", err)
	}
	event.Sequence = sequence
	if err := insertLifecycleMetadata(ctx, exec, event.EventID, input); err != nil {
		return nil, fmt.Errorf("append lifecycle metadata: %w", err)
	}
	event.Invocation = input.invocation
	event.Gate = input.gate
	event.CI = input.ci
	event.PR = input.pr
	return event, nil
}

func validateMetadataEventInput(input MetadataEventInput) error {
	sourceUTC := input.SourceTimestamp.UTC()
	if input.SourceTimestamp.IsZero() || sourceUTC.Year() < 1 || sourceUTC.Year() > 9999 {
		return ErrInvalidMetadataEvent
	}
	if input.RunID != strings.TrimSpace(input.RunID) || len(input.RunID) > 128 || strings.ContainsAny(input.RunID, "\r\n\t ") {
		return ErrInvalidMetadataEvent
	}
	if input.PayloadVersion <= 0 {
		return ErrInvalidMetadataEvent
	}
	if !metadataVersionMatches(string(input.Type), input.PayloadVersion, MaxMetadataEventTypeBytes, metadataEventTypePattern) {
		return ErrInvalidMetadataEvent
	}
	if !metadataVersionMatches(string(input.PayloadSchema), input.PayloadVersion, MaxMetadataPayloadSchemaBytes, metadataPayloadSchemaPattern) {
		return ErrInvalidMetadataEvent
	}
	return validateLifecycleMetadata(input)
}

func metadataVersionMatches(value string, version, maxBytes int, pattern *regexp.Regexp) bool {
	if value == "" || len(value) > maxBytes {
		return false
	}
	match := pattern.FindStringSubmatch(value)
	if len(match) != 2 {
		return false
	}
	parsed, err := strconv.Atoi(match[1])
	return err == nil && parsed == version
}

// AppendMetadataEventBestEffort is the fail-independent seam for out-of-scope
// callers whose primary state transition must continue if event persistence
// fails. In-scope TW-36 mutations do NOT use this path; they use
// CommitWithEvent so state and event share one transaction (events_tx.go).
func (d *DB) AppendMetadataEventBestEffort(ctx context.Context, input MetadataEventInput) (*MetadataEvent, MetadataEventDiagnostic) {
	event, err := d.AppendMetadataEvent(ctx, input)
	if err == nil {
		return event, ""
	}
	diagnostic := MetadataEventDiagnosticWriteFailed
	slog.Warn("metadata event append skipped", "reason", diagnostic)
	return nil, diagnostic
}

// ReadMetadataEvents returns the next bounded sequence-ordered batch. It is a
// storage primitive only, not a subscription, checkpoint, or cursor protocol;
// those semantics belong to TW-37.
func (d *DB) ReadMetadataEvents(ctx context.Context, afterSequence int64, limit int) ([]*MetadataEvent, error) {
	if afterSequence < 0 || limit <= 0 || limit > MaxMetadataEventReadBatch {
		return nil, ErrInvalidMetadataEventRead
	}
	rows, err := d.sql.QueryContext(ctx, `
		SELECT sequence, event_id, source_timestamp, event_type, payload_schema,
		       payload_version, content_class, run_id, traceparent, tracestate, recorded_at
		FROM event_log
		WHERE sequence > ?
		ORDER BY sequence ASC
		LIMIT ?`, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("read metadata events: %w", err)
	}

	events := make([]*MetadataEvent, 0, limit)
	for rows.Next() {
		event, err := scanMetadataEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read metadata events: %w", err)
	}
	// Release the single pooled connection before loading fixed family metadata.
	// Holding rows open while issuing the detail queries would deadlock because
	// DB intentionally caps the pool at one connection.
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("read metadata events: close: %w", err)
	}
	if err := d.loadLifecycleMetadata(ctx, events); err != nil {
		return nil, err
	}
	return events, nil
}

// ReadRecentMetadataEventsForRun returns the newest bounded set of lifecycle
// facts for one run in ascending source-log order. It is an internal projection
// primitive for optional metadata observers, not a new IPC query surface.
func (d *DB) ReadRecentMetadataEventsForRun(ctx context.Context, runID string, limit int) ([]*MetadataEvent, error) {
	if !validBoundedID(runID) || limit <= 0 || limit > MaxMetadataEventReadBatch {
		return nil, ErrInvalidMetadataEventRead
	}
	rows, err := d.sql.QueryContext(ctx, `
		SELECT sequence, event_id, source_timestamp, event_type, payload_schema,
		       payload_version, content_class, run_id, traceparent, tracestate, recorded_at
		FROM event_log
		WHERE run_id = ?
		ORDER BY sequence DESC
		LIMIT ?`, runID, limit)
	if err != nil {
		return nil, fmt.Errorf("read run metadata events: %w", err)
	}
	events := make([]*MetadataEvent, 0, limit)
	for rows.Next() {
		event, err := scanMetadataEvent(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read run metadata events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("read run metadata events: close: %w", err)
	}
	for left, right := 0, len(events)-1; left < right; left, right = left+1, right-1 {
		events[left], events[right] = events[right], events[left]
	}
	if err := d.loadLifecycleMetadata(ctx, events); err != nil {
		return nil, err
	}
	return events, nil
}

func scanMetadataEvent(row interface{ Scan(...any) error }) (*MetadataEvent, error) {
	var event MetadataEvent
	var sourceTimestamp string
	var runID, traceparent, tracestate sql.NullString
	var recordedAt int64
	if err := row.Scan(
		&event.Sequence,
		&event.EventID,
		&sourceTimestamp,
		&event.Type,
		&event.PayloadSchema,
		&event.PayloadVersion,
		&event.ContentClass,
		&runID,
		&traceparent,
		&tracestate,
		&recordedAt,
	); err != nil {
		return nil, fmt.Errorf("read metadata events: scan: %w", err)
	}
	parsedSource, err := time.Parse(time.RFC3339Nano, sourceTimestamp)
	if err != nil || event.ContentClass != MetadataContentClass || event.Sequence <= 0 || event.EventID == "" {
		return nil, ErrInvalidMetadataEventRecord
	}
	if !metadataVersionMatches(string(event.Type), event.PayloadVersion, MaxMetadataEventTypeBytes, metadataEventTypePattern) ||
		!metadataVersionMatches(string(event.PayloadSchema), event.PayloadVersion, MaxMetadataPayloadSchemaBytes, metadataPayloadSchemaPattern) {
		return nil, ErrInvalidMetadataEventRecord
	}
	event.SourceTimestamp = parsedSource
	event.RecordedAt = time.UnixMilli(recordedAt).UTC()
	if runID.Valid {
		value := runID.String
		event.RunID = &value
	}
	if tracestate.Valid && !traceparent.Valid {
		return nil, ErrInvalidMetadataEventRecord
	}
	if traceparent.Valid {
		validated := tracecontext.Parse(traceparent.String, tracestate.String)
		if validated.Context == nil || len(validated.Diagnostics) != 0 {
			return nil, ErrInvalidMetadataEventRecord
		}
		event.TraceContext = validated.Context
	}
	return &event, nil
}

// CleanupMetadataEvents deletes at most limit expired events in ascending
// sequence order. Events linked to pending or running runs are never eligible.
// Retention uses local acceptance time rather than a caller-controlled source
// timestamp, so late source events receive the full retention window.
func (d *DB) CleanupMetadataEvents(ctx context.Context, retention time.Duration, reference time.Time, limit int) (int64, error) {
	if retention <= 0 || reference.IsZero() || limit <= 0 || limit > MaxMetadataEventCleanupBatch {
		return 0, ErrInvalidMetadataEventRetention
	}
	// The database's ordinary 5-second busy timeout is appropriate for state
	// writes but too long for best-effort retention. Pin cleanup to one
	// connection with a much shorter busy timeout, then restore the normal
	// setting before releasing it. This keeps a locked database from delaying
	// daemon readiness or active work.
	conn, err := d.sql.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("cleanup metadata events: acquire connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", metadataEventCleanupBusyMS)); err != nil {
		return 0, fmt.Errorf("cleanup metadata events: configure busy timeout: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "PRAGMA busy_timeout=5000")
	}()

	cutoff := reference.UTC().Add(-retention).UnixMilli()

	// Persist the highest deleted sequence as the retention watermark inside the
	// same transaction as the delete. A global subscriber (TW-37) resuming with a
	// cursor strictly below the watermark is answered with a typed cursor-expired
	// error rather than silently skipping the events retention removed. The
	// MAX-of-the-eligible-set query and the DELETE share one predicate and one
	// transaction, so the watermark is exactly the largest sequence deleted.
	const eligibleSet = `
		SELECT event.sequence
		FROM event_log AS event
		LEFT JOIN runs AS run ON run.id = event.run_id
		WHERE event.recorded_at < ?
		  AND (event.run_id IS NULL OR run.id IS NULL OR run.status NOT IN (?, ?))
		ORDER BY event.sequence ASC
		LIMIT ?`

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("cleanup metadata events: begin: %w", err)
	}
	defer tx.Rollback()

	var maxDeleted sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(sequence) FROM (`+eligibleSet+`)`,
		cutoff, types.RunPending, types.RunRunning, limit,
	).Scan(&maxDeleted); err != nil {
		return 0, fmt.Errorf("cleanup metadata events: scan watermark: %w", err)
	}

	result, err := tx.ExecContext(ctx, `DELETE FROM event_log WHERE sequence IN (`+eligibleSet+`)`,
		cutoff, types.RunPending, types.RunRunning, limit,
	)
	if err != nil {
		return 0, fmt.Errorf("cleanup metadata events: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cleanup metadata events: rows affected: %w", err)
	}
	if maxDeleted.Valid && maxDeleted.Int64 > 0 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE event_log_state SET purged_through = ? WHERE id = 1 AND purged_through < ?`,
			maxDeleted.Int64, maxDeleted.Int64,
		); err != nil {
			return 0, fmt.Errorf("cleanup metadata events: advance watermark: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("cleanup metadata events: commit: %w", err)
	}
	return deleted, nil
}

// PurgedThroughSequence returns the retention watermark: the highest event
// sequence that CleanupMetadataEvents has ever deleted (0 if none). A global
// subscriber resuming with a cursor strictly below this value is missing
// history that no longer exists and must resync (the ipc layer maps this to a
// typed cursor-expired error).
func (d *DB) PurgedThroughSequence(ctx context.Context) (int64, error) {
	var watermark int64
	err := d.sql.QueryRowContext(ctx, `SELECT purged_through FROM event_log_state WHERE id = 1`).Scan(&watermark)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read event retention watermark: %w", err)
	}
	return watermark, nil
}

// LatestEventSequence returns the highest assigned event sequence (0 when the
// log is empty). It lets a subscriber bound its initial catch-up read.
func (d *DB) LatestEventSequence(ctx context.Context) (int64, error) {
	var latest sql.NullInt64
	if err := d.sql.QueryRowContext(ctx, `SELECT MAX(sequence) FROM event_log`).Scan(&latest); err != nil {
		return 0, fmt.Errorf("read latest event sequence: %w", err)
	}
	if !latest.Valid {
		return 0, nil
	}
	return latest.Int64, nil
}
