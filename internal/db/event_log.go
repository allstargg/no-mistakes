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
// class. Later typed lifecycle payloads can extend this seam without creating
// a generic content dumping channel.
type MetadataEventInput struct {
	SourceTimestamp time.Time
	Type            MetadataEventType
	PayloadSchema   MetadataPayloadSchema
	PayloadVersion  int
	RunID           string
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
}

// MetadataEventDiagnostic is a bounded, value-free failure reason safe for
// logs. It never contains caller input, a database error, or event metadata.
type MetadataEventDiagnostic string

const (
	MetadataEventDiagnosticWriteFailed   MetadataEventDiagnostic = "metadata_event_write_failed"
	MetadataEventDiagnosticCleanupFailed MetadataEventDiagnostic = "metadata_event_cleanup_failed"
)

// AppendMetadataEvent appends one validated metadata-only event. A linked
// event copies the run's already-persisted TW-38 trace context so correlation
// survives daemon and storage restarts. State writes are intentionally not
// transaction-coupled here; that boundary belongs to TW-36.
func (d *DB) AppendMetadataEvent(ctx context.Context, input MetadataEventInput) (*MetadataEvent, error) {
	return d.appendMetadataEventAt(ctx, input, time.Now().UTC())
}

func (d *DB) appendMetadataEventAt(ctx context.Context, input MetadataEventInput, recordedAt time.Time) (*MetadataEvent, error) {
	if err := validateMetadataEventInput(input); err != nil {
		return nil, err
	}
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
		err := d.sql.QueryRowContext(ctx,
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

	result, err := d.sql.ExecContext(ctx, `
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
	return nil
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

// AppendMetadataEventBestEffort is the fail-independent prototype seam for
// callers whose primary state transition must continue if event persistence
// fails. The strict AppendMetadataEvent method remains available for TW-36's
// future transactional integration.
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
	defer rows.Close()

	events := make([]*MetadataEvent, 0, limit)
	for rows.Next() {
		event, err := scanMetadataEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read metadata events: %w", err)
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
	result, err := conn.ExecContext(ctx, `
		DELETE FROM event_log
		WHERE sequence IN (
			SELECT event.sequence
			FROM event_log AS event
			LEFT JOIN runs AS run ON run.id = event.run_id
			WHERE event.recorded_at < ?
			  AND (event.run_id IS NULL OR run.id IS NULL OR run.status NOT IN (?, ?))
			ORDER BY event.sequence ASC
			LIMIT ?
		)`,
		cutoff, types.RunPending, types.RunRunning, limit,
	)
	if err != nil {
		return 0, fmt.Errorf("cleanup metadata events: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cleanup metadata events: rows affected: %w", err)
	}
	return deleted, nil
}
