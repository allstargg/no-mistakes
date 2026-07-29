package oteltrace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"

	"go.opentelemetry.io/otel/trace"
)

type spanIdentityKey struct{}

func withSpanIdentity(ctx context.Context, identity string) context.Context {
	return context.WithValue(ctx, spanIdentityKey{}, identity)
}

// deterministicIDGenerator gives one durable lifecycle identity the same OTel
// identity if a process retries reconstruction after a crash. This is not an
// exactly-once delivery mechanism. A collector can still receive the same span
// more than once, but each copy has the same trace/span identity.
type deterministicIDGenerator struct{}

func (deterministicIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	identity, _ := ctx.Value(spanIdentityKey{}).(string)
	if identity == "" {
		return randomIDs()
	}
	traceHash := sha256.Sum256([]byte("trace:" + identity))
	spanHash := sha256.Sum256([]byte("span:" + identity))
	var traceID trace.TraceID
	var spanID trace.SpanID
	copy(traceID[:], traceHash[:len(traceID)])
	copy(spanID[:], spanHash[:len(spanID)])
	return traceID, spanID
}

func (deterministicIDGenerator) NewSpanID(ctx context.Context, _ trace.TraceID) trace.SpanID {
	identity, _ := ctx.Value(spanIdentityKey{}).(string)
	if identity == "" {
		_, spanID := randomIDs()
		return spanID
	}
	hash := sha256.Sum256([]byte("span:" + identity))
	var spanID trace.SpanID
	copy(spanID[:], hash[:len(spanID)])
	return spanID
}

func randomIDs() (trace.TraceID, trace.SpanID) {
	var traceID trace.TraceID
	var spanID trace.SpanID
	for !traceID.IsValid() {
		_, _ = rand.Read(traceID[:])
	}
	for !spanID.IsValid() {
		_, _ = rand.Read(spanID[:])
	}
	return traceID, spanID
}
