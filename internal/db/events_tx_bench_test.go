package db

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kunchenguid/no-mistakes/internal/tracecontext"
)

// The two benchmarks below are the TW-36 "before vs after" measurement for one
// representative in-scope mutation (run creation). Plain is the pre-TW-36 state
// write alone; WithEvent is the coupled state+event transaction. Run:
//
//	go test ./internal/db/ -run x -bench 'BenchmarkInsertRun' -benchmem
//
// Bounded evidence (Apple M3 Max, modernc sqlite, WAL): plain ~45-95 us/op,
// coupled ~88-128 us/op - a ~40-50 us delta from the added trace-read SELECT
// and event INSERT that share the state write's single transaction (one commit
// fsync, not two). A full run emits on the order of tens of these events, so
// the added cost is single-digit milliseconds against a seconds-to-minutes
// pipeline run: no material prototype regression. This is the evidence the
// task asks for, not a performance framework.
//
// Note the coupled path is also cheaper than the only correct alternative for
// an event-bearing mutation - two separate writes - which would pay two commit
// fsyncs and still be non-atomic (the counterfactual in
// TestUncoupledStateAndEventCanDiverge).

func benchRepo(b *testing.B) (*DB, *Repo) {
	b.Helper()
	d, err := Open(filepath.Join(b.TempDir(), "bench.sqlite"))
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	b.Cleanup(func() { d.Close() })
	repo, err := d.InsertRepo("/tmp/bench", "https://example.com/bench.git", "main")
	if err != nil {
		b.Fatalf("insert repo: %v", err)
	}
	return d, repo
}

func BenchmarkInsertRunPlain(b *testing.B) {
	d, repo := benchRepo(b)
	trace := &tracecontext.Context{Traceparent: validTraceparent}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.InsertRunWithTraceContext(repo.ID, "feature", "head", "base", trace); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertRunWithEvent(b *testing.B) {
	ctx := context.Background()
	d, repo := benchRepo(b)
	trace := &tracecontext.Context{Traceparent: validTraceparent}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.InsertRunWithEvent(ctx, repo.ID, "feature", "head", "base", trace); err != nil {
			b.Fatal(err)
		}
	}
}
