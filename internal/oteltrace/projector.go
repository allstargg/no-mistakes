package oteltrace

import (
	"context"
	"strings"
	"time"
)

const (
	projectionQueueSize = 128
	projectionTimeout   = 500 * time.Millisecond
	projectionSeenLimit = 1024
)

// NotifyRun is the fail-independent post-commit seam. It performs no database
// or network work and never waits for queue space. A dropped wake-up loses only
// optional telemetry; it cannot delay or alter the durable transition.
func (r *Runtime) NotifyRun(runID string) {
	if r == nil || !r.enabled || r.notify == nil || runID == "" || len(runID) > 128 || strings.ContainsAny(runID, "\r\n\t ") {
		return
	}
	select {
	case r.notify <- runID:
	default:
	}
}

func (r *Runtime) startProjector() {
	r.notify = make(chan string, projectionQueueSize)
	r.stop = make(chan struct{})
	r.done = make(chan struct{})
	go r.projectLoop()
}

func (r *Runtime) projectLoop() {
	defer close(r.done)
	seen := make(map[string]struct{}, projectionSeenLimit)
	order := make([]string, 0, projectionSeenLimit)
	for {
		select {
		case <-r.stop:
			return
		case runID := <-r.notify:
			if _, duplicate := seen[runID]; duplicate {
				continue
			}
			if r.projectRun(runID) {
				seen[runID] = struct{}{}
				order = append(order, runID)
				if len(order) > projectionSeenLimit {
					delete(seen, order[0])
					order = order[1:]
				}
			}
		}
	}
}

func (r *Runtime) projectRun(runID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), projectionTimeout)
	defer cancel()
	run, err := r.database.GetRunContext(ctx, runID)
	if err != nil || run == nil || !terminalRun(run.Status) {
		return false
	}
	steps, err := r.database.GetStepsByRunContext(ctx, runID)
	if err != nil {
		return false
	}
	events, err := r.database.ReadRecentMetadataEventsForRun(ctx, runID, 1000)
	if err != nil {
		return false
	}
	r.EmitSnapshot(Snapshot{Run: run, Steps: steps, Events: events})
	return true
}
