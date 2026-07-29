package oteltrace

import (
	"context"
	"strings"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/db"
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

	// Recover a bounded recent set after the worker is live. Stable span IDs
	// make trace reprojection identifiable, while the durable invocation claim
	// suppresses duplicate additive metrics across daemon restart.
	ctx, cancel := context.WithTimeout(context.Background(), projectionTimeout)
	defer cancel()
	if runIDs, err := r.database.RecentTerminalRunIDs(ctx, db.MaxTerminalRunProjectionBatch); err == nil {
		// The query is newest-first. Feed oldest-first so the bounded rolling
		// coverage window retains the newest durable invocations after replay.
		for i := len(runIDs) - 1; i >= 0; i-- {
			r.NotifyRun(runIDs[i])
		}
	}
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
	events, err := r.database.ReadRecentMetadataEventsForRun(ctx, runID, db.MaxMetadataEventReadBatch)
	if err != nil {
		return false
	}
	invocations, err := r.database.GetAgentInvocationsByRunContext(ctx, runID, db.MaxOTLPMetricProjectionBatch)
	if err != nil {
		return false
	}
	metricInvocations := r.claimMetricInvocations(ctx, invocations)
	r.EmitSnapshot(Snapshot{
		Run: run, Steps: steps, Events: events,
		Invocations: invocations, MetricInvocations: metricInvocations,
	})
	return true
}

func (r *Runtime) claimMetricInvocations(ctx context.Context, invocations []db.AgentInvocation) []db.AgentInvocation {
	if !r.metricsEnabled || len(invocations) == 0 {
		return nil
	}
	ids := make([]string, 0, len(invocations))
	for i := range invocations {
		ids = append(ids, invocations[i].ID)
	}
	claimed, err := r.database.ClaimOTLPMetricInvocations(ctx, ids)
	if err != nil || len(claimed) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(claimed))
	for _, id := range claimed {
		allowed[id] = struct{}{}
	}
	out := make([]db.AgentInvocation, 0, len(claimed))
	for i := range invocations {
		if _, ok := allowed[invocations[i].ID]; ok {
			out = append(out, invocations[i])
		}
	}
	return out
}
