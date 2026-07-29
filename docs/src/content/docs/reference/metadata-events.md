---
title: Metadata Events
description: Typed local lifecycle facts available through the replayable event subscription.
---

No-mistakes keeps a local, append-only metadata event log for prototype consumers such as Tracewake. The daemon exposes it through the feature-detected `subscribe_events` JSON-RPC method. Events replay in global sequence order, survive daemon restarts, and can be resumed with an opaque cursor.

This is not a generic audit log. It has no arbitrary payload, attribute map, prompt, output, diff, log, command, check name, or error-text field. Every lifecycle family has a fixed typed shape, and `content_class` is always `metadata`.

## Common envelope

Every event has:

| Field | Meaning |
| --- | --- |
| `sequence` | Global monotonic ordering key |
| `event_id` | Unique event identity |
| `type` | Versioned `io.no_mistakes.*.vN` event name |
| `payload_schema` / `payload_version` | Versioned fixed metadata shape |
| `source_timestamp_ms` | Time reported by the authoritative source mutation |
| `recorded_at_ms` | Time the local event transaction accepted the fact |
| `run_id` | Optional local run correlation |
| `trace_context` | Validated W3C context copied from the linked run when present |

Older retained events remain readable. New family metadata fields are optional in the subscription envelope, so a consumer that only reads the common fields remains compatible.

## Lifecycle events

### Invocation

| Event | Fixed metadata |
| --- | --- |
| `io.no_mistakes.invocation.started.v1` | `invocation_id`, `phase`, bounded `step`, bounded `purpose`, bounded `session_mode` |
| `io.no_mistakes.invocation.completed.v1` | Start fields plus bounded `outcome`, optional bounded `failure_category`, `duration_ms`, exact reported `usage`, and fixed count-only `activity` |

The durable `agent_invocations` row already owns both source timestamps. No-mistakes commits that row and the ordered start/completion pair together after the attempt ends. The start event therefore preserves the true source start time but does not claim a separately durable in-progress invocation state.

Usage fields are optional. A missing field means the adapter did not report it; it is never rendered as a false zero. When usage is reported, the event can include input, output, cache-read, cache-creation, fresh-input, reasoning, and per-round delta counters already owned by local invocation telemetry. The activity summary is limited to fixed counters for model round trips, tool categories, workload size, and finding count. It includes no tool labels or text.

`invocation_id` is the durable invocation row ID and correlates the pair.

### Gate

| Event | Fixed metadata |
| --- | --- |
| `io.no_mistakes.gate.entered.v1` | `gate_id`, `phase`, `step`, `class` |
| `io.no_mistakes.gate.exited.v1` | Enter fields plus bounded `outcome` and `wait_duration_ms` |

`step` identifies the pipeline gate, `class` is `approval` or `fix_review`, and `gate_id` distinguishes repeated waits at the same step. The identity is persisted with the run marker, so an exit after daemon restart still correlates with its enter. Exit outcomes are limited to `approved`, `fix_requested`, `skipped`, `aborted`, `reconciled`, `cancelled`, `terminal`, `failed`, and `unknown`.

A transition guard emits only when the durable marker enters or exits. Retrying the same operation, replaying after restart, or clearing an already-clear marker produces no duplicate fact.

### CI

| Event | Meaning | Bounded outcomes where applicable |
| --- | --- | --- |
| `io.no_mistakes.ci.running.v1` | Checks are running or provider status is unresolved | `checks`, `unknown` |
| `io.no_mistakes.ci.green.v1` | Reported checks are green, including the existing no-checks-ready case | `passed`, `no_checks` |
| `io.no_mistakes.ci.failure.v1` | Known check failure and/or merge conflict | `checks`, `merge_conflict`, `checks_and_merge_conflict` |
| `io.no_mistakes.ci.merge_wait.v1` | Checks and known mergeability are ready; the open PR is waiting for review/merge | `passed`, `no_checks` |
| `io.no_mistakes.ci.terminal.v1` | The monitor observed terminal PR truth | `merged`, `closed` |

No event includes check names, provider errors, logs, or URLs. Consecutive identical observations do not emit. A later real transition, such as green checks starting again, can emit `running` again.

### Pull request

| Event | Meaning |
| --- | --- |
| `io.no_mistakes.pr.created.v1` | The PR step created and durably linked a new PR |
| `io.no_mistakes.pr.opened.v1` | An existing or newly observed PR became the run's durable open PR |
| `io.no_mistakes.pr.checks_wait.v1` | The open PR is waiting on checks or resolved mergeability |
| `io.no_mistakes.pr.review_wait.v1` | The open PR is ready and waiting for review/merge |
| `io.no_mistakes.pr.merged.v1` | The normalized PR state transitioned to merged |
| `io.no_mistakes.pr.closed.v1` | The normalized PR state reached a terminal non-merge outcome |

The PR URL remains in run state and is not copied into event metadata. Merged and closed observations are monotonic. A delayed open observation cannot regress a terminal state or emit another open fact.

## Transaction and replay behavior

Lifecycle state and its describing events share the database transaction owned by `CommitWithEvent` / `CommitWithEvents` or the equivalent transition-guarded path:

- an event never claims a state mutation that rolled back
- a committed covered transition never lacks its event metadata
- invocation start and completion order is stable
- terminal PR truth, CI terminal truth, run/CI finalization, and an active gate exit commit together

Event persistence adds no network request. Invocation performance recording and gate observability remain best effort at their existing call sites, while PR and CI state retain their existing error handling. Subscribers are independent of pipeline execution: a slow consumer is disconnected and resumes from durable storage rather than blocking a write.

Use `capabilities` to detect `subscribe_events` version `1`. A fresh subscription replays retained history; a resume uses the last returned `nm1:<sequence>` cursor. Retention can return the typed cursor-expired error, in which case the consumer starts a fresh replay. See [`event_log_retention`](/no-mistakes/reference/global-config/#event_log_retention) for the retention window.
