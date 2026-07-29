package db

const schemaSQL = `
CREATE TABLE IF NOT EXISTS repos (
    id             TEXT PRIMARY KEY,
    working_path   TEXT NOT NULL UNIQUE,
    upstream_url   TEXT NOT NULL,
    fork_url       TEXT,
    default_branch TEXT NOT NULL DEFAULT 'main',
    created_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id                   TEXT PRIMARY KEY,
    repo_id              TEXT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    branch               TEXT NOT NULL,
    head_sha                TEXT NOT NULL,
    base_sha                TEXT NOT NULL,
    traceparent             TEXT,
    tracestate              TEXT,
    submitted_head_sha      TEXT,
    review_approved_head_sha TEXT,
    status                  TEXT NOT NULL DEFAULT 'pending',
    pr_url                  TEXT,
    pr_state                TEXT,
    pr_state_observed_at    INTEGER,
    pr_activity             TEXT,
    ci_ready_at             INTEGER,
    ci_state                TEXT,
    ci_outcome              TEXT,
    last_pushed_sha         TEXT,
    push_target_kind        TEXT,
    push_target_fingerprint TEXT,
    push_ref                TEXT,
    last_pushed_at          INTEGER,
    push_generation         INTEGER,
    push_active             INTEGER NOT NULL DEFAULT 0,
    error                   TEXT,
    awaiting_agent_since    INTEGER,
    awaiting_agent_gate_id  TEXT,
    awaiting_agent_step     TEXT,
    awaiting_agent_class    TEXT,
    parked_ms               INTEGER,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    sequence         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id         TEXT NOT NULL UNIQUE,
    source_timestamp TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    payload_schema   TEXT NOT NULL,
    payload_version  INTEGER NOT NULL CHECK (payload_version > 0),
    content_class    TEXT NOT NULL DEFAULT 'metadata' CHECK (content_class = 'metadata'),
    run_id           TEXT,
    traceparent      TEXT,
    tracestate       TEXT,
    recorded_at      INTEGER NOT NULL,
    CHECK (tracestate IS NULL OR traceparent IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_event_log_run_sequence
    ON event_log (run_id, sequence);

CREATE INDEX IF NOT EXISTS idx_event_log_recorded_sequence
    ON event_log (recorded_at, sequence);

-- Lifecycle metadata is stored in fixed, family-specific tables rather than a
-- generic JSON payload. This keeps the event API metadata-only by construction
-- while allowing TW-34 facts to remain typed, bounded, and independently
-- versioned through event_log.payload_schema.
CREATE TABLE IF NOT EXISTS event_invocation_metadata (
    event_id                 TEXT PRIMARY KEY REFERENCES event_log(event_id) ON DELETE CASCADE,
    invocation_id            TEXT NOT NULL,
    phase                    TEXT NOT NULL CHECK (phase IN ('started', 'completed')),
    step_name                TEXT NOT NULL CHECK (step_name IN ('intent', 'rebase', 'review', 'test', 'document', 'lint', 'push', 'pr', 'ci', 'unknown')),
    purpose                  TEXT NOT NULL CHECK (purpose IN ('intent', 'intent-fix', 'rebase', 'rebase-fix', 'review', 'review-fix', 'test', 'test-evidence', 'test-fix', 'document', 'document-fix', 'housekeeping', 'lint', 'lint-fix', 'push', 'pr', 'ci', 'ci-fix', 'other')),
    session_mode             TEXT NOT NULL CHECK (session_mode IN ('cold', 'started', 'resumed', 'fallback', 'other')),
    outcome                  TEXT CHECK (outcome IN ('ok', 'error', 'cancelled', 'unknown')),
    failure_category         TEXT CHECK (failure_category IN ('parse', 'exit', 'spawn', 'cancelled', 'other')),
    duration_ms              INTEGER CHECK (duration_ms >= 0),
    input_tokens             INTEGER CHECK (input_tokens >= 0),
    output_tokens            INTEGER CHECK (output_tokens >= 0),
    cache_read_tokens        INTEGER CHECK (cache_read_tokens >= 0),
    cache_creation_tokens    INTEGER CHECK (cache_creation_tokens >= 0),
    fresh_input_tokens       INTEGER CHECK (fresh_input_tokens >= 0),
    reasoning_tokens         INTEGER CHECK (reasoning_tokens >= 0),
    delta_input_tokens       INTEGER CHECK (delta_input_tokens >= 0),
    delta_output_tokens      INTEGER CHECK (delta_output_tokens >= 0),
    delta_cache_read_tokens  INTEGER CHECK (delta_cache_read_tokens >= 0),
    model_roundtrips         INTEGER CHECK (model_roundtrips >= 0),
    tool_calls               INTEGER CHECK (tool_calls >= 0),
    tool_wait_calls          INTEGER CHECK (tool_wait_calls >= 0),
    tool_test_lint_calls     INTEGER CHECK (tool_test_lint_calls >= 0),
    tool_edit_calls          INTEGER CHECK (tool_edit_calls >= 0),
    tool_read_calls          INTEGER CHECK (tool_read_calls >= 0),
    tool_git_calls           INTEGER CHECK (tool_git_calls >= 0),
    tool_other_calls         INTEGER CHECK (tool_other_calls >= 0),
    workload_files           INTEGER CHECK (workload_files >= 0),
    workload_lines           INTEGER CHECK (workload_lines >= 0),
    finding_count            INTEGER CHECK (finding_count >= 0),
    CHECK ((phase = 'started' AND outcome IS NULL AND duration_ms IS NULL) OR phase = 'completed')
);

CREATE TABLE IF NOT EXISTS event_gate_metadata (
    event_id          TEXT PRIMARY KEY REFERENCES event_log(event_id) ON DELETE CASCADE,
    gate_id           TEXT NOT NULL,
    phase             TEXT NOT NULL CHECK (phase IN ('entered', 'exited')),
    step_name         TEXT NOT NULL CHECK (step_name IN ('intent', 'rebase', 'review', 'test', 'document', 'lint', 'push', 'pr', 'ci', 'unknown')),
    gate_class        TEXT NOT NULL CHECK (gate_class IN ('approval', 'fix_review', 'unknown')),
    outcome           TEXT CHECK (outcome IN ('approved', 'fix_requested', 'skipped', 'aborted', 'reconciled', 'cancelled', 'terminal', 'failed', 'unknown')),
    wait_duration_ms  INTEGER CHECK (wait_duration_ms >= 0),
    CHECK ((phase = 'entered' AND outcome IS NULL AND wait_duration_ms IS NULL) OR (phase = 'exited' AND outcome IS NOT NULL AND wait_duration_ms IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS event_ci_metadata (
    event_id  TEXT PRIMARY KEY REFERENCES event_log(event_id) ON DELETE CASCADE,
    state     TEXT NOT NULL CHECK (state IN ('running', 'green', 'failure', 'merge_wait', 'terminal')),
    outcome   TEXT NOT NULL CHECK (outcome IN ('checks', 'passed', 'no_checks', 'merge_conflict', 'checks_and_merge_conflict', 'merged', 'closed', 'unknown')),
    CHECK ((state = 'running' AND outcome IN ('checks', 'unknown'))
        OR (state IN ('green', 'merge_wait') AND outcome IN ('passed', 'no_checks'))
        OR (state = 'failure' AND outcome IN ('checks', 'merge_conflict', 'checks_and_merge_conflict'))
        OR (state = 'terminal' AND outcome IN ('merged', 'closed')))
);

CREATE TABLE IF NOT EXISTS event_pr_metadata (
    event_id  TEXT PRIMARY KEY REFERENCES event_log(event_id) ON DELETE CASCADE,
    state     TEXT NOT NULL CHECK (state IN ('created', 'open', 'checks_wait', 'review_wait', 'merged', 'closed'))
);

-- Single-row retention watermark for the durable event log. purged_through is
-- the highest sequence that retention has deleted, so a global subscriber's
-- resume cursor strictly below it can be answered with a typed cursor-expired
-- error instead of silently skipping history that no longer exists. A legacy
-- database seeds it at 0; no cursor client predates this table, so an
-- under-reported watermark on an old database cannot strand a real consumer.
CREATE TABLE IF NOT EXISTS event_log_state (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    purged_through INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO event_log_state (id, purged_through) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS step_results (
    id               TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_name        TEXT NOT NULL,
    step_order       INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    exit_code        INTEGER,
    duration_ms      INTEGER,
    log_path         TEXT,
    findings_json    TEXT,
    error            TEXT,
    started_at       INTEGER,
    completed_at     INTEGER,
    last_activity_at INTEGER,
    last_activity    TEXT,
    agent_pid        INTEGER,
    auto_fix_limit   INTEGER
);

CREATE TABLE IF NOT EXISTS step_rounds (
    id                   TEXT PRIMARY KEY,
    step_result_id       TEXT NOT NULL REFERENCES step_results(id) ON DELETE CASCADE,
    round                INTEGER NOT NULL,
    trigger_type         TEXT NOT NULL,
    findings_json        TEXT,
    reviewed_head_sha    TEXT,
    user_findings_json   TEXT,
    selected_finding_ids TEXT,
    selection_source     TEXT,
    fix_summary          TEXT,
    duration_ms          INTEGER NOT NULL,
    created_at           INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_invocations (
    id                    TEXT PRIMARY KEY,
    run_id                TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_name             TEXT NOT NULL,
    round                 INTEGER NOT NULL,
    purpose               TEXT NOT NULL,
    agent                 TEXT NOT NULL,
    model                 TEXT,
    model_provider        TEXT,
    session_mode          TEXT NOT NULL,
    session_key           TEXT,
    fallback_reason       TEXT,
    started_at            INTEGER NOT NULL,
    completed_at          INTEGER NOT NULL,
    duration_ms           INTEGER NOT NULL,
    subprocess_wait_ms    INTEGER,
    exit_status           TEXT NOT NULL,
    failure_category      TEXT,
    input_tokens          INTEGER,
    output_tokens         INTEGER,
    cache_read_tokens     INTEGER,
    cache_creation_tokens INTEGER,
    fresh_input_tokens    INTEGER,
    reasoning_tokens      INTEGER,
    delta_input_tokens    INTEGER,
    delta_output_tokens   INTEGER,
    delta_cache_read_tokens INTEGER,
    model_roundtrips      INTEGER,
    tool_calls            INTEGER,
    tool_wait_calls       INTEGER,
    tool_test_lint_calls  INTEGER,
    tool_edit_calls       INTEGER,
    tool_read_calls       INTEGER,
    tool_git_calls        INTEGER,
    tool_other_calls      INTEGER,
    workload_files        INTEGER,
    workload_lines        INTEGER,
    finding_count         INTEGER
);

CREATE INDEX IF NOT EXISTS idx_agent_invocations_run_started_id
    ON agent_invocations (run_id, started_at, id);

-- Optional native OTLP metrics are submitted at most once per durable
-- invocation across daemon restart/reprojection. This is a bounded local
-- checkpoint, not a delivery broker: a claimed measurement can still be lost
-- if the process dies before its asynchronous exporter sends it.
CREATE TABLE IF NOT EXISTS otlp_metric_projections (
    invocation_id TEXT PRIMARY KEY REFERENCES agent_invocations(id) ON DELETE CASCADE,
    claimed_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS run_agent_sessions (
    run_id     TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    role       TEXT NOT NULL,
    agent      TEXT NOT NULL,
    session_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (run_id, role)
);

CREATE TABLE IF NOT EXISTS intent_cache (
    cache_key   TEXT PRIMARY KEY,
    summary     TEXT NOT NULL,
    agent_name  TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
`

// migrationStatements hold additive schema changes applied to databases that
// were created before the referenced columns existed. Each statement must be
// idempotent via its error being tolerated when the column already exists.
var migrationStatements = []string{
	`ALTER TABLE repos ADD COLUMN fork_url TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN selected_finding_ids TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN selection_source TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN fix_summary TEXT`,
	`ALTER TABLE step_rounds ADD COLUMN user_findings_json TEXT`,
	// A parked round may retain the reviewed commit as a non-authoritative
	// candidate. Only atomic review completion promotes it onto the run.
	`ALTER TABLE step_rounds ADD COLUMN reviewed_head_sha TEXT`,
	// Incoming W3C parent context is nullable and never backfilled. A legacy
	// run without a parent remains an independent trace root.
	`ALTER TABLE runs ADD COLUMN traceparent TEXT`,
	`ALTER TABLE runs ADD COLUMN tracestate TEXT`,
	`ALTER TABLE runs ADD COLUMN intent TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_source TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_session_id TEXT`,
	`ALTER TABLE runs ADD COLUMN intent_score REAL`,
	`ALTER TABLE runs ADD COLUMN awaiting_agent_since INTEGER`,
	`ALTER TABLE runs ADD COLUMN parked_ms INTEGER`,
	// Branch synchronization provenance is intentionally nullable. Historical
	// rows stay unbound because mutable head_sha cannot prove a successful push.
	`ALTER TABLE runs ADD COLUMN submitted_head_sha TEXT`,
	// Review authority is nullable and never backfilled. A historical mutable
	// head_sha cannot prove which exact commit a completed review approved.
	`ALTER TABLE runs ADD COLUMN review_approved_head_sha TEXT`,
	`ALTER TABLE runs ADD COLUMN last_pushed_sha TEXT`,
	`ALTER TABLE runs ADD COLUMN push_target_kind TEXT`,
	`ALTER TABLE runs ADD COLUMN push_target_fingerprint TEXT`,
	`ALTER TABLE runs ADD COLUMN push_ref TEXT`,
	`ALTER TABLE runs ADD COLUMN last_pushed_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN push_generation INTEGER`,
	`ALTER TABLE runs ADD COLUMN push_active INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE runs ADD COLUMN pr_state TEXT`,
	`ALTER TABLE runs ADD COLUMN pr_state_observed_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN pr_activity TEXT`,
	`ALTER TABLE runs ADD COLUMN ci_ready_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN ci_state TEXT`,
	`ALTER TABLE runs ADD COLUMN ci_outcome TEXT`,
	// Custody return is nullable: NULL means the pipeline still owns any
	// unpublished head this run produced; a timestamp means an explicit
	// guarded recovery ended that ownership (internal/branchsync).
	`ALTER TABLE runs ADD COLUMN custody_returned_at INTEGER`,
	`ALTER TABLE runs ADD COLUMN awaiting_agent_gate_id TEXT`,
	`ALTER TABLE runs ADD COLUMN awaiting_agent_step TEXT`,
	`ALTER TABLE runs ADD COLUMN awaiting_agent_class TEXT`,
	`ALTER TABLE step_results ADD COLUMN last_activity_at INTEGER`,
	`ALTER TABLE step_results ADD COLUMN last_activity TEXT`,
	`ALTER TABLE step_results ADD COLUMN agent_pid INTEGER`,
	`ALTER TABLE step_results ADD COLUMN auto_fix_limit INTEGER`,
	// Session-fidelity telemetry columns (all nullable so pre-existing rows read
	// back as unknown, never a fabricated zero).
	`ALTER TABLE agent_invocations ADD COLUMN model_provider TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN fallback_reason TEXT`,
	`ALTER TABLE agent_invocations ADD COLUMN subprocess_wait_ms INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN fresh_input_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN reasoning_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_input_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_output_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN delta_cache_read_tokens INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN model_roundtrips INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_wait_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_test_lint_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_edit_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_read_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_git_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN tool_other_calls INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN workload_files INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN workload_lines INTEGER`,
	`ALTER TABLE agent_invocations ADD COLUMN finding_count INTEGER`,
}
