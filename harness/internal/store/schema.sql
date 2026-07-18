PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE node (
  singleton          INTEGER PRIMARY KEY CHECK (singleton = 1),
  peer_id            TEXT NOT NULL UNIQUE,
  origin_epoch       TEXT NOT NULL,
  next_origin_seq    INTEGER NOT NULL DEFAULT 1 CHECK (next_origin_seq > 0),
  active_asset_rev   TEXT NOT NULL,
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL
);

CREATE TRIGGER node_identity_immutable
BEFORE UPDATE OF peer_id, origin_epoch ON node
WHEN NEW.peer_id <> OLD.peer_id OR NEW.origin_epoch <> OLD.origin_epoch
BEGIN SELECT RAISE(ABORT, 'node identity is immutable'); END;

CREATE TRIGGER node_no_delete BEFORE DELETE ON node
BEGIN SELECT RAISE(ABORT, 'node identity cannot be deleted in place'); END;

CREATE TABLE profiles (
  profile_id         TEXT NOT NULL PRIMARY KEY CHECK (profile_id = 'teamwork-default'),
  principal          TEXT NOT NULL UNIQUE,
  workspace_root     TEXT NOT NULL,
  host               TEXT NOT NULL,
  runtime_kind       TEXT NOT NULL,
  credential_hash    BLOB NOT NULL UNIQUE,
  active_asset_rev   TEXT NOT NULL,
  handling_budget_json BLOB NOT NULL,
  enabled            INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  CHECK (
    (host = 'codex' AND runtime_kind = 'codex-app-server')
    OR (host = 'claude-code' AND runtime_kind = 'claude-cli')
  )
);

CREATE UNIQUE INDEX profiles_one_enabled_teamwork_idx
  ON profiles(enabled) WHERE enabled = 1;

CREATE TRIGGER profiles_identity_immutable
BEFORE UPDATE OF profile_id, principal, workspace_root, credential_hash, created_at ON profiles
WHEN NEW.profile_id <> OLD.profile_id
  OR NEW.principal <> OLD.principal
  OR NEW.workspace_root <> OLD.workspace_root
  OR NEW.credential_hash <> OLD.credential_hash
  OR NEW.created_at <> OLD.created_at
BEGIN SELECT RAISE(ABORT, 'Profile identity is immutable'); END;

CREATE TRIGGER profiles_no_delete BEFORE DELETE ON profiles
BEGIN SELECT RAISE(ABORT, 'Profile identity cannot be deleted in place'); END;

CREATE TABLE operations (
  operation_id       TEXT PRIMARY KEY,
  profile_id         TEXT NOT NULL REFERENCES profiles(profile_id),
  agent_run_id       TEXT NOT NULL,
  client_key_hash    BLOB NOT NULL,
  context_hash       BLOB,
  kind               TEXT NOT NULL CHECK (kind IN (
    'teamwork.offer',
    'teamwork.accept',
    'teamwork.decline',
    'teamwork.deliver',
    'teamwork.rework',
    'teamwork.close',
    'teamwork.cancel',
    'agent.resolve.no-action',
    'agent.resolve.retry',
    'agent.resolve.reject'
  )),
  request_digest     BLOB NOT NULL,
  status             TEXT NOT NULL CHECK (status IN ('started','committed','rejected')),
  lease_owner        TEXT,
  lease_until        TEXT,
  capture_json       BLOB,
  result_json        BLOB,
  created_at         TEXT NOT NULL,
  finished_at        TEXT,
  UNIQUE (profile_id, client_key_hash),
  FOREIGN KEY (agent_run_id, profile_id)
    REFERENCES agent_runs(run_id, profile_id),
  CHECK (
    (status = 'started'
      AND lease_owner IS NOT NULL
      AND lease_until IS NOT NULL
      AND result_json IS NULL
      AND finished_at IS NULL)
    OR (status IN ('committed','rejected')
      AND lease_owner IS NULL
      AND lease_until IS NULL
      AND result_json IS NOT NULL
      AND finished_at IS NOT NULL)
  )
);

CREATE INDEX operations_reclaim_idx
  ON operations(status, lease_until) WHERE status = 'started';

CREATE UNIQUE INDEX operations_one_started_context_idx
  ON operations(profile_id, context_hash)
  WHERE status = 'started' AND context_hash IS NOT NULL;

CREATE TRIGGER operations_identity_immutable
BEFORE UPDATE OF operation_id, profile_id, agent_run_id, client_key_hash, context_hash, kind, request_digest, created_at ON operations
WHEN NEW.operation_id <> OLD.operation_id
  OR NEW.profile_id <> OLD.profile_id
  OR NEW.agent_run_id <> OLD.agent_run_id
  OR NEW.client_key_hash <> OLD.client_key_hash
  OR NEW.context_hash IS NOT OLD.context_hash
  OR NEW.kind <> OLD.kind
  OR NEW.request_digest <> OLD.request_digest
  OR NEW.created_at <> OLD.created_at
BEGIN SELECT RAISE(ABORT, 'operation identity is immutable'); END;

CREATE TRIGGER operations_capture_checkpoint_immutable
BEFORE UPDATE OF capture_json ON operations
WHEN OLD.capture_json IS NOT NULL AND NEW.capture_json IS NOT OLD.capture_json
BEGIN SELECT RAISE(ABORT, 'operation capture checkpoint is immutable'); END;

CREATE TRIGGER operations_terminal_immutable BEFORE UPDATE ON operations
WHEN OLD.status IN ('committed','rejected')
BEGIN SELECT RAISE(ABORT, 'terminal operation is immutable'); END;

CREATE TRIGGER operations_no_delete BEFORE DELETE ON operations
BEGIN SELECT RAISE(ABORT, 'operations are durable evidence'); END;

CREATE TABLE events (
  event_id           TEXT PRIMARY KEY,
  schema_version     INTEGER NOT NULL CHECK (schema_version = 1),
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  origin_seq         INTEGER NOT NULL CHECK (origin_seq > 0),
  channel_seq        INTEGER NOT NULL CHECK (channel_seq > 0),
  origin_member_revision INTEGER NOT NULL,
  origin_member_record_hash BLOB NOT NULL,
  publication_roster_revision INTEGER NOT NULL,
  publication_roster_hash BLOB NOT NULL,
  source             TEXT NOT NULL CHECK (source IN ('local','imported')),
  actor_principal    TEXT NOT NULL,
  event_type         TEXT NOT NULL CHECK (event_type IN (
    'review.offered',
    'review.accept.requested',
    'review.decline.requested',
    'review.delivery.ready',
    'review.accepted',
    'review.accept_rejected',
    'review.delivered',
    'review.rework_requested',
    'review.closed',
    'review.declined',
    'review.cancelled',
    'review.expired',
    'review.outcome'
  )),
  audience_json      BLOB NOT NULL,
  resource_json      BLOB NOT NULL,
  work_home_peer_id  TEXT NOT NULL,
  work_id            TEXT NOT NULL,
  summary            TEXT NOT NULL,
  payload_json       BLOB NOT NULL,
  artifact_roots_json BLOB NOT NULL,
  caused_by_json     BLOB NOT NULL,
  canonical_event_json BLOB NOT NULL,
  event_digest       BLOB NOT NULL,
  canonical_publication_json BLOB NOT NULL,
  publication_digest BLOB NOT NULL,
  origin_signature   BLOB NOT NULL,
  created_at         TEXT NOT NULL,
  accepted_at        TEXT NOT NULL,
  UNIQUE (origin_peer_id, origin_epoch, event_id),
  UNIQUE (origin_peer_id, origin_epoch, origin_seq),
  UNIQUE (channel_id, origin_peer_id, origin_epoch, channel_seq),
  FOREIGN KEY (channel_id, origin_member_revision, origin_member_record_hash)
    REFERENCES channel_members(channel_id, revision, record_hash),
  FOREIGN KEY (channel_id, publication_roster_revision, publication_roster_hash)
    REFERENCES channel_members(channel_id, revision, record_hash),
  CHECK (publication_roster_revision >= origin_member_revision)
);

CREATE TRIGGER events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'events are immutable'); END;
CREATE TRIGGER events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events are immutable'); END;

CREATE TRIGGER events_local_origin_insert BEFORE INSERT ON events
WHEN NEW.source = 'local' AND NOT EXISTS (
  SELECT 1 FROM node
  WHERE peer_id = NEW.origin_peer_id AND origin_epoch = NEW.origin_epoch
)
BEGIN SELECT RAISE(ABORT, 'local event origin must equal node identity'); END;

CREATE TRIGGER events_publication_member_insert BEFORE INSERT ON events
WHEN (
  NOT EXISTS (
    SELECT 1 FROM channel_members
    WHERE channel_id = NEW.channel_id
      AND revision = NEW.origin_member_revision
      AND record_hash = NEW.origin_member_record_hash
      AND member_peer_id = NEW.origin_peer_id
      AND origin_epoch = NEW.origin_epoch
      AND status = 'active'
  )
  OR COALESCE((
    SELECT status FROM channel_members
    WHERE channel_id = NEW.channel_id
      AND member_peer_id = NEW.origin_peer_id
    ORDER BY revision DESC LIMIT 1
  ), '') <> 'active'
)
BEGIN SELECT RAISE(ABORT, 'event publication origin membership mismatch'); END;

CREATE TABLE works (
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  home_peer_id       TEXT NOT NULL,
  work_id            TEXT NOT NULL,
  participant_roster_revision INTEGER NOT NULL CHECK (participant_roster_revision > 0),
  version            INTEGER NOT NULL CHECK (version > 0),
  iteration          INTEGER NOT NULL DEFAULT 1 CHECK (iteration BETWEEN 1 AND 2),
  deadline_unix_nano INTEGER NOT NULL CHECK (
    typeof(deadline_unix_nano) = 'integer' AND deadline_unix_nano > 0
  ),
  state              TEXT NOT NULL CHECK (
    state IN ('OFFERED','ACTIVE','DELIVERED','REWORK','CLOSED','DECLINED','EXPIRED','CANCELLED')
  ),
  state_json         BLOB NOT NULL,
  updated_by_event   TEXT NOT NULL REFERENCES events(event_id),
  updated_at         TEXT NOT NULL,
  PRIMARY KEY (home_peer_id, work_id),
  UNIQUE (channel_id, home_peer_id, work_id)
);

CREATE INDEX works_due_idx
  ON works(deadline_unix_nano, home_peer_id, work_id)
  WHERE state IN ('OFFERED','ACTIVE','REWORK');

CREATE TRIGGER works_deadline_immutable
BEFORE UPDATE OF deadline_unix_nano ON works
WHEN NEW.deadline_unix_nano <> OLD.deadline_unix_nano
BEGIN SELECT RAISE(ABORT, 'work deadline is immutable'); END;

CREATE TRIGGER works_event_scope_insert BEFORE INSERT ON works
WHEN NOT EXISTS (
  SELECT 1 FROM events
  WHERE event_id = NEW.updated_by_event
    AND channel_id = NEW.channel_id
    AND origin_peer_id = NEW.home_peer_id
    AND work_home_peer_id = NEW.home_peer_id
    AND work_id = NEW.work_id
)
BEGIN SELECT RAISE(ABORT, 'work event identity mismatch'); END;

CREATE TRIGGER works_event_scope_update
BEFORE UPDATE OF channel_id, home_peer_id, work_id, updated_by_event ON works
WHEN NOT EXISTS (
  SELECT 1 FROM events
  WHERE event_id = NEW.updated_by_event
    AND channel_id = NEW.channel_id
    AND origin_peer_id = NEW.home_peer_id
    AND work_home_peer_id = NEW.home_peer_id
    AND work_id = NEW.work_id
)
BEGIN SELECT RAISE(ABORT, 'work event identity mismatch'); END;

CREATE TABLE work_members (
  channel_id         TEXT NOT NULL,
  home_peer_id       TEXT NOT NULL,
  work_id            TEXT NOT NULL,
  peer_id            TEXT NOT NULL,
  role               TEXT NOT NULL CHECK (role IN ('initiator','reviewer')),
  PRIMARY KEY (home_peer_id, work_id, peer_id, role),
  UNIQUE (home_peer_id, work_id, role),
  FOREIGN KEY (channel_id, home_peer_id, work_id)
    REFERENCES works(channel_id, home_peer_id, work_id)
);

CREATE TRIGGER work_members_no_update BEFORE UPDATE ON work_members
BEGIN SELECT RAISE(ABORT, 'work participant snapshot is immutable'); END;
CREATE TRIGGER work_members_no_delete BEFORE DELETE ON work_members
BEGIN SELECT RAISE(ABORT, 'work participant snapshot is immutable'); END;

CREATE TABLE work_derivations (
  operation_id       TEXT NOT NULL REFERENCES operations(operation_id),
  child_ordinal      INTEGER NOT NULL CHECK (child_ordinal BETWEEN 0 AND 6),
  child_channel_id   TEXT NOT NULL,
  child_home_peer_id TEXT NOT NULL,
  child_work_id      TEXT NOT NULL,
  parent_channel_id  TEXT NOT NULL,
  parent_home_peer_id TEXT NOT NULL,
  parent_work_id     TEXT NOT NULL,
  parent_version     INTEGER NOT NULL CHECK (parent_version > 0),
  parent_event_id    TEXT NOT NULL REFERENCES events(event_id),
  created_at         TEXT NOT NULL,
  PRIMARY KEY (child_home_peer_id, child_work_id),
  UNIQUE (operation_id, child_ordinal),
  FOREIGN KEY (child_channel_id, child_home_peer_id, child_work_id)
    REFERENCES works(channel_id, home_peer_id, work_id),
  FOREIGN KEY (parent_channel_id, parent_home_peer_id, parent_work_id)
    REFERENCES works(channel_id, home_peer_id, work_id),
  CHECK (
    child_home_peer_id <> parent_home_peer_id
    OR child_work_id <> parent_work_id
  )
);

CREATE TRIGGER work_derivations_scope_insert BEFORE INSERT ON work_derivations
WHEN NOT EXISTS (
  SELECT 1 FROM operations
  WHERE operation_id = NEW.operation_id
    AND kind = 'teamwork.offer'
    AND status = 'committed'
    AND context_hash IS NOT NULL
)
OR NOT EXISTS (
  SELECT 1 FROM events
  WHERE event_id = NEW.parent_event_id
    AND channel_id = NEW.parent_channel_id
    AND work_home_peer_id = NEW.parent_home_peer_id
    AND work_id = NEW.parent_work_id
)
OR NOT EXISTS (
  SELECT 1 FROM works
  WHERE channel_id = NEW.parent_channel_id
    AND home_peer_id = NEW.parent_home_peer_id
    AND work_id = NEW.parent_work_id
    AND version = NEW.parent_version
    AND updated_by_event = NEW.parent_event_id
    AND state IN ('ACTIVE','REWORK')
)
BEGIN SELECT RAISE(ABORT, 'work derivation scope mismatch'); END;

CREATE TRIGGER work_derivations_no_update BEFORE UPDATE ON work_derivations
BEGIN SELECT RAISE(ABORT, 'work derivation is immutable'); END;
CREATE TRIGGER work_derivations_no_delete BEFORE DELETE ON work_derivations
BEGIN SELECT RAISE(ABORT, 'work derivation is immutable'); END;

CREATE TABLE agent_handlings (
  handling_id        TEXT PRIMARY KEY,
  profile_id         TEXT NOT NULL REFERENCES profiles(profile_id),
  event_id           TEXT NOT NULL REFERENCES events(event_id),
  status             TEXT NOT NULL CHECK (status IN ('pending','claimed','completed','rejected','dead')),
  priority           INTEGER NOT NULL DEFAULT 0,
  available_at       TEXT NOT NULL,
  claim_owner        TEXT,
  claim_token_hash   BLOB,
  lease_until        TEXT,
  attempts           INTEGER NOT NULL DEFAULT 0,
  last_disposition   TEXT,
  outcome_event_id   TEXT REFERENCES events(event_id),
  last_error         TEXT,
  recovery_count     INTEGER NOT NULL DEFAULT 0 CHECK (recovery_count >= 0),
  dead_at            TEXT,
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  UNIQUE (profile_id, event_id),
  UNIQUE (handling_id, profile_id)
);

CREATE INDEX agent_handlings_ready_idx
  ON agent_handlings(profile_id, status, available_at, priority DESC, created_at);

CREATE UNIQUE INDEX agent_handlings_one_claimed_profile_idx
  ON agent_handlings(profile_id) WHERE status = 'claimed';

CREATE TRIGGER agent_handlings_identity_immutable
BEFORE UPDATE OF handling_id, profile_id, event_id, priority, created_at ON agent_handlings
WHEN NEW.handling_id <> OLD.handling_id
  OR NEW.profile_id <> OLD.profile_id
  OR NEW.event_id <> OLD.event_id
  OR NEW.priority <> OLD.priority
  OR NEW.created_at <> OLD.created_at
BEGIN SELECT RAISE(ABORT, 'handling creation identity is immutable'); END;

CREATE TABLE agent_runs (
  run_id             TEXT PRIMARY KEY,
  profile_id         TEXT NOT NULL REFERENCES profiles(profile_id),
  handling_id        TEXT,
  cause_json         BLOB NOT NULL,
  handling_attempt   INTEGER CHECK (handling_attempt > 0),
  handling_recovery  INTEGER NOT NULL DEFAULT 0 CHECK (handling_recovery >= 0),
  claim_fence_hash   BLOB,
  lease_until        TEXT,
  attachment_token_hash BLOB,
  attachment_expires_at TEXT,
  attached_at        TEXT,
  launcher           TEXT NOT NULL,
  runtime_kind       TEXT NOT NULL,
  launcher_diagnostic_json BLOB NOT NULL,
  runtime_ids_json   BLOB NOT NULL,
  status             TEXT NOT NULL CHECK (
    status IN (
      'starting','running','runtime_finished','outcome_accepted',
      'requeued','rejected','failed','dead'
    )
  ),
  runtime_started_at TEXT CHECK (
    runtime_started_at IS NULL OR (
      length(runtime_started_at) = 30
      AND runtime_started_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]Z'
      AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(runtime_started_at, 1, 19) || 'Z')) IS NOT NULL
      AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(runtime_started_at, 1, 19) || 'Z')) = substr(runtime_started_at, 1, 19)
      AND runtime_started_at >= '1677-09-21T00:12:43.145224192Z'
      AND runtime_started_at <= '2262-04-11T23:47:16.854775807Z'
    )
  ),
  wake_delivered_at  TEXT,
  wake_receipt_json  BLOB,
  started_at         TEXT NOT NULL,
  finished_at        TEXT,
  completion_at      TEXT CHECK (
    completion_at IS NULL OR (
      length(completion_at) = 30
      AND completion_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]Z'
      AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(completion_at, 1, 19) || 'Z')) IS NOT NULL
      AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(completion_at, 1, 19) || 'Z')) = substr(completion_at, 1, 19)
      AND completion_at >= '1677-09-21T00:12:43.145224192Z'
      AND completion_at <= '2262-04-11T23:47:16.854775807Z'
    )
  ),
  current_read_receipt_json BLOB,
  outcome_receipt_json BLOB,
  completion_receipt_json BLOB,
  error              TEXT,
  UNIQUE (run_id, profile_id),
  FOREIGN KEY (handling_id, profile_id)
    REFERENCES agent_handlings(handling_id, profile_id),
  CHECK (
    (handling_id IS NULL AND handling_attempt IS NULL AND handling_recovery = 0
      AND claim_fence_hash IS NULL AND lease_until IS NULL)
    OR
    (handling_id IS NOT NULL AND handling_attempt IS NOT NULL
      AND claim_fence_hash IS NOT NULL AND lease_until IS NOT NULL)
  ),
  CHECK (
    (attachment_token_hash IS NULL AND attachment_expires_at IS NULL AND attached_at IS NULL)
    OR (attachment_token_hash IS NOT NULL AND attachment_expires_at IS NOT NULL)
  ),
  CHECK (
    (status IN ('starting','running') AND finished_at IS NULL AND completion_at IS NULL)
    OR
    (status IN ('runtime_finished','outcome_accepted','requeued','rejected','failed','dead')
      AND finished_at IS NOT NULL)
  ),
  CHECK (
    (completion_at IS NULL AND completion_receipt_json IS NULL)
    OR (completion_at IS NOT NULL AND completion_receipt_json IS NOT NULL)
  ),
  CHECK (
    (wake_delivered_at IS NULL AND wake_receipt_json IS NULL)
    OR (wake_delivered_at IS NOT NULL AND wake_receipt_json IS NOT NULL
      AND length(wake_receipt_json) > 2)
  ),
  CHECK (
    completion_at IS NULL OR (
      completion_at >= started_at
      AND (wake_delivered_at IS NULL OR completion_at >= wake_delivered_at)
      AND (finished_at IS NULL OR completion_at >= finished_at)
    )
  ),
  CHECK (
    runtime_started_at IS NULL OR (
      runtime_started_at >= started_at
      AND (finished_at IS NULL OR runtime_started_at <= finished_at)
      AND length(launcher_diagnostic_json) > 2
      AND length(runtime_ids_json) > 2
    )
  ),
  CHECK (
    attached_at IS NULL OR (
      launcher <> 'mnemond-wake'
      OR (runtime_started_at IS NOT NULL AND attached_at >= runtime_started_at)
    )
  ),
  CHECK (
    launcher <> 'mnemond-wake'
    OR status <> 'starting'
    OR runtime_started_at IS NULL
  ),
  CHECK (
    launcher <> 'mnemond-wake'
    OR status <> 'running'
    OR runtime_started_at IS NOT NULL
  ),
  CHECK (
    wake_delivered_at IS NULL OR (
      (launcher <> 'mnemond-wake' OR runtime_started_at IS NOT NULL)
      AND (runtime_started_at IS NULL OR wake_delivered_at >= runtime_started_at)
      AND (finished_at IS NULL OR wake_delivered_at <= finished_at)
    )
  ),
  CHECK (
    status <> 'runtime_finished'
    OR (completion_at IS NOT NULL AND completion_at = finished_at)
  )
);

CREATE UNIQUE INDEX agent_runs_handling_generation_attempt_idx
  ON agent_runs(handling_id, handling_recovery, handling_attempt)
  WHERE handling_id IS NOT NULL;

CREATE INDEX agent_runs_incomplete_managed_idx
  ON agent_runs(started_at, run_id)
  WHERE launcher = 'mnemond-wake' AND completion_receipt_json IS NULL;

CREATE TRIGGER agent_runs_claim_snapshot_immutable
BEFORE UPDATE OF handling_id, handling_attempt, handling_recovery, claim_fence_hash, lease_until ON agent_runs
WHEN NEW.handling_id IS NOT OLD.handling_id
  OR NEW.handling_attempt IS NOT OLD.handling_attempt
  OR NEW.handling_recovery IS NOT OLD.handling_recovery
  OR NEW.claim_fence_hash IS NOT OLD.claim_fence_hash
  OR NEW.lease_until IS NOT OLD.lease_until
BEGIN SELECT RAISE(ABORT, 'agent run claim snapshot is immutable'); END;

CREATE TRIGGER agent_runs_attachment_identity_immutable
BEFORE UPDATE OF attachment_token_hash, attachment_expires_at ON agent_runs
WHEN NEW.attachment_token_hash IS NOT OLD.attachment_token_hash
  OR NEW.attachment_expires_at IS NOT OLD.attachment_expires_at
BEGIN SELECT RAISE(ABORT, 'agent run attachment identity is immutable'); END;

CREATE TRIGGER agent_runs_evidence_once
BEFORE UPDATE OF attached_at, runtime_started_at, wake_delivered_at, wake_receipt_json, current_read_receipt_json,
  outcome_receipt_json, completion_at, completion_receipt_json, launcher_diagnostic_json,
  runtime_ids_json, error ON agent_runs
WHEN (OLD.attached_at IS NOT NULL AND NEW.attached_at IS NOT OLD.attached_at)
  OR (OLD.runtime_started_at IS NOT NULL AND NEW.runtime_started_at IS NOT OLD.runtime_started_at)
  OR (OLD.wake_delivered_at IS NOT NULL AND NEW.wake_delivered_at IS NOT OLD.wake_delivered_at)
  OR (OLD.wake_receipt_json IS NOT NULL AND NEW.wake_receipt_json IS NOT OLD.wake_receipt_json)
  OR (OLD.current_read_receipt_json IS NOT NULL
      AND NEW.current_read_receipt_json IS NOT OLD.current_read_receipt_json)
  OR (OLD.outcome_receipt_json IS NOT NULL
      AND NEW.outcome_receipt_json IS NOT OLD.outcome_receipt_json)
  OR (OLD.completion_at IS NOT NULL AND NEW.completion_at IS NOT OLD.completion_at)
  OR (OLD.completion_receipt_json IS NOT NULL
      AND NEW.completion_receipt_json IS NOT OLD.completion_receipt_json)
  OR ((OLD.wake_delivered_at IS NOT NULL OR OLD.completion_receipt_json IS NOT NULL)
      AND NEW.launcher_diagnostic_json IS NOT OLD.launcher_diagnostic_json)
  OR ((OLD.wake_delivered_at IS NOT NULL OR OLD.completion_receipt_json IS NOT NULL)
      AND NEW.runtime_ids_json IS NOT OLD.runtime_ids_json)
  OR (OLD.launcher = 'mnemond-wake' AND OLD.runtime_started_at IS NOT NULL
      AND NEW.launcher_diagnostic_json IS NOT OLD.launcher_diagnostic_json)
  OR (OLD.launcher = 'mnemond-wake' AND OLD.runtime_started_at IS NOT NULL
      AND NEW.runtime_ids_json IS NOT OLD.runtime_ids_json)
  OR (OLD.completion_receipt_json IS NOT NULL AND NEW.error IS NOT OLD.error)
BEGIN SELECT RAISE(ABORT, 'agent run evidence is write-once'); END;

CREATE TABLE artifact_roots (
  root_digest        TEXT PRIMARY KEY,
  manifest_json      BLOB NOT NULL,
  manifest_digest    BLOB NOT NULL,
  total_bytes        INTEGER NOT NULL CHECK (total_bytes >= 0),
  state              TEXT NOT NULL CHECK (state IN ('staged','verified')),
  created_at         TEXT NOT NULL CHECK (
    length(created_at) = 30
    AND created_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]Z'
    AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(created_at, 1, 19) || 'Z')) IS NOT NULL
    AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(created_at, 1, 19) || 'Z')) = substr(created_at, 1, 19)
    AND created_at BETWEEN '1677-09-21T00:12:43.145224192Z' AND '2262-04-11T23:47:16.854775807Z'
  ),
  verified_at        TEXT CHECK (
    verified_at IS NULL OR (
      length(verified_at) = 30
      AND verified_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]Z'
      AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(verified_at, 1, 19) || 'Z')) IS NOT NULL
      AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(verified_at, 1, 19) || 'Z')) = substr(verified_at, 1, 19)
      AND verified_at BETWEEN '1677-09-21T00:12:43.145224192Z' AND '2262-04-11T23:47:16.854775807Z'
    )
  ),
  CHECK ((state = 'verified') = (verified_at IS NOT NULL)),
  CHECK (verified_at IS NULL OR verified_at >= created_at)
);

CREATE TRIGGER artifact_roots_content_immutable
BEFORE UPDATE OF root_digest, manifest_json, manifest_digest, total_bytes, created_at ON artifact_roots
WHEN NEW.root_digest <> OLD.root_digest
  OR NEW.manifest_json <> OLD.manifest_json
  OR NEW.manifest_digest <> OLD.manifest_digest
  OR NEW.total_bytes <> OLD.total_bytes
  OR NEW.created_at <> OLD.created_at
BEGIN SELECT RAISE(ABORT, 'artifact root content is immutable'); END;

CREATE TRIGGER artifact_roots_no_unverify BEFORE UPDATE OF state ON artifact_roots
WHEN OLD.state = 'verified' AND NEW.state <> 'verified'
BEGIN SELECT RAISE(ABORT, 'verified artifact root cannot regress'); END;

CREATE TRIGGER artifact_roots_verified_at_immutable
BEFORE UPDATE OF verified_at ON artifact_roots
WHEN OLD.verified_at IS NOT NULL AND NEW.verified_at IS NOT OLD.verified_at
BEGIN SELECT RAISE(ABORT, 'artifact verification time is immutable'); END;

CREATE TABLE artifact_blocks (
  block_digest       TEXT PRIMARY KEY,
  size_bytes         INTEGER NOT NULL CHECK (size_bytes >= 0),
  created_at         TEXT NOT NULL CHECK (
    length(created_at) = 30
    AND created_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]Z'
    AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(created_at, 1, 19) || 'Z')) IS NOT NULL
    AND strftime('%Y-%m-%dT%H:%M:%S', julianday(substr(created_at, 1, 19) || 'Z')) = substr(created_at, 1, 19)
    AND created_at BETWEEN '1677-09-21T00:12:43.145224192Z' AND '2262-04-11T23:47:16.854775807Z'
  )
);

CREATE TRIGGER artifact_blocks_no_update BEFORE UPDATE ON artifact_blocks
BEGIN SELECT RAISE(ABORT, 'artifact blocks are immutable'); END;

CREATE TABLE artifact_root_blocks (
  root_digest        TEXT NOT NULL REFERENCES artifact_roots(root_digest) ON DELETE CASCADE,
  ordinal            INTEGER NOT NULL,
  logical_path       TEXT NOT NULL,
  offset_bytes       INTEGER NOT NULL CHECK (offset_bytes >= 0),
  length_bytes       INTEGER NOT NULL CHECK (length_bytes >= 0),
  block_digest       TEXT NOT NULL REFERENCES artifact_blocks(block_digest),
  mode               INTEGER NOT NULL,
  PRIMARY KEY (root_digest, ordinal),
  UNIQUE (root_digest, logical_path, offset_bytes)
);

CREATE TRIGGER artifact_root_blocks_no_update BEFORE UPDATE ON artifact_root_blocks
BEGIN SELECT RAISE(ABORT, 'artifact root block map is immutable'); END;

CREATE TRIGGER artifact_root_blocks_verified_insert
BEFORE INSERT ON artifact_root_blocks
WHEN EXISTS (
  SELECT 1 FROM artifact_roots
  WHERE root_digest = NEW.root_digest AND state = 'verified'
)
BEGIN SELECT RAISE(ABORT, 'verified artifact root block map is sealed'); END;

CREATE TRIGGER artifact_root_blocks_verified_delete
BEFORE DELETE ON artifact_root_blocks
WHEN EXISTS (
  SELECT 1 FROM artifact_roots
  WHERE root_digest = OLD.root_digest AND state = 'verified'
)
BEGIN SELECT RAISE(ABORT, 'verified artifact root block map is sealed'); END;

CREATE TRIGGER artifact_root_blocks_provenance_delete
BEFORE DELETE ON artifact_root_blocks
WHEN EXISTS (
  SELECT 1 FROM artifact_provenance WHERE root_digest = OLD.root_digest
)
BEGIN SELECT RAISE(ABORT, 'accepted artifact closure cannot be deleted'); END;

CREATE TABLE artifact_pins (
  root_digest        TEXT NOT NULL REFERENCES artifact_roots(root_digest),
  owner_kind         TEXT NOT NULL CHECK (
    owner_kind IN ('event','handling','publication','delivery','inbox','retention')
  ),
  owner_id           TEXT NOT NULL,
  expires_at         TEXT,
  created_at         TEXT NOT NULL,
  PRIMARY KEY (root_digest, owner_kind, owner_id)
);

CREATE TABLE artifact_provenance (
  root_digest        TEXT NOT NULL REFERENCES artifact_roots(root_digest),
  producer_event_id  TEXT NOT NULL REFERENCES events(event_id),
  producer_origin_peer_id TEXT NOT NULL,
  local_agent_run_id TEXT REFERENCES agent_runs(run_id),
  operation_id       TEXT REFERENCES operations(operation_id),
  relation           TEXT NOT NULL CHECK (relation IN ('local_capture','replica')),
  created_at         TEXT NOT NULL,
  PRIMARY KEY (root_digest, producer_event_id),
  CHECK (
    (relation = 'local_capture' AND local_agent_run_id IS NOT NULL AND operation_id IS NOT NULL)
    OR
    (relation = 'replica' AND local_agent_run_id IS NULL AND operation_id IS NULL)
  )
);

CREATE TRIGGER artifact_provenance_event_insert BEFORE INSERT ON artifact_provenance
WHEN NOT EXISTS (
  SELECT 1 FROM events
  WHERE event_id = NEW.producer_event_id
    AND origin_peer_id = NEW.producer_origin_peer_id
    AND (
      (NEW.relation = 'local_capture' AND source = 'local')
      OR (NEW.relation = 'replica' AND source = 'imported')
    )
)
OR (
  NEW.relation = 'local_capture'
  AND NOT EXISTS (
    SELECT 1 FROM operations
    WHERE operation_id = NEW.operation_id
      AND agent_run_id = NEW.local_agent_run_id
  )
)
BEGIN SELECT RAISE(ABORT, 'artifact producer event mismatch'); END;

CREATE TRIGGER artifact_provenance_no_update BEFORE UPDATE ON artifact_provenance
BEGIN SELECT RAISE(ABORT, 'artifact provenance is immutable'); END;

CREATE TRIGGER artifact_provenance_no_delete BEFORE DELETE ON artifact_provenance
BEGIN SELECT RAISE(ABORT, 'artifact provenance is immutable'); END;

CREATE TABLE channels (
  channel_id         TEXT PRIMARY KEY,
  name               TEXT NOT NULL,
  local_alias        TEXT NOT NULL UNIQUE,
  owner_peer_id      TEXT NOT NULL,
  owner_public_key   BLOB NOT NULL,
  descriptor_json    BLOB NOT NULL,
  descriptor_digest  BLOB NOT NULL UNIQUE,
  descriptor_signature BLOB NOT NULL,
  member_limit       INTEGER NOT NULL DEFAULT 8 CHECK (member_limit BETWEEN 2 AND 8),
  roster_head_revision INTEGER NOT NULL CHECK (roster_head_revision BETWEEN 1 AND 64),
  roster_head_hash   BLOB NOT NULL,
  status             TEXT NOT NULL CHECK (
    status IN ('active','leaving','conflicted','left','closed','abandoned')
  ),
  topic_state        TEXT NOT NULL CHECK (
    topic_state IN ('not_joined','joining','joined','blocked','left')
  ),
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  FOREIGN KEY (channel_id, roster_head_revision, roster_head_hash)
    REFERENCES channel_members(channel_id, revision, record_hash)
    DEFERRABLE INITIALLY DEFERRED,
  CHECK (
    status = 'active'
    OR (status = 'conflicted' AND topic_state IN ('blocked','left'))
    OR (status IN ('leaving','left','closed','abandoned') AND topic_state = 'left')
  )
);

CREATE TRIGGER channels_nonterminal_limit_insert
BEFORE INSERT ON channels
WHEN NEW.status IN ('active','leaving','conflicted')
 AND (
   SELECT COUNT(*) FROM channels
   WHERE status IN ('active','leaving','conflicted')
 ) + (SELECT COUNT(*) FROM channel_join_reservations) >= 8
BEGIN SELECT RAISE(ABORT, 'node_channel_limit'); END;

CREATE TRIGGER channels_descriptor_immutable
BEFORE UPDATE OF channel_id, name, owner_peer_id, owner_public_key, descriptor_json,
  descriptor_digest, descriptor_signature, member_limit, created_at ON channels
WHEN NEW.channel_id <> OLD.channel_id
  OR NEW.name <> OLD.name
  OR NEW.owner_peer_id <> OLD.owner_peer_id
  OR NEW.owner_public_key <> OLD.owner_public_key
  OR NEW.descriptor_json <> OLD.descriptor_json
  OR NEW.descriptor_digest <> OLD.descriptor_digest
  OR NEW.descriptor_signature <> OLD.descriptor_signature
  OR NEW.member_limit <> OLD.member_limit
  OR NEW.created_at <> OLD.created_at
BEGIN SELECT RAISE(ABORT, 'channel descriptor evidence is immutable'); END;

CREATE TRIGGER channels_no_delete BEFORE DELETE ON channels
BEGIN SELECT RAISE(ABORT, 'channel descriptor evidence is permanent'); END;

CREATE TRIGGER channels_roster_head_monotonic
BEFORE UPDATE OF roster_head_revision, roster_head_hash ON channels
WHEN NEW.roster_head_revision < OLD.roster_head_revision
  OR (
    NEW.roster_head_revision = OLD.roster_head_revision
    AND NEW.roster_head_hash <> OLD.roster_head_hash
  )
BEGIN SELECT RAISE(ABORT, 'channel roster head cannot regress or fork in place'); END;

CREATE TRIGGER channels_terminal_status_update
BEFORE UPDATE OF status ON channels
WHEN OLD.status IN ('left','closed','abandoned') AND NEW.status <> OLD.status
  AND NOT (OLD.status = 'left' AND NEW.status = 'closed')
  AND NOT (OLD.status IN ('left','closed') AND NEW.status = 'conflicted')
BEGIN SELECT RAISE(ABORT, 'terminal channel cannot reactivate'); END;

CREATE TRIGGER channels_conflicted_status_update
BEFORE UPDATE OF status ON channels
WHEN OLD.status = 'conflicted' AND NEW.status NOT IN ('conflicted','abandoned')
BEGIN SELECT RAISE(ABORT, 'conflicted channel can only be abandoned'); END;

CREATE TRIGGER channels_leaving_status_update
BEFORE UPDATE OF status ON channels
WHEN OLD.status = 'leaving' AND NEW.status NOT IN ('leaving','conflicted','left','closed','abandoned')
BEGIN SELECT RAISE(ABORT, 'leaving channel cannot reactivate'); END;

CREATE TABLE channel_members (
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  revision           INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 64),
  record_hash        BLOB NOT NULL,
  previous_hash      BLOB,
  member_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  display_label      TEXT NOT NULL,
  public_key         BLOB NOT NULL,
  multiaddrs_json    BLOB NOT NULL,
  protocols_json     BLOB NOT NULL,
  limits_json        BLOB NOT NULL,
  status             TEXT NOT NULL CHECK (status IN ('active','left','revoked')),
  signed_record_json BLOB NOT NULL,
  owner_signature    BLOB NOT NULL,
  created_at         TEXT NOT NULL,
  PRIMARY KEY (channel_id, revision),
  UNIQUE (channel_id, record_hash),
  UNIQUE (channel_id, revision, record_hash),
  UNIQUE (channel_id, revision, record_hash, member_peer_id),
  UNIQUE (
    channel_id, revision, record_hash, member_peer_id, origin_epoch, public_key,
    multiaddrs_json, protocols_json, limits_json
  ),
  FOREIGN KEY (channel_id, previous_hash)
    REFERENCES channel_members(channel_id, record_hash)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE TRIGGER channel_members_no_update BEFORE UPDATE ON channel_members
BEGIN SELECT RAISE(ABORT, 'channel member records are append-only'); END;
CREATE TRIGGER channel_members_no_delete BEFORE DELETE ON channel_members
BEGIN SELECT RAISE(ABORT, 'channel member records are append-only'); END;

CREATE TABLE channel_conflicts (
  conflict_id        TEXT PRIMARY KEY,
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  revision           INTEGER NOT NULL CHECK (revision > 0),
  incumbent_record_hash BLOB NOT NULL,
  incumbent_signed_record_json BLOB NOT NULL,
  incumbent_owner_signature BLOB NOT NULL,
  challenger_record_hash BLOB NOT NULL,
  challenger_signed_record_json BLOB NOT NULL,
  challenger_owner_signature BLOB NOT NULL,
  transport_peer_id  TEXT NOT NULL,
  detected_at        TEXT NOT NULL,
  UNIQUE (channel_id, revision, challenger_record_hash),
  FOREIGN KEY (channel_id, revision, incumbent_record_hash)
    REFERENCES channel_members(channel_id, revision, record_hash),
  CHECK (challenger_record_hash <> incumbent_record_hash)
);

CREATE TRIGGER channel_conflicts_no_update BEFORE UPDATE ON channel_conflicts
BEGIN SELECT RAISE(ABORT, 'channel conflict evidence is immutable'); END;
CREATE TRIGGER channel_conflicts_no_delete BEFORE DELETE ON channel_conflicts
BEGIN SELECT RAISE(ABORT, 'channel conflict evidence is permanent'); END;

CREATE TRIGGER channel_conflicts_limit_insert BEFORE INSERT ON channel_conflicts
WHEN (SELECT COUNT(*) FROM channel_conflicts WHERE channel_id = NEW.channel_id) >= 8
BEGIN SELECT RAISE(ABORT, 'channel conflict evidence limit reached'); END;

CREATE TRIGGER channels_conflicted_requires_evidence
BEFORE UPDATE OF status ON channels
WHEN NEW.status = 'conflicted' AND OLD.status <> 'conflicted' AND NOT EXISTS (
  SELECT 1 FROM channel_conflicts WHERE channel_id = NEW.channel_id
)
BEGIN SELECT RAISE(ABORT, 'conflicted channel requires durable challenger evidence'); END;

CREATE TRIGGER channel_members_no_reactivate BEFORE INSERT ON channel_members
WHEN NEW.status = 'active' AND EXISTS (
  SELECT 1 FROM channel_members
  WHERE channel_id = NEW.channel_id
    AND member_peer_id = NEW.member_peer_id
    AND status IN ('left','revoked')
)
BEGIN SELECT RAISE(ABORT, 'terminal member peer id cannot reactivate'); END;

CREATE TRIGGER channel_members_capacity_insert
BEFORE INSERT ON channel_members
WHEN NEW.status = 'active'
 AND NOT EXISTS (
   SELECT 1 FROM channel_members
   WHERE channel_id = NEW.channel_id
     AND member_peer_id = NEW.member_peer_id
 )
 AND (
   SELECT COUNT(*)
   FROM channel_members current
   WHERE current.channel_id = NEW.channel_id
     AND current.status = 'active'
     AND current.revision = (
       SELECT MAX(latest.revision)
       FROM channel_members latest
       WHERE latest.channel_id = current.channel_id
         AND latest.member_peer_id = current.member_peer_id
     )
 ) >= (
   SELECT member_limit FROM channels WHERE channel_id = NEW.channel_id
 )
BEGIN SELECT RAISE(ABORT, 'channel_full'); END;

CREATE TABLE enrollment_grants (
  grant_id           TEXT PRIMARY KEY,
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  verifier           BLOB NOT NULL,
  expires_at         TEXT NOT NULL,
  max_uses           INTEGER NOT NULL CHECK (max_uses BETWEEN 1 AND 7),
  used_uses          INTEGER NOT NULL DEFAULT 0 CHECK (used_uses >= 0 AND used_uses <= max_uses),
  status             TEXT NOT NULL CHECK (status IN ('open','closed','expired','exhausted')),
  created_at         TEXT NOT NULL,
  closed_at          TEXT,
  UNIQUE (grant_id, channel_id),
  CHECK (expires_at > created_at),
  CHECK (closed_at IS NULL OR closed_at >= created_at),
  CHECK (
    (status = 'open' AND used_uses < max_uses AND closed_at IS NULL)
    OR (status = 'exhausted' AND used_uses = max_uses AND closed_at IS NOT NULL)
    OR (status IN ('closed','expired') AND closed_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX enrollment_grants_one_open_idx
  ON enrollment_grants(channel_id) WHERE status = 'open';

CREATE TRIGGER enrollment_grants_initial_state_insert
BEFORE INSERT ON enrollment_grants
WHEN NEW.used_uses <> 0 OR NEW.status <> 'open' OR NEW.closed_at IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'enrollment grant must begin open and unused'); END;

CREATE TRIGGER enrollment_grants_lifecycle_insert
BEFORE INSERT ON enrollment_grants
WHEN EXISTS (
  SELECT 1 FROM enrollment_grants prior
  WHERE prior.channel_id = NEW.channel_id
    AND (
      NEW.created_at <= prior.created_at
      OR (prior.closed_at IS NOT NULL AND NEW.created_at < prior.closed_at)
    )
)
BEGIN SELECT RAISE(ABORT, 'enrollment grant lifecycle cannot regress'); END;

CREATE TRIGGER enrollment_grants_identity_immutable
BEFORE UPDATE OF grant_id, channel_id, verifier, expires_at, max_uses, created_at ON enrollment_grants
WHEN NEW.grant_id <> OLD.grant_id
  OR NEW.channel_id <> OLD.channel_id
  OR NEW.verifier <> OLD.verifier
  OR NEW.expires_at <> OLD.expires_at
  OR NEW.max_uses <> OLD.max_uses
  OR NEW.created_at <> OLD.created_at
BEGIN SELECT RAISE(ABORT, 'enrollment grant identity is immutable'); END;

CREATE TRIGGER enrollment_grants_no_delete BEFORE DELETE ON enrollment_grants
BEGIN SELECT RAISE(ABORT, 'enrollment grant evidence is permanent'); END;

CREATE TRIGGER enrollment_grants_state_update
BEFORE UPDATE OF used_uses, status, closed_at ON enrollment_grants
WHEN NEW.used_uses < OLD.used_uses
  OR NEW.used_uses > OLD.used_uses + 1
  OR NEW.used_uses <> (
    SELECT COUNT(*) FROM enrollment_grant_uses WHERE grant_id = NEW.grant_id
  )
  OR (
    OLD.status <> 'open'
    AND (
      NEW.used_uses <> OLD.used_uses
      OR NEW.status <> OLD.status
      OR NEW.closed_at IS NOT OLD.closed_at
    )
  )
  OR (OLD.closed_at IS NOT NULL AND NEW.closed_at IS NOT OLD.closed_at)
  OR (
    NEW.closed_at IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM enrollment_grant_uses use_record
      WHERE use_record.grant_id = NEW.grant_id
        AND use_record.used_at > NEW.closed_at
    )
  )
  OR (NEW.status = 'closed' AND (NEW.used_uses = NEW.max_uses OR NEW.closed_at >= NEW.expires_at))
  OR (NEW.status = 'expired' AND (NEW.used_uses = NEW.max_uses OR NEW.closed_at < NEW.expires_at))
  OR (
    NEW.status = 'exhausted'
    AND (
      NEW.used_uses <> NEW.max_uses
      OR NEW.closed_at IS NOT (
        SELECT MAX(use_record.used_at) FROM enrollment_grant_uses use_record
        WHERE use_record.grant_id = NEW.grant_id
      )
    )
  )
BEGIN SELECT RAISE(ABORT, 'enrollment grant state is monotonic and terminal'); END;

CREATE TABLE enrollment_grant_uses (
  use_id             TEXT PRIMARY KEY,
  grant_id           TEXT NOT NULL REFERENCES enrollment_grants(grant_id),
  channel_id         TEXT NOT NULL,
  member_peer_id     TEXT NOT NULL,
  join_identity_digest BLOB NOT NULL,
  member_revision    INTEGER NOT NULL,
  member_record_hash BLOB NOT NULL,
  used_at            TEXT NOT NULL,
  CHECK (member_revision > 1),
  UNIQUE (grant_id, member_peer_id),
  UNIQUE (grant_id, join_identity_digest),
  UNIQUE (channel_id, member_peer_id),
  UNIQUE (use_id, channel_id, member_peer_id),
  UNIQUE (
    use_id, channel_id, member_peer_id, member_revision, member_record_hash
  ),
  FOREIGN KEY (grant_id, channel_id)
    REFERENCES enrollment_grants(grant_id, channel_id),
  FOREIGN KEY (channel_id, member_revision, member_record_hash, member_peer_id)
    REFERENCES channel_members(channel_id, revision, record_hash, member_peer_id)
);

CREATE TRIGGER enrollment_grant_uses_validate_insert
BEFORE INSERT ON enrollment_grant_uses
WHEN NOT EXISTS (
  SELECT 1
  FROM enrollment_grants grant_state
  JOIN channel_members member
    ON member.channel_id = NEW.channel_id
    AND member.revision = NEW.member_revision
    AND member.record_hash = NEW.member_record_hash
    AND member.member_peer_id = NEW.member_peer_id
  WHERE grant_state.grant_id = NEW.grant_id
    AND grant_state.channel_id = NEW.channel_id
    AND grant_state.status = 'open'
    AND grant_state.used_uses < grant_state.max_uses
    AND grant_state.used_uses = (
      SELECT COUNT(*) FROM enrollment_grant_uses prior
      WHERE prior.grant_id = NEW.grant_id
    )
    AND member.status = 'active'
    AND member.revision > 1
    AND member.created_at >= grant_state.created_at
    AND NEW.used_at >= grant_state.created_at
    AND NEW.used_at < grant_state.expires_at
    AND NEW.used_at >= member.created_at
)
BEGIN SELECT RAISE(ABORT, 'grant use requires open consistent grant and active join record'); END;

CREATE TRIGGER enrollment_grant_uses_account_insert
AFTER INSERT ON enrollment_grant_uses
BEGIN
  UPDATE enrollment_grants
  SET used_uses = used_uses + 1,
      status = CASE WHEN used_uses + 1 = max_uses THEN 'exhausted' ELSE 'open' END,
      closed_at = CASE WHEN used_uses + 1 = max_uses THEN NEW.used_at ELSE NULL END
  WHERE grant_id = NEW.grant_id;
END;

CREATE TRIGGER enrollment_grant_uses_no_update BEFORE UPDATE ON enrollment_grant_uses
BEGIN SELECT RAISE(ABORT, 'enrollment grant use evidence is immutable'); END;
CREATE TRIGGER enrollment_grant_uses_no_delete BEFORE DELETE ON enrollment_grant_uses
BEGIN SELECT RAISE(ABORT, 'enrollment grant use evidence is permanent'); END;

CREATE TABLE enrollment_receipts (
  receipt_id         TEXT PRIMARY KEY,
  owner_use_id       TEXT UNIQUE,
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  member_peer_id     TEXT NOT NULL,
  roster_head_revision INTEGER NOT NULL,
  roster_head_hash   BLOB NOT NULL,
  receipt_json       BLOB NOT NULL,
  owner_signature    BLOB NOT NULL,
  created_at         TEXT NOT NULL,
  CHECK (roster_head_revision > 1),
  FOREIGN KEY (channel_id, roster_head_revision, roster_head_hash, member_peer_id)
    REFERENCES channel_members(channel_id, revision, record_hash, member_peer_id),
  FOREIGN KEY (
    owner_use_id, channel_id, member_peer_id, roster_head_revision, roster_head_hash
  ) REFERENCES enrollment_grant_uses(
    use_id, channel_id, member_peer_id, member_revision, member_record_hash
  )
);

CREATE TRIGGER enrollment_receipts_owner_evidence_insert
BEFORE INSERT ON enrollment_receipts
WHEN NEW.owner_use_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM enrollment_grant_uses
  WHERE use_id = NEW.owner_use_id
    AND channel_id = NEW.channel_id
    AND member_peer_id = NEW.member_peer_id
    AND member_revision = NEW.roster_head_revision
    AND member_record_hash = NEW.roster_head_hash
    AND used_at <= NEW.created_at
)
BEGIN SELECT RAISE(ABORT, 'owner receipt must match its grant use evidence'); END;

CREATE TRIGGER enrollment_receipts_no_update BEFORE UPDATE ON enrollment_receipts
BEGIN SELECT RAISE(ABORT, 'enrollment receipt evidence is immutable'); END;
CREATE TRIGGER enrollment_receipts_no_delete BEFORE DELETE ON enrollment_receipts
BEGIN SELECT RAISE(ABORT, 'enrollment receipt evidence is permanent'); END;

-- A joiner reserves one local Channel slot before sending EnrollInit. The row
-- contains no bearer material and survives response loss/restart until the
-- signed replica is installed or a verified remote rejection releases it.
CREATE TABLE channel_join_reservations (
  request_id          TEXT PRIMARY KEY,
  channel_id          TEXT NOT NULL UNIQUE,
  grant_id            TEXT NOT NULL,
  join_identity_digest BLOB NOT NULL UNIQUE,
  descriptor_digest   BLOB NOT NULL,
  local_peer_id       TEXT NOT NULL,
  origin_epoch        TEXT NOT NULL,
  local_alias         TEXT NOT NULL UNIQUE,
  attempt             INTEGER NOT NULL CHECK (attempt BETWEEN 1 AND 9223372036854775807),
  state               TEXT NOT NULL CHECK (state IN ('reserved','commit_unknown')),
  created_at          TEXT NOT NULL,
  updated_at          TEXT NOT NULL,
  CHECK (updated_at >= created_at)
);

CREATE TRIGGER channel_join_reservations_limit_insert
BEFORE INSERT ON channel_join_reservations
WHEN (
  SELECT COUNT(*) FROM channels
  WHERE status IN ('active','leaving','conflicted')
) + (SELECT COUNT(*) FROM channel_join_reservations) >= 8
BEGIN SELECT RAISE(ABORT, 'node_channel_limit'); END;

CREATE TRIGGER channel_join_reservations_scope_insert
BEFORE INSERT ON channel_join_reservations
WHEN EXISTS (
  SELECT 1 FROM channels
  WHERE channel_id = NEW.channel_id OR local_alias = NEW.local_alias
)
BEGIN SELECT RAISE(ABORT, 'channel join reservation conflicts with installed Channel'); END;

CREATE TRIGGER channel_join_reservations_identity_immutable
BEFORE UPDATE OF request_id,channel_id,grant_id,join_identity_digest,
  descriptor_digest,local_peer_id,origin_epoch,local_alias,created_at
ON channel_join_reservations
WHEN NEW.request_id <> OLD.request_id
  OR NEW.channel_id <> OLD.channel_id
  OR NEW.grant_id <> OLD.grant_id
  OR NEW.join_identity_digest <> OLD.join_identity_digest
  OR NEW.descriptor_digest <> OLD.descriptor_digest
  OR NEW.local_peer_id <> OLD.local_peer_id
  OR NEW.origin_epoch <> OLD.origin_epoch
  OR NEW.local_alias <> OLD.local_alias
  OR NEW.created_at <> OLD.created_at
BEGIN SELECT RAISE(ABORT, 'channel join reservation identity is immutable'); END;

CREATE TRIGGER channel_join_reservations_attempt_monotonic
BEFORE UPDATE OF attempt ON channel_join_reservations
WHEN NEW.attempt <> OLD.attempt + 1
BEGIN SELECT RAISE(ABORT, 'channel join reservation attempt must advance once'); END;

CREATE TRIGGER channel_join_reservations_state_time_monotonic
BEFORE UPDATE OF state,updated_at ON channel_join_reservations
WHEN (OLD.state = 'commit_unknown' AND NEW.state <> 'commit_unknown')
  OR NEW.updated_at < OLD.updated_at
BEGIN SELECT RAISE(ABORT, 'channel join reservation state or time regressed'); END;

CREATE TRIGGER channels_join_reservation_insert
BEFORE INSERT ON channels
WHEN EXISTS (
  SELECT 1 FROM channel_join_reservations
  WHERE channel_id = NEW.channel_id OR local_alias = NEW.local_alias
)
BEGIN SELECT RAISE(ABORT, 'Channel conflicts with reserved join'); END;

CREATE TABLE channel_leave_requests (
  request_id         TEXT PRIMARY KEY,
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  member_peer_id     TEXT NOT NULL,
  request_digest     BLOB NOT NULL UNIQUE,
  request_json       BLOB NOT NULL,
  member_signature   BLOB NOT NULL,
  status             TEXT NOT NULL CHECK (status IN ('queued','sent','accepted','rejected')),
  attempts           INTEGER NOT NULL DEFAULT 0,
  next_attempt_at    TEXT NOT NULL,
  receipt_json       BLOB,
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL
);

CREATE UNIQUE INDEX channel_leave_requests_one_open_idx
  ON channel_leave_requests(channel_id, member_peer_id)
  WHERE status IN ('queued','sent');

CREATE TABLE peer_bindings (
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  peer_id            TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  effective_alias    TEXT NOT NULL,
  public_key         BLOB NOT NULL,
  multiaddrs_json    BLOB NOT NULL,
  protocols_json     BLOB NOT NULL,
  limits_json        BLOB NOT NULL,
  member_revision    INTEGER NOT NULL,
  member_record_hash BLOB NOT NULL,
  state              TEXT NOT NULL CHECK (state IN ('pending','active','revoked')),
  reachability       TEXT NOT NULL DEFAULT 'unknown' CHECK (
    reachability IN ('unknown','reachable','unreachable')
  ),
  joined_at          TEXT NOT NULL,
  last_seen_at       TEXT,
  PRIMARY KEY (channel_id, peer_id),
  UNIQUE (channel_id, effective_alias),
  FOREIGN KEY (
    channel_id, member_revision, member_record_hash, peer_id, origin_epoch, public_key,
    multiaddrs_json, protocols_json, limits_json
  ) REFERENCES channel_members(
    channel_id, revision, record_hash, member_peer_id, origin_epoch, public_key,
    multiaddrs_json, protocols_json, limits_json
  )
);

CREATE TRIGGER peer_bindings_no_self_insert BEFORE INSERT ON peer_bindings
WHEN EXISTS (SELECT 1 FROM node WHERE peer_id = NEW.peer_id)
BEGIN SELECT RAISE(ABORT, 'self peer binding forbidden'); END;

CREATE TRIGGER peer_bindings_member_state_insert BEFORE INSERT ON peer_bindings
WHEN NOT EXISTS (
  SELECT 1 FROM channel_members
  WHERE channel_id = NEW.channel_id
    AND revision = NEW.member_revision
    AND record_hash = NEW.member_record_hash
    AND member_peer_id = NEW.peer_id
    AND (
      (NEW.state IN ('pending','active') AND status = 'active')
      OR (NEW.state = 'revoked' AND status IN ('left','revoked'))
    )
)
BEGIN SELECT RAISE(ABORT, 'binding state must match member record'); END;

CREATE TRIGGER peer_bindings_member_state_update
BEFORE UPDATE OF channel_id, peer_id, member_revision, member_record_hash, state ON peer_bindings
WHEN NOT EXISTS (
  SELECT 1 FROM channel_members
  WHERE channel_id = NEW.channel_id
    AND revision = NEW.member_revision
    AND record_hash = NEW.member_record_hash
    AND member_peer_id = NEW.peer_id
    AND (
      (NEW.state IN ('pending','active') AND status = 'active')
      OR (NEW.state = 'revoked' AND status IN ('left','revoked'))
    )
)
BEGIN SELECT RAISE(ABORT, 'binding state must match member record'); END;

CREATE TRIGGER peer_bindings_no_self_update
BEFORE UPDATE OF peer_id ON peer_bindings
WHEN EXISTS (SELECT 1 FROM node WHERE peer_id = NEW.peer_id)
BEGIN SELECT RAISE(ABORT, 'self peer binding forbidden'); END;

CREATE TRIGGER peer_bindings_identity_epoch_immutable
BEFORE UPDATE OF channel_id, peer_id, origin_epoch ON peer_bindings
WHEN NEW.channel_id <> OLD.channel_id
  OR NEW.peer_id <> OLD.peer_id
  OR NEW.origin_epoch <> OLD.origin_epoch
BEGIN SELECT RAISE(ABORT, 'binding identity and origin epoch are immutable'); END;

CREATE TRIGGER peer_bindings_revoked_terminal
BEFORE UPDATE OF state ON peer_bindings
WHEN OLD.state = 'revoked' AND NEW.state <> 'revoked'
BEGIN SELECT RAISE(ABORT, 'revoked binding is terminal'); END;

CREATE TRIGGER peer_bindings_active_no_pending
BEFORE UPDATE OF state ON peer_bindings
WHEN OLD.state = 'active' AND NEW.state = 'pending'
BEGIN SELECT RAISE(ABORT, 'active binding cannot return to pending'); END;

CREATE TRIGGER events_imported_binding_insert
BEFORE INSERT ON events
WHEN NEW.source = 'imported' AND (
  NOT EXISTS (
    SELECT 1 FROM peer_bindings
    WHERE channel_id = NEW.channel_id
      AND peer_id = NEW.origin_peer_id
      AND origin_epoch = NEW.origin_epoch
      AND state = 'active'
  )
)
BEGIN SELECT RAISE(ABORT, 'imported event binding or epoch mismatch'); END;

CREATE TABLE gossip_publications (
  event_id           TEXT PRIMARY KEY REFERENCES events(event_id),
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  channel_seq        INTEGER NOT NULL CHECK (channel_seq > 0),
  status             TEXT NOT NULL CHECK (
    status IN ('queued','leased','published','blocked','abandoned')
  ),
  attempts           INTEGER NOT NULL DEFAULT 0 CHECK (
    attempts BETWEEN 0 AND 4294967295
  ),
  next_attempt_at    TEXT NOT NULL,
  lease_owner        TEXT,
  lease_until        TEXT,
  lease_fence_json   BLOB,
  published_at       TEXT,
  last_error         TEXT,
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  UNIQUE (channel_id, origin_peer_id, origin_epoch, channel_seq),
  CHECK (
    (status = 'leased' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
    OR (status <> 'leased' AND lease_owner IS NULL AND lease_until IS NULL)
  ),
  CHECK (
    (attempts = 0 AND lease_fence_json IS NULL)
    OR (
      attempts > 0
      AND typeof(lease_fence_json) = 'blob'
      AND length(lease_fence_json) BETWEEN 1 AND 4096
    )
  ),
  CHECK ((status = 'published') = (published_at IS NOT NULL))
);

CREATE TRIGGER gossip_publications_event_scope_insert
BEFORE INSERT ON gossip_publications
WHEN NOT EXISTS (
  SELECT 1 FROM events
  WHERE event_id = NEW.event_id
    AND channel_id = NEW.channel_id
    AND origin_peer_id = NEW.origin_peer_id
    AND origin_epoch = NEW.origin_epoch
    AND channel_seq = NEW.channel_seq
    AND source = 'local'
)
BEGIN SELECT RAISE(ABORT, 'gossip publication event mismatch'); END;

CREATE TRIGGER gossip_publications_identity_immutable
BEFORE UPDATE OF event_id, channel_id, origin_peer_id, origin_epoch, channel_seq
ON gossip_publications
WHEN NEW.event_id <> OLD.event_id
  OR NEW.channel_id <> OLD.channel_id
  OR NEW.origin_peer_id <> OLD.origin_peer_id
  OR NEW.origin_epoch <> OLD.origin_epoch
  OR NEW.channel_seq <> OLD.channel_seq
BEGIN SELECT RAISE(ABORT, 'gossip publication identity is immutable'); END;

-- A claim is the only transition allowed to advance attempts and replace the
-- canonical capability. Recovery, retry, publish and authority blocking must
-- retain the exact last capability so a response-loss replay can be checked
-- byte-for-byte after lease_owner/lease_until have been cleared.
CREATE TRIGGER gossip_publications_lease_fence_update
BEFORE UPDATE OF status, attempts, lease_owner, lease_until, lease_fence_json
ON gossip_publications
WHEN (
  NEW.status = 'leased'
  AND (
    OLD.status <> 'queued'
    OR NEW.attempts <> OLD.attempts + 1
    OR NEW.lease_fence_json IS NULL
    OR NEW.lease_fence_json IS OLD.lease_fence_json
  )
) OR (
  NEW.status <> 'leased'
  AND (
    NEW.attempts <> OLD.attempts
    OR NEW.lease_fence_json IS NOT OLD.lease_fence_json
  )
)
BEGIN SELECT RAISE(ABORT, 'publication lease fence transition is invalid'); END;

CREATE INDEX gossip_publications_ready_idx
  ON gossip_publications(status, next_attempt_at);

CREATE TABLE peer_deliveries (
  delivery_id        TEXT PRIMARY KEY,
  channel_id         TEXT NOT NULL,
  target_peer_id     TEXT NOT NULL,
  event_id           TEXT NOT NULL REFERENCES events(event_id),
  status             TEXT NOT NULL CHECK (
    status IN ('pending','scanned','blocked','abandoned')
  ),
  scanned_at         TEXT,
  last_error         TEXT,
  created_at         TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  UNIQUE (channel_id, target_peer_id, event_id),
  FOREIGN KEY (channel_id, target_peer_id)
    REFERENCES peer_bindings(channel_id, peer_id),
  CHECK ((status = 'scanned') = (scanned_at IS NOT NULL))
);

CREATE TRIGGER peer_deliveries_event_scope_insert
BEFORE INSERT ON peer_deliveries
WHEN NOT EXISTS (
  SELECT 1 FROM events
  WHERE event_id = NEW.event_id
    AND channel_id = NEW.channel_id
    AND source = 'local'
)
BEGIN SELECT RAISE(ABORT, 'peer delivery event channel mismatch'); END;

CREATE TRIGGER peer_deliveries_identity_immutable
BEFORE UPDATE OF channel_id, target_peer_id, event_id ON peer_deliveries
WHEN NEW.channel_id <> OLD.channel_id
  OR NEW.target_peer_id <> OLD.target_peer_id
  OR NEW.event_id <> OLD.event_id
BEGIN SELECT RAISE(ABORT, 'peer delivery identity is immutable'); END;

CREATE TRIGGER peer_deliveries_scanned_requires_cursor
BEFORE UPDATE OF status, scanned_at ON peer_deliveries
WHEN NEW.status = 'scanned' AND OLD.status <> 'scanned' AND NOT EXISTS (
  SELECT 1
  FROM events e
  JOIN peer_pull_acks a
    ON a.channel_id = NEW.channel_id
   AND a.target_peer_id = NEW.target_peer_id
   AND a.origin_peer_id = e.origin_peer_id
   AND a.origin_epoch = e.origin_epoch
  WHERE e.event_id = NEW.event_id
    AND a.acknowledged_channel_seq >= e.channel_seq
)
BEGIN SELECT RAISE(ABORT, 'peer delivery scan requires contiguous cursor'); END;

CREATE TABLE peer_inbox (
  inbox_id           TEXT PRIMARY KEY,
  channel_id         TEXT NOT NULL,
  transport_peer_id  TEXT NOT NULL,
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  origin_seq         INTEGER NOT NULL,
  channel_seq        INTEGER NOT NULL CHECK (channel_seq > 0),
  event_id           TEXT NOT NULL,
  event_digest       BLOB NOT NULL,
  origin_member_revision INTEGER NOT NULL,
  origin_member_record_hash BLOB NOT NULL,
  publication_roster_revision INTEGER NOT NULL,
  publication_roster_hash BLOB NOT NULL,
  publication_digest BLOB NOT NULL,
  origin_signature   BLOB NOT NULL,
  publication_json   BLOB NOT NULL,
  arrival_source     TEXT NOT NULL CHECK (arrival_source IN ('gossip','pull')),
  is_audience        INTEGER NOT NULL CHECK (is_audience IN (0,1)),
  semantic_nonce     BLOB,
  required_artifact_roots_json BLOB NOT NULL,
  status             TEXT NOT NULL CHECK (
    status IN (
      'stored','waiting_artifact','ready','processing','accepted','rejected',
      'conflicted','retry','quarantined','ignored'
    )
  ),
  attempts           INTEGER NOT NULL DEFAULT 0 CHECK (
    attempts BETWEEN 0 AND 4294967295
  ),
  next_attempt_at    TEXT NOT NULL,
  lease_owner        TEXT,
  lease_until        TEXT,
  local_event_id     TEXT,
  decision_json      BLOB,
  receipt_event_id   TEXT REFERENCES events(event_id),
  diagnostic         TEXT,
  received_at        TEXT NOT NULL,
  updated_at         TEXT NOT NULL,
  UNIQUE (origin_peer_id, origin_epoch, origin_seq),
  UNIQUE (channel_id, origin_peer_id, origin_epoch, channel_seq),
  UNIQUE (event_id),
  UNIQUE (origin_peer_id, origin_epoch, event_id),
  FOREIGN KEY (local_event_id) REFERENCES events(event_id),
  FOREIGN KEY (channel_id, transport_peer_id)
    REFERENCES peer_bindings(channel_id, peer_id),
  FOREIGN KEY (channel_id, origin_peer_id)
    REFERENCES peer_bindings(channel_id, peer_id),
  FOREIGN KEY (channel_id, origin_member_revision, origin_member_record_hash)
    REFERENCES channel_members(channel_id, revision, record_hash),
  FOREIGN KEY (channel_id, publication_roster_revision, publication_roster_hash)
    REFERENCES channel_members(channel_id, revision, record_hash),
  CHECK (arrival_source <> 'pull' OR transport_peer_id = origin_peer_id),
  CHECK (
    semantic_nonce IS NULL
    OR (typeof(semantic_nonce) = 'blob' AND length(semantic_nonce) = 32)
  ),
  CHECK (is_audience = 1 OR semantic_nonce IS NULL),
  CHECK (is_audience = 0 OR status = 'quarantined' OR semantic_nonce IS NOT NULL),
  CHECK (
    (is_audience = 0 AND status IN ('ignored','quarantined')
      AND local_event_id IS NULL AND receipt_event_id IS NULL)
    OR (is_audience = 1 AND status <> 'ignored')
  ),
  CHECK (
    (status IN ('waiting_artifact','processing')
      AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
    OR (status NOT IN ('waiting_artifact','processing')
      AND lease_owner IS NULL AND lease_until IS NULL)
  )
);

-- Pending Inbox pressure is materialized at both scopes so admission never
-- scans permanent Inbox or historical Channel evidence.
CREATE TABLE peer_inbox_pressure (
  channel_id         TEXT PRIMARY KEY REFERENCES channels(channel_id),
  pending_bytes      INTEGER NOT NULL CHECK (
    typeof(pending_bytes) = 'integer'
    AND pending_bytes BETWEEN 0 AND 67108864
  )
);

CREATE TRIGGER peer_inbox_pressure_no_delete
BEFORE DELETE ON peer_inbox_pressure
BEGIN SELECT RAISE(ABORT, 'peer inbox channel pressure is permanent'); END;

CREATE TRIGGER peer_inbox_pressure_identity_immutable
BEFORE UPDATE OF channel_id ON peer_inbox_pressure
WHEN NEW.channel_id <> OLD.channel_id
BEGIN SELECT RAISE(ABORT, 'peer inbox channel pressure identity is immutable'); END;

CREATE TABLE peer_inbox_node_pressure (
  singleton_id       INTEGER PRIMARY KEY CHECK (singleton_id = 1),
  pending_bytes      INTEGER NOT NULL CHECK (
    typeof(pending_bytes) = 'integer'
    AND pending_bytes BETWEEN 0 AND 268435456
  )
);

INSERT INTO peer_inbox_node_pressure(singleton_id, pending_bytes) VALUES(1, 0);

CREATE TRIGGER peer_inbox_node_pressure_no_delete
BEFORE DELETE ON peer_inbox_node_pressure
BEGIN SELECT RAISE(ABORT, 'peer inbox node pressure is permanent'); END;

CREATE TRIGGER peer_inbox_node_pressure_identity_immutable
BEFORE UPDATE OF singleton_id ON peer_inbox_node_pressure
WHEN NEW.singleton_id <> OLD.singleton_id
BEGIN SELECT RAISE(ABORT, 'peer inbox node pressure identity is immutable'); END;

CREATE TRIGGER peer_inbox_pressure_insert_limit
BEFORE INSERT ON peer_inbox
WHEN NEW.status IN ('stored','waiting_artifact','ready','processing','retry')
 AND (
   COALESCE((
     SELECT pending_bytes FROM peer_inbox_pressure WHERE channel_id = NEW.channel_id
   ), 0) + length(NEW.publication_json) + length(NEW.required_artifact_roots_json) > 67108864
   OR COALESCE((SELECT pending_bytes FROM peer_inbox_node_pressure WHERE singleton_id = 1),
      268435457) + length(NEW.publication_json)
      + length(NEW.required_artifact_roots_json) > 268435456
 )
BEGIN SELECT RAISE(ABORT, 'peer inbox pending budget exceeded'); END;

CREATE TRIGGER peer_inbox_pressure_insert_account
AFTER INSERT ON peer_inbox
WHEN NEW.status IN ('stored','waiting_artifact','ready','processing','retry')
BEGIN
  INSERT INTO peer_inbox_pressure(channel_id, pending_bytes)
  VALUES (
    NEW.channel_id,
    length(NEW.publication_json) + length(NEW.required_artifact_roots_json)
  )
  ON CONFLICT(channel_id) DO UPDATE SET
    pending_bytes = pending_bytes + excluded.pending_bytes;
  UPDATE peer_inbox_node_pressure
  SET pending_bytes = pending_bytes
    + length(NEW.publication_json) + length(NEW.required_artifact_roots_json)
  WHERE singleton_id = 1;
END;

CREATE TRIGGER peer_inbox_pressure_status_leave_account
AFTER UPDATE OF status ON peer_inbox
WHEN OLD.status IN ('stored','waiting_artifact','ready','processing','retry')
 AND NEW.status NOT IN ('stored','waiting_artifact','ready','processing','retry')
BEGIN
  UPDATE peer_inbox_pressure
  SET pending_bytes = pending_bytes
    - length(OLD.publication_json) - length(OLD.required_artifact_roots_json)
  WHERE channel_id = OLD.channel_id;
  UPDATE peer_inbox_node_pressure
  SET pending_bytes = pending_bytes
    - length(OLD.publication_json) - length(OLD.required_artifact_roots_json)
  WHERE singleton_id = 1;
END;

CREATE TRIGGER peer_inbox_terminal_status_immutable
BEFORE UPDATE OF status ON peer_inbox
WHEN OLD.status IN ('accepted','rejected','conflicted','quarantined','ignored')
 AND NEW.status <> OLD.status
BEGIN SELECT RAISE(ABORT, 'terminal peer inbox status is immutable'); END;

CREATE TRIGGER peer_inbox_no_delete BEFORE DELETE ON peer_inbox
BEGIN SELECT RAISE(ABORT, 'peer inbox evidence is permanent'); END;

CREATE TRIGGER peer_inbox_event_scope_insert
BEFORE INSERT ON peer_inbox
WHEN NEW.local_event_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM events WHERE event_id = NEW.local_event_id AND channel_id = NEW.channel_id
)
BEGIN SELECT RAISE(ABORT, 'inbox event channel mismatch'); END;

CREATE TRIGGER peer_inbox_binding_epoch_insert
BEFORE INSERT ON peer_inbox
WHEN NOT EXISTS (
  SELECT 1 FROM peer_bindings
  WHERE channel_id = NEW.channel_id
    AND peer_id = NEW.origin_peer_id
    AND origin_epoch = NEW.origin_epoch
    AND state = 'active'
)
BEGIN SELECT RAISE(ABORT, 'inbox origin epoch mismatch'); END;

CREATE TRIGGER peer_inbox_transport_binding_insert
BEFORE INSERT ON peer_inbox
WHEN NOT EXISTS (
  SELECT 1 FROM peer_bindings
  WHERE channel_id = NEW.channel_id
    AND peer_id = NEW.transport_peer_id
    AND state = 'active'
)
BEGIN SELECT RAISE(ABORT, 'inbox transport peer is not active in channel'); END;

CREATE TRIGGER peer_inbox_publication_member_insert
BEFORE INSERT ON peer_inbox
WHEN NOT EXISTS (
  SELECT 1 FROM channel_members
  WHERE channel_id = NEW.channel_id
    AND revision = NEW.origin_member_revision
    AND record_hash = NEW.origin_member_record_hash
    AND member_peer_id = NEW.origin_peer_id
    AND origin_epoch = NEW.origin_epoch
    AND status = 'active'
) OR COALESCE((
  SELECT status FROM channel_members
  WHERE channel_id = NEW.channel_id
    AND member_peer_id = NEW.origin_peer_id
  ORDER BY revision DESC LIMIT 1
), '') <> 'active'
BEGIN SELECT RAISE(ABORT, 'inbox publication origin membership mismatch'); END;

CREATE TRIGGER peer_inbox_identity_immutable
BEFORE UPDATE OF channel_id, transport_peer_id, origin_peer_id, origin_epoch,
  origin_seq, channel_seq, event_id, event_digest, origin_member_revision,
  origin_member_record_hash, publication_roster_revision, publication_roster_hash,
  publication_digest, origin_signature, publication_json, arrival_source, is_audience,
  required_artifact_roots_json
ON peer_inbox
WHEN NEW.channel_id <> OLD.channel_id
  OR NEW.transport_peer_id <> OLD.transport_peer_id
  OR NEW.origin_peer_id <> OLD.origin_peer_id
  OR NEW.origin_epoch <> OLD.origin_epoch
  OR NEW.origin_seq <> OLD.origin_seq
  OR NEW.channel_seq <> OLD.channel_seq
  OR NEW.event_id <> OLD.event_id
  OR NEW.event_digest <> OLD.event_digest
  OR NEW.origin_member_revision <> OLD.origin_member_revision
  OR NEW.origin_member_record_hash <> OLD.origin_member_record_hash
  OR NEW.publication_roster_revision <> OLD.publication_roster_revision
  OR NEW.publication_roster_hash <> OLD.publication_roster_hash
  OR NEW.publication_digest <> OLD.publication_digest
  OR NEW.origin_signature <> OLD.origin_signature
  OR NEW.publication_json <> OLD.publication_json
  OR NEW.arrival_source <> OLD.arrival_source
  OR NEW.is_audience <> OLD.is_audience
  OR NEW.required_artifact_roots_json <> OLD.required_artifact_roots_json
BEGIN SELECT RAISE(ABORT, 'peer inbox publication identity is immutable'); END;

CREATE TRIGGER peer_inbox_semantic_nonce_immutable
BEFORE UPDATE OF semantic_nonce ON peer_inbox
BEGIN SELECT RAISE(ABORT, 'peer inbox semantic nonce is immutable'); END;

CREATE TRIGGER peer_inbox_event_scope_update
BEFORE UPDATE OF local_event_id ON peer_inbox
WHEN NEW.local_event_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM events WHERE event_id = NEW.local_event_id AND channel_id = NEW.channel_id
)
BEGIN SELECT RAISE(ABORT, 'inbox event channel mismatch'); END;

CREATE TRIGGER peer_inbox_receipt_scope_insert
BEFORE INSERT ON peer_inbox
WHEN NEW.receipt_event_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM events WHERE event_id = NEW.receipt_event_id AND channel_id = NEW.channel_id
)
BEGIN SELECT RAISE(ABORT, 'inbox receipt channel mismatch'); END;

CREATE TRIGGER peer_inbox_receipt_scope_update
BEFORE UPDATE OF receipt_event_id ON peer_inbox
WHEN NEW.receipt_event_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM events WHERE event_id = NEW.receipt_event_id AND channel_id = NEW.channel_id
)
BEGIN SELECT RAISE(ABORT, 'inbox receipt channel mismatch'); END;

CREATE INDEX peer_inbox_work_idx
  ON peer_inbox(status, next_attempt_at, received_at);

CREATE TABLE publication_conflicts (
  conflict_id        TEXT PRIMARY KEY,
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  claimed_origin_seq INTEGER NOT NULL CHECK (claimed_origin_seq > 0),
  claimed_channel_seq INTEGER NOT NULL CHECK (claimed_channel_seq > 0),
  claimed_event_id   TEXT NOT NULL,
  claimed_event_digest BLOB NOT NULL,
  claimed_publication_digest BLOB NOT NULL,
  origin_signature  BLOB NOT NULL,
  conflicting_publication_json BLOB NOT NULL,
  transport_peer_id TEXT NOT NULL,
  arrival_source    TEXT NOT NULL CHECK (arrival_source IN ('gossip','pull')),
  existing_inbox_id TEXT REFERENCES peer_inbox(inbox_id),
  reason            TEXT NOT NULL CHECK (
    reason IN (
      'origin_key_conflict','event_key_conflict','publication_key_conflict','digest_conflict'
    )
  ),
  detected_at       TEXT NOT NULL,
  UNIQUE (conflict_id, channel_id, origin_peer_id, origin_epoch),
  UNIQUE (channel_id, origin_peer_id, origin_epoch, claimed_publication_digest),
  FOREIGN KEY (channel_id, origin_peer_id)
    REFERENCES peer_bindings(channel_id, peer_id),
  FOREIGN KEY (channel_id, transport_peer_id)
    REFERENCES peer_bindings(channel_id, peer_id),
  CHECK (arrival_source <> 'pull' OR transport_peer_id = origin_peer_id)
);

CREATE TRIGGER publication_conflicts_no_update BEFORE UPDATE ON publication_conflicts
BEGIN SELECT RAISE(ABORT, 'publication conflict evidence is immutable'); END;
CREATE TRIGGER publication_conflicts_no_delete BEFORE DELETE ON publication_conflicts
BEGIN SELECT RAISE(ABORT, 'publication conflict evidence is permanent'); END;

CREATE TRIGGER publication_conflicts_existing_scope_insert
BEFORE INSERT ON publication_conflicts
WHEN NEW.existing_inbox_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM peer_inbox
  WHERE inbox_id = NEW.existing_inbox_id
    AND channel_id = NEW.channel_id
    AND origin_peer_id = NEW.origin_peer_id
    AND origin_epoch = NEW.origin_epoch
)
BEGIN SELECT RAISE(ABORT, 'publication conflict incumbent scope mismatch'); END;

CREATE TABLE origin_quarantines (
  channel_id         TEXT NOT NULL,
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  first_conflict_id  TEXT NOT NULL,
  reason             TEXT NOT NULL,
  detected_at        TEXT NOT NULL,
  PRIMARY KEY (channel_id, origin_peer_id, origin_epoch),
  FOREIGN KEY (channel_id, origin_peer_id)
    REFERENCES peer_bindings(channel_id, peer_id),
  FOREIGN KEY (first_conflict_id, channel_id, origin_peer_id, origin_epoch)
    REFERENCES publication_conflicts(conflict_id, channel_id, origin_peer_id, origin_epoch)
);

CREATE TRIGGER origin_quarantines_no_update BEFORE UPDATE ON origin_quarantines
BEGIN SELECT RAISE(ABORT, 'origin quarantine is permanent in this channel epoch'); END;
CREATE TRIGGER origin_quarantines_no_delete BEFORE DELETE ON origin_quarantines
BEGIN SELECT RAISE(ABORT, 'origin quarantine is permanent in this channel epoch'); END;

CREATE TABLE peer_cursors (
  channel_id         TEXT NOT NULL,
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  baseline_channel_seq INTEGER NOT NULL CHECK (baseline_channel_seq >= 0),
  contiguous_channel_seq INTEGER NOT NULL CHECK (contiguous_channel_seq >= 0),
  observed_channel_seq INTEGER NOT NULL CHECK (observed_channel_seq >= 0),
  updated_at         TEXT NOT NULL,
  PRIMARY KEY (channel_id, origin_peer_id, origin_epoch),
  FOREIGN KEY (channel_id, origin_peer_id)
    REFERENCES peer_bindings(channel_id, peer_id),
  CHECK (contiguous_channel_seq >= baseline_channel_seq),
  CHECK (observed_channel_seq >= contiguous_channel_seq)
);

CREATE TRIGGER peer_cursors_binding_epoch_insert BEFORE INSERT ON peer_cursors
WHEN NOT EXISTS (
  SELECT 1 FROM peer_bindings
  WHERE channel_id = NEW.channel_id
    AND peer_id = NEW.origin_peer_id
    AND origin_epoch = NEW.origin_epoch
)
BEGIN SELECT RAISE(ABORT, 'cursor binding epoch mismatch'); END;

CREATE TRIGGER peer_cursors_initial_baseline_insert BEFORE INSERT ON peer_cursors
WHEN NEW.contiguous_channel_seq <> NEW.baseline_channel_seq
  OR NEW.observed_channel_seq <> NEW.baseline_channel_seq
BEGIN SELECT RAISE(ABORT, 'cursor must start at its exact binding baseline'); END;

CREATE TRIGGER peer_cursors_binding_epoch_update
BEFORE UPDATE OF channel_id, origin_peer_id, origin_epoch ON peer_cursors
WHEN NOT EXISTS (
  SELECT 1 FROM peer_bindings
  WHERE channel_id = NEW.channel_id
    AND peer_id = NEW.origin_peer_id
    AND origin_epoch = NEW.origin_epoch
)
BEGIN SELECT RAISE(ABORT, 'cursor binding epoch mismatch'); END;

CREATE TRIGGER peer_cursors_identity_baseline_immutable
BEFORE UPDATE OF channel_id, origin_peer_id, origin_epoch, baseline_channel_seq ON peer_cursors
WHEN NEW.channel_id <> OLD.channel_id
  OR NEW.origin_peer_id <> OLD.origin_peer_id
  OR NEW.origin_epoch <> OLD.origin_epoch
  OR NEW.baseline_channel_seq <> OLD.baseline_channel_seq
BEGIN SELECT RAISE(ABORT, 'cursor identity and binding baseline are immutable'); END;

CREATE TRIGGER peer_cursors_monotonic_update
BEFORE UPDATE OF contiguous_channel_seq, observed_channel_seq ON peer_cursors
WHEN NEW.contiguous_channel_seq < OLD.contiguous_channel_seq
  OR NEW.observed_channel_seq < OLD.observed_channel_seq
BEGIN SELECT RAISE(ABORT, 'cursor cannot move backward'); END;

CREATE TRIGGER peer_cursors_no_delete BEFORE DELETE ON peer_cursors
BEGIN SELECT RAISE(ABORT, 'peer cursor baseline evidence is durable'); END;

-- peer_repairs is the restart-safe anti-entropy checkpoint for one inbound
-- origin log. It is deliberately not a task queue: the current Channel,
-- binding and cursor remain the authority from which due work is derived.
CREATE TABLE peer_repairs (
  channel_id         TEXT NOT NULL,
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  baseline_channel_seq INTEGER NOT NULL CHECK (baseline_channel_seq >= 0),
  status             TEXT NOT NULL CHECK (
    status IN ('ready','progress','caught_up','retry','paused','terminal')
  ),
  generation         INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
  retry_count        INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
  source_floor_channel_seq INTEGER CHECK (source_floor_channel_seq > 0),
  source_head_channel_seq INTEGER CHECK (source_head_channel_seq >= 0),
  diagnostic_code    TEXT CHECK (diagnostic_code IN (
    'busy','transport_unavailable','protocol_invalid','history_gap',
    'not_origin','not_member','member_revoked','channel_closed',
    'origin_epoch_mismatch'
  )),
  paused_authority_digest BLOB CHECK (
    paused_authority_digest IS NULL OR length(paused_authority_digest) = 32
  ),
  next_attempt_at    TEXT,
  updated_at         TEXT NOT NULL,
  PRIMARY KEY (channel_id, origin_peer_id, origin_epoch),
  FOREIGN KEY (channel_id, origin_peer_id, origin_epoch)
    REFERENCES peer_cursors(channel_id, origin_peer_id, origin_epoch),
  CHECK (retry_count <= generation),
  CHECK (
    (status = 'ready'
      AND generation = 0
      AND retry_count = 0
      AND source_floor_channel_seq IS NULL
      AND source_head_channel_seq = baseline_channel_seq
      AND diagnostic_code IS NULL
      AND paused_authority_digest IS NULL
      AND next_attempt_at IS NOT NULL)
    OR (status IN ('progress','caught_up')
      AND source_floor_channel_seq IS NOT NULL
      AND source_head_channel_seq IS NOT NULL
      AND diagnostic_code IS NULL
      AND paused_authority_digest IS NULL
      AND next_attempt_at IS NOT NULL)
    OR (status = 'retry'
      AND diagnostic_code IN ('busy','transport_unavailable')
      AND paused_authority_digest IS NULL
      AND next_attempt_at IS NOT NULL)
    OR (status = 'paused'
      AND diagnostic_code IN (
        'not_origin','not_member','member_revoked','channel_closed'
      )
      AND paused_authority_digest IS NOT NULL
      AND next_attempt_at IS NOT NULL)
    OR (status = 'terminal'
      AND diagnostic_code IN (
        'protocol_invalid','history_gap','origin_epoch_mismatch'
      )
      AND paused_authority_digest IS NULL
      AND next_attempt_at IS NULL)
  ),
  CHECK (
    diagnostic_code <> 'history_gap'
    OR source_floor_channel_seq IS NOT NULL
  )
);

CREATE TRIGGER peer_repairs_from_cursor_insert
AFTER INSERT ON peer_cursors
BEGIN
  INSERT INTO peer_repairs(
    channel_id,origin_peer_id,origin_epoch,baseline_channel_seq,status,
    generation,retry_count,source_head_channel_seq,next_attempt_at,updated_at
  ) VALUES(
    NEW.channel_id,NEW.origin_peer_id,NEW.origin_epoch,
    NEW.baseline_channel_seq,'ready',0,0,NEW.baseline_channel_seq,
    NEW.updated_at,NEW.updated_at
  );
END;

CREATE TRIGGER peer_repairs_initial_projection_insert
BEFORE INSERT ON peer_repairs
WHEN NEW.status <> 'ready'
  OR NEW.generation <> 0
  OR NEW.retry_count <> 0
  OR NEW.source_floor_channel_seq IS NOT NULL
  OR NEW.source_head_channel_seq <> NEW.baseline_channel_seq
  OR NEW.diagnostic_code IS NOT NULL
  OR NEW.paused_authority_digest IS NOT NULL
  OR NEW.next_attempt_at <> NEW.updated_at
  OR NOT EXISTS (
    SELECT 1 FROM peer_cursors cursor
    WHERE cursor.channel_id = NEW.channel_id
      AND cursor.origin_peer_id = NEW.origin_peer_id
      AND cursor.origin_epoch = NEW.origin_epoch
      AND cursor.baseline_channel_seq = NEW.baseline_channel_seq
      AND cursor.updated_at = NEW.updated_at
  )
BEGIN SELECT RAISE(ABORT, 'repair must initialize from exact inbound cursor'); END;

CREATE TRIGGER peer_repairs_identity_immutable
BEFORE UPDATE OF channel_id,origin_peer_id,origin_epoch,baseline_channel_seq ON peer_repairs
WHEN NEW.channel_id <> OLD.channel_id
  OR NEW.origin_peer_id <> OLD.origin_peer_id
  OR NEW.origin_epoch <> OLD.origin_epoch
  OR NEW.baseline_channel_seq <> OLD.baseline_channel_seq
BEGIN SELECT RAISE(ABORT, 'repair identity and baseline are immutable'); END;

CREATE TRIGGER peer_repairs_generation_monotonic
BEFORE UPDATE ON peer_repairs
WHEN NEW.generation <> OLD.generation + 1
  OR NEW.retry_count < OLD.retry_count
  OR NEW.retry_count > OLD.retry_count + 1
BEGIN SELECT RAISE(ABORT, 'repair generation or retry evidence is invalid'); END;

CREATE TRIGGER peer_repairs_source_monotonic
BEFORE UPDATE OF source_floor_channel_seq,source_head_channel_seq ON peer_repairs
WHEN (OLD.source_floor_channel_seq IS NOT NULL AND (
    NEW.source_floor_channel_seq IS NULL
    OR NEW.source_floor_channel_seq < OLD.source_floor_channel_seq
  ))
  OR (OLD.source_head_channel_seq IS NOT NULL AND (
    NEW.source_head_channel_seq IS NULL
    OR NEW.source_head_channel_seq < OLD.source_head_channel_seq
  ))
BEGIN SELECT RAISE(ABORT, 'repair source floor or head cannot regress'); END;

CREATE TRIGGER peer_repairs_time_monotonic
BEFORE UPDATE OF updated_at,next_attempt_at ON peer_repairs
WHEN NEW.updated_at < OLD.updated_at
  OR (NEW.next_attempt_at IS NOT NULL AND NEW.next_attempt_at < NEW.updated_at)
BEGIN SELECT RAISE(ABORT, 'repair time cannot regress'); END;

CREATE TRIGGER peer_repairs_terminal_immutable BEFORE UPDATE ON peer_repairs
WHEN OLD.status = 'terminal'
BEGIN SELECT RAISE(ABORT, 'terminal repair evidence is immutable'); END;

CREATE TRIGGER peer_repairs_no_delete BEFORE DELETE ON peer_repairs
BEGIN SELECT RAISE(ABORT, 'repair evidence is durable'); END;

CREATE TRIGGER peer_bindings_no_active_insert BEFORE INSERT ON peer_bindings
WHEN NEW.state = 'active'
BEGIN SELECT RAISE(ABORT, 'binding must activate through baseline transaction'); END;

CREATE TRIGGER peer_bindings_activate_requires_cursor
BEFORE UPDATE OF state ON peer_bindings
WHEN NEW.state = 'active' AND OLD.state <> 'active' AND (
  NOT EXISTS (
    SELECT 1 FROM peer_cursors
    WHERE channel_id = NEW.channel_id
      AND origin_peer_id = NEW.peer_id
      AND origin_epoch = NEW.origin_epoch
  )
  OR COALESCE((
    SELECT status FROM channel_members
    WHERE channel_id = NEW.channel_id AND member_peer_id = NEW.peer_id
    ORDER BY revision DESC LIMIT 1
  ), '') <> 'active'
)
BEGIN SELECT RAISE(ABORT, 'binding activation requires inbound baseline cursor'); END;

CREATE TABLE publication_epochs (
  channel_id         TEXT NOT NULL REFERENCES channels(channel_id),
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  source_floor_channel_seq INTEGER NOT NULL DEFAULT 1 CHECK (source_floor_channel_seq > 0),
  source_head_channel_seq INTEGER NOT NULL DEFAULT 0 CHECK (source_head_channel_seq >= 0),
  updated_at         TEXT NOT NULL,
  PRIMARY KEY (channel_id, origin_peer_id, origin_epoch),
  CHECK (source_floor_channel_seq <= source_head_channel_seq + 1)
);

CREATE TRIGGER publication_epochs_local_origin_insert
BEFORE INSERT ON publication_epochs
WHEN NOT EXISTS (
  SELECT 1 FROM node
  WHERE peer_id = NEW.origin_peer_id AND origin_epoch = NEW.origin_epoch
)
BEGIN SELECT RAISE(ABORT, 'publication epoch origin must equal node identity'); END;

CREATE TRIGGER publication_epochs_identity_immutable
BEFORE UPDATE OF channel_id, origin_peer_id, origin_epoch ON publication_epochs
WHEN NEW.channel_id <> OLD.channel_id
  OR NEW.origin_peer_id <> OLD.origin_peer_id
  OR NEW.origin_epoch <> OLD.origin_epoch
BEGIN SELECT RAISE(ABORT, 'publication epoch identity is immutable'); END;

CREATE TRIGGER publication_epochs_monotonic_update
BEFORE UPDATE OF source_floor_channel_seq, source_head_channel_seq ON publication_epochs
WHEN NEW.source_floor_channel_seq < OLD.source_floor_channel_seq
  OR NEW.source_head_channel_seq < OLD.source_head_channel_seq
  OR NEW.source_head_channel_seq > OLD.source_head_channel_seq + 1
BEGIN SELECT RAISE(ABORT, 'publication floor/head must advance monotonically and head cannot skip'); END;

CREATE TRIGGER publication_epochs_no_delete BEFORE DELETE ON publication_epochs
BEGIN SELECT RAISE(ABORT, 'publication epoch history is durable'); END;

CREATE TABLE peer_pull_acks (
  channel_id         TEXT NOT NULL,
  target_peer_id     TEXT NOT NULL,
  origin_peer_id     TEXT NOT NULL,
  origin_epoch       TEXT NOT NULL,
  baseline_channel_seq INTEGER NOT NULL CHECK (baseline_channel_seq >= 0),
  acknowledged_channel_seq INTEGER NOT NULL CHECK (acknowledged_channel_seq >= 0),
  baseline_confirmed_at TEXT,
  updated_at         TEXT NOT NULL,
  PRIMARY KEY (channel_id, target_peer_id, origin_peer_id, origin_epoch),
  FOREIGN KEY (channel_id, target_peer_id)
    REFERENCES peer_bindings(channel_id, peer_id),
  FOREIGN KEY (channel_id, origin_peer_id, origin_epoch)
    REFERENCES publication_epochs(channel_id, origin_peer_id, origin_epoch),
  CHECK (acknowledged_channel_seq >= baseline_channel_seq)
);

CREATE TRIGGER peer_pull_acks_initial_baseline_insert BEFORE INSERT ON peer_pull_acks
WHEN NEW.acknowledged_channel_seq <> NEW.baseline_channel_seq
  OR NEW.baseline_confirmed_at IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'pull ack must start at an unconfirmed exact binding baseline'); END;

CREATE TRIGGER peer_pull_acks_identity_baseline_immutable
BEFORE UPDATE OF channel_id, target_peer_id, origin_peer_id, origin_epoch,
  baseline_channel_seq ON peer_pull_acks
WHEN NEW.channel_id <> OLD.channel_id
  OR NEW.target_peer_id <> OLD.target_peer_id
  OR NEW.origin_peer_id <> OLD.origin_peer_id
  OR NEW.origin_epoch <> OLD.origin_epoch
  OR NEW.baseline_channel_seq <> OLD.baseline_channel_seq
BEGIN SELECT RAISE(ABORT, 'pull ack identity and binding baseline are immutable'); END;

CREATE TRIGGER peer_pull_acks_monotonic_update
BEFORE UPDATE OF acknowledged_channel_seq ON peer_pull_acks
WHEN NEW.acknowledged_channel_seq < OLD.acknowledged_channel_seq
BEGIN SELECT RAISE(ABORT, 'pull ack cannot move backward'); END;

CREATE TRIGGER peer_pull_acks_confirmation_immutable
BEFORE UPDATE OF baseline_confirmed_at ON peer_pull_acks
WHEN OLD.baseline_confirmed_at IS NOT NULL
  AND NEW.baseline_confirmed_at IS NOT OLD.baseline_confirmed_at
BEGIN SELECT RAISE(ABORT, 'baseline confirmation is immutable'); END;

CREATE TRIGGER peer_pull_acks_no_delete BEFORE DELETE ON peer_pull_acks
BEGIN SELECT RAISE(ABORT, 'pull ack baseline evidence is durable'); END;

CREATE TRIGGER peer_deliveries_binding_ready_insert BEFORE INSERT ON peer_deliveries
WHEN NOT EXISTS (
  SELECT 1
  FROM events e
  JOIN peer_bindings b
    ON b.channel_id = NEW.channel_id AND b.peer_id = NEW.target_peer_id
  JOIN peer_pull_acks a
    ON a.channel_id = NEW.channel_id
   AND a.target_peer_id = NEW.target_peer_id
   AND a.origin_peer_id = e.origin_peer_id
   AND a.origin_epoch = e.origin_epoch
  WHERE e.event_id = NEW.event_id
    AND b.state = 'active'
    AND a.baseline_confirmed_at IS NOT NULL
)
BEGIN SELECT RAISE(ABORT, 'peer delivery binding baseline not ready'); END;

CREATE TRIGGER peer_deliveries_binding_ready_update
BEFORE UPDATE OF channel_id, target_peer_id, event_id ON peer_deliveries
WHEN NOT EXISTS (
  SELECT 1
  FROM events e
  JOIN peer_bindings b
    ON b.channel_id = NEW.channel_id AND b.peer_id = NEW.target_peer_id
  JOIN peer_pull_acks a
    ON a.channel_id = NEW.channel_id
   AND a.target_peer_id = NEW.target_peer_id
   AND a.origin_peer_id = e.origin_peer_id
   AND a.origin_epoch = e.origin_epoch
  WHERE e.event_id = NEW.event_id
    AND b.state = 'active'
    AND a.baseline_confirmed_at IS NOT NULL
)
BEGIN SELECT RAISE(ABORT, 'peer delivery binding baseline not ready'); END;

PRAGMA user_version = 1;
