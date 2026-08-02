CREATE TABLE authority_clock (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    origin_sequence INTEGER NOT NULL CHECK (origin_sequence >= 0)
) STRICT;

INSERT INTO authority_clock(singleton, origin_sequence) VALUES(1, 0);

CREATE TABLE principals (
    principal_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE attachments (
    attachment_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(principal_id),
    mode TEXT NOT NULL CHECK (mode IN ('interactive', 'managed')),
    credential_digest TEXT NOT NULL
        CHECK (length(credential_digest) = 71 AND
               substr(credential_digest, 1, 7) = 'sha256:' AND
               substr(credential_digest, 8) NOT GLOB '*[^0-9a-f]*' AND
               credential_digest !=
                   'sha256:0000000000000000000000000000000000000000000000000000000000000000'),
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    CHECK (expires_at > issued_at)
) STRICT;

CREATE TABLE verified_artifacts (
    digest TEXT PRIMARY KEY,
    byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
    verified_at TEXT NOT NULL
) STRICT;

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    event_digest TEXT NOT NULL UNIQUE,
    origin_sequence INTEGER NOT NULL UNIQUE CHECK (origin_sequence > 0),
    source_principal_id TEXT NOT NULL REFERENCES principals(principal_id),
    request_digest TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    canonical_json BLOB NOT NULL
) STRICT;

CREATE TABLE event_artifacts (
    event_id TEXT NOT NULL REFERENCES events(event_id),
    artifact_digest TEXT NOT NULL REFERENCES verified_artifacts(digest),
    PRIMARY KEY(event_id, artifact_digest)
) STRICT, WITHOUT ROWID;

CREATE TABLE operations (
    actor_principal_id TEXT NOT NULL REFERENCES principals(principal_id),
    operation_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('accepted', 'rejected')),
    event_id TEXT REFERENCES events(event_id),
    receipt_digest TEXT NOT NULL,
    receipt_json BLOB NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY(actor_principal_id, operation_key),
    CHECK ((outcome = 'accepted' AND event_id IS NOT NULL) OR
           (outcome = 'rejected' AND event_id IS NULL))
) STRICT, WITHOUT ROWID;

CREATE TABLE current_operations (
    attachment_id TEXT NOT NULL REFERENCES attachments(attachment_id),
    operation_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    view_handle TEXT NOT NULL,
    authority_digest TEXT NOT NULL,
    authority_json BLOB NOT NULL,
    agent_view_digest TEXT NOT NULL,
    agent_view_json BLOB NOT NULL,
    PRIMARY KEY(attachment_id, operation_key),
    UNIQUE(attachment_id, view_handle)
) STRICT, WITHOUT ROWID;

CREATE TABLE handlings (
    handling_id TEXT PRIMARY KEY,
    target_principal_id TEXT NOT NULL REFERENCES principals(principal_id),
    head_event_id TEXT NOT NULL REFERENCES events(event_id),
    state TEXT NOT NULL CHECK (state IN ('open', 'terminal')),
    outcome TEXT CHECK (outcome IN ('completed', 'declined', 'unresolved')),
    claim_attachment_id TEXT REFERENCES attachments(attachment_id),
    claim_fence INTEGER NOT NULL DEFAULT 0 CHECK (claim_fence >= 0),
    claim_until TEXT,
    created_sequence INTEGER NOT NULL CHECK (created_sequence > 0),
    CHECK ((state = 'open' AND outcome IS NULL) OR
           (state = 'terminal' AND outcome IS NOT NULL)),
    CHECK ((claim_attachment_id IS NULL AND claim_until IS NULL) OR
           (state = 'open' AND claim_attachment_id IS NOT NULL AND
            claim_until IS NOT NULL AND claim_fence > 0))
) STRICT;

CREATE INDEX handlings_claimable
ON handlings(target_principal_id, state, created_sequence);

CREATE UNIQUE INDEX handlings_attachment_slot
ON handlings(claim_attachment_id)
WHERE claim_attachment_id IS NOT NULL;

CREATE TABLE active_references (
    reference_key TEXT PRIMARY KEY,
    head_event_id TEXT NOT NULL REFERENCES events(event_id),
    state TEXT NOT NULL CHECK (state IN ('active', 'retracted')),
    artifact_digest TEXT REFERENCES verified_artifacts(digest),
    CHECK ((state = 'active' AND artifact_digest IS NOT NULL) OR
           (state = 'retracted' AND artifact_digest IS NULL))
) STRICT;

CREATE TABLE reference_lineage (
    event_id TEXT PRIMARY KEY REFERENCES events(event_id),
    reference_key TEXT NOT NULL,
    previous_event_id TEXT REFERENCES events(event_id),
    state TEXT NOT NULL CHECK (state IN ('active', 'retracted')),
    artifact_digest TEXT REFERENCES verified_artifacts(digest),
    CHECK ((state = 'active' AND artifact_digest IS NOT NULL) OR
           (state = 'retracted' AND artifact_digest IS NULL))
) STRICT;

CREATE INDEX reference_lineage_key
ON reference_lineage(reference_key, event_id);

PRAGMA application_id = 1296978487;
PRAGMA user_version = 2;
