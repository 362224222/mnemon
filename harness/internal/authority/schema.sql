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
    mode TEXT NOT NULL CHECK (mode = 'interactive'),
    credential_digest TEXT NOT NULL
        CHECK (length(credential_digest) = 71 AND
               substr(credential_digest, 1, 7) = 'sha256:' AND
               substr(credential_digest, 8) NOT GLOB '*[^0-9a-f]*' AND
               credential_digest !=
                   'sha256:0000000000000000000000000000000000000000000000000000000000000000'),
    begin_operation_key TEXT NOT NULL UNIQUE,
    begin_request_digest TEXT NOT NULL
        CHECK (length(begin_request_digest) = 71 AND
               substr(begin_request_digest, 1, 7) = 'sha256:' AND
               substr(begin_request_digest, 8) NOT GLOB '*[^0-9a-f]*' AND
               begin_request_digest !=
                   'sha256:0000000000000000000000000000000000000000000000000000000000000000'),
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
	ended_at TEXT,
	CHECK (expires_at > issued_at),
	CHECK (ended_at IS NULL OR ended_at >= issued_at)
) STRICT;

CREATE UNIQUE INDEX attachments_one_live_per_principal
ON attachments(principal_id)
WHERE ended_at IS NULL;

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
    causal_depth INTEGER NOT NULL CHECK (causal_depth >= 0 AND causal_depth <= 32),
    accepted_at TEXT NOT NULL,
    canonical_json BLOB NOT NULL
) STRICT;

CREATE TABLE peer_routes (
    route_id TEXT PRIMARY KEY,
    public_alias TEXT NOT NULL UNIQUE,
    remote_peer_id TEXT NOT NULL UNIQUE,
    remote_public_key BLOB NOT NULL CHECK (length(remote_public_key) = 32),
    transport_address TEXT NOT NULL CHECK (length(transport_address) BETWEEN 1 AND 512),
    remote_target_alias TEXT NOT NULL,
    inbound_target_alias TEXT NOT NULL,
    local_target_principal_id TEXT NOT NULL REFERENCES principals(principal_id),
    surrogate_source_principal_id TEXT NOT NULL UNIQUE REFERENCES principals(principal_id),
    state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
    enrolled_at TEXT NOT NULL,
    revoked_at TEXT,
    CHECK ((state = 'active' AND revoked_at IS NULL) OR
           (state = 'revoked' AND revoked_at IS NOT NULL))
) STRICT;

CREATE TABLE peer_outbox (
    delivery_id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL REFERENCES peer_routes(route_id),
    origin_event_id TEXT NOT NULL REFERENCES events(event_id),
    reply_anchor_handling_id TEXT REFERENCES handlings(handling_id),
    expected_reply_root_event_id TEXT,
    expected_reply_root_event_digest TEXT,
    envelope_digest TEXT NOT NULL UNIQUE,
    delivery_json BLOB NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'settled', 'expired')),
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    receipt_digest TEXT,
    receipt_json BLOB,
    receipt_signature BLOB,
    settled_at TEXT,
    CHECK ((reply_anchor_handling_id IS NULL AND expected_reply_root_event_id IS NULL AND
            expected_reply_root_event_digest IS NULL) OR
           (reply_anchor_handling_id IS NOT NULL AND expected_reply_root_event_id IS NOT NULL AND
            expected_reply_root_event_digest IS NOT NULL AND
            length(expected_reply_root_event_digest) = 71 AND
            substr(expected_reply_root_event_digest, 1, 7) = 'sha256:' AND
            substr(expected_reply_root_event_digest, 8) NOT GLOB '*[^0-9a-f]*' AND
            expected_reply_root_event_digest !=
                'sha256:0000000000000000000000000000000000000000000000000000000000000000')),
    CHECK ((state = 'pending' AND receipt_digest IS NULL AND receipt_json IS NULL AND
            receipt_signature IS NULL AND settled_at IS NULL) OR
           (state = 'settled' AND receipt_digest IS NOT NULL AND receipt_json IS NOT NULL AND
            length(receipt_signature) = 64 AND settled_at IS NOT NULL) OR
           (state = 'expired' AND receipt_digest IS NULL AND receipt_json IS NULL AND
            receipt_signature IS NULL AND settled_at IS NOT NULL))
) STRICT;

CREATE INDEX peer_outbox_pending
ON peer_outbox(state, expires_at, created_at);

CREATE UNIQUE INDEX peer_outbox_origin_route
ON peer_outbox(origin_event_id, route_id);

CREATE INDEX peer_outbox_reply_match
ON peer_outbox(route_id, expected_reply_root_event_id,
               expected_reply_root_event_digest, reply_anchor_handling_id)
WHERE reply_anchor_handling_id IS NOT NULL;

CREATE TABLE peer_inbox (
    delivery_id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL REFERENCES peer_routes(route_id),
    in_reply_to_delivery_id TEXT,
    envelope_digest TEXT NOT NULL,
    delivery_json BLOB NOT NULL,
    delivery_signature BLOB NOT NULL CHECK (length(delivery_signature) = 64),
    state TEXT NOT NULL CHECK (state IN ('staged', 'settled', 'expired')),
    expires_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    local_event_id TEXT REFERENCES events(event_id),
    receipt_digest TEXT,
    receipt_json BLOB,
    settled_at TEXT,
    CHECK ((state = 'staged' AND local_event_id IS NULL AND receipt_digest IS NULL AND
            receipt_json IS NULL AND settled_at IS NULL) OR
           (state = 'settled' AND receipt_digest IS NOT NULL AND receipt_json IS NOT NULL AND
            settled_at IS NOT NULL) OR
           (state = 'expired' AND local_event_id IS NULL AND receipt_digest IS NULL AND
            receipt_json IS NULL AND settled_at IS NOT NULL))
) STRICT;

CREATE INDEX peer_inbox_staged
ON peer_inbox(state, expires_at, received_at);

CREATE UNIQUE INDEX peer_inbox_local_event
ON peer_inbox(local_event_id)
WHERE local_event_id IS NOT NULL;

-- Several signed candidates may cite one outbound request, but only one may
-- become its locally accepted observation. Rejected and merely staged rows do
-- not consume this authority slot because they have no local Event.
CREATE UNIQUE INDEX peer_inbox_accepted_reply
ON peer_inbox(in_reply_to_delivery_id)
WHERE local_event_id IS NOT NULL;

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
ON handlings(target_principal_id, state, claim_fence, created_sequence, handling_id);

CREATE UNIQUE INDEX handlings_attachment_slot
ON handlings(claim_attachment_id)
WHERE claim_attachment_id IS NOT NULL;

CREATE TABLE claim_dispositions (
    disposition_key TEXT PRIMARY KEY,
	disposition_kind TEXT NOT NULL CHECK (disposition_kind IN ('expiry', 'boundary_end')),
    request_digest TEXT NOT NULL,
    handling_id TEXT NOT NULL REFERENCES handlings(handling_id),
    attachment_id TEXT NOT NULL REFERENCES attachments(attachment_id),
    claim_fence INTEGER NOT NULL CHECK (claim_fence > 0),
    claim_until TEXT NOT NULL,
    outcome_digest TEXT NOT NULL,
    outcome_json BLOB NOT NULL,
    recorded_at TEXT NOT NULL,
    UNIQUE(handling_id, claim_fence)
) STRICT;

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

-- This table is a rebuildable bounded-read projection, not a third domain
-- state. Missing rows mean that no accepted terminal Event directly cited the
-- exact Reference head.
CREATE TABLE reference_outcome_projection (
    reference_event_id TEXT PRIMARY KEY REFERENCES reference_lineage(event_id),
    completed_count INTEGER NOT NULL DEFAULT 0 CHECK (completed_count >= 0),
    declined_count INTEGER NOT NULL DEFAULT 0 CHECK (declined_count >= 0),
    unresolved_count INTEGER NOT NULL DEFAULT 0 CHECK (unresolved_count >= 0),
    CHECK (completed_count > 0 OR declined_count > 0 OR unresolved_count > 0)
) STRICT;

PRAGMA application_id = 1296978487;
PRAGMA user_version = 11;
