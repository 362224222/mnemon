CREATE TABLE selections (
    selection_id TEXT PRIMARY KEY,
    descriptor_json BLOB NOT NULL,
    local_participant TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('awaiting_seed', 'active', 'observed')),
    seed_principal_id TEXT,
    seed_event_id TEXT,
    seed_event_digest TEXT,
    initial_preference TEXT CHECK (initial_preference IN ('A', 'B')),
    current_preference TEXT CHECK (current_preference IN ('A', 'B')),
    signed_margin INTEGER NOT NULL DEFAULT 0,
    completed_rounds INTEGER NOT NULL DEFAULT 0 CHECK (completed_rounds >= 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    observation_digest TEXT,
    observation_json BLOB,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (phase = 'awaiting_seed' AND seed_principal_id IS NULL AND
         seed_event_id IS NULL AND seed_event_digest IS NULL AND
         initial_preference IS NULL AND current_preference IS NULL AND
         signed_margin = 0 AND completed_rounds = 0 AND
         observation_digest IS NULL AND observation_json IS NULL)
        OR
        (phase = 'active' AND seed_principal_id IS NOT NULL AND
         seed_event_id IS NOT NULL AND seed_event_digest IS NOT NULL AND
         initial_preference IS NOT NULL AND current_preference IS NOT NULL AND
         observation_digest IS NULL AND observation_json IS NULL)
        OR
        (phase = 'observed' AND seed_principal_id IS NOT NULL AND
         seed_event_id IS NOT NULL AND seed_event_digest IS NOT NULL AND
         initial_preference IS NOT NULL AND current_preference IS NOT NULL AND
         observation_digest IS NOT NULL AND observation_json IS NOT NULL)
    )
) STRICT;

CREATE TABLE pending_rounds (
    selection_id TEXT PRIMARY KEY REFERENCES selections(selection_id) ON DELETE CASCADE,
    round INTEGER NOT NULL CHECK (round > 0),
    nonce_digest TEXT NOT NULL,
    sample_json BLOB NOT NULL,
    deadline TEXT NOT NULL,
    state_revision INTEGER NOT NULL CHECK (state_revision > 0),
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE settled_rounds (
    selection_id TEXT NOT NULL REFERENCES selections(selection_id) ON DELETE CASCADE,
    round INTEGER NOT NULL CHECK (round > 0),
    nonce_digest TEXT NOT NULL,
    sample_json BLOB NOT NULL,
    deadline TEXT NOT NULL,
    state_revision INTEGER NOT NULL CHECK (state_revision > 0),
    vote_set_digest TEXT NOT NULL,
    result_revision INTEGER NOT NULL CHECK (result_revision > state_revision),
    settled_at TEXT NOT NULL,
    PRIMARY KEY (selection_id, round)
) STRICT;

PRAGMA application_id = 1296978488;
PRAGMA user_version = 1;
