import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { test } from "node:test";

import { fieldsDigest, syncedEventEnvelopeFromMaterial } from "../src/contract.mjs";
import { MnemonHubCore } from "../src/hub.mjs";
import { createMemoryHandler } from "../src/index.mjs";
import { MemoryHubStore } from "../src/storage.mjs";

const fixtureRoot = resolve("../../testdata/mnemonhub/sync-abi");

for (const fixture of await loadFixtures()) {
  test(`sync ABI: ${fixture.name}`, async () => {
    switch (fixture.kind) {
      case "push":
        await runPushFixture(fixture);
        break;
      case "push_replay":
        await runReplayFixture(fixture);
        break;
      case "push_conflict":
        await runConflictFixture(fixture);
        break;
      case "pull":
        await runPullFixture(fixture);
        break;
      case "status":
        await runStatusFixture(fixture);
        break;
      case "http_auth":
        await runHTTPAuthFixture(fixture);
        break;
      default:
        throw new Error(`unknown fixture kind ${fixture.kind}`);
    }
  });
}

async function loadFixtures() {
  const names = (await readdir(fixtureRoot)).filter((name) => name.endsWith(".json")).sort();
  const fixtures = [];
  for (const name of names) {
    fixtures.push(JSON.parse(await readFile(join(fixtureRoot, name), "utf8")));
  }
  assert.ok(fixtures.length > 0, "expected sync ABI fixtures");
  return fixtures;
}

function openFixtureHub(fixture) {
  const grants = {};
  if (fixture.principal) {
    grants[fixture.principal] = { principal: fixture.principal, scopes: fixture.grant_scopes ?? [] };
  }
  if (fixture.seed_principal) {
    grants[fixture.seed_principal] = { principal: fixture.seed_principal, scopes: fixture.grant_scopes ?? [] };
  }
  const store = new MemoryHubStore();
  return { hub: new MnemonHubCore({ grants, store, now: () => "2026-06-12T00:00:00Z" }), store };
}

async function runPushFixture(fixture) {
  const { hub } = openFixtureHub(fixture);
  const response = await hub.push(fixture.principal, {
    replica_id: fixture.replica_id,
    batch_id: fixture.name,
    events: await fixtureEvents(fixture.events ?? [])
  });
  assertCounts(fixture, response);
}

async function runReplayFixture(fixture) {
  const { hub, store } = openFixtureHub(fixture);
  const request = {
    replica_id: fixture.replica_id,
    batch_id: fixture.name,
    events: await fixtureEvents(fixture.events ?? [])
  };
  const first = await hub.push(fixture.principal, request);
  const second = await hub.push(fixture.principal, request);
  assertCounts(fixture, second);
  assert.deepEqual(second.accepted, first.accepted);
  assert.equal((await store.read()).events.length, fixture.want.stored_events);
}

async function runConflictFixture(fixture) {
  const { hub } = openFixtureHub(fixture);
  await hub.push(fixture.principal, {
    replica_id: fixture.replica_id,
    batch_id: `${fixture.name}-first`,
    events: await fixtureEvents((fixture.events ?? []).slice(0, 1))
  });
  const response = await hub.push(fixture.principal, {
    replica_id: fixture.replica_id,
    batch_id: `${fixture.name}-second`,
    events: await fixtureEvents((fixture.events ?? []).slice(1))
  });
  assertCounts(fixture, response);
}

async function runPullFixture(fixture) {
  const { hub } = openFixtureHub(fixture);
  await seedFixture(hub, fixture);
  const response = await hub.pull(fixture.principal, { replica_id: fixture.replica_id });
  assert.equal(response.events.length, fixture.want.events);
  if (fixture.want.after_cursor_events >= 0 && response.next_cursor) {
    const after = await hub.pull(fixture.principal, {
      replica_id: fixture.replica_id,
      remote_cursor: response.next_cursor
    });
    assert.equal(after.events.length, fixture.want.after_cursor_events);
  }
}

async function runStatusFixture(fixture) {
  const { hub } = openFixtureHub(fixture);
  await seedFixture(hub, fixture);
  const response = await hub.status(fixture.principal);
  assert.equal(response.hub_events_received, fixture.want.hub_events_received);
}

async function runHTTPAuthFixture(fixture) {
  const memory = { kind: "memory", id: "project" };
  const handler = createMemoryHandler({
    grants: { "replica-a@team": { principal: "replica-a@team", scopes: [memory] } },
    tokens: { "tok-a": "replica-a@team" },
    store: new MemoryHubStore(),
    audit: []
  });
  const body = JSON.stringify({
    replica_id: "local-a",
    batch_id: fixture.name,
    events: await fixtureEvents([{
      origin_replica_id: "local-a",
      local_decision_id: "dec-auth",
      resource_ref: memory,
      fields: { content: "auth" }
    }])
  });
  const request = new Request(`https://mnemon.test${fixture.route}`, {
    method: fixture.method,
    headers: fixture.token ? { authorization: `Bearer ${fixture.token}` } : {},
    body
  });
  const response = await handler(request);
  assert.equal(response.status, fixture.want.status_code);
}

async function seedFixture(hub, fixture) {
  if (!fixture.seed_events?.length) {
    return;
  }
  await hub.push(fixture.seed_principal, {
    replica_id: fixture.seed_replica_id,
    batch_id: `${fixture.name}-seed`,
    events: await fixtureEvents(fixture.seed_events)
  });
}

async function fixtureEvents(fixtures) {
  const events = [];
  for (const fixture of fixtures) {
    const material = await fixtureMaterial(fixture);
    if (fixture.corrupt_digest) {
      material.fields_digest = "corrupt";
    }
    events.push(await syncedEventEnvelopeFromMaterial(material));
  }
  return events;
}

async function fixtureMaterial(fixture) {
  const fields = fixture.fields ?? {};
  return {
    origin_replica_id: fixture.origin_replica_id,
    local_decision_id: fixture.local_decision_id,
    local_ingest_seq: 1,
    actor: "codex@project",
    correlation_id: "",
    resource_ref: fixture.resource_ref,
    resource_version: 1,
    fields_digest: await fieldsDigest(fields),
    fields,
    decided_at: "2026-06-12T00:00:00Z",
    status: "pending"
  };
}

function assertCounts(fixture, response) {
  assert.equal(response.accepted?.length ?? 0, fixture.want.accepted);
  assert.equal(response.rejected?.length ?? 0, fixture.want.rejected);
  assert.equal(response.conflicts?.length ?? 0, fixture.want.conflicts);
}
