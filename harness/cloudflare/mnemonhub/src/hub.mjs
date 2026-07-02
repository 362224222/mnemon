import {
  BadRequestError,
  SyncVerbPull,
  SyncVerbPush,
  SyncVerbStatus,
  clampRefs,
  eventExchangeResultFromEnvelope,
  refKey,
  syncedEventEnvelopeFromMaterial,
  syncedEventMaterialFromEnvelope,
  validateSyncedEventMaterial
} from "./contract.mjs";

export class MnemonHubCore {
  constructor({ grants = {}, store, now = () => new Date().toISOString() } = {}) {
    this.grants = grants;
    this.store = store;
    this.now = now;
  }

  async push(principal, request) {
    const grant = this.grant(principal, SyncVerbPush);
    if (!grant) {
      throw new Error(`principal ${JSON.stringify(principal)} has no replica grant for ${SyncVerbPush}`);
    }
    const replicaID = String(request?.replica_id ?? "").trim();
    if (!replicaID) {
      throw new BadRequestError("sync push requires replica_id");
    }
    const response = { accepted: [], rejected: [], conflicts: [] };
    for (const envelope of Array.isArray(request?.events) ? request.events : []) {
      let material;
      try {
        material = syncedEventMaterialFromEnvelope(envelope);
      } catch (error) {
        response.rejected.push(eventExchangeResultFromEnvelope(envelope, "rejected", error.message));
        continue;
      }
      if (material.origin_replica_id !== replicaID) {
        throw new BadRequestError(`sync push replica_id ${JSON.stringify(replicaID)} does not match event origin ${JSON.stringify(material.origin_replica_id)}`);
      }
      const diagnostic = await validateSyncedEventMaterial(material);
      if (diagnostic) {
        response.rejected.push(eventExchangeResultFromEnvelope(envelope, "rejected", diagnostic));
        continue;
      }
      try {
        clampRefs(principal, grant.scopes, [material.resource_ref]);
      } catch (error) {
        response.rejected.push(eventExchangeResultFromEnvelope(envelope, "rejected", error.message));
        continue;
      }
      const record = await this.recordRemoteSyncedEvent(String(principal), material);
      switch (record.status) {
        case "accepted": {
          const acceptedEnvelope = await syncedEventEnvelopeFromMaterial(record.material);
          acceptedEnvelope.meta.cursor = String(record.remote_seq);
          response.accepted.push(eventExchangeResultFromEnvelope(acceptedEnvelope, "accepted", ""));
          response.next_cursor = String(record.remote_seq);
          break;
        }
        case "conflict":
          response.conflicts.push(eventExchangeResultFromEnvelope(envelope, "conflict", record.diagnostic));
          break;
        default:
          response.rejected.push(eventExchangeResultFromEnvelope(envelope, record.status, record.diagnostic ?? ""));
      }
    }
    return response;
  }

  async pull(principal, request) {
    const grant = this.grant(principal, SyncVerbPull);
    if (!grant) {
      throw new Error(`principal ${JSON.stringify(principal)} has no replica grant for ${SyncVerbPull}`);
    }
    const replicaID = String(request?.replica_id ?? "").trim();
    if (!replicaID) {
      throw new BadRequestError("sync pull requires replica_id");
    }
    let cursor = 0;
    if (String(request?.remote_cursor ?? "").trim()) {
      cursor = Number.parseInt(String(request.remote_cursor), 10);
      if (!Number.isFinite(cursor)) {
        throw new BadRequestError(`parse remote_cursor: invalid syntax`);
      }
    }
    const scopes = clampRefs(principal, grant.scopes, request?.scopes ?? []);
    const scopeSet = new Set(scopes.map(refKey));
    const state = await this.store.read();
    const records = state.events
      .filter((record) => record.remote_seq > cursor)
      .filter((record) => record.status === "accepted")
      .filter((record) => record.material.origin_replica_id !== replicaID)
      .filter((record) => scopeSet.has(refKey(record.material.resource_ref)))
      .sort((a, b) => a.remote_seq - b.remote_seq)
      .slice(0, 100);
    let next = cursor;
    const events = [];
    for (const record of records) {
      if (record.remote_seq > next) {
        next = record.remote_seq;
      }
      const env = await syncedEventEnvelopeFromMaterial(record.material);
      env.meta.cursor = String(record.remote_seq);
      events.push(env);
    }
    if (records.length > 0) {
      state.served_total += records.length;
    }
    state.serve_cursors[String(principal)] = next;
    await this.store.write(state);
    return { next_cursor: String(next), events };
  }

  async status(principal) {
    if (!this.grant(principal, SyncVerbStatus)) {
      throw new Error(`principal ${JSON.stringify(principal)} has no replica grant for ${SyncVerbStatus}`);
    }
    const state = await this.store.read();
    const cursors = {};
    for (const [principalID, cursor] of Object.entries(state.serve_cursors)) {
      cursors[principalID] = String(cursor);
    }
    const response = {
      principal,
      remote_workspace: "connected",
      hub_events_received: state.events.filter((record) => record.status === "accepted").length,
      hub_events_served: state.served_total
    };
    if (Object.keys(cursors).length > 0) {
      response.hub_replica_cursors = cursors;
    }
    return response;
  }

  grant(principal, verb) {
    if (![SyncVerbPush, SyncVerbPull, SyncVerbStatus].includes(verb)) {
      return null;
    }
    const grant = this.grants[String(principal)];
    if (!grant) {
      return null;
    }
    return { principal: grant.principal || String(principal), scopes: Array.isArray(grant.scopes) ? grant.scopes : [] };
  }

  async recordRemoteSyncedEvent(remotePeerID, material) {
    const state = await this.store.read();
    const existing = state.events.find((record) =>
      record.remote_peer_id === remotePeerID &&
      record.material.origin_replica_id === material.origin_replica_id &&
      record.material.local_decision_id === material.local_decision_id
    );
    if (existing) {
      if (sameSyncedEventMaterial(existing.material, material)) {
        return existing;
      }
      return {
        remote_peer_id: remotePeerID,
        material,
        status: "conflict",
        diagnostic: "sync idempotency key reused with different event"
      };
    }
    const accepted = JSON.parse(JSON.stringify(material));
    accepted.status = "accepted";
    const record = {
      remote_seq: state.next_seq++,
      remote_peer_id: remotePeerID,
      material: accepted,
      received_at: this.now(),
      status: "accepted",
      diagnostic: ""
    };
    state.events.push(record);
    await this.store.write(state);
    return record;
  }
}

function sameSyncedEventMaterial(a, b) {
  return Number(a.local_ingest_seq) === Number(b.local_ingest_seq) &&
    String(a.actor ?? "") === String(b.actor ?? "") &&
    String(a.correlation_id ?? "") === String(b.correlation_id ?? "") &&
    refKey(a.resource_ref) === refKey(b.resource_ref) &&
    Number(a.resource_version) === Number(b.resource_version) &&
    String(a.fields_digest ?? "") === String(b.fields_digest ?? "");
}
