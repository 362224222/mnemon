import { sha256Hex } from "./json.mjs";

export const SyncVerbPush = "sync.push";
export const SyncVerbPull = "sync.pull";
export const SyncVerbStatus = "sync.status";

const phaseMeta = {
  synced: {
    origin_mnemond: "string",
    cursor: "string",
    digest: "string",
    synced_at: "string"
  }
};

const payloadSections = new Set(["rule", "narrative", "refs"]);
const phaseMetaKeys = new Set(["external_id", "host", "lifecycle", "decision_id", "ingest_seq", "accepted_at", "accepted_by", "origin_mnemond", "cursor", "digest", "synced_at", "derived_at", "expires_at", "presentation_hint", "suggested_event_types"]);

export function subject(kind, id) {
  kind = String(kind ?? "").trim();
  id = String(id ?? "").trim();
  if (!kind || !id) {
    return "";
  }
  return `${kind}/${id}`;
}

export function resourceRefFromSubject(value) {
  const [kind, id, extra] = String(value ?? "").split("/");
  if (!kind || !id || extra !== undefined) {
    throw new BadRequestError(`event subject ${JSON.stringify(value)} is not kind/id`);
  }
  return { kind, id };
}

export function localDecisionID(eventID) {
  return String(eventID ?? "").trim().split(":", 1)[0];
}

export async function fieldsDigest(fields) {
  return sha256Hex(fields ?? {});
}

export async function syncedEventEnvelopeFromMaterial(material) {
  const ref = material.resource_ref;
  const eventSubject = subject(ref?.kind, ref?.id);
  const decidedAt = String(material.decided_at ?? "");
  const event = {
    schema_version: 2,
    id: `${String(material.local_decision_id || "decision").trim()}:${eventSubject}`,
    type: `${ref.kind}.accepted`,
    subject: eventSubject,
    actor: String(material.actor ?? ""),
    audience: "",
    payload: {
      rule: {
        resource_version: Number(material.resource_version ?? 0),
        fields: cloneFields(material.fields)
      }
    },
    caused_by: [],
    correlation_id: String(material.correlation_id ?? ""),
    ttl: "",
    created_at: decidedAt
  };
  const env = {
    schema_version: 2,
    phase: "synced",
    event,
    meta: {
      origin_mnemond: String(material.origin_replica_id ?? "").trim(),
      cursor: String(material.local_ingest_seq ?? ""),
      digest: String(material.fields_digest ?? "").trim(),
      synced_at: decidedAt
    }
  };
  validateEnvelope(env);
  return env;
}

export function syncedEventMaterialFromEnvelope(env) {
  validateEnvelope(env);
  if (env.phase !== "synced") {
    throw new Error(`sync event must have phase "synced", got ${JSON.stringify(env.phase)}`);
  }
  const ref = resourceRefFromSubject(env.event.subject);
  const decisionID = localDecisionID(env.event.id);
  if (!decisionID) {
    throw new Error(`synced event ${JSON.stringify(env.event.id)} does not carry a decision identity`);
  }
  const rule = env.event.payload?.rule;
  const resourceVersion = Number(rule?.resource_version);
  if (!Number.isFinite(resourceVersion)) {
    throw new Error(`synced event ${JSON.stringify(env.event.id)} has invalid resource_version`);
  }
  if (!isPlainObject(rule?.fields)) {
    throw new Error(`synced event ${JSON.stringify(env.event.id)} payload.rule.fields must be an object`);
  }
  return {
    origin_replica_id: String(env.meta.origin_mnemond ?? "").trim(),
    local_decision_id: decisionID,
    local_ingest_seq: parseInt(String(env.meta.cursor ?? "0"), 10) || 0,
    actor: String(env.event.actor ?? ""),
    correlation_id: String(env.event.correlation_id ?? ""),
    resource_ref: ref,
    resource_version: resourceVersion,
    fields_digest: String(env.meta.digest ?? "").trim(),
    fields: cloneFields(rule.fields),
    decided_at: String(env.meta.synced_at ?? "").trim(),
    status: "pending"
  };
}

export function eventExchangeResultFromEnvelope(env, status, diagnostic = "") {
  return {
    origin_mnemond: String(env?.meta?.origin_mnemond ?? "").trim(),
    event_id: String(env?.event?.id ?? ""),
    subject: String(env?.event?.subject ?? ""),
    status,
    diagnostic
  };
}

export async function validateSyncedEventMaterial(material) {
  if (!String(material.origin_replica_id ?? "").trim()) {
    return "origin_replica_id is required";
  }
  if (!String(material.local_decision_id ?? "").trim()) {
    return "local_decision_id is required";
  }
  if (!String(material.actor ?? "").trim()) {
    return "actor is required";
  }
  if (!String(material.resource_ref?.kind ?? "").trim() || !String(material.resource_ref?.id ?? "").trim()) {
    return "resource_ref is required";
  }
  if (!isPlainObject(material.fields)) {
    return "fields are required";
  }
  if (!String(material.fields_digest ?? "").trim()) {
    return "fields_digest is required";
  }
  const digest = await fieldsDigest(material.fields);
  if (material.fields_digest !== digest) {
    return "fields_digest does not match fields";
  }
  return "";
}

export function clampRefs(principal, grantScopes, requestedScopes = []) {
  const grant = Array.isArray(grantScopes) ? grantScopes : [];
  const requested = Array.isArray(requestedScopes) && requestedScopes.length > 0 ? requestedScopes : grant;
  const allowed = new Set(grant.map(refKey));
  for (const ref of requested) {
    if (!allowed.has(refKey(ref))) {
      throw new Error(`principal ${JSON.stringify(principal)} is not granted access to ${refKey(ref)}`);
    }
  }
  return requested.map((ref) => ({ kind: String(ref.kind), id: String(ref.id) }));
}

export function refKey(ref) {
  return `${String(ref?.kind ?? "")}/${String(ref?.id ?? "")}`;
}

export class BadRequestError extends Error {
  constructor(message) {
    super(message);
    this.name = "BadRequestError";
  }
}

export function validateEnvelope(env) {
  if (!isPlainObject(env)) {
    throw new Error("event envelope must be an object");
  }
  if (!(Number(env.schema_version) > 0)) {
    throw new Error("event envelope schema_version must be positive");
  }
  if (env.phase !== "synced") {
    throw new Error(`unknown event envelope phase ${JSON.stringify(env.phase)}`);
  }
  if (!isPlainObject(env.event)) {
    throw new Error("event is required");
  }
  validateEvent(env.event);
  validateMeta(env.phase, env.meta);
}

function validateEvent(event) {
  if (event.schema_version !== 2) {
    throw new Error("event schema_version must be 2");
  }
  for (const key of ["id", "type", "subject", "actor"]) {
    if (!String(event[key] ?? "").trim()) {
      throw new Error(`event ${key} is required`);
    }
  }
  validatePayload(event.payload);
}

function validatePayload(payload) {
  if (!isPlainObject(payload)) {
    throw new Error("event payload must be an object");
  }
  for (const [key, value] of Object.entries(payload)) {
    if (phaseMetaKeys.has(key)) {
      throw new Error(`event payload must not carry phase meta key ${JSON.stringify(key)}`);
    }
    if (!payloadSections.has(key)) {
      throw new Error(`event payload must not carry business key ${JSON.stringify(key)} outside rule/narrative/refs`);
    }
    if (!isPlainObject(value)) {
      throw new Error(`event payload section ${JSON.stringify(key)} must be an object`);
    }
    for (const sectionKey of Object.keys(value)) {
      if (phaseMetaKeys.has(sectionKey)) {
        throw new Error(`event payload section ${JSON.stringify(key)} must not carry phase meta key ${JSON.stringify(sectionKey)}`);
      }
    }
  }
}

function validateMeta(phase, meta) {
  if (!isPlainObject(meta)) {
    throw new Error(`${phase} event envelope meta is required`);
  }
  const schema = phaseMeta[phase];
  for (const key of Object.keys(meta)) {
    if (!(key in schema)) {
      throw new Error(`${phase} event envelope meta has unknown key ${JSON.stringify(key)}`);
    }
  }
  for (const key of Object.keys(schema)) {
    const value = meta[key];
    if (typeof value !== "string" || !value.trim()) {
      throw new Error(`${phase} event envelope meta key ${JSON.stringify(key)} must be a non-empty string`);
    }
  }
}

export function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function cloneFields(fields) {
  if (!isPlainObject(fields)) {
    return {};
  }
  return JSON.parse(JSON.stringify(fields));
}
