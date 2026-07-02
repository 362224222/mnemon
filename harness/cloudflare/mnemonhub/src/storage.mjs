const stateKey = "mnemonhub:v1";

export class MemoryHubStore {
  constructor(initialState) {
    this.state = normalizeState(initialState);
  }

  async read() {
    return normalizeState(this.state);
  }

  async write(state) {
    this.state = normalizeState(state);
  }
}

export class DurableObjectHubStore {
  constructor(storage) {
    this.storage = storage;
  }

  async read() {
    const state = await this.storage.get(stateKey);
    return normalizeState(state);
  }

  async write(state) {
    await this.storage.put(stateKey, normalizeState(state));
  }
}

export function normalizeState(state = {}) {
  return {
    next_seq: Number.isInteger(state.next_seq) && state.next_seq > 0 ? state.next_seq : 1,
    events: Array.isArray(state.events) ? JSON.parse(JSON.stringify(state.events)) : [],
    serve_cursors: isRecord(state.serve_cursors) ? { ...state.serve_cursors } : {},
    served_total: Number.isInteger(state.served_total) && state.served_total >= 0 ? state.served_total : 0,
    audit_records: Array.isArray(state.audit_records) ? JSON.parse(JSON.stringify(state.audit_records)) : []
  };
}

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
