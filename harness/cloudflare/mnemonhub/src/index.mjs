import { StaticAuthenticator, authFromEnv, grantsFromEnv } from "./auth.mjs";
import { MnemonHubCore } from "./hub.mjs";
import { DurableObjectHubStore } from "./storage.mjs";
import { handleSyncRequest } from "./http.mjs";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const workspace = url.searchParams.get("workspace") || env.MNEMON_HUB_WORKSPACE || "default";
    const id = env.MNEMON_HUB.idFromName(workspace);
    return env.MNEMON_HUB.get(id).fetch(request);
  }
};

export class MnemonHubDO {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.audit = [];
  }

  async fetch(request) {
    const hub = new MnemonHubCore({
      grants: grantsFromEnv(this.env),
      store: new DurableObjectHubStore(this.ctx.storage)
    });
    return handleSyncRequest(request, hub, authFromEnv(this.env), this.audit);
  }
}

export function createMemoryHandler({ grants, tokens, store, audit }) {
  const hub = new MnemonHubCore({ grants, store });
  const auth = new StaticAuthenticator(tokens);
  return (request) => handleSyncRequest(request, hub, auth, audit);
}
