export class StaticAuthenticator {
  constructor(tokens = {}) {
    this.tokens = tokens;
  }

  authenticate(request) {
    const header = request.headers.get("authorization") ?? "";
    const token = header.trim().replace(/^Bearer\s+/i, "").trim();
    if (!token || !Object.prototype.hasOwnProperty.call(this.tokens, token)) {
      throw new Error("unrecognized bearer token");
    }
    const principal = String(this.tokens[token] ?? "").trim();
    if (!principal) {
      throw new Error("unrecognized bearer token");
    }
    return principal;
  }
}

export function authFromEnv(env = {}) {
  return new StaticAuthenticator(parseJSONRecord(env.MNEMON_HUB_TOKENS_JSON));
}

export function grantsFromEnv(env = {}) {
  const raw = parseJSONRecord(env.MNEMON_HUB_GRANTS_JSON);
  const out = {};
  for (const [principal, value] of Object.entries(raw)) {
    if (Array.isArray(value)) {
      out[principal] = { principal, scopes: value };
    } else if (value && typeof value === "object" && Array.isArray(value.scopes)) {
      out[principal] = { principal: value.principal || principal, scopes: value.scopes };
    }
  }
  return out;
}

function parseJSONRecord(value) {
  if (!value) {
    return {};
  }
  if (typeof value === "object") {
    return value;
  }
  try {
    const parsed = JSON.parse(String(value));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}
