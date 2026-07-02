import { BadRequestError, SyncVerbPull, SyncVerbPush, SyncVerbStatus } from "./contract.mjs";

const maxBodyBytes = 8 << 20;

export async function handleSyncRequest(request, hub, authenticator, audit = null) {
  const url = new URL(request.url);
  if (url.pathname === "/sync/push") {
    const method = allowMethod(request, ["POST"], SyncVerbPush, audit);
    if (method) return method;
    const auth = authenticate(request, authenticator, SyncVerbPush, audit);
    if (auth.response) return auth.response;
    const decoded = await decodeJSON(request, SyncVerbPush, auth.principal, audit);
    if (decoded.response) return decoded.response;
    return respond(await hubCall(() => hub.push(auth.principal, decoded.value), auth.principal, SyncVerbPush, audit));
  }
  if (url.pathname === "/sync/pull") {
    const method = allowMethod(request, ["POST"], SyncVerbPull, audit);
    if (method) return method;
    const auth = authenticate(request, authenticator, SyncVerbPull, audit);
    if (auth.response) return auth.response;
    const decoded = await decodeJSON(request, SyncVerbPull, auth.principal, audit);
    if (decoded.response) return decoded.response;
    return respond(await hubCall(() => hub.pull(auth.principal, decoded.value), auth.principal, SyncVerbPull, audit));
  }
  if (url.pathname === "/sync/status") {
    const method = allowMethod(request, ["GET", "POST"], SyncVerbStatus, audit);
    if (method) return method;
    const auth = authenticate(request, authenticator, SyncVerbStatus, audit);
    if (auth.response) return auth.response;
    return respond(await hubCall(() => hub.status(auth.principal), auth.principal, SyncVerbStatus, audit));
  }
  return new Response("not found\n", { status: 404 });
}

function allowMethod(request, allowed, verb, audit) {
  if (allowed.includes(request.method)) {
    return null;
  }
  auditLine(audit, "-", verb, "bad_request");
  return new Response("method not allowed\n", {
    status: 405,
    headers: { allow: allowed.join(", ") }
  });
}

function authenticate(request, authenticator, verb, audit) {
  try {
    return { principal: authenticator.authenticate(request) };
  } catch (error) {
    auditLine(audit, "-", verb, "unauthorized");
    return { response: new Response(`${error.message}\n`, { status: 401 }) };
  }
}

async function decodeJSON(request, verb, principal, audit) {
  try {
    const text = await request.text();
    if (text.length > maxBodyBytes) {
      throw new BadRequestError("request body too large");
    }
    return { value: text.trim() ? JSON.parse(text) : {} };
  } catch (error) {
    auditLine(audit, principal, verb, "bad_request");
    return { response: new Response(`${error.message}\n`, { status: 400 }) };
  }
}

async function hubCall(call, principal, verb, audit) {
  try {
    const value = await call();
    auditLine(audit, principal, verb, "ok");
    return { value };
  } catch (error) {
    if (error instanceof BadRequestError) {
      auditLine(audit, principal, verb, "bad_request");
      return { response: new Response(`${error.message}\n`, { status: 400 }) };
    }
    auditLine(audit, principal, verb, "denied");
    return { response: new Response(`${error.message}\n`, { status: 403 }) };
  }
}

function respond(result) {
  if (result.response) {
    return result.response;
  }
  return new Response(JSON.stringify(result.value) + "\n", {
    headers: { "content-type": "application/json" }
  });
}

function auditLine(audit, principal, verb, result) {
  if (!audit || typeof audit.push !== "function") {
    return;
  }
  audit.push(`${new Date().toISOString()} principal=${principal} verb=${verb} result=${result}`);
}
