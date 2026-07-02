const textEncoder = new TextEncoder();

export function stableStringify(value) {
  return JSON.stringify(sortJSON(value));
}

function sortJSON(value) {
  if (Array.isArray(value)) {
    return value.map(sortJSON);
  }
  if (value && typeof value === "object") {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortJSON(value[key]);
    }
    return out;
  }
  return value;
}

export async function sha256Hex(value) {
  const data = typeof value === "string" ? value : stableStringify(value);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", textEncoder.encode(data));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
