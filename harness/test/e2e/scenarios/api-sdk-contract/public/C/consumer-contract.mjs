import assert from "node:assert/strict";

const schemaPath = process.argv[2];
if (!schemaPath) throw new Error("usage: node consumer-contract.mjs OPENAPI_JSON");
const schema = JSON.parse(await (await import("node:fs/promises")).readFile(schemaPath, "utf8"));
const operation = schema.paths?.["/items"]?.get;
assert.ok(operation, "GET /items is required");
assert.ok(operation.parameters?.some((parameter) => parameter.name === "cursor"));
assert.ok(operation.responses?.["400"], "invalid cursor response is required");
assert.ok(schema.components?.schemas?.Cursor, "Cursor schema is required");
assert.ok(schema.components?.schemas?.Problem, "Problem error schema is required");
