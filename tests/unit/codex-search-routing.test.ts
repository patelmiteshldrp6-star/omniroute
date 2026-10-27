import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

process.env.API_KEY_SECRET = "codex-search-test-crc-secret";

const core = await import("../../src/lib/db/core.ts");
const providers = await import("../../src/lib/db/providers.ts");
const settings = await import("../../src/lib/db/settings.ts");
const apiKeys = await import("../../src/lib/db/apiKeys.ts");
const route = await import("../../src/app/api/v1/search/route.ts");
const { executeWebSearch } = await import("../../src/lib/search/executeWebSearch.ts");

const { getCallLogs, closeCallLogSaves } = await import("../../src/lib/usage/callLogs.ts");
const fixture = readFileSync(
  new URL("../fixtures/codex-search/real_en_ref_param.sse", import.meta.url),
  "utf8"
);
let sequence = 0;

test.beforeEach(async () => {
  for (const connection of await providers.getProviderConnections()) {
    await providers.deleteProviderConnection(connection.id as string);
  }
  await settings.updateSettings({ searchTimeoutMs: null, blockedProviders: [] });
});

test.after(async () => {
  await closeCallLogSaves(500);
  core.resetDbInstance();
});

async function seedCodex() {
  return providers.createProviderConnection({
    provider: "codex",
    authType: "oauth",
    name: `codex-search-${sequence++}`,
    accessToken: "synthetic-codex-token",
    isActive: true,
    testStatus: "active",
    providerSpecificData: { workspaceId: "synthetic-workspace", quotaPreflightEnabled: false },
  });
}

async function request(body: Record<string, unknown> = {}, key?: string) {
  return route.POST(
    new Request("http://localhost/api/v1/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(key ? { Authorization: `Bearer ${key}` } : {}),
      },
      body: JSON.stringify({ query: `codex routing test ${sequence++}`, ...body }),
    })
  );
}

async function stub<T>(run: (calls: string[]) => Promise<T>) {
  const original = globalThis.fetch;
  const calls: string[] = [];
  globalThis.fetch = async (input) => {
    const url = String(input);
    calls.push(url);
    if (url === "https://chatgpt.com/backend-api/codex/responses") return new Response(fixture);
    if (url.includes("duckduckgo.com")) return new Response("<html></html>");
    if (url.includes("api.search.brave.com")) return Response.json({ web: { results: [] } });
    throw new Error(`Unexpected mocked fetch: ${url}`);
  };
  try {
    return await run(calls);
  } finally {
    globalThis.fetch = original;
  }
}

test("without Codex credentials last-resort search still uses DuckDuckGo", async () => {
  await stub(async (calls) => {
    const response = await request();
    assert.equal(response.status, 200);
    assert.equal((await response.json()).provider, "duckduckgo-free");
    assert.ok(calls.length > 0 && calls.every((url) => url.includes("duckduckgo.com")));
  });
});

test("with a usable Codex OAuth connection last-resort search precedes DuckDuckGo", async () => {
  const connection = await seedCodex();
  await stub(async (calls) => {
    const response = await request();
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.provider, "codex-search");
    assert.equal(body.results[0].title, "NousResearch/hermes-agent releases");
    assert.deepEqual(calls, ["https://chatgpt.com/backend-api/codex/responses"]);
    const logs = await getCallLogs({ provider: "codex-search" });
    const log = logs.find((row) => row.connectionId === connection.id);
    assert.ok(log, "call log attributes search to the Codex connection");
    assert.equal(log.tokens.in, 8807);
    assert.equal(log.tokens.out, 92);
  });
});

test("connection scope excluding Codex allows DuckDuckGo only with the existing noauth opt-in", async () => {
  await seedCodex();
  const key = await apiKeys.createApiKey("codex-search-excluded", "codex-search-machine", [], {
    allowedConnections: ["noauth"],
  });
  await stub(async (calls) => {
    const response = await request({}, key.key as string);
    assert.equal(response.status, 200);
    assert.equal((await response.json()).provider, "duckduckgo-free");
    assert.ok(calls.every((url) => url.includes("duckduckgo.com")));
  });
  const deniedKey = await apiKeys.createApiKey(
    "codex-search-no-keyless",
    "codex-search-machine",
    [],
    { allowedConnections: ["00000000-0000-4000-8000-00000000dead"] }
  );
  await stub(async (calls) => {
    const response = await request({}, deniedKey.key as string);
    assert.notEqual(response.status, 200);
    assert.deepEqual(calls, []);
  });
});

test("explicit codex_search alias uses scoped Codex credentials and news search", async () => {
  const connection = await seedCodex();
  const key = await apiKeys.createApiKey("codex-search-included", "codex-search-machine", [], {
    allowedConnections: [connection.id as string],
  });
  await stub(async (calls) => {
    const response = await request(
      { provider: "codex_search", search_type: "news" },
      key.key as string
    );
    assert.equal(response.status, 200);
    assert.equal((await response.json()).provider, "codex-search");
    assert.equal(calls.length, 1);
  });
});

test("chat/MCP execution mirror prefers Codex to DuckDuckGo with no paid credentials", async () => {
  await seedCodex();
  await stub(async (calls) => {
    const result = await executeWebSearch({ query: `mirror codex routing ${sequence++}` });
    assert.equal(result.data.provider, "codex-search");
    assert.deepEqual(calls, ["https://chatgpt.com/backend-api/codex/responses"]);
  });
});

test("blocked codex-search is not auto-selected", async () => {
  await seedCodex();
  await settings.updateSettings({ blockedProviders: ["codex-search"] });
  try {
    await stub(async (calls) => {
      const response = await request();
      assert.equal(response.status, 200);
      assert.equal((await response.json()).provider, "duckduckgo-free");
      assert.ok(calls.every((url) => url.includes("duckduckgo.com")));
    });
  } finally {
    await settings.updateSettings({ blockedProviders: [] });
  }
});

test("configured paid search remains primary even with Codex available", async () => {
  await seedCodex();
  const connection = await providers.createProviderConnection({
    provider: "brave-search",
    authType: "apikey",
    name: "codex-paid-control",
    apiKey: "synthetic-brave",
    isActive: true,
    testStatus: "active",
  });
  try {
    await stub(async (calls) => {
      const response = await request();
      assert.equal(response.status, 200);
      assert.equal((await response.json()).provider, "brave-search");
      assert.ok(calls.every((url) => url.includes("api.search.brave.com")));
    });
  } finally {
    await providers.deleteProviderConnection(connection.id as string);
  }
});

for (const [budget, expected] of [
  [undefined, 60_000],
  [5000, 5000],
  [90_000, 60_000],
] as const) {
  test(`route Codex timeout uses ${expected}ms with operator setting ${budget}`, async (t) => {
    await seedCodex();
    if (budget !== undefined) await settings.updateSettings({ searchTimeoutMs: budget });
    const delays: number[] = [];
    const original = globalThis.setTimeout;
    t.mock.method(
      globalThis,
      "setTimeout",
      (callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
        delays.push(Number(delay));
        return original(callback, delay, ...args);
      }
    );
    await stub(async () => {
      const response = await request({ provider: "codex-search" });
      assert.equal(response.status, 200);
    });
    assert.ok(
      delays.some((delay) => delay > expected - 1000 && delay <= expected),
      JSON.stringify(delays)
    );
    t.mock.restoreAll();
  });
}
