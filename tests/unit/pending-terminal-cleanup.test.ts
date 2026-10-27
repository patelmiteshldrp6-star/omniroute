import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { createTempDataDir } from "../_setup/tempDataDir.ts";

const { cleanup } = createTempDataDir("omniroute-pending-terminal-");
const history = await import("../../src/lib/usage/usageHistory.ts");
const completed = await import("../../src/lib/usage/completedRequestDetails.ts");
const { translateSseResponse } = await import("../../open-sse/executors/glm.ts");
const { runStreamingTail } = await import("../../open-sse/handlers/chatCore/streamingTail.ts");
const costRules = await import("../../src/domain/costRules.ts");
const { STREAM_IDLE_TIMEOUT_MS } = await import("../../open-sse/config/constants.ts");
const { createPassthroughStreamWithLogger, PENDING_REQUEST_CLEARED_MARKER } =
  await import("../../open-sse/utils/stream.ts");
const { createRequestLogger } = await import("../../open-sse/utils/requestLogger.ts");
await import("../../open-sse/translator/bootstrap.ts");
const { translateRequest } = await import("../../open-sse/translator/index.ts");
const { createChatStreamController } =
  await import("../../open-sse/handlers/chatCore/chatStreamController.ts");
const { finalizePendingScope, updatePendingScope } =
  await import("../../src/lib/usage/pendingRequestScope.ts");
const { acquireTurnExecution, getTurnExecutionSnapshot } =
  await import("../../open-sse/handlers/chatCore/turnExecutionGuard.ts");
const { clearGeminiThoughtSignatures } =
  await import("../../open-sse/services/geminiThoughtSignatureStore.ts");
// Request events run in the background; preload their imports so fixture writes drain before teardown.
await Promise.all([
  import("../../src/lib/gamification/events.ts"),
  import("../../src/lib/gamification/antiCheat.ts"),
  import("../../src/lib/gamification/streaks.ts"),
  import("../../src/lib/gamification/leaderboard.ts"),
  import("../../src/lib/gamification/notifications.ts"),
  import("../../src/lib/db/gamification.ts"),
]);

function captureOwner(id: string) {
  return "capturePendingRequestOwner" in history ? history.capturePendingRequestOwner(id) : id;
}
type FixturePendingIdentity = ReturnType<typeof captureOwner>;

const MODEL = "terminal-fixture";
const PROVIDER = "openai";
const CONN = "terminal-connection";
const KEY = `${MODEL} (${PROVIDER})`;
const API_KEY_ID = "terminal-billing-fixture";
const BILLED_COST = 0.125;
function pending() {
  const id = history.trackPendingRequest(MODEL, PROVIDER, CONN, true, {
    clientRequest: { messages: [{ role: "user", content: "fixture request" }] },
    providerRequest: { model: MODEL, messages: [{ role: "user", content: "fixture request" }] },
  });
  assert.ok(id);
  return id;
}
function assertOnlySurvivor(id: string, expectedBytes: number) {
  assert.deepEqual([...history.getPendingById().keys()], [id]);
  assert.equal(history.getPendingRetainedBytes(), expectedBytes);
  assert.equal(history.getPendingRequests().byModel[KEY], 1);
  assert.equal(history.getPendingRequests().byAccount[CONN]?.[KEY], 1);
  const state = history.getPendingRequests() as ReturnType<typeof history.getPendingRequests> & {
    details: Record<string, Record<string, Array<{ id: string }>>>;
  };
  assert.deepEqual(
    state.details[CONN]?.[KEY]?.map((detail) => detail.id),
    [id]
  );
}

async function stream(
  id: string,
  failBookkeeping: boolean | "connection",
  options: {
    idle?: () => void;
    targetFormat?: string;
    clientResponseFormat?: string;
    rawConnectionId?: string | null;
    currentConnectionId?: string | null;
    pendingConnectionId?: string | null;
    pendingRequestOwner?: FixturePendingIdentity;
    lifecycle?: "cancel" | "abort" | "error" | "translated-error";
    readinessFailure?: boolean;
    model?: string;
    provider?: string;
    wire?: string;
    reqLogger?: Awaited<ReturnType<typeof createRequestLogger>>;
    glmTransport?: boolean;
  } = {}
) {
  const model = options.model ?? MODEL;
  const provider = options.provider ?? PROVIDER;
  const body = { model, stream: true, messages: [{ role: "user", content: "fixture" }] };
  const chunks = [
    {
      id: "completion-fixture",
      object: "chat.completion.chunk",
      model,
      choices: [
        { index: 0, delta: { role: "assistant", content: "fixture answer" }, finish_reason: null },
      ],
    },
    {
      id: "completion-fixture",
      object: "chat.completion.chunk",
      model,
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      usage: { prompt_tokens: 10, completion_tokens: 2, total_tokens: 12, cost: BILLED_COST },
    },
  ];
  const wire =
    options.wire ??
    chunks.map((chunk) => `data: ${JSON.stringify(chunk)}\n\n`).join("") + "data: [DONE]\n\n";
  const firstEvent =
    options.targetFormat === "openai-responses"
      ? { type: "response.created", response: { id: "resp_fixture", status: "in_progress" } }
      : chunks[0];
  let providerStream: ReadableStreamDefaultController<Uint8Array> | undefined;
  const providerBody =
    options.idle || options.lifecycle
      ? new ReadableStream({
          start(controller) {
            providerStream = controller;
            controller.enqueue(
              new TextEncoder().encode(
                options.glmTransport ? GLM_PARTIAL_WIRE : `data: ${JSON.stringify(firstEvent)}\n\n`
              )
            );
          },
        })
      : wire;
  const abort = new AbortController();
  let writes = 0;
  let released = 0;
  let failures = 0;
  let connectionReads = 0;
  const leaseKey = `fixture-turn-${id}`;
  const lease = acquireTurnExecution(leaseKey);
  assert.equal(lease.acquired, true);
  const releaseTurnExecution = () => {
    released++;
    lease.release();
  };
  const connection: { connectionId?: string | null } = { connectionId: CONN };
  if (Object.hasOwn(options, "rawConnectionId")) {
    if (options.rawConnectionId === undefined) delete connection.connectionId;
    else connection.connectionId = options.rawConnectionId;
  }
  const pendingConnectionId = Object.hasOwn(options, "pendingConnectionId")
    ? (options.pendingConnectionId ?? null)
    : CONN;
  const currentConnectionId =
    options.currentConnectionId === undefined ? CONN : options.currentConnectionId;
  const pendingRequestOwner = pendingConnectionId
    ? (options.pendingRequestOwner ?? captureOwner(id))
    : undefined;
  let pipelineError:
    ((event: { error?: unknown; message: string; statusCode: number }) => boolean) | null = null;
  let disconnectFinalize: ((event: { reason: string; duration: number }) => boolean) | null = null;
  const controller = createChatStreamController({
    getClientDisconnectFinalize: () => disconnectFinalize,
    getPipelineStreamError: () => pipelineError,
    pendingScope: {
      id,
      model,
      provider,
      connectionId: pendingConnectionId,
      owner: typeof pendingRequestOwner === "string" ? undefined : pendingRequestOwner,
    },
    finalizePendingScope,
    onDisconnect: null,
    provider,
    model,
    connectionId: connection.connectionId,
    pendingRequestId: id,
    clientResponseFormat: options.clientResponseFormat ?? "openai",
    clientAbortSignal: abort.signal,
    allowCompletedToolHandoffGrace: false,
  });
  controller.signal.addEventListener(
    "abort",
    () => {
      try {
        providerStream?.error(new DOMException("fixture upstream aborted", "AbortError"));
      } catch {
        // A cancelled fixture source may already be settled.
      }
    },
    { once: true }
  );
  const outcome = await runStreamingTail({
    agentGoalPolicy: { detected: false },
    body,
    bodyForCacheWrite: body,
    clientResponseFormat: options.clientResponseFormat ?? "openai",
    targetFormat: options.targetFormat ?? "openai",
    apiKeyInfo: options.glmTransport ? {} : { id: API_KEY_ID },
    provider,
    model,
    effectiveModel: model,
    currentModel: model,
    modelInfo: { model },
    ...connection,
    pendingConnId: pendingConnectionId,
    pendingRequestId: id,
    pendingRequestOwner,
    credentials: { connectionId: currentConnectionId },
    getCurrentConnectionId: () => {
      if (++connectionReads > 1 && failBookkeeping === "connection")
        throw new Error("controlled connection bookkeeping failure");
      return currentConnectionId;
    },
    executor: {},
    startTime: Date.now(),
    stream: true,
    providerResponse: options.readinessFailure
      ? new Response(JSON.stringify({ error: { message: "fixture readiness failure" } }), {
          status: 502,
          headers: { "content-type": "application/json" },
        })
      : options.glmTransport
        ? translateSseResponse(
            new Response(providerBody, { headers: { "content-type": "text/event-stream" } }),
            provider,
            model
          )
        : new Response(providerBody, { headers: { "content-type": "text/event-stream" } }),
    providerHeaders: {},
    translatedBody: body,
    finalBody: body,
    clientRawRequest: new Request("http://localhost/v1/chat/completions"),
    streamController: controller,
    registerStreamFinalizers: (pipeline, disconnect) => {
      pipelineError = pipeline;
      disconnectFinalize = disconnect;
    },
    reqLogger: options.reqLogger ?? {},
    log: { debug: () => {}, warn: () => {}, info: () => {} },
    resolveReportedServiceTier: () => {
      if (failBookkeeping === true) throw new Error("controlled bookkeeping failure");
      return null;
    },
    attachCompressionUsageReceiptAfterAnalytics: () => {},
    persistAttemptLogs: () => {
      writes++;
    },
    persistFailureUsage: () => {
      failures++;
    },
    releaseTurnExecution,
  });
  if (!outcome.carry.turnExecutionHandedOffToStream) {
    // The barrel retains lease ownership when the tail returns before stream handoff.
    releaseTurnExecution();
  }
  let text = "";
  let readError: unknown;
  let abortState: {
    pendingCount: number;
    pendingIds: string[];
    bytes: number;
  } | null = null;
  try {
    options.idle?.();
    if (options.lifecycle === "cancel") {
      await outcome.result.response.body?.cancel("fixture client cancellation");
    } else if (options.lifecycle === "abort") {
      const reader = outcome.result.response.body?.getReader();
      assert.ok(reader);
      const first = await reader.read();
      if (first.value) text = new TextDecoder().decode(first.value);
      abort.abort(new DOMException("fixture client abort", "AbortError"));
      outcome.carry.onPipelineStreamError?.({
        message: "settle fixture disconnect",
        statusCode: 499,
      });
      abortState = {
        pendingCount: history.getPendingRequests().byModel[`${model} (${provider})`],
        pendingIds: [...history.getPendingById().keys()],
        bytes: history.getPendingRetainedBytes(),
      };
      // Assert the abort result separately; cancel only closes the fixture's owned reader.
      await reader.cancel("fixture abort reader cleanup");
      reader.releaseLock();
    } else {
      if (options.lifecycle === "error") {
        providerStream?.error(new Error("fixture upstream failure"));
      }
      if (options.lifecycle === "translated-error") {
        providerStream?.enqueue(
          new TextEncoder().encode(
            'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"GLM translated failure"}}\n\n'
          )
        );
        providerStream?.close();
      }
      text = await outcome.result.response.text();
    }
  } catch (error) {
    if (!options.idle && !options.lifecycle) throw error;
    readError = error;
  }
  if (options.lifecycle === "cancel" || options.lifecycle === "abort") {
    outcome.carry.onPipelineStreamError?.({
      message: "settle fixture disconnect",
      statusCode: 499,
    });
  }
  assert.equal(released, 1);
  assert.equal(getTurnExecutionSnapshot(leaseKey), null);
  return {
    text,
    readError,
    abortState,
    outcome,
    controller,
    writes: () => writes,
    released: () => released,
    failures: () => failures,
  };
}

test.beforeEach(() => {
  history.clearPendingRequests();
  costRules.resetCostData();
});
test.afterEach(async () => {
  await new Promise<void>((resolve) => setImmediate(resolve));
});
test.after(async () => {
  history.clearPendingRequests();
  costRules.resetCostData();
  await cleanup();
});

test("completed retention failure releases only the terminal attempt and its bytes", (t) => {
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes();
  const terminal = pending();
  const fault = new Error("controlled completed storage failure");
  const clone = t.mock.method(globalThis, "structuredClone", () => {
    throw fault;
  });
  assert.throws(
    () => history.finalizePendingRequestById(terminal, { status: 200 }),
    (error) => error === fault
  );
  clone.mock.restore();
  assertOnlySurvivor(survivor, survivorBytes);
  assert.equal(completed.getCompletedDetails().has(terminal), false);
  assert.equal(history.finalizePendingRequestById(terminal, { status: 200 }), false);
  assertOnlySurvivor(survivor, survivorBytes);
});

test("terminal streaming bookkeeping failure releases pending without changing client output", async (t) => {
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes();
  const terminal = pending();
  const warnings: unknown[][] = [];
  t.mock.method(console, "debug", (...args: unknown[]) => {
    warnings.push(args);
  });
  const result = await stream(terminal, true);
  assert.match(result.text, /fixture answer/);
  assert.match(result.text, /\[DONE\]/);
  assert.equal(result.outcome.result.success, true);
  assert.equal(result.released(), 1);
  assert.ok(warnings.some((args) => String(args[0]).includes("onComplete callback error")));
  assertOnlySurvivor(survivor, survivorBytes);
  assert.equal(completed.getCompletedDetails().get(terminal)?.status, 200);
  result.outcome.carry.onPipelineStreamError?.({ message: "late failure", statusCode: 502 });
  assert.equal(result.writes(), 0);
  assert.equal(result.failures(), 0);
  assert.equal(costRules.getCostSummary(API_KEY_ID).totalEntries, 0);
  assertOnlySurvivor(survivor, survivorBytes);
});

test("successful stream records once and duplicate terminal signals leave concurrent attempts untouched", async () => {
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes();
  const terminal = pending();
  const result = await stream(terminal, false);
  for (let i = 0; i < 100 && costRules.getCostSummary(API_KEY_ID).totalEntries === 0; i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  assert.equal(costRules.getCostSummary(API_KEY_ID).totalEntries, 1);
  assert.equal(costRules.getDailyTotal(API_KEY_ID), BILLED_COST);
  assert.match(result.text, /fixture answer/);
  assert.equal(result.writes(), 1);
  assert.equal(result.failures(), 0);
  assert.equal(result.released(), 1);
  assertOnlySurvivor(survivor, survivorBytes);
  const normal = completed.getCompletedDetails().get(terminal);
  assert.equal(normal?.status, 200);
  assert.ok(normal?.clientResponse);
  assert.ok(normal?.providerResponse);
  const stats = completed.getCompletedDetailsCacheStats();
  assert.equal(stats.entries, 1);
  assert.equal(stats.cleanupTimers, 1);
  assert.ok(stats.bytes > 0 && stats.bytes <= completed.MAX_COMPLETED_DETAILS_BYTES);
  for (let i = 0; i < 2; i++) {
    assert.equal(
      result.outcome.carry.onPipelineStreamError?.({
        message: "late terminal signal",
        statusCode: 502,
      }),
      true
    );
    assert.equal(
      result.outcome.carry.onClientDisconnectFinalize?.({ reason: "late disconnect", duration: 0 }),
      true
    );
    assert.equal(history.finalizePendingRequestById(terminal, { status: 499 }), false);
  }
  assert.equal(result.writes(), 1);
  assert.equal(result.failures(), 0);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(costRules.getCostSummary(API_KEY_ID).totalEntries, 1);
  assert.equal(costRules.getDailyTotal(API_KEY_ID), BILLED_COST);
  assert.deepEqual(completed.getCompletedDetailsCacheStats(), stats);
  assert.equal(completed.getCompletedDetails().get(terminal), normal);
  assertOnlySurvivor(survivor, survivorBytes);
});

test("completed cleanup scheduling failure still releases pending after storing the normal record", (t) => {
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes();
  const terminal = pending();
  const fault = new Error("controlled completed timer failure");
  const original = globalThis.setTimeout;
  const timer = t.mock.method(
    globalThis,
    "setTimeout",
    (...args: Parameters<typeof setTimeout>) => {
      if (args[1] === 120_000) throw fault;
      return original(...args);
    }
  );
  assert.throws(
    () =>
      history.finalizePendingRequestById(terminal, {
        status: 200,
        providerResponse: { content: "provider fixture" },
        clientResponse: { content: "client fixture" },
      }),
    (error) => error === fault
  );
  timer.mock.restore();
  assertOnlySurvivor(survivor, survivorBytes);
  assert.equal(completed.getCompletedDetails().get(terminal)?.status, 200);
  assert.equal(history.finalizePendingRequestById(terminal, { status: 200 }), false);
});

test("connection bookkeeping failure still finalizes the exact pending ID", async (t) => {
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes();
  const terminal = pending();
  t.mock.method(console, "debug", () => {});
  const result = await stream(terminal, "connection");
  assert.match(result.text, /fixture answer/);
  assert.equal(result.outcome.result.success, true);
  assert.equal(result.writes(), 0);
  assertOnlySurvivor(survivor, survivorBytes);
  assert.equal(completed.getCompletedDetails().get(terminal)?.connectionId, CONN);
});

test("terminal metadata normalization failure also releases the pending attempt", () => {
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes();
  const terminal = pending();
  const fault = new Error("controlled terminal metadata failure");
  assert.throws(
    () =>
      history.finalizePendingRequestById(terminal, {
        get status(): number {
          throw fault;
        },
      }),
    (error) => error === fault
  );
  assertOnlySurvivor(survivor, survivorBytes);
  assert.equal(completed.getCompletedDetails().has(terminal), false);
});

test("normal finalization releases the original pending bytes when response metadata changes", () => {
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes();
  const terminal = pending();
  const metadata = {
    status: 200,
    providerRequest: { model: MODEL, messages: [{ role: "user", content: "replaced request" }] },
    providerResponse: { content: "provider fixture".repeat(10) },
    clientResponse: { content: "client fixture".repeat(20) },
  };
  assert.equal(history.finalizePendingRequestById(terminal, metadata), true);
  assertOnlySurvivor(survivor, survivorBytes);
  const normal = completed.getCompletedDetails().get(terminal);
  assert.equal(normal?.status, 200);
  assert.deepEqual(normal?.providerRequest, metadata.providerRequest);
  assert.deepEqual(normal?.providerResponse, metadata.providerResponse);
  assert.deepEqual(normal?.clientResponse, metadata.clientResponse);
  assert.ok(typeof normal?.completedAt === "number");
  assert.ok(typeof normal?.durationMs === "number" && normal.durationMs >= 0);
  assert.equal(history.finalizePendingRequestById(terminal, { status: 499 }), false);
  assertOnlySurvivor(survivor, survivorBytes);
  assert.equal(history.finalizePendingRequestById(survivor, metadata), true);
  assert.equal(history.getPendingById().size, 0);
  assert.equal(history.getPendingRetainedBytes(), 0);
  assert.deepEqual(Object.keys(history.getPendingRequests().byModel), []);
  assert.deepEqual(Object.keys(history.getPendingRequests().byAccount), []);
});

for (const formats of [
  { name: "passthrough", targetFormat: "openai", clientResponseFormat: "openai" },
  { name: "translation", targetFormat: "openai", clientResponseFormat: "claude" },
  {
    name: "Responses translation",
    targetFormat: "openai-responses",
    clientResponseFormat: "openai",
  },
]) {
  for (const owner of [
    { name: "raw connection", rawConnectionId: CONN },
    { name: "credential connection with null raw ID", rawConnectionId: null },
    { name: "credential connection with omitted raw ID", rawConnectionId: undefined },
    {
      name: "credentials rotated after pending admission",
      rawConnectionId: null,
      currentConnectionId: "terminal-provider-connection",
    },
  ]) {
    test(`${formats.name}: ${owner.name}: idle failure with a throwing completion callback preserves the newer sibling`, async (t) => {
      const terminal = pending();
      const terminalBytes = history.getPendingRetainedBytes();
      const survivor = pending();
      const survivorBytes = history.getPendingRetainedBytes() - terminalBytes;
      let idleTick: (() => void) | undefined;
      const original = globalThis.setInterval;
      t.mock.method(globalThis, "setInterval", (callback: () => void, delay: number) => {
        if (delay === 10_000) idleTick = callback;
        return original(callback, delay);
      });
      const warnings: unknown[][] = [];
      t.mock.method(console, "debug", (...args: unknown[]) => warnings.push(args));
      t.mock.method(console, "warn", () => {});
      const result = await stream(terminal, true, {
        ...formats,
        rawConnectionId: owner.rawConnectionId,
        currentConnectionId: owner.currentConnectionId,
        idle: () => {
          assert.ok(idleTick, "the real SSE watchdog must have registered");
          const now = Date.now();
          t.mock.method(Date, "now", () => now + STREAM_IDLE_TIMEOUT_MS + 1_000);
          idleTick();
        },
      });
      assert.ok(
        warnings.some((args) => String(args[0]).includes("onFailure callback error (idle_timeout)"))
      );
      assertOnlySurvivor(survivor, survivorBytes);
      assert.match(result.text, /Idle timeout/);
      assert.equal(result.readError, undefined);
      assert.equal(completed.getCompletedDetails().get(terminal)?.status, 504);
      assert.equal(result.writes(), 0);
      assert.equal(result.failures(), 0);
      assert.equal(result.released(), 1);
      assert.equal(costRules.getCostSummary(API_KEY_ID).totalEntries, 0);
      for (let i = 0; i < 2; i++) {
        assert.equal(
          result.outcome.carry.onPipelineStreamError?.({
            message: "duplicate timeout",
            statusCode: 504,
          }),
          true
        );
        assert.equal(
          result.outcome.carry.onClientDisconnectFinalize?.({
            reason: "duplicate timeout",
            duration: 0,
          }),
          true
        );
        assert.equal(history.finalizePendingRequestById(terminal, { status: 499 }), false);
        assertOnlySurvivor(survivor, survivorBytes);
      }
      assert.equal(result.released(), 1);
      assert.equal(result.writes(), 0);
      assert.equal(result.failures(), 0);
    });
  }
}

for (const rawConnectionId of [null, undefined]) {
  test(`credential-only Gemini stream preserves follow-up signatures with raw ID ${String(rawConnectionId)}`, async () => {
    clearGeminiThoughtSignatures();
    try {
      const model = "gemini-2.5-flash";
      const signature = "fixture-gemini-thought-signature";
      const id = history.trackPendingRequest(model, "gemini", CONN, true);
      assert.ok(id);
      const chunk = {
        candidates: [
          {
            content: {
              role: "model",
              parts: [
                {
                  functionCall: { id: "call_fixture", name: "lookup", args: { place: "Tokyo" } },
                  thoughtSignature: signature,
                },
              ],
            },
            finishReason: "STOP",
          },
        ],
        usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 2, totalTokenCount: 12 },
      };
      const result = await stream(id, false, {
        provider: "gemini",
        model,
        targetFormat: "gemini",
        rawConnectionId,
        wire: `data: ${JSON.stringify(chunk)}\n\n`,
      });
      type ToolCall = { id?: string };
      const calls = result.text
        .split("\n")
        .filter((line) => line.startsWith("data: {"))
        .flatMap((line) => {
          const payload = JSON.parse(line.slice(6)) as {
            choices?: Array<{ delta?: { tool_calls?: ToolCall[] } }>;
          };
          return payload.choices?.flatMap((choice) => choice.delta?.tool_calls ?? []) ?? [];
        });
      const callId = calls.find((call) => call.id)?.id;
      assert.ok(callId, "the real Gemini stream must emit a client tool-call ID");
      const followup = translateRequest(
        "openai",
        "gemini",
        model,
        {
          model,
          messages: [
            { role: "user", content: "Look up Tokyo" },
            {
              role: "assistant",
              content: null,
              tool_calls: [
                {
                  id: callId,
                  type: "function",
                  function: { name: "lookup", arguments: JSON.stringify({ place: "Tokyo" }) },
                },
              ],
            },
            { role: "tool", tool_call_id: callId, content: "sunny" },
          ],
        },
        false,
        { connectionId: CONN },
        "gemini",
        null,
        { signatureNamespace: rawConnectionId }
      ) as {
        contents: Array<{ parts: Array<{ functionCall?: unknown; thoughtSignature?: string }> }>;
      };
      const part = followup.contents
        .flatMap((content) => content.parts)
        .find((entry) => entry.functionCall);
      assert.equal(part?.thoughtSignature, signature);
      assert.equal(history.getPendingById().has(id), false);
      assert.equal(history.getPendingRetainedBytes(), 0);
    } finally {
      clearGeminiThoughtSignatures();
    }
  });
}

async function idleFallback(
  t: TestContext,
  id: FixturePendingIdentity | null,
  connectionId: string | null,
  reqLogger: Awaited<ReturnType<typeof createRequestLogger>> | null = null
) {
  let idleTick: (() => void) | undefined;
  const original = globalThis.setInterval;
  t.mock.method(globalThis, "setInterval", (callback: () => void, delay: number) => {
    if (delay === 10_000) idleTick = callback;
    return original(callback, delay);
  });
  t.mock.method(console, "warn", () => {});
  const warnings: unknown[][] = [];
  t.mock.method(console, "debug", (...args: unknown[]) => warnings.push(args));
  const transform = createPassthroughStreamWithLogger(
    PROVIDER,
    reqLogger,
    null,
    MODEL,
    connectionId,
    null,
    null,
    null,
    () => {
      throw new Error("controlled fallback callback failure");
    },
    null,
    null,
    null,
    id
  );
  assert.ok(idleTick);
  const now = Date.now();
  t.mock.method(Date, "now", () => now + STREAM_IDLE_TIMEOUT_MS + 1_000);
  idleTick();
  await assert.rejects(new Response(transform.readable).text(), /Idle timeout/);
  assert.ok(
    warnings.some((args) => String(args[0]).includes("onFailure callback error (idle_timeout)"))
  );
}

test("fallback resolves a live explicit ID's registered owner with no raw connection", async (t) => {
  const terminal = pending();
  const terminalBytes = history.getPendingRetainedBytes();
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes() - terminalBytes;
  await idleFallback(t, captureOwner(terminal), null);
  assertOnlySurvivor(survivor, survivorBytes);
});

test("an explicit ID absent before stream construction leaves its sibling counters intact", async (t) => {
  const terminal = pending();
  history.trackPendingRequest(MODEL, PROVIDER, CONN, false, undefined, terminal);
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes();
  await idleFallback(t, terminal, null);
  assertOnlySurvivor(survivor, survivorBytes);
});

test("fallback without an explicit ID preserves legacy FIFO cleanup", async (t) => {
  pending();
  const firstBytes = history.getPendingRetainedBytes();
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes() - firstBytes;
  await idleFallback(t, null, CONN);
  assertOnlySurvivor(survivor, survivorBytes);
});

test("fallback without an explicit ID preserves model-only cleanup", async (t) => {
  history.trackPendingRequest(MODEL, PROVIDER, null, true);
  assert.equal(history.getPendingRequests().byModel[KEY], 1);
  await idleFallback(t, null, null);
  assert.equal(history.getPendingRequests().byModel[KEY], 0);
  assert.equal(history.getPendingById().size, 0);
  assert.equal(history.getPendingRetainedBytes(), 0);
});

for (const mode of ["success", "idle", "cancel", "abort", "error", "readiness"] as const) {
  test(`model-only generated-ID ${mode} releases one of two same-model admissions`, async (t) => {
    assert.equal(history.trackPendingRequest(MODEL, PROVIDER, null, true), undefined);
    assert.equal(history.trackPendingRequest(MODEL, PROVIDER, null, true), undefined);
    assert.equal(history.getPendingRequests().byModel[KEY], 2);
    let idleTick: (() => void) | undefined;
    const original = globalThis.setInterval;
    t.mock.method(globalThis, "setInterval", (callback: () => void, delay: number) => {
      if (delay === 10_000) idleTick = callback;
      return original(callback, delay);
    });
    t.mock.method(console, "warn", () => {});
    t.mock.method(console, "debug", () => {});
    const result = await stream(`generated-model-only-log-${mode}`, mode === "idle", {
      rawConnectionId: null,
      pendingConnectionId: null,
      currentConnectionId: null,
      readinessFailure: mode === "readiness",
      lifecycle: mode === "cancel" || mode === "abort" || mode === "error" ? mode : undefined,
      idle:
        mode === "idle"
          ? () => {
              assert.ok(idleTick);
              const now = Date.now();
              t.mock.method(Date, "now", () => now + STREAM_IDLE_TIMEOUT_MS + 1_000);
              idleTick();
            }
          : undefined,
    });
    if (mode === "success") assert.match(result.text, /fixture answer/);
    if (mode === "abort") {
      assert.deepEqual(result.abortState, {
        pendingCount: 1,
        pendingIds: [],
        bytes: 0,
      });
    }
    if (mode === "idle") assert.match(result.text, /Idle timeout/);
    if (mode === "readiness") assert.equal(result.outcome.result.success, false);
    assert.equal(result.released(), 1);
    assert.equal(history.getPendingRequests().byModel[KEY], 1);
    assert.equal(history.getPendingById().size, 0);
    assert.equal(history.getPendingRetainedBytes(), 0);
    for (let i = 0; i < 2; i++) {
      result.controller.handleDisconnect("duplicate disconnect");
      result.outcome.carry.onPipelineStreamError?.({ message: "duplicate error", statusCode: 502 });
      assert.equal(history.getPendingRequests().byModel[KEY], 1);
      assert.equal(result.released(), 1);
    }
  });
}

for (const rawConnectionId of [null, undefined]) {
  for (const stale of [false, true]) {
    test(`real indexed controller cancellation preserves its sibling (raw ${String(rawConnectionId)}, stale ${stale})`, async () => {
      const terminal = pending();
      const terminalBytes = history.getPendingRetainedBytes();
      if (stale) history.trackPendingRequest(MODEL, PROVIDER, CONN, false, undefined, terminal);
      const survivor = pending();
      const survivorBytes = stale
        ? history.getPendingRetainedBytes()
        : history.getPendingRetainedBytes() - terminalBytes;
      const result = await stream(terminal, false, { rawConnectionId, lifecycle: "cancel" });
      assert.equal(result.released(), 1);
      assertOnlySurvivor(survivor, survivorBytes);
      result.outcome.carry.onPipelineStreamError?.({
        message: "settle cancellation",
        statusCode: 499,
      });
      result.controller.handleDisconnect("duplicate cancellation");
      assertOnlySurvivor(survivor, survivorBytes);
      assert.equal(result.released(), 1);
    });
  }
}

async function captureLogger(id: string) {
  const logger = await createRequestLogger("openai", "openai", MODEL, {
    requestId: id,
    model: MODEL,
    provider: PROVIDER,
    connectionId: CONN,
  });
  logger.appendProviderChunk("captured provider fixture ".repeat(256));
  logger.appendOpenAIChunk("captured intermediate fixture");
  logger.appendConvertedChunk("captured client fixture");
  assert.match(
    history.getPendingById().get(id)?.streamChunks?.provider?.[0] ?? "",
    /captured provider fixture/
  );
  assert.ok(logger.getPipelinePayloads()?.streamChunks?.client?.length);
  return logger;
}

for (const failure of ["none", "storage", "scheduling"] as const) {
  test(`real logger capture and preview updates conserve sibling bytes after ${failure} finalization`, async (t) => {
    const survivor = pending();
    const survivorBytes = history.getPendingRetainedBytes();
    const terminal = pending();
    const logger = await captureLogger(terminal);
    const providerResponse = { content: "updated provider preview".repeat(10) };
    assert.equal(history.updatePendingRequestById(terminal, { providerResponse }), true);
    logger.appendProviderChunk("captured after the first preview update".repeat(64));
    const clientResponse = { content: "updated client preview".repeat(20) };
    history.updatePendingRequest(MODEL, PROVIDER, CONN, { clientResponse });
    const detail = history.getPendingById().get(terminal);
    assert.deepEqual(detail?.providerResponse, providerResponse);
    assert.deepEqual(detail?.clientResponse, clientResponse);
    assert.equal(detail?.streamChunks?.provider?.length, 2);
    const fault = new Error(`controlled captured ${failure} failure`);
    const clone =
      failure === "storage"
        ? t.mock.method(globalThis, "structuredClone", () => {
            throw fault;
          })
        : null;
    const original = globalThis.setTimeout;
    const timer =
      failure === "scheduling"
        ? t.mock.method(globalThis, "setTimeout", (...args: Parameters<typeof setTimeout>) => {
            if (args[1] === 120_000) throw fault;
            return original(...args);
          })
        : null;
    if (failure === "none") {
      assert.equal(history.finalizePendingRequestById(terminal, { status: 200 }), true);
    } else {
      assert.throws(
        () => history.finalizePendingRequestById(terminal, { status: 200 }),
        (error) => error === fault
      );
    }
    clone?.mock.restore();
    timer?.mock.restore();
    assertOnlySurvivor(survivor, survivorBytes);
    if (failure !== "storage") {
      const stored = completed.getCompletedDetails().get(terminal);
      assert.deepEqual(stored?.providerResponse, providerResponse);
      assert.deepEqual(stored?.clientResponse, clientResponse);
      assert.match(stored?.streamChunks?.provider?.[0] ?? "", /captured provider fixture/);
      assert.equal(stored?.streamChunks?.provider?.length, 2);
    }
    for (let i = 0; i < 2; i++) {
      assert.equal(history.finalizePendingRequestById(terminal, { status: 499 }), false);
      history.trackPendingRequest(MODEL, PROVIDER, CONN, false, undefined, terminal);
      assertOnlySurvivor(survivor, survivorBytes);
    }
    assert.equal(history.finalizePendingRequestById(survivor, { status: 200 }), true);
    assert.equal(history.getPendingRetainedBytes(), 0);
    assert.equal(history.getPendingById().size, 0);
  });
}

for (const formats of [
  { name: "passthrough", targetFormat: "openai", clientResponseFormat: "openai" },
  { name: "translation", targetFormat: "openai", clientResponseFormat: "claude" },
  {
    name: "Responses translation",
    targetFormat: "openai-responses",
    clientResponseFormat: "openai",
  },
]) {
  for (const mode of ["success", "bookkeeping", "idle", "cancel"] as const) {
    test(`real logger ${formats.name} ${mode} preserves captured output and sibling accounting`, async (t) => {
      const survivor = pending();
      const survivorBytes = history.getPendingRetainedBytes();
      const terminal = pending();
      const logger = await captureLogger(terminal);
      history.updatePendingRequestById(terminal, {
        providerRequest: {
          model: MODEL,
          messages: [{ role: "user", content: "updated after capture" }],
        },
      });
      let idleTick: (() => void) | undefined;
      const original = globalThis.setInterval;
      t.mock.method(globalThis, "setInterval", (callback: () => void, delay: number) => {
        if (delay === 10_000) idleTick = callback;
        return original(callback, delay);
      });
      t.mock.method(console, "debug", () => {});
      t.mock.method(console, "warn", () => {});
      const responseMessage = {
        type: "message",
        id: "msg_fixture",
        role: "assistant",
        status: "completed",
        content: [{ type: "output_text", text: "fixture answer" }],
      };
      const responsesWire = [
        { type: "response.created", response: { id: "resp_fixture", status: "in_progress" } },
        {
          type: "response.output_item.added",
          output_index: 0,
          item: { ...responseMessage, status: "in_progress", content: [] },
        },
        {
          type: "response.output_text.delta",
          item_id: "msg_fixture",
          output_index: 0,
          content_index: 0,
          delta: "fixture answer",
        },
        { type: "response.output_item.done", output_index: 0, item: responseMessage },
        {
          type: "response.completed",
          response: {
            id: "resp_fixture",
            status: "completed",
            output: [responseMessage],
            usage: { input_tokens: 10, output_tokens: 2, total_tokens: 12 },
          },
        },
      ]
        .map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`)
        .join("");
      const result = await stream(terminal, mode === "bookkeeping" || mode === "idle", {
        ...formats,
        wire: formats.targetFormat === "openai-responses" ? responsesWire : undefined,
        rawConnectionId: null,
        reqLogger: logger,
        lifecycle: mode === "cancel" ? "cancel" : undefined,
        idle:
          mode === "idle"
            ? () => {
                assert.ok(idleTick, "the real SSE watchdog must have registered");
                const now = Date.now();
                t.mock.method(Date, "now", () => now + STREAM_IDLE_TIMEOUT_MS + 1_000);
                idleTick();
              }
            : undefined,
      });
      t.after(() => result.controller.handleDisconnect("close captured fixture after assertions"));
      assertOnlySurvivor(survivor, survivorBytes);
      assert.ok(logger.getPipelinePayloads()?.streamChunks?.provider?.length);
      if (mode !== "cancel") {
        const stored = completed.getCompletedDetails().get(terminal);
        assert.match(stored?.streamChunks?.provider?.[0] ?? "", /captured provider fixture/);
        assert.match(result.text, mode === "idle" ? /Idle timeout/ : /fixture answer|\[DONE\]/);
        assert.ok((stored?.streamChunks?.provider?.length ?? 0) > 1);
      }
      const writes = result.writes();
      const failures = result.failures();
      for (let i = 0; i < 2; i++) {
        result.controller.handleDisconnect("duplicate captured terminal signal");
        result.outcome.carry.onPipelineStreamError?.({
          message: "duplicate captured error",
          statusCode: 502,
        });
        assert.equal(history.finalizePendingRequestById(terminal, { status: 499 }), false);
        assertOnlySurvivor(survivor, survivorBytes);
        assert.equal(result.released(), 1);
        assert.equal(result.writes(), writes);
        assert.equal(result.failures(), failures);
      }
    });
  }
}

test("real captured logger fallback releases its exact ID and preserves sibling bytes", async (t) => {
  const terminal = pending();
  const terminalBytes = history.getPendingRetainedBytes();
  const survivor = pending();
  const survivorBytes = history.getPendingRetainedBytes() - terminalBytes;
  const logger = await captureLogger(terminal);
  history.updatePendingRequestById(terminal, { clientResponse: { content: "fallback preview" } });
  await idleFallback(t, captureOwner(terminal), null, logger);
  assertOnlySurvivor(survivor, survivorBytes);
  assert.match(
    logger.getPipelinePayloads()?.streamChunks?.provider?.[0] ?? "",
    /captured provider fixture/
  );
  for (let i = 0; i < 2; i++) {
    history.trackPendingRequest(MODEL, PROVIDER, CONN, false, undefined, terminal);
    assert.equal(history.finalizePendingRequestById(terminal, { status: 499 }), false);
    assertOnlySurvivor(survivor, survivorBytes);
  }
});

function correlatedAttempts(sameConnection: boolean) {
  const connectionB = sameConnection ? CONN : "terminal-hedge-B";
  const correlationId = "shared-client-correlation";
  const id = history.trackPendingRequest(MODEL, PROVIDER, CONN, true, {
    correlationId,
    clientRequest: { messages: [{ role: "user", content: "attempt A".repeat(4) }] },
  });
  assert.ok(id);
  const ownerA = captureOwner(id);
  const detailA = history.getPendingById().get(id);
  assert.ok(detailA);
  const bytesA = history.getPendingRetainedBytes();
  const secondId = history.trackPendingRequest(MODEL, PROVIDER, connectionB, true, {
    correlationId,
    clientRequest: { messages: [{ role: "user", content: "attempt B".repeat(16) }] },
  });
  assert.equal(secondId, id);
  const ownerB = captureOwner(id);
  const detailB = history.getPendingById().get(id);
  assert.ok(detailB);
  assert.notEqual(detailB, detailA);
  const bytesB = history.getPendingRetainedBytes() - bytesA;
  const bucket = () => {
    const state = history.getPendingRequests() as ReturnType<typeof history.getPendingRequests> & {
      details: Record<string, Record<string, Array<typeof detailA>>>;
    };
    return Object.values(state.details).flatMap((models) => Object.values(models).flat());
  };
  return { id, ownerA, ownerB, detailA, detailB, bytesA, bytesB, connectionB, bucket };
}

for (const scenario of [
  {
    name: "different connections: A completes",
    sameConnection: false,
    terminal: "A",
    mode: "success",
  },
  {
    name: "different connections: A idle bookkeeping failure",
    sameConnection: false,
    terminal: "A",
    mode: "idle",
  },
  {
    name: "different connections: A lower fallback",
    sameConnection: false,
    terminal: "A",
    mode: "fallback",
  },
  {
    name: "different connections: A controller cancellation",
    sameConnection: false,
    terminal: "A",
    mode: "cancel",
  },
  { name: "same connection: A completes", sameConnection: true, terminal: "A", mode: "success" },
  {
    name: "same connection: B completes first",
    sameConnection: true,
    terminal: "B",
    mode: "success",
  },
  {
    name: "same connection: B idle bookkeeping failure",
    sameConnection: true,
    terminal: "B",
    mode: "idle",
  },
] as const) {
  test(`admission ownership: ${scenario.name}`, async (t) => {
    const pair = correlatedAttempts(scenario.sameConnection);
    const terminalOwner = scenario.terminal === "A" ? pair.ownerA : pair.ownerB;
    const terminalConnection = scenario.terminal === "A" ? CONN : pair.connectionB;
    const survivor = scenario.terminal === "A" ? pair.detailB : pair.detailA;
    const survivorOwner = scenario.terminal === "A" ? pair.ownerB : pair.ownerA;
    const survivorBytes = scenario.terminal === "A" ? pair.bytesB : pair.bytesA;
    if (scenario.mode === "fallback") {
      await idleFallback(t, terminalOwner, terminalConnection);
    } else {
      let idleTick: (() => void) | undefined;
      const original = globalThis.setInterval;
      t.mock.method(globalThis, "setInterval", (callback: () => void, delay: number) => {
        if (delay === 10_000) idleTick = callback;
        return original(callback, delay);
      });
      t.mock.method(console, "debug", () => {});
      t.mock.method(console, "warn", () => {});
      const result = await stream(pair.id, scenario.mode === "idle", {
        rawConnectionId: terminalConnection,
        currentConnectionId: terminalConnection,
        pendingConnectionId: terminalConnection,
        pendingRequestOwner: terminalOwner,
        lifecycle: scenario.mode === "cancel" ? "cancel" : undefined,
        idle:
          scenario.mode === "idle"
            ? () => {
                assert.ok(idleTick);
                const now = Date.now();
                t.mock.method(Date, "now", () => now + STREAM_IDLE_TIMEOUT_MS + 1_000);
                idleTick();
              }
            : undefined,
      });
      t.after(() => result.controller.handleDisconnect("close shared-correlation fixture"));
      assert.equal(result.outcome.result.response.headers.get("x-omniroute-request-id"), pair.id);
      assert.equal(result.released(), 1);
      result.outcome.carry.onPipelineStreamError?.({
        message: "duplicate terminal callback",
        statusCode: 502,
      });
      result.controller.handleDisconnect("duplicate shared-correlation disconnect");
      assert.equal(result.released(), 1);
    }
    assert.deepEqual(pair.bucket(), [survivor]);
    assert.equal(history.getPendingRetainedBytes(), survivorBytes);
    assert.equal(history.getPendingRequests().byModel[KEY], 1);
    assert.equal(history.getPendingRequests().byAccount[survivor.connectionId!]?.[KEY], 1);
    assert.equal(
      history.getPendingById().get(pair.id),
      scenario.terminal === "A" ? survivor : undefined
    );
    assert.equal(history.finalizePendingRequestById(terminalOwner, { status: 499 }), false);
    history.trackPendingRequest(
      MODEL,
      PROVIDER,
      terminalConnection,
      false,
      undefined,
      terminalOwner
    );
    assert.deepEqual(pair.bucket(), [survivor]);
    assert.equal(history.getPendingRetainedBytes(), survivorBytes);
    assert.equal(history.finalizePendingRequestById(survivorOwner, { status: 200 }), true);
    assert.equal(history.getPendingRetainedBytes(), 0);
    assert.equal(history.getPendingById().size, 0);
    assert.deepEqual(pair.bucket(), []);
    assert.equal(history.getPendingRequests().byModel[KEY] ?? 0, 0);
  });
}

for (const sameConnection of [false, true]) {
  test(`admission ownership: late A after sequential ID reuse (same connection ${sameConnection})`, async (t) => {
    const correlationId = "sequential-correlation";
    const id = history.trackPendingRequest(MODEL, PROVIDER, CONN, true, { correlationId });
    assert.ok(id);
    const ownerA = captureOwner(id);
    let idleTick: (() => void) | undefined;
    const original = globalThis.setInterval;
    t.mock.method(globalThis, "setInterval", (callback: () => void, delay: number) => {
      if (delay === 10_000) idleTick = callback;
      return original(callback, delay);
    });
    t.mock.method(console, "debug", () => {});
    t.mock.method(console, "warn", () => {});
    const transform = createPassthroughStreamWithLogger(
      PROVIDER,
      null,
      null,
      MODEL,
      CONN,
      null,
      null,
      null,
      () => {
        throw new Error("controlled late A fallback failure");
      },
      null,
      null,
      null,
      ownerA
    );
    history.trackPendingRequest(MODEL, PROVIDER, CONN, false, undefined, ownerA);
    const connectionB = sameConnection ? CONN : "sequential-connection-B";
    assert.equal(
      history.trackPendingRequest(MODEL, PROVIDER, connectionB, true, { correlationId }),
      id
    );
    const ownerB = captureOwner(id);
    const detailB = history.getPendingById().get(id);
    const bytesB = history.getPendingRetainedBytes();
    assert.ok(idleTick);
    const now = Date.now();
    t.mock.method(Date, "now", () => now + STREAM_IDLE_TIMEOUT_MS + 1_000);
    idleTick();
    await assert.rejects(new Response(transform.readable).text(), /Idle timeout/);
    assert.equal(history.getPendingById().get(id), detailB);
    assert.equal(history.getPendingRequests().byModel[KEY], 1);
    assert.equal(history.getPendingRequests().byAccount[connectionB]?.[KEY], 1);
    assert.equal(history.getPendingRetainedBytes(), bytesB);
    assert.equal(history.updatePendingRequestById(ownerA, { status: 499 }), false);
    assert.equal(history.finalizePendingRequestById(ownerA, { status: 499 }), false);
    assert.equal(history.finalizePendingRequestById(ownerB, { status: 200 }), true);
    assert.equal(history.getPendingRetainedBytes(), 0);
  });

  test(`admission ownership: scoped updates stay on A (same connection ${sameConnection})`, () => {
    const pair = correlatedAttempts(sameConnection);
    const scopeA = {
      id: pair.id,
      model: MODEL,
      provider: PROVIDER,
      connectionId: CONN,
      owner: typeof pair.ownerA === "string" ? undefined : pair.ownerA,
    };
    const payload = { content: "A-specific preview" };
    updatePendingScope(scopeA, { providerResponse: payload });
    assert.deepEqual(pair.detailA.providerResponse, payload);
    assert.equal(pair.detailB.providerResponse, undefined);
    finalizePendingScope(scopeA, { status: 200 });
    assert.deepEqual(pair.bucket(), [pair.detailB]);
    assert.equal(history.getPendingById().get(pair.id), pair.detailB);
    assert.equal(history.getPendingRetainedBytes(), pair.bytesB);
    assert.equal(history.finalizePendingRequestById(pair.ownerB, { status: 200 }), true);
    assert.equal(history.getPendingRetainedBytes(), 0);
  });
}

for (const failure of ["normalization", "storage", "scheduling"] as const) {
  test(`admission ownership: B-first ${failure} fault preserves older A in the same bucket`, (t) => {
    const pair = correlatedAttempts(true);
    const fault = new Error(`controlled owned ${failure} fault`);
    if (failure === "storage")
      t.mock.method(globalThis, "structuredClone", () => {
        throw fault;
      });
    const original = globalThis.setTimeout;
    if (failure === "scheduling")
      t.mock.method(globalThis, "setTimeout", (...args: Parameters<typeof setTimeout>) => {
        if (args[1] === 120_000) throw fault;
        return original(...args);
      });
    const metadata =
      failure === "normalization"
        ? {
            get status(): number {
              throw fault;
            },
          }
        : { status: 200 };
    assert.throws(
      () => history.finalizePendingRequestById(pair.ownerB, metadata),
      (error) => error === fault
    );
    t.mock.restoreAll();
    assert.deepEqual(pair.bucket(), [pair.detailA]);
    assert.equal(history.getPendingById().has(pair.id), false);
    assert.equal(history.getPendingRetainedBytes(), pair.bytesA);
    assert.equal(history.getPendingRequests().byModel[KEY], 1);
    assert.equal(history.finalizePendingRequestById(pair.ownerB, { status: 499 }), false);
    assert.equal(history.finalizePendingRequestById(pair.ownerA, { status: 200 }), true);
    assert.equal(history.getPendingRetainedBytes(), 0);
  });
}

test("admission ownership: an empty indexed owner never decrements model-only counters", () => {
  history.trackPendingRequest(MODEL, PROVIDER, null, true);
  history.trackPendingRequest(MODEL, PROVIDER, null, true);
  const missing = captureOwner("withdrawn-indexed-attempt");
  history.trackPendingRequest(MODEL, PROVIDER, null, false, undefined, missing);
  assert.equal(history.getPendingRequests().byModel[KEY], 2);
  assert.equal(history.updatePendingRequestById(missing, { status: 499 }), false);
  assert.equal(history.finalizePendingRequestById(missing, { status: 499 }), false);
  assert.equal(history.getPendingRetainedBytes(), 0);
});

for (const sameConnection of [false, true]) {
  test(`legacy string fallback stays connection anchored after correlation reuse (same connection ${sameConnection})`, async (t) => {
    const pair = correlatedAttempts(sameConnection);
    await idleFallback(t, pair.id, CONN);
    assert.deepEqual(pair.bucket(), [pair.detailB]);
    assert.equal(history.getPendingById().get(pair.id), pair.detailB);
    assert.equal(history.getPendingRetainedBytes(), pair.bytesB);
    assert.equal(history.getPendingRequests().byModel[KEY], 1);
    assert.equal(history.finalizePendingRequestById(pair.ownerB, { status: 200 }), true);
    assert.equal(history.getPendingRetainedBytes(), 0);
  });
}

test("legacy string fallback without an owner connection does not promote the latest map entry", async (t) => {
  const pair = correlatedAttempts(false);
  await idleFallback(t, pair.id, null);
  assert.deepEqual(pair.bucket(), [pair.detailA, pair.detailB]);
  assert.equal(history.getPendingById().get(pair.id), pair.detailB);
  assert.equal(history.getPendingRetainedBytes(), pair.bytesA + pair.bytesB);
  assert.equal(history.getPendingRequests().byModel[KEY], 2);
  assert.equal(history.finalizePendingRequestById(pair.ownerA, { status: 200 }), true);
  assert.equal(history.finalizePendingRequestById(pair.ownerB, { status: 200 }), true);
  assert.equal(history.getPendingRetainedBytes(), 0);
});

test("owned finalization rechecks object membership after normalization withdraws the attempt", () => {
  const pair = correlatedAttempts(true);
  let withdrawals = 0;
  assert.equal(
    history.finalizePendingRequestById(pair.ownerA, {
      get status() {
        if (withdrawals++ === 0) {
          history.trackPendingRequest(MODEL, PROVIDER, CONN, false, undefined, pair.ownerA);
        }
        return 200;
      },
    }),
    true
  );
  assert.deepEqual(pair.bucket(), [pair.detailB]);
  assert.equal(history.getPendingById().get(pair.id), pair.detailB);
  assert.equal(history.getPendingRetainedBytes(), pair.bytesB);
  assert.equal(history.getPendingRequests().byModel[KEY], 1);
  assert.equal(history.finalizePendingRequestById(pair.ownerB, { status: 200 }), true);
  assert.equal(history.getPendingRetainedBytes(), 0);
});

test("reentrant pending update cannot charge an owned attempt removed during normalization", () => {
  const pair = correlatedAttempts(true);
  let removed = false;
  const updated = history.updatePendingRequestById(pair.ownerA, {
    clientResponse: { content: "new preview for withdrawn A" },
    get status() {
      if (!removed) {
        removed = true;
        history.trackPendingRequest(MODEL, PROVIDER, CONN, false, undefined, pair.ownerA);
      }
      return 200;
    },
  });
  assert.deepEqual(pair.bucket(), [pair.detailB]);
  assert.equal(history.getPendingById().get(pair.id), pair.detailB);
  assert.equal(history.getPendingRetainedBytes(), pair.bytesB);
  assert.equal(updated, false);
  assert.equal(history.finalizePendingRequestById(pair.ownerB, { status: 200 }), true);
  assert.equal(history.getPendingRetainedBytes(), 0);
});

test("reentrant preview update during finalization releases the attempt's current charge", () => {
  const pair = correlatedAttempts(true);
  let updated = false;
  assert.equal(
    history.finalizePendingRequestById(pair.ownerA, {
      get status() {
        if (!updated) {
          updated = true;
          history.updatePendingRequestById(pair.ownerA, {
            clientResponse: { content: "reentrant charged A preview" },
          });
        }
        return 200;
      },
    }),
    true
  );
  assert.deepEqual(pair.bucket(), [pair.detailB]);
  assert.equal(history.getPendingById().get(pair.id), pair.detailB);
  assert.equal(history.getPendingRetainedBytes(), pair.bytesB);
  assert.equal(history.finalizePendingRequestById(pair.ownerB, { status: 200 }), true);
  assert.equal(history.getPendingRetainedBytes(), 0);
});

test("legacy preview update rechecks its selected detail after normalization withdraws it", () => {
  const pair = correlatedAttempts(true);
  let removed = false;
  history.updatePendingRequest(MODEL, PROVIDER, CONN, {
    clientResponse: { content: "new preview for withdrawn B" },
    get status() {
      if (!removed) {
        removed = true;
        history.trackPendingRequest(MODEL, PROVIDER, CONN, false, undefined, pair.ownerB);
      }
      return 200;
    },
  });
  assert.deepEqual(pair.bucket(), [pair.detailA]);
  assert.equal(history.getPendingRetainedBytes(), pair.bytesA);
  assert.equal(history.finalizePendingRequestById(pair.ownerA, { status: 200 }), true);
  assert.equal(history.getPendingRetainedBytes(), 0);
});

const GLM_PROVIDER = "glm";
const GLM_MODEL = "glm-5";
const GLM_KEY = GLM_MODEL + " (" + GLM_PROVIDER + ")";
const GLM_PARTIAL_WIRE = [
  [
    "message_start",
    {
      type: "message_start",
      message: {
        id: "msg_glm",
        type: "message",
        role: "assistant",
        model: GLM_MODEL,
        content: [],
        stop_reason: null,
        usage: { input_tokens: 10, output_tokens: 0 },
      },
    },
  ],
  [
    "content_block_start",
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
  ],
  [
    "content_block_delta",
    {
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: "actual GLM answer" },
    },
  ],
]
  .map(([event, data]) => "event: " + event + "\ndata: " + JSON.stringify(data) + "\n\n")
  .join("");
const GLM_WIRE =
  GLM_PARTIAL_WIRE +
  [
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    [
      "message_delta",
      {
        type: "message_delta",
        delta: { stop_reason: "end_turn", stop_sequence: null },
        usage: { output_tokens: 2 },
      },
    ],
    ["message_stop", { type: "message_stop" }],
  ]
    .map(([event, data]) => "event: " + event + "\ndata: " + JSON.stringify(data) + "\n\n")
    .join("");

for (const indexed of [false, true]) {
  for (const mode of [
    "success",
    "bookkeeping",
    "idle",
    "error",
    "translated-error",
    "cancel",
  ] as const) {
    test(
      "real GLM " +
        (indexed ? "indexed" : "model-only") +
        " " +
        mode +
        " preserves a concurrent admission",
      async (t) => {
        const a = history.trackPendingRequest(
          GLM_MODEL,
          GLM_PROVIDER,
          indexed ? CONN : null,
          true,
          { clientRequest: { attempt: "A" } }
        );
        const b = history.trackPendingRequest(
          GLM_MODEL,
          GLM_PROVIDER,
          indexed ? CONN : null,
          true,
          { clientRequest: { attempt: "B" } }
        );
        const survivor = b ? history.getPendingById().get(b) : undefined;
        const survivorBytes = b ? history.getPendingRetainedBytes() / 2 : 0;
        assert.equal(history.getPendingRequests().byModel[GLM_KEY], 2);
        const intervals: Array<() => void> = [];
        const original = globalThis.setInterval;
        t.mock.method(globalThis, "setInterval", (callback: () => void, delay: number) => {
          if (delay === 10_000) intervals.push(callback);
          return original(callback, delay);
        });
        t.mock.method(console, "warn", () => {});
        t.mock.method(console, "debug", () => {});
        const result = await stream(
          a ?? "generated-glm-" + mode,
          mode === "bookkeeping" || mode === "idle",
          {
            rawConnectionId: indexed ? CONN : null,
            pendingConnectionId: indexed ? CONN : null,
            currentConnectionId: indexed ? CONN : null,
            provider: GLM_PROVIDER,
            model: GLM_MODEL,
            wire: GLM_WIRE,
            glmTransport: true,
            lifecycle:
              mode === "error" || mode === "translated-error" || mode === "cancel"
                ? mode
                : undefined,
            idle:
              mode === "idle"
                ? () => {
                    assert.ok(intervals[0], "real inner GLM stream arms its idle watchdog");
                    const now = Date.now();
                    t.mock.method(Date, "now", () => now + STREAM_IDLE_TIMEOUT_MS + 1_000);
                    intervals[0]();
                  }
                : undefined,
          }
        );
        if (mode === "success" || mode === "bookkeeping")
          assert.match(result.text, /actual GLM answer/);
        assert.equal(history.getPendingRequests().byModel[GLM_KEY], 1);
        assert.equal(history.getPendingRetainedBytes(), survivorBytes);
        if (b) {
          assert.equal(history.getPendingById().get(b), survivor);
          assert.equal(history.getPendingRequests().byAccount[CONN]?.[GLM_KEY], 1);
          assert.equal(history.getPendingById().has(a!), false);
          const state = history.getPendingRequests() as ReturnType<
            typeof history.getPendingRequests
          > & { details: Record<string, Record<string, Array<{ id: string }>>> };
          assert.deepEqual(
            state.details[CONN]?.[GLM_KEY]?.map((d) => d.id),
            [b]
          );
        }
        result.controller.handleDisconnect("duplicate GLM terminal");
        result.outcome.carry.onPipelineStreamError?.({
          message: "duplicate GLM terminal",
          statusCode: 502,
        });
        assert.equal(history.getPendingRequests().byModel[GLM_KEY], 1);
        assert.equal(history.getPendingRetainedBytes(), survivorBytes);
        assert.equal(result.released(), 1);
      }
    );
  }
}

for (const indexed of [false, true]) {
  test(
    "GLM transport error leaves outer cleanup unclaimed for " +
      (indexed ? "indexed" : "model-only") +
      " admissions",
    async () => {
      const a = history.trackPendingRequest(GLM_MODEL, GLM_PROVIDER, indexed ? CONN : null, true, {
        clientRequest: { attempt: "A" },
      });
      const b = history.trackPendingRequest(GLM_MODEL, GLM_PROVIDER, indexed ? CONN : null, true, {
        clientRequest: { attempt: "B" },
      });
      const bytes = history.getPendingRetainedBytes();
      const wire =
        GLM_PARTIAL_WIRE +
        'event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"GLM translated failure"}}\n\n';
      const translated = translateSseResponse(
        new Response(wire, { headers: { "content-type": "text/event-stream" } }),
        GLM_PROVIDER,
        GLM_MODEL
      );
      await assert.rejects(translated.text(), (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /GLM translated failure/);
        assert.notEqual(
          (error as Error & Record<string, unknown>)[PENDING_REQUEST_CLEARED_MARKER],
          true
        );
        return true;
      });
      assert.equal(history.getPendingRequests().byModel[GLM_KEY], 2);
      assert.equal(history.getPendingRetainedBytes(), bytes);
      if (a && b) {
        assert.ok(history.getPendingById().has(a));
        assert.ok(history.getPendingById().has(b));
        assert.equal(history.getPendingRequests().byAccount[CONN]?.[GLM_KEY], 2);
      }
    }
  );
}
