/**
 * CodeBuddy International `/v3/config` model catalog.
 *
 * `GET /v3/config?repos[]=` is the authoritative source of models and their
 * capabilities (the curated registry had drifted: 8 models missing, 5 stale).
 * The endpoint needs authentication, so it can never be mirrored at build time
 * — it is fetched at runtime and merged into the curated list.
 *
 * Parsing is deliberately defensive: the gateway is an external contract, so a
 * shape change must degrade to "no models from the gateway" instead of throwing
 * and taking model discovery down with it.
 *
 * // UNVERIFIED: the exact key names of the `/v3/config` model entries were not
 * captured, so every field below is read through a list of plausible aliases
 * (camelCase AND snake_case). Accepting an extra alias is harmless — an unknown
 * key simply never matches — while guessing a single spelling would silently
 * drop capabilities. Narrow this list once a real payload is captured.
 *
 * Live evidence (2026-10-26): `GET /v3/config?repos[]=` answers 28 entries that
 * OmniRoute turns into 28 models, and the ids are served correctly, so the
 * extraction path works. What was NOT captured is the per-entry field spelling
 * (capabilities/limits), because the fetch is consumed inside the server and
 * only the derived `{id, name, supportsVision}` reaches the models route.
 */

export interface CodeBuddyCatalogModel {
  id: string;
  name: string;
  reasoning?: boolean;
  vision?: boolean;
  toolCall?: boolean;
  maxTokens?: number;
  contextWindow?: number;
}

export interface MergeResult<T extends { id: string }> {
  models: Array<T & Partial<CodeBuddyCatalogModel>>;
  /** Ids the gateway lists that the curated list does not have. */
  added: string[];
  /** Curated ids the gateway no longer lists (kept, but flagged). */
  stale: string[];
}

type JsonRecord = Record<string, unknown>;

function isRecord(value: unknown): value is JsonRecord {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function asString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function asBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function firstRecord(...values: unknown[]): JsonRecord {
  for (const value of values) {
    if (isRecord(value)) return value;
  }
  return {};
}

/**
 * First alias that yields a defined value, mirroring a `a ?? b ?? c` chain over
 * the same reader. The gateway field spelling is unknown (see the file header),
 * so every capability is read through a list of plausible keys; folding the
 * chain into a helper keeps `parseEntry` under the complexity ratchet's ceiling.
 */
function coalesce<T>(read: (value: unknown) => T | undefined, ...values: unknown[]): T | undefined {
  for (const value of values) {
    const parsed = read(value);
    if (parsed !== undefined) return parsed;
  }
  return undefined;
}

/**
 * First alias that yields a NON-EMPTY value, mirroring the `a || b || c` chains
 * used for identifiers and names, where an empty string must fall through.
 */
function coalesceNonEmpty(read: (value: unknown) => string, ...values: unknown[]): string {
  for (const value of values) {
    const parsed = read(value);
    if (parsed) return parsed;
  }
  return "";
}

/** Locate the model array regardless of how deeply the gateway nests it. */
function findModelArray(payload: unknown): unknown[] {
  if (Array.isArray(payload)) return payload;
  if (!isRecord(payload)) return [];

  for (const candidate of [payload.models, payload.data, payload.result, payload.list]) {
    if (Array.isArray(candidate)) return candidate;
  }

  const data = payload.data;
  if (isRecord(data)) {
    for (const candidate of [data.models, data.list, data.result, data.items]) {
      if (Array.isArray(candidate)) return candidate;
    }
  }

  const result = payload.result;
  if (isRecord(result) && Array.isArray(result.models)) return result.models;

  return [];
}

function parseEntry(entry: unknown): CodeBuddyCatalogModel | null {
  if (!isRecord(entry)) return null;

  const id = coalesceNonEmpty(asString, entry.id, entry.model, entry.modelId, entry.model_id);
  if (!id) return null;

  const capabilities = firstRecord(entry.capabilities, entry.capability, entry.features);
  const limits = firstRecord(entry.limits, entry.limit, capabilities.limits);

  const model: CodeBuddyCatalogModel = {
    id,
    name:
      coalesceNonEmpty(
        asString,
        entry.name,
        entry.displayName,
        entry.display_name,
        entry.label,
        entry.title
      ) || id,
  };

  applyCapabilities(model, entry, capabilities, limits);
  return model;
}

/**
 * Copies the optional capability numbers/flags onto `model`, leaving a field
 * absent when no alias carried it (so callers can tell "not advertised" from
 * "advertised as false"). Split out of `parseEntry` to keep both functions
 * under the complexity and max-lines ratchets.
 */
function applyCapabilities(
  model: CodeBuddyCatalogModel,
  entry: JsonRecord,
  capabilities: JsonRecord,
  limits: JsonRecord
): void {
  const reasoning = coalesce(
    asBoolean,
    entry.reasoning,
    entry.supportsReasoning,
    entry.supports_reasoning,
    capabilities.reasoning,
    capabilities.supportsReasoning,
    capabilities.supports_reasoning
  );
  if (reasoning !== undefined) model.reasoning = reasoning;

  const vision = coalesce(
    asBoolean,
    entry.vision,
    entry.supportsVision,
    entry.supports_vision,
    capabilities.vision,
    capabilities.supportsVision,
    capabilities.supports_vision,
    capabilities.image
  );
  if (vision !== undefined) model.vision = vision;

  const toolCall = coalesce(
    asBoolean,
    entry.toolCall,
    entry.tool_call,
    entry.supportsToolCall,
    entry.supports_tool_call,
    entry.functionCall,
    capabilities.toolCall,
    capabilities.tool_call,
    capabilities.functionCall
  );
  if (toolCall !== undefined) model.toolCall = toolCall;

  const maxTokens = coalesce(
    asNumber,
    entry.maxTokens,
    entry.max_tokens,
    entry.maxOutputTokens,
    entry.max_output_tokens,
    capabilities.maxTokens,
    limits.maxTokens,
    limits.max_tokens
  );
  if (maxTokens !== undefined) model.maxTokens = maxTokens;

  const contextWindow = coalesce(
    asNumber,
    entry.contextWindow,
    entry.context_window,
    entry.maxInputTokens,
    entry.max_input_tokens,
    capabilities.contextWindow,
    capabilities.context_window,
    limits.contextWindow,
    limits.context_window
  );
  if (contextWindow !== undefined) model.contextWindow = contextWindow;
}

/** Parse a `/v3/config` payload into catalog entries. Never throws. */
export function parseCodeBuddyConfig(payload: unknown): CodeBuddyCatalogModel[] {
  const seen = new Set<string>();
  const models: CodeBuddyCatalogModel[] = [];

  for (const entry of findModelArray(payload)) {
    const model = parseEntry(entry);
    if (!model || seen.has(model.id)) continue;
    seen.add(model.id);
    models.push(model);
  }

  return models;
}

/**
 * Merge the gateway catalog into the curated list.
 *
 * - Curated entries keep their hand-written metadata (descriptions, pricing,
 *   aliases) but receive the gateway's capabilities.
 * - Gateway-only models are ADDED (they are the ones a curated list misses).
 * - Curated models the gateway omits are KEPT and reported as `stale`; dropping
 *   a model the user may still have configured would break their routing.
 *
 * The return type is intentionally non-generic in the added entries: a model the
 * curated list never declared has no `T`-specific fields to invent, so it is
 * surfaced as a plain catalog model instead of pretending to be `T`.
 */
export function mergeCatalogWithCurated<T extends { id: string }>(
  parsed: CodeBuddyCatalogModel[],
  curated: T[]
): MergeResult<T> {
  const parsedById = new Map(parsed.map((model) => [model.id, model]));
  const curatedIds = new Set(curated.map((model) => model.id));

  const models: Array<T & Partial<CodeBuddyCatalogModel>> = curated.map((entry) => {
    const fromGateway = parsedById.get(entry.id);
    if (!fromGateway) return { ...entry };
    return { ...fromGateway, ...entry };
  });

  const added: string[] = [];
  for (const model of parsed) {
    if (curatedIds.has(model.id)) continue;
    added.push(model.id);
    // Gateway-only entry: every field comes from the catalog, so `T`'s extra
    // properties are genuinely absent (the cast documents that, it does not
    // fabricate values).
    models.push({ ...model } as T & Partial<CodeBuddyCatalogModel>);
  }

  const stale = curated.filter((entry) => !parsedById.has(entry.id)).map((entry) => entry.id);

  return { models, added, stale };
}
