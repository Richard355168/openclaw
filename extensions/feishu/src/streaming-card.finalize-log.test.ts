// Process-log panel tests (patch 0006 v2): the process lane lives in an
// append-only collapsible_panel element created with the card; log rows
// stream via prefix-extending content PUTs, and the panel is collapsed (or
// removed when empty) after the settings close. Panel finalize failures are
// logged only — the finalize contract stays untouched.
import type { LookupFn } from "openclaw/plugin-sdk/ssrf-runtime";
import { withFetchPreconnect } from "openclaw/plugin-sdk/test-env";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CardHeaderConfig } from "./send.js";
import { FeishuStreamingSession } from "./streaming-card.js";
import { createStreamingSession, type StreamingFetchDeps } from "./streaming-card.test-support.js";

type StreamingSessionState = {
  cardId: string;
  messageId?: string;
  sequence: number;
  currentText: string;
  sentText: string;
  hasNote: boolean;
  processLogSent: string;
  processLogDesired: string;
  processLogEntries: number;
};

const hermeticPublicLookup: LookupFn = async () => [{ address: "93.184.216.34", family: 4 }];

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function createMemoryFetch(
  handler: (url: URL, body: string, method: string) => Response | Promise<Response>,
): StreamingFetchDeps {
  return {
    fetchImpl: withFetchPreconnect(
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(input instanceof Request ? input.url : input.toString());
        const body = typeof init?.body === "string" ? init.body : "";
        return await handler(url, body, init?.method ?? "GET");
      }),
    ) as typeof fetch,
    lookupFn: hermeticPublicLookup,
  };
}

function setStreamingSessionInternals(
  session: FeishuStreamingSession,
  values: { state?: Partial<StreamingSessionState> & { cardId: string } },
): void {
  const internals = session as unknown as {
    state: StreamingSessionState | null;
  };
  if (values.state !== undefined) {
    internals.state = {
      messageId: undefined,
      sequence: 1,
      currentText: "",
      sentText: "",
      hasNote: false,
      processLogSent: "",
      processLogDesired: "",
      processLogEntries: 0,
      ...values.state,
    } as StreamingSessionState;
  }
}

function createActiveSession(
  deps: StreamingFetchDeps,
  options: { cardId: string; messageId: string; text?: string; log?: (message: string) => void },
): FeishuStreamingSession {
  const session = createStreamingSession(
    {} as never,
    { appId: options.cardId, appSecret: "secret" },
    options.log,
    deps,
  );
  setStreamingSessionInternals(session, {
    state: {
      cardId: options.cardId,
      messageId: options.messageId,
      currentText: options.text ?? "",
      sentText: options.text ?? "",
    },
  });
  return session;
}

async function drainQueue(session: FeishuStreamingSession): Promise<void> {
  // The log channel is throttled (~update cadence): let the flush timer fire
  // before draining the write queue.
  await new Promise((resolve) => setTimeout(resolve, 180));
  await (session as unknown as { queue: Promise<void> }).queue;
}

type RecordedRequest = { path: string; method: string; body: Record<string, unknown> };

function createRecordingDeps(params?: { failPanelFinalize?: boolean }): {
  deps: StreamingFetchDeps;
  requests: RecordedRequest[];
} {
  const requests: RecordedRequest[] = [];
  const deps = createMemoryFetch((url, body, method) => {
    if (url.pathname.includes("/auth/")) {
      return jsonResponse({ code: 0, msg: "ok", tenant_access_token: "token", expire: 7200 });
    }
    requests.push({
      path: url.pathname,
      method,
      body: body ? (JSON.parse(body) as Record<string, unknown>) : {},
    });
    if (params?.failPanelFinalize && url.pathname.endsWith("/elements/process_log")) {
      return jsonResponse({ code: 91400, msg: "panel rejected" }, 400);
    }
    return jsonResponse({ code: 0, msg: "ok" });
  });
  return { deps, requests };
}

describe("FeishuStreamingSession process-log panel", () => {
  beforeEach(() => {
    vi.useRealTimers();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("starts cards with an expandable process panel after the content element", async () => {
    const createBodies: string[] = [];
    const deps = createMemoryFetch((url, body, method) => {
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({ code: 0, msg: "ok", tenant_access_token: "token", expire: 7200 });
      }
      if (method === "POST" && url.pathname.endsWith("/cardkit/v1/cards")) {
        createBodies.push(body);
        return jsonResponse({ code: 0, msg: "ok", data: { card_id: "card_new" } });
      }
      return jsonResponse({ code: 0, msg: "ok" });
    });
    const client = {
      im: {
        message: {
          create: vi.fn(async () => ({ code: 0, msg: "ok", data: { message_id: "om_1" } })),
        },
      },
    } as unknown as ConstructorParameters<typeof FeishuStreamingSession>[0];
    const session = createStreamingSession(
      client,
      { appId: "acct", appSecret: "secret" },
      undefined,
      deps,
    );

    await session.start("chat_1", "chat_id", { header: { title: "OpenClaw", template: "blue" } });

    expect(createBodies).toHaveLength(1);
    // The create request wraps the card JSON in the {type, data} envelope.
    const envelope = JSON.parse(createBodies[0]) as { type: string; data: string };
    expect(envelope.type).toBe("card_json");
    const card = JSON.parse(envelope.data) as {
      schema: string;
      header?: Record<string, unknown>;
      body: { elements: Array<Record<string, unknown>> };
    };
    expect(card.schema).toBe("2.0");
    expect(card.header).toMatchObject({
      title: { tag: "plain_text", content: "OpenClaw" },
      template: "blue",
    });
    expect(card.body.elements[0]).toMatchObject({ tag: "markdown", element_id: "content" });
    expect(card.body.elements[1]).toMatchObject({
      tag: "hr",
      element_id: "process_log_divider",
    });
    expect(card.body.elements[2]).toMatchObject({
      tag: "collapsible_panel",
      element_id: "process_log",
      expanded: false,
    });
    expect(card.body.elements[2]).toMatchObject({
      header: { title: { tag: "plain_text", content: "执行过程" } },
    });
    expect(card.body.elements[2]).toMatchObject({
      elements: [{ tag: "markdown", element_id: "process_log_content" }],
    });
  });

  it("streams cumulative log text into the panel element and coalesces identical appends", async () => {
    const { deps, requests } = createRecordingDeps();
    const session = createActiveSession(deps, { cardId: "card_log", messageId: "om_1" });

    await session.appendProcessLog("🔎 Web Search", 1);
    await drainQueue(session);
    await session.appendProcessLog("🔎 Web Search\n🛠️ Exec: pnpm test", 2);
    await drainQueue(session);
    await session.appendProcessLog("🔎 Web Search\n🛠️ Exec: pnpm test", 2);
    await drainQueue(session);

    const logWrites = requests.filter(({ path }) =>
      path.endsWith("/elements/process_log_content/content"),
    );
    expect(logWrites.map(({ body }) => body.content)).toEqual([
      "🔎 Web Search",
      "🔎 Web Search\n🛠️ Exec: pnpm test",
    ]);
    expect(session.isActive()).toBe(true);
  });

  it("collapses the process panel with the entry count after the settings close", async () => {
    const { deps, requests } = createRecordingDeps();
    const session = createActiveSession(deps, {
      cardId: "card_log",
      messageId: "om_1",
      text: "preview",
    });

    await session.appendProcessLog("🔎 Web Search\n🛠️ Exec: pnpm test", 2);
    await drainQueue(session);
    const result = await session.closeWithResult("final answer");

    expect(result).toEqual({ visibleReplySent: true, content: "final answer", messageId: "om_1" });
    expect(requests.map(({ path }) => path)).toEqual([
      "/open-apis/cardkit/v1/cards/card_log/elements/process_log_content/content",
      "/open-apis/cardkit/v1/cards/card_log/elements/content/content",
      "/open-apis/cardkit/v1/cards/card_log/settings",
      "/open-apis/cardkit/v1/cards/card_log/elements/process_log",
    ]);
    const collapse = requests.at(-1);
    expect(collapse?.method).toBe("PATCH");
    const partial = JSON.parse(collapse?.body.partial_element as string) as {
      expanded: boolean;
      header: { title: { content: string } };
    };
    expect(partial.expanded).toBe(false);
    expect(partial.header.title.content).toBe("执行过程 · 2 步");
    expect(session.isActive()).toBe(false);
  });

  it("removes the panel for a single-row log so one-step turns keep no divider block", async () => {
    const { deps, requests } = createRecordingDeps();
    const session = createActiveSession(deps, {
      cardId: "card_log",
      messageId: "om_1",
      text: "preview",
    });

    await session.appendProcessLog("🔎 Web Search", 1);
    await drainQueue(session);
    await session.closeWithResult("final answer");

    const removals = requests.filter(
      ({ path, method }) =>
        (path.endsWith("/elements/process_log") ||
          path.endsWith("/elements/process_log_divider")) &&
        method === "DELETE",
    );
    expect(removals.map(({ path }) => path)).toEqual([
      "/open-apis/cardkit/v1/cards/card_log/elements/process_log",
      "/open-apis/cardkit/v1/cards/card_log/elements/process_log_divider",
    ]);
  });

  it("keeps the collapsed panel when the only row is a failure escalation", async () => {
    const { deps, requests } = createRecordingDeps();
    const session = createActiveSession(deps, {
      cardId: "card_log",
      messageId: "om_1",
      text: "preview",
    });

    await session.appendProcessLog("⚠️ Web Search failed", 1, true);
    await drainQueue(session);
    await session.closeWithResult("final answer");

    const collapse = requests.filter(({ path }) => path.endsWith("/elements/process_log")).at(-1);
    expect(collapse?.method).toBe("PATCH");
    const partial = JSON.parse(collapse?.body.partial_element as string) as {
      expanded: boolean;
    };
    expect(partial.expanded).toBe(false);
  });

  it("writes the pending log rows on close when the throttle timer has not fired", async () => {
    const { deps, requests } = createRecordingDeps();
    const session = createActiveSession(deps, { cardId: "card_log", messageId: "om_1" });

    await session.appendProcessLog("🔎 Web Search", 1);
    const result = await session.closeWithResult("final answer");

    expect(result).toEqual({ visibleReplySent: true, content: "final answer", messageId: "om_1" });
    const logWrites = requests.filter(({ path }) =>
      path.endsWith("/elements/process_log_content/content"),
    );
    expect(logWrites.map(({ body }) => body.content)).toEqual(["🔎 Web Search"]);
  });

  it("removes an empty process panel when the turn produced no log rows", async () => {
    const { deps, requests } = createRecordingDeps();
    const session = createActiveSession(deps, {
      cardId: "card_log",
      messageId: "om_1",
      text: "preview",
    });

    await session.closeWithResult("final answer");

    const removals = requests.filter(({ path, method }) =>
      path.endsWith("/elements/process_log") || path.endsWith("/elements/process_log_divider")
        ? method === "DELETE"
        : false,
    );
    expect(removals.map(({ path }) => path)).toEqual([
      "/open-apis/cardkit/v1/cards/card_log/elements/process_log",
      "/open-apis/cardkit/v1/cards/card_log/elements/process_log_divider",
    ]);
  });

  it("keeps the finalize contract when the panel finalize request fails", async () => {
    const logs: string[] = [];
    const { deps, requests } = createRecordingDeps({ failPanelFinalize: true });
    const session = createActiveSession(deps, {
      cardId: "card_log",
      messageId: "om_1",
      text: "preview",
      log: (message) => logs.push(message),
    });

    const result = await session.closeWithResult("final answer");

    expect(result).toEqual({ visibleReplySent: true, content: "final answer", messageId: "om_1" });
    expect(logs.some((message) => message.includes("Process panel finalize skipped"))).toBe(true);
    expect(requests.filter(({ path }) => path.endsWith("/elements/process_log"))).toHaveLength(1);
  });
});
