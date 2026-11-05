// Feishu tests cover streaming card plugin behavior.
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { LookupFn } from "openclaw/plugin-sdk/ssrf-runtime";
import { withFetchPreconnect } from "openclaw/plugin-sdk/test-env";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readRequestBody, stripDispatcher, writeOversizedJson } from "./http.test-support.js";
import {
  FeishuStreamingFinalizationError,
  FeishuStreamingSession,
  mergeStreamingText,
} from "./streaming-card.js";
import { createStreamingSession, type StreamingFetchDeps } from "./streaming-card.test-support.js";

const FEISHU_JSON_MAX_BYTES = 16 * 1024 * 1024;
type FeishuStreamingFetch = typeof fetch;

type StreamingSessionState = {
  cardId: string;
  messageId: string;
  sequence: number;
  currentText: string;
  sentText: string;
  hasNote: boolean;
};

type LocalServer = {
  port: number;
  stop: () => Promise<void>;
};

type StreamingRequest = {
  url: URL;
  body: string;
  req: IncomingMessage;
  res: ServerResponse;
};

const serverStops: Array<() => Promise<void>> = [];
const HERMETIC_PUBLIC_LOOKUP_ADDRESS = "93.184.216.34";

const hermeticPublicLookup: LookupFn = async () => [
  { address: HERMETIC_PUBLIC_LOOKUP_ADDRESS, family: 4 },
];

async function startLocalServer(
  handler: (request: StreamingRequest) => void | Promise<void>,
): Promise<LocalServer> {
  return await new Promise<LocalServer>((resolve, reject) => {
    const server = createServer((req, res) => {
      void (async () => {
        const url = new URL(req.url ?? "/", "http://127.0.0.1");
        const body = await readRequestBody(req);
        await handler({ url, body, req, res });
      })().catch((error: unknown) => {
        if (!res.headersSent) {
          res.writeHead(500, { "content-type": "text/plain" });
        }
        res.end(String(error));
      });
    });
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") {
        reject(new Error("local test server did not expose a TCP port"));
        return;
      }
      resolve({
        port: addr.port,
        stop: async () =>
          await new Promise<void>((innerResolve, innerReject) => {
            server.close((err) => (err ? innerReject(err) : innerResolve()));
          }),
      });
    });
  });
}

function createLocalRedirectFetch(port: number): FeishuStreamingFetch {
  const realFetch = globalThis.fetch.bind(globalThis);
  return withFetchPreconnect(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(input instanceof Request ? input.url : input.toString());
    if (url.hostname === "open.feishu.cn" || url.hostname === "open.larksuite.com") {
      const loopback = new URL(`${url.pathname}${url.search}`, `http://127.0.0.1:${port}`);
      return await realFetch(loopback, stripDispatcher(init));
    }
    return await realFetch(input, init);
  });
}

async function createStreamingFetch(
  handler: (request: StreamingRequest) => void | Promise<void>,
): Promise<StreamingFetchDeps> {
  const server = await startLocalServer(handler);
  serverStops.push(server.stop);
  return {
    fetchImpl: createLocalRedirectFetch(server.port),
    lookupFn: hermeticPublicLookup,
  };
}

function createMemoryFetch(
  handler: (url: URL, body: string) => Response | Promise<Response>,
): StreamingFetchDeps {
  return {
    fetchImpl: withFetchPreconnect(
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(input instanceof Request ? input.url : input.toString());
        const body = typeof init?.body === "string" ? init.body : "";
        return await handler(url, body);
      }),
    ) as FeishuStreamingFetch,
    lookupFn: hermeticPublicLookup,
  };
}

function writeJson(res: ServerResponse, payload: unknown, status = 200): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(payload));
}

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function setStreamingSessionInternals(
  session: FeishuStreamingSession,
  values: {
    state: StreamingSessionState;
    lastUpdateTime?: number;
  },
): void {
  const internals = session as unknown as {
    state: StreamingSessionState;
    lastUpdateTime: number;
  };
  internals.state = values.state;
  if (values.lastUpdateTime !== undefined) {
    internals.lastUpdateTime = values.lastUpdateTime;
  }
}

function createActiveSession(
  deps: StreamingFetchDeps,
  options: {
    cardId: string;
    messageId: string;
    text?: string;
    hasNote?: boolean;
    lastUpdateTime?: number;
    log?: (message: string) => void;
  },
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
      sequence: 1,
      currentText: options.text ?? "",
      sentText: options.text ?? "",
      hasNote: options.hasNote ?? false,
    },
    lastUpdateTime: options.lastUpdateTime,
  });
  return session;
}

describe("FeishuStreamingSession", () => {
  beforeEach(() => {
    vi.useRealTimers();
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    vi.useRealTimers();
    while (serverStops.length > 0) {
      await serverStops.pop()?.();
    }
  });

  function mockFetches(
    updateBodies: string[],
    failedContentUpdateIndexes: ReadonlySet<number> = new Set<number>(),
    replaceBodies: string[] = [],
    failedContentUpdateStatuses: ReadonlyMap<number, number> = new Map<number, number>(),
    failedReplaceStatuses: ReadonlyMap<number, number> = new Map<number, number>(),
  ): StreamingFetchDeps {
    return createMemoryFetch((url, body) => {
      let status = 200;
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({
          code: 0,
          msg: "ok",
          tenant_access_token: "token",
          expire: 7200,
        });
      }
      if (url.pathname.includes("/elements/content/content")) {
        const updateIndex = updateBodies.length;
        updateBodies.push(body);
        if (failedContentUpdateIndexes.has(updateIndex)) {
          throw new Error(`content update ${updateIndex} failed`);
        }
        const failedStatus = failedContentUpdateStatuses.get(updateIndex);
        if (failedStatus !== undefined) {
          status = failedStatus;
        }
      } else if (url.pathname.includes("/elements/content")) {
        const replaceIndex = replaceBodies.length;
        replaceBodies.push(body);
        const failedStatus = failedReplaceStatuses.get(replaceIndex);
        if (failedStatus !== undefined) {
          status = failedStatus;
        }
      }
      return jsonResponse({ code: 0, msg: "ok" }, status);
    });
  }

  function mockStreamingTokenStart(resolveAuthJson: (token: string) => Record<string, unknown>): {
    authTokens: string[];
    client: ConstructorParameters<typeof FeishuStreamingSession>[0];
    deps: StreamingFetchDeps;
  } {
    const authTokens: string[] = [];
    const deps = createMemoryFetch((url) => {
      if (url.pathname.includes("/auth/")) {
        const token = `token-${authTokens.length + 1}`;
        authTokens.push(token);
        return jsonResponse(resolveAuthJson(token));
      }
      return jsonResponse({
        code: 0,
        msg: "ok",
        data: { card_id: `card-${authTokens.length}` },
      });
    });
    const client = {
      im: {
        message: {
          create: vi.fn(async () => ({ code: 0, msg: "ok", data: { message_id: "om_1" } })),
        },
      },
    } as unknown as ConstructorParameters<typeof FeishuStreamingSession>[0];
    return { authTokens, client, deps };
  }

  function mockAcceptedStreamingCard(params: {
    accountId: string;
    response?: { code: number; msg: string; data?: { message_id?: string } };
    rejectClose?: boolean;
    rejectClear?: boolean;
  }) {
    const requests: Array<{ path: string; body: Record<string, unknown> }> = [];
    const deps = createMemoryFetch((url, body) => {
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({
          code: 0,
          msg: "ok",
          tenant_access_token: "token",
          expire: 7200,
        });
      }
      if (url.pathname.endsWith("/cardkit/v1/cards")) {
        return jsonResponse({ code: 0, msg: "ok", data: { card_id: "card_accepted" } });
      }
      requests.push({ path: url.pathname, body: JSON.parse(body) as Record<string, unknown> });
      return jsonResponse(
        params.rejectClose && url.pathname.endsWith("/settings")
          ? { code: 91400, msg: "close rejected" }
          : params.rejectClear &&
              url.pathname.endsWith("/elements/content/content") &&
              (JSON.parse(body) as { content?: string }).content === ""
            ? { code: 19002, msg: "clear rejected" }
            : { code: 0, msg: "ok" },
      );
    });
    const response = params.response ?? { code: 0, msg: "ok", data: {} };
    const create = vi.fn(async () => response);
    const reply = vi.fn(async () => response);
    const remove = vi.fn(async () => ({ code: 0, msg: "ok" }));
    const client = {
      im: { message: { create, reply, delete: remove } },
    } as unknown as ConstructorParameters<typeof FeishuStreamingSession>[0];
    const session = createStreamingSession(
      client,
      { appId: params.accountId, appSecret: "test-secret" },
      undefined,
      deps,
    );
    return { session, requests, create, reply, remove };
  }

  it.each([
    { mode: "create", options: undefined, method: "create" },
    { mode: "root_create", options: { rootId: "root_1" }, method: "create" },
    {
      mode: "reply",
      options: { replyToMessageId: "inbound_1", replyInThread: true, rootId: "root_1" },
      method: "reply",
    },
  ] as const)(
    "updates and closes an accepted $mode card when its optional message receipt is absent",
    async ({ mode, options, method }) => {
      const { session, requests, create, reply } = mockAcceptedStreamingCard({
        accountId: `accepted-without-receipt-${mode}`,
      });

      await session.start("chat_1", "chat_id", options);
      await session.update("The accepted answer");

      await expect(session.closeWithResult("The accepted answer")).resolves.toEqual({
        visibleReplySent: true,
        content: "The accepted answer",
      });
      expect(method === "reply" ? reply : create).toHaveBeenCalledOnce();
      expect(method === "reply" ? create : reply).not.toHaveBeenCalled();
      if (mode === "root_create") {
        expect(create).toHaveBeenCalledWith(
          expect.objectContaining({
            data: expect.objectContaining({ root_id: "root_1" }),
          }),
        );
      } else if (mode === "create") {
        expect(create).toHaveBeenCalledWith(
          expect.objectContaining({
            data: expect.not.objectContaining({ root_id: expect.anything() }),
          }),
        );
      } else {
        expect(reply).toHaveBeenCalledWith(
          expect.objectContaining({
            path: { message_id: "inbound_1" },
            data: expect.objectContaining({ reply_in_thread: true }),
          }),
        );
      }
      expect(requests.map(({ path }) => path)).toEqual([
        "/open-apis/cardkit/v1/cards/card_accepted/elements/content/content",
        "/open-apis/cardkit/v1/cards/card_accepted/settings",
      ]);
      expect(session.isActive()).toBe(false);
    },
  );

  it("clears an accepted preview without requesting deletion with an absent message receipt", async () => {
    const { session, requests, remove } = mockAcceptedStreamingCard({
      accountId: "discard-without-receipt",
    });

    await session.start("chat_1");
    await session.update("Transient preview");
    await session.discard();

    expect(remove).not.toHaveBeenCalled();
    expect(requests.map(({ path }) => path)).toEqual([
      "/open-apis/cardkit/v1/cards/card_accepted/elements/content/content",
      "/open-apis/cardkit/v1/cards/card_accepted/elements/content/content",
      "/open-apis/cardkit/v1/cards/card_accepted/settings",
    ]);
    expect(requests[1]?.body).toMatchObject({ content: "" });
    expect(session.isActive()).toBe(false);
  });

  it("deletes an accepted preview normally when its message receipt is present", async () => {
    const { session, requests, remove } = mockAcceptedStreamingCard({
      accountId: "discard-with-receipt",
      response: { code: 0, msg: "ok", data: { message_id: "om_accepted" } },
    });

    await session.start("chat_1");
    await session.update("Transient preview");
    await session.discard();

    expect(remove).toHaveBeenCalledExactlyOnceWith({ path: { message_id: "om_accepted" } });
    expect(requests.map(({ path }) => path)).toEqual([
      "/open-apis/cardkit/v1/cards/card_accepted/elements/content/content",
    ]);
    expect(session.isActive()).toBe(false);
  });

  it.each([undefined, "om_accepted"])(
    "retains accepted content and receipt %s when preview removal fails",
    async (messageId) => {
      const { session, remove } = mockAcceptedStreamingCard({
        accountId: `failed-discard-${messageId ?? "without-receipt"}`,
        response: { code: 0, msg: "ok", data: { message_id: messageId } },
        rejectClear: true,
      });
      remove.mockResolvedValue({ code: 230001, msg: "delete rejected" });
      await session.start("chat_1");
      await session.update("Already visible answer");

      await expect(session.discard()).rejects.toMatchObject({
        name: "FeishuStreamingFinalizationError",
        result: {
          visibleReplySent: true,
          content: "Already visible answer",
          ...(messageId ? { messageId } : {}),
        },
      });
      expect(remove).toHaveBeenCalledTimes(messageId ? 1 : 0);
      expect(session.isActive()).toBe(false);
    },
  );

  it("preserves accepted visible card content when receipt-free finalization fails", async () => {
    const { session } = mockAcceptedStreamingCard({
      accountId: "failed-close-without-receipt",
      rejectClose: true,
    });

    await session.start("chat_1");
    await session.update("Already visible answer");

    await expect(session.closeWithResult("Already visible answer")).rejects.toMatchObject({
      name: "FeishuStreamingFinalizationError",
      result: {
        visibleReplySent: true,
        content: "Already visible answer",
      },
    });
  });

  it("still rejects provider-declined streaming card messages", async () => {
    const { session } = mockAcceptedStreamingCard({
      accountId: "declined-streaming-card",
      response: { code: 230099, msg: "message rejected" },
    });

    await expect(session.start("chat_1")).rejects.toThrow("Send card failed: message rejected");
    expect(session.isActive()).toBe(false);
  });

  it("rejects oversized streaming tenant-token JSON before buffering the full body", async () => {
    let streamState: ReturnType<typeof writeOversizedJson> | undefined;
    const deps = await createStreamingFetch(({ url, res }) => {
      if (url.pathname.includes("/auth/")) {
        streamState = writeOversizedJson(
          res,
          FEISHU_JSON_MAX_BYTES,
          '{"code":0,"msg":"ok","tenant_access_token":"token","padding":"',
        );
        return;
      }
      writeJson(res, { code: 0, msg: "ok", data: { card_id: "card_oversized_token" } });
    });

    const session = createStreamingSession(
      {} as never,
      {
        appId: "app_oversized_token",
        appSecret: "secret",
      },
      undefined,
      deps,
    );

    await expect(session.start("chat_id", "open_id")).rejects.toThrow(
      /feishu\.streaming-card\.token: JSON response exceeds \d+ bytes/,
    );
    await expect(streamState?.clientClosed).resolves.toBeUndefined();
    expect(streamState?.canceled()).toBe(true);
    expect(streamState?.bytesPulled()).toBeLessThan(FEISHU_JSON_MAX_BYTES * 2);
    console.log(
      `[feishu streaming-card bound proof] token over-cap: bytes_pulled=${streamState?.bytesPulled()} cap=${FEISHU_JSON_MAX_BYTES} canceled=${streamState?.canceled()}`,
    );
  });

  it("aborts a stalled Feishu tenant-token request after the configured timeout", async () => {
    vi.useFakeTimers();
    let authRequestReceived = false;
    const deps: StreamingFetchDeps = {
      fetchImpl: withFetchPreconnect(
        vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
          const url = new URL(input instanceof Request ? input.url : input.toString());
          if (!url.pathname.includes("/auth/")) {
            return jsonResponse({ code: 0, msg: "ok", data: { card_id: "card_should_not_reach" } });
          }
          authRequestReceived = true;
          return await new Promise<Response>((_resolve, reject) => {
            const signal = init?.signal;
            if (!signal) {
              reject(new Error("missing guarded fetch signal"));
              return;
            }
            signal.addEventListener(
              "abort",
              () => {
                const reason = signal.reason;
                reject(
                  reason instanceof Error
                    ? reason
                    : new Error("request aborted", { cause: reason }),
                );
              },
              { once: true },
            );
          });
        }),
      ) as FeishuStreamingFetch,
      lookupFn: hermeticPublicLookup,
    };

    const session = createStreamingSession(
      {} as never,
      {
        appId: "app_stalled_token",
        appSecret: "secret",
        httpTimeoutMs: 25,
      },
      undefined,
      deps,
    );

    const result = expect(session.start("chat_id", "open_id")).rejects.toSatisfy(
      (error: unknown) => {
        expect(error).toBeInstanceOf(Error);
        const message = (error as Error).message;
        expect(message).toMatch(/timed out/i);
        return true;
      },
    );
    await vi.advanceTimersByTimeAsync(26);
    await result;
    expect(authRequestReceived).toBe(true);
    console.log(
      "[feishu streaming-card stall proof] stalled tenant-token fetch honored configured timeout",
    );
  });

  it("rejects oversized streaming card-create JSON before buffering the full body", async () => {
    let streamState: ReturnType<typeof writeOversizedJson> | undefined;
    const deps = await createStreamingFetch(({ url, res }) => {
      if (url.pathname.includes("/auth/")) {
        writeJson(res, {
          code: 0,
          msg: "ok",
          tenant_access_token: "token",
          expire: 7200,
        });
        return;
      }
      streamState = writeOversizedJson(
        res,
        FEISHU_JSON_MAX_BYTES,
        '{"code":0,"msg":"ok","tenant_access_token":"token","padding":"',
      );
    });

    const session = createStreamingSession(
      {
        im: {
          message: {
            create: vi.fn(),
          },
        },
      } as unknown as ConstructorParameters<typeof FeishuStreamingSession>[0],
      {
        appId: "app_oversized_card_create",
        appSecret: "secret",
      },
      undefined,
      deps,
    );

    await expect(session.start("chat_id", "open_id")).rejects.toThrow(
      /feishu\.streaming-card\.create: JSON response exceeds \d+ bytes/,
    );
    await expect(streamState?.clientClosed).resolves.toBeUndefined();
    expect(streamState?.canceled()).toBe(true);
    expect(streamState?.bytesPulled()).toBeLessThan(FEISHU_JSON_MAX_BYTES * 2);
    console.log(
      `[feishu streaming-card bound proof] card-create over-cap: bytes_pulled=${streamState?.bytesPulled()} cap=${FEISHU_JSON_MAX_BYTES} canceled=${streamState?.canceled()}`,
    );
  });

  it("flushes only the latest authoritative snapshot after the throttle window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const updateBodies: string[] = [];
    const deps = mockFetches(updateBodies);

    const session = createActiveSession(deps, {
      cardId: "card_1",
      messageId: "om_1",
      text: "visible",
      lastUpdateTime: 1_000,
    });

    await session.update("draft one");
    await session.update("draft two");
    expect(updateBodies).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(160);

    expect(updateBodies).toHaveLength(1);
    expect(JSON.parse(updateBodies[0] ?? "{}")).toEqual({
      content: "draft two",
      sequence: 2,
      uuid: "s_card_1_2",
    });
  });

  it("retries the same throttled snapshot after a CardKit body error", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_500);
    const updateBodies: string[] = [];
    const deps = createMemoryFetch((url, body) => {
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({
          code: 0,
          msg: "ok",
          tenant_access_token: "token",
          expire: 7200,
        });
      }
      if (url.pathname.includes("/elements/content/content")) {
        updateBodies.push(body);
        return jsonResponse(
          updateBodies.length === 1
            ? { code: 19_001, msg: "sequence rejected" }
            : { code: 0, msg: "ok" },
        );
      }
      return jsonResponse({ code: 0, msg: "ok" });
    });
    const log = vi.fn();
    const session = createActiveSession(deps, {
      cardId: "card_rejected_flush",
      messageId: "om_rejected_flush",
      text: "hello",
      lastUpdateTime: 1_500,
      log,
    });

    await session.update("hello small");
    await vi.advanceTimersByTimeAsync(160);
    await session.update("hello small");
    await vi.advanceTimersByTimeAsync(160);

    expect(log).toHaveBeenCalledWith(
      "Update failed: FeishuCardKitError: Update card content failed: sequence rejected (code=19001)",
    );
    expect(updateBodies).toHaveLength(2);
    expect(updateBodies.map((body) => JSON.parse(body).content)).toEqual([
      "hello small",
      "hello small",
    ]);
  });

  it("observes a rejected write as unaccepted and keeps the text retryable", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_500);
    const updateBodies: string[] = [];
    const deps = createMemoryFetch((url, body) => {
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({
          code: 0,
          msg: "ok",
          tenant_access_token: "token",
          expire: 7200,
        });
      }
      if (url.pathname.includes("/elements/content/content")) {
        updateBodies.push(body);
        return jsonResponse(
          updateBodies.length === 1
            ? { code: 19_001, msg: "sequence rejected" }
            : { code: 0, msg: "ok" },
        );
      }
      return jsonResponse({ code: 0, msg: "ok" });
    });
    const session = new FeishuStreamingSession(
      {} as never,
      { appId: "app_confirmed_reject", appSecret: "secret" },
      vi.fn(),
      deps,
    );
    setStreamingSessionInternals(session, {
      state: {
        cardId: "card_confirmed_reject",
        messageId: "om_confirmed_reject",
        sequence: 1,
        currentText: "seed",
        sentText: "seed",
        hasNote: false,
      },
      lastUpdateTime: 1_500,
    });

    // Outside the throttle window so the write is attempted at once.
    // Production registers the observer before the write: an immediate
    // rejected attempt settles it inside update() with no later cycle.
    vi.setSystemTime(2_000);
    const observed = session.registerContentObserver("draft A");
    const write = session.update("draft A");
    await vi.advanceTimersByTimeAsync(0);
    await write;
    await expect(observed).resolves.toBe(false);
    expect(updateBodies).toHaveLength(1);

    // The caller-driven retry of the identical text is what proves the
    // rejection was not cached as rendered.
    const retryObserved = session.registerContentObserver("draft A");
    const retryWrite = session.update("draft A");
    await vi.advanceTimersByTimeAsync(200);
    await retryWrite;
    await expect(retryObserved).resolves.toBe(true);
    expect(updateBodies.map((body) => JSON.parse(body).content)).toEqual(["draft A", "draft A"]);
  });

  it("resolves observed acceptance true once the throttled flush lands", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const updateBodies: string[] = [];
    const deps = mockFetches(updateBodies);
    const session = new FeishuStreamingSession(
      {} as never,
      { appId: "app_confirmed_throttle", appSecret: "secret" },
      undefined,
      deps,
    );
    setStreamingSessionInternals(session, {
      state: {
        cardId: "card_confirmed_throttle",
        messageId: "om_confirmed_throttle",
        sequence: 1,
        currentText: "seed",
        sentText: "seed",
        hasNote: false,
      },
      lastUpdateTime: 1_000,
    });

    const observed = session.registerContentObserver("throttled draft");
    const write = session.update("throttled draft");
    await vi.advanceTimersByTimeAsync(200);
    await write;
    await expect(observed).resolves.toBe(true);
    expect(updateBodies.map((body) => JSON.parse(body).content)).toEqual(["throttled draft"]);
  });

  it("resolves a pending acceptance observation as unacknowledged after close", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const updateBodies: string[] = [];
    const deps = mockFetches(updateBodies);
    const session = new FeishuStreamingSession(
      {} as never,
      { appId: "app_confirmed_close", appSecret: "secret" },
      undefined,
      deps,
    );
    setStreamingSessionInternals(session, {
      state: {
        cardId: "card_confirmed_close",
        messageId: "om_confirmed_close",
        sequence: 1,
        currentText: "seed",
        sentText: "seed",
        hasNote: false,
      },
      lastUpdateTime: 1_000,
    });

    const observed = session.registerContentObserver("never lands");
    const write = session.update("never lands");
    await session.closeWithResult("final answer");
    await write;
    await expect(observed).resolves.toBe(false);
  });

  it("coalesces a snapshot burst into one write and confirms each observer", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    const updateBodies: string[] = [];
    const deps = mockFetches(updateBodies);
    const session = new FeishuStreamingSession(
      {} as never,
      { appId: "app_observed_burst", appSecret: "secret" },
      undefined,
      deps,
    );
    setStreamingSessionInternals(session, {
      state: {
        cardId: "card_observed_burst",
        messageId: "om_observed_burst",
        sequence: 1,
        currentText: "seed",
        sentText: "seed",
        hasNote: false,
      },
      lastUpdateTime: 1_000,
    });

    // Rapid snapshots all land inside one throttle window; only the latest
    // reaches the card, while every observer still learns its own outcome.
    const first = session.registerContentObserver("snapshot one");
    const w1 = session.update("snapshot one");
    const second = session.registerContentObserver("snapshot two");
    const w2 = session.update("snapshot two");
    const third = session.registerContentObserver("snapshot three");
    const w3 = session.update("snapshot three");
    await vi.advanceTimersByTimeAsync(200);
    await Promise.all([w1, w2, w3]);
    await expect(third).resolves.toBe(true);
    await expect(second).resolves.toBe(false);
    await expect(first).resolves.toBe(false);
    expect(updateBodies).toHaveLength(1);
    expect(JSON.parse(updateBodies[0] ?? "{}").content).toBe("snapshot three");
  });

  it("pushes natural-boundary updates immediately inside the throttle window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(2_000);
    const updateBodies: string[] = [];
    const deps = mockFetches(updateBodies);

    const session = createActiveSession(deps, {
      cardId: "card_2",
      messageId: "om_2",
      text: "hello",
      lastUpdateTime: 2_000,
    });

    await session.update("hello!");

    expect(updateBodies).toHaveLength(1);
    expect(JSON.parse(updateBodies[0] ?? "{}")).toEqual({
      content: "hello!",
      sequence: 2,
      uuid: "s_card_2_2",
    });
  });

  it("closes with a throttled divergent latest snapshot without merging it", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(2_750);
    const updateBodies: string[] = [];
    const replaceBodies: string[] = [];
    const deps = mockFetches(updateBodies, new Set<number>(), replaceBodies);
    const previous = "> Thinking one\n\n---\n\nanswer";
    const next = "> Thinking two\n\n---\n\nanswer more";
    const session = createActiveSession(deps, {
      cardId: "card_pending_reasoning_close",
      messageId: "om_pending_reasoning_close",
      text: previous,
      lastUpdateTime: 2_750,
    });

    await session.update(next);
    expect(updateBodies).toHaveLength(0);
    expect(replaceBodies).toHaveLength(0);

    await session.closeWithResult();

    expect(updateBodies).toHaveLength(1);
    expect(replaceBodies).toHaveLength(0);
    expect(JSON.parse(updateBodies[0] ?? "{}")).toMatchObject({ content: next });
  });

  it("retains prior visible content when CardKit rejects a divergent close replacement", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(2_900);
    const replaceBodies: string[] = [];
    const settingsBodies: string[] = [];
    const previous = "> Thinking one\n\n---\n\nanswer";
    const next = "> Thinking two\n\n---\n\nanswer more";
    let sentTextWhenSettingsClosed: string | undefined;
    const state: StreamingSessionState = {
      cardId: "card_rejected_pending_reasoning_close",
      messageId: "om_rejected_pending_reasoning_close",
      sequence: 1,
      currentText: previous,
      sentText: previous,
      hasNote: false,
    };
    const deps = createMemoryFetch((url, body) => {
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({
          code: 0,
          msg: "ok",
          tenant_access_token: "token",
          expire: 7200,
        });
      }
      if (url.pathname.endsWith("/elements/content/content")) {
        replaceBodies.push(body);
        return jsonResponse({ code: 19_002, msg: "replacement rejected" });
      }
      if (url.pathname.includes("/settings")) {
        settingsBodies.push(body);
        sentTextWhenSettingsClosed = state.sentText;
      }
      return jsonResponse({ code: 0, msg: "ok" });
    });
    const log = vi.fn();
    const session = createStreamingSession(
      {} as never,
      { appId: "app_rejected_pending_reasoning_close", appSecret: "secret" },
      log,
      deps,
    );
    setStreamingSessionInternals(session, {
      state,
      lastUpdateTime: 2_900,
    });

    await session.update(next);
    await expect(session.closeWithResult()).rejects.toMatchObject({
      name: "FeishuStreamingFinalizationError",
      result: { visibleReplySent: true, content: previous },
    });

    expect(replaceBodies).toHaveLength(1);
    expect(settingsBodies).toHaveLength(1);
    expect(sentTextWhenSettingsClosed).toBe(previous);
    const settingsPayload = JSON.parse(settingsBodies[0] ?? "{}") as { settings?: string };
    const settings = JSON.parse(settingsPayload.settings ?? "{}") as {
      config?: { summary?: { content?: string } };
    };
    expect(settings.config?.summary?.content).toBe(previous.replaceAll("\n", " ").trim());
    expect(settings.config?.summary?.content).not.toContain("Thinking two");
    expect(log).toHaveBeenCalledWith(
      "Final update failed: FeishuCardKitError: Update card content failed: replacement rejected (code=19002)",
    );
  });

  it("logs CardKit body errors for note updates and streaming close settings", async () => {
    const noteBodies: string[] = [];
    const settingsBodies: string[] = [];
    const deps = createMemoryFetch((url, body) => {
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({
          code: 0,
          msg: "ok",
          tenant_access_token: "token",
          expire: 7200,
        });
      }
      if (url.pathname.includes("/elements/note/content")) {
        noteBodies.push(body);
        return jsonResponse({ code: 19_003, msg: "note rejected" });
      }
      if (url.pathname.includes("/settings")) {
        settingsBodies.push(body);
        return jsonResponse({ code: 19_004, msg: "settings rejected" });
      }
      return jsonResponse({ code: 0, msg: "ok" });
    });
    const log = vi.fn();
    const session = createActiveSession(deps, {
      cardId: "card_rejected_note_and_close",
      messageId: "om_rejected_note_and_close",
      text: "visible answer",
      hasNote: true,
      log,
    });

    const error = await session
      .closeWithResult(undefined, { note: "model note" })
      .catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(FeishuStreamingFinalizationError);
    expect(error).toMatchObject({
      result: {
        visibleReplySent: true,
        content: "visible answer",
        messageId: "om_rejected_note_and_close",
      },
    });

    expect(noteBodies).toHaveLength(1);
    expect(settingsBodies).toHaveLength(1);
    expect(log).toHaveBeenCalledWith(
      "Note update failed: FeishuCardKitError: Update card note failed: note rejected (code=19003)",
    );
    expect(log).toHaveBeenCalledWith(
      "Close failed: FeishuCardKitError: Close streaming card failed: settings rejected (code=19004)",
    );
  });

  it("retries cumulative content after a failed streaming update", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(3_000);
    const updateBodies: string[] = [];
    const deps = mockFetches(updateBodies, new Set([0]));

    const session = createActiveSession(deps, {
      cardId: "card_3",
      messageId: "om_3",
      text: "hello",
      lastUpdateTime: 2_000,
    });

    await session.update("hello world");
    await session.update("hello world!");

    expect(updateBodies).toHaveLength(2);
    expect(JSON.parse(updateBodies[0] ?? "{}")).toEqual({
      content: "hello world",
      sequence: 2,
      uuid: "s_card_3_2",
    });
    expect(JSON.parse(updateBodies[1] ?? "{}")).toEqual({
      content: "hello world!",
      sequence: 3,
      uuid: "s_card_3_3",
    });
  });

  it("retries cumulative content after a non-OK streaming update", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(3_500);
    const updateBodies: string[] = [];
    const deps = mockFetches(updateBodies, new Set<number>(), [], new Map([[0, 429]]));

    const session = createActiveSession(deps, {
      cardId: "card_5",
      messageId: "om_5",
      text: "hello",
      lastUpdateTime: 2_000,
    });

    await session.update("hello world");
    await session.update("hello world!");

    expect(updateBodies).toHaveLength(2);
    expect(JSON.parse(updateBodies[0] ?? "{}")).toEqual({
      content: "hello world",
      sequence: 2,
      uuid: "s_card_5_2",
    });
    expect(JSON.parse(updateBodies[1] ?? "{}")).toEqual({
      content: "hello world!",
      sequence: 3,
      uuid: "s_card_5_3",
    });
  });

  it("rewrites streamed content when the final text drops transient status", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(4_000);
    const updateBodies: string[] = [];
    const replaceBodies: string[] = [];
    const deps = mockFetches(updateBodies, new Set<number>(), replaceBodies);

    const session = createActiveSession(deps, {
      cardId: "card_4",
      messageId: "om_4",
      text: "Web Search\n\nfinal answer",
      lastUpdateTime: 3_000,
    });

    await session.closeWithResult("final answer");

    expect(updateBodies).toHaveLength(1);
    expect(replaceBodies).toHaveLength(0);
    expect(JSON.parse(updateBodies[0] ?? "{}")).toEqual({
      content: "final answer",
      sequence: 2,
      uuid: "s_card_4_2",
    });
  });

  it("drops a surrogate pair whole when truncating the closeout summary", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(4_200);
    // 46 'a' + 😀 (U+1F600, UTF-16 indices 46-47) + 20 'b' = 68-char string.
    // truncateSummary's default max is 50, so it slices at max-3 = 47, which
    // lands between the high and low surrogate halves of the emoji.
    const finalText = `${"a".repeat(46)}\u{1F600}${"b".repeat(20)}`;
    const settingsBodies: string[] = [];
    const deps = await createStreamingFetch(({ url, body, res }) => {
      if (url.pathname.includes("/auth/")) {
        writeJson(res, {
          code: 0,
          msg: "ok",
          tenant_access_token: "token",
          expire: 7200,
        });
        return;
      }
      if (url.pathname.includes("/settings")) {
        settingsBodies.push(body);
      }
      writeJson(res, { code: 0, msg: "ok" });
    });

    const session = createActiveSession(deps, {
      cardId: "card_surrogate",
      messageId: "om_surrogate",
      lastUpdateTime: 3_000,
    });

    await session.closeWithResult(finalText);

    expect(settingsBodies).toHaveLength(1);
    const settingsPayload = JSON.parse(settingsBodies[0] ?? "{}") as { settings?: string };
    const settings = JSON.parse(settingsPayload.settings ?? "{}") as {
      config?: { summary?: { content?: string } };
    };
    const summary = settings.config?.summary?.content ?? "";
    // The half-emoji must be dropped whole: 46 a's + "...", and the summary
    // must NOT end with a lone high surrogate (which Feishu renders as �).
    expect(summary).toBe(`${"a".repeat(46)}...`);
    expect(summary).not.toContain("\uD83D");
    expect(summary.charCodeAt(summary.length - 4)).not.toBe(0xd83d);
  });

  it("logs a final content-write failure when CardKit returns non-OK", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(4_500);
    const updateBodies: string[] = [];
    const replaceBodies: string[] = [];
    const deps = mockFetches(updateBodies, new Set<number>(), replaceBodies, new Map([[0, 500]]));
    const log = vi.fn();

    const session = createActiveSession(deps, {
      cardId: "card_6",
      messageId: "om_6",
      text: "working\n\nfinal answer",
      lastUpdateTime: 3_000,
      log,
    });

    await expect(session.closeWithResult("final answer")).rejects.toMatchObject({
      name: "FeishuStreamingFinalizationError",
      result: { visibleReplySent: true, content: "working\n\nfinal answer" },
    });

    expect(updateBodies).toHaveLength(1);
    expect(replaceBodies).toHaveLength(0);
    expect(log).toHaveBeenCalledWith(
      "Final update failed: Error: Update card content failed with HTTP 500",
    );
  });

  it("reports the final text as not accepted when CardKit rejects the final update after a visible preview", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(4_600);
    const updateBodies: string[] = [];
    const replaceBodies: string[] = [];
    // CardKit closes streaming mode ~10 minutes after it was enabled; every later
    // content write is rejected while the earlier preview stays visible in the chat.
    const deps = mockFetches(updateBodies, new Set<number>(), replaceBodies, new Map([[0, 500]]));
    const log = vi.fn();

    const session = createActiveSession(deps, {
      cardId: "card_8",
      messageId: "om_8",
      text: "working",
      lastUpdateTime: 3_000,
      log,
    });

    await expect(session.closeWithResult("working\n\nfinal answer")).rejects.toMatchObject({
      name: "FeishuStreamingFinalizationError",
      result: {
        visibleReplySent: true,
        content: "working",
        messageId: "om_8",
        finalTextAccepted: false,
      },
    });

    expect(updateBodies).toHaveLength(1);
    expect(replaceBodies).toHaveLength(0);
    expect(log).toHaveBeenCalledWith(
      "Final update failed: Error: Update card content failed with HTTP 500",
    );
  });

  it("reports no visible content when final close update fails before any accepted text", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(4_800);
    const updateBodies: string[] = [];
    const replaceBodies: string[] = [];
    const deps = mockFetches(updateBodies, new Set<number>(), replaceBodies, new Map([[0, 500]]));
    const log = vi.fn();

    const session = createActiveSession(deps, {
      cardId: "card_7",
      messageId: "om_7",
      lastUpdateTime: 3_000,
      log,
    });

    await expect(session.closeWithResult("final answer")).rejects.toMatchObject({
      name: "FeishuStreamingFinalizationError",
      result: { visibleReplySent: false },
    });

    expect(updateBodies).toHaveLength(1);
    expect(replaceBodies).toHaveLength(0);
    expect(log).toHaveBeenCalledWith(
      "Final update failed: Error: Update card content failed with HTTP 500",
    );
  });

  it("bounds streaming token cache lifetime when token expiry overflows", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-05-29T12:00:00.000Z"));
    const { authTokens, client, deps } = mockStreamingTokenStart((token) => ({
      code: 0,
      msg: "ok",
      tenant_access_token: token,
      expire: Number.MAX_SAFE_INTEGER,
    }));

    await createStreamingSession(
      client,
      {
        appId: "app_unsafe_token_expiry",
        appSecret: "secret",
      },
      undefined,
      deps,
    ).start("chat_id", "open_id");
    expect(authTokens).toEqual(["token-1"]);

    vi.setSystemTime(Date.now() + 7200 * 1000 - 60_000 + 1);
    await createStreamingSession(
      client,
      {
        appId: "app_unsafe_token_expiry",
        appSecret: "secret",
      },
      undefined,
      deps,
    ).start("chat_id", "open_id");

    expect(authTokens).toEqual(["token-1", "token-2"]);
  });

  it("bounds streaming token fallback lifetime when the process clock is invalid", async () => {
    const dateNow = vi.spyOn(Date, "now").mockReturnValue(8_640_000_000_000_001);
    const { authTokens, client, deps } = mockStreamingTokenStart((token) => ({
      code: 0,
      msg: "ok",
      tenant_access_token: token,
    }));

    await createStreamingSession(
      client,
      {
        appId: "app_invalid_clock_token_expiry",
        appSecret: "secret",
      },
      undefined,
      deps,
    ).start("chat_id", "open_id");
    expect(authTokens).toEqual(["token-1"]);

    dateNow.mockReturnValue(7200 * 1000 - 60_000 + 1);
    await createStreamingSession(
      client,
      {
        appId: "app_invalid_clock_token_expiry",
        appSecret: "secret",
      },
      undefined,
      deps,
    ).start("chat_id", "open_id");

    expect(authTokens).toEqual(["token-1", "token-2"]);
    dateNow.mockRestore();
  });

  it("treats an invalid process clock as a streaming token cache miss", async () => {
    const dateNow = vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-05-29T12:00:00.000Z"));
    const { authTokens, client, deps } = mockStreamingTokenStart((token) => ({
      code: 0,
      msg: "ok",
      tenant_access_token: token,
      expire: 7200,
    }));

    await createStreamingSession(
      client,
      {
        appId: "app_invalid_clock_cache_miss",
        appSecret: "secret",
      },
      undefined,
      deps,
    ).start("chat_id", "open_id");
    expect(authTokens).toEqual(["token-1"]);

    dateNow.mockReturnValue(8_640_000_000_000_001);
    await createStreamingSession(
      client,
      {
        appId: "app_invalid_clock_cache_miss",
        appSecret: "secret",
      },
      undefined,
      deps,
    ).start("chat_id", "open_id");

    expect(authTokens).toEqual(["token-1", "token-2"]);
    dateNow.mockRestore();
  });
});

describe("mergeStreamingText", () => {
  it("keeps previous text when the next partial is empty or redundant", () => {
    expect(mergeStreamingText("hello", "")).toBe("hello");
    expect(mergeStreamingText("hello world", "hello")).toBe("hello world");
  });

  it("appends fragmented chunks without injecting newlines", () => {
    expect(mergeStreamingText("hello wor", "ld")).toBe("hello world");
    expect(mergeStreamingText("line1", "line2")).toBe("line1line2");
  });

  it("merges overlap between adjacent partial snapshots", () => {
    expect(mergeStreamingText("好的，让我", "让我再读取一遍")).toBe("好的，让我再读取一遍");
    expect(mergeStreamingText("revision_id: 552", "2，一点变化都没有")).toBe(
      "revision_id: 552，一点变化都没有",
    );
    expect(mergeStreamingText("abc", "cabc")).toBe("cabc");
  });
});

describe("FeishuStreamingSession streaming-mode expiry", () => {
  afterEach(async () => {
    vi.restoreAllMocks();
    while (serverStops.length > 0) {
      await serverStops.pop()?.();
    }
  });

  function setSessionClock(session: FeishuStreamingSession, startedAtMs: number): void {
    (session as unknown as { startedAtMs: number }).startedAtMs = startedAtMs;
  }

  function expiryFetch(options?: { failRearm?: boolean; failFirstPut?: boolean }): {
    deps: StreamingFetchDeps;
    contentPuts: string[];
    cardCreates: string[];
    patches: string[];
  } {
    const contentPuts: string[] = [];
    const cardCreates: string[] = [];
    const patches: string[] = [];
    const deps = createMemoryFetch((url, body) => {
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({ code: 0, msg: "ok", tenant_access_token: "token", expire: 7200 });
      }
      if (url.pathname.endsWith("/cardkit/v1/cards")) {
        cardCreates.push(body);
        return jsonResponse({
          code: 0,
          msg: "ok",
          data: { card_id: `card-${cardCreates.length - 1}` },
        });
      }
      if (url.pathname.includes("/elements/content/content")) {
        contentPuts.push(body);
        // Emulate the platform closing streaming mode on the first write.
        if (options?.failFirstPut && contentPuts.length === 1) {
          return jsonResponse({ code: 300309, msg: "streaming mode is closed" });
        }
        return jsonResponse({ code: 0, msg: "ok" });
      }
      if (url.pathname.includes("/settings")) {
        patches.push(body);
        if (options?.failRearm) {
          return jsonResponse({ code: 99901, msg: "settings rejected" });
        }
      }
      return jsonResponse({ code: 0, msg: "ok" });
    });
    return { deps, contentPuts, cardCreates, patches };
  }

  function createSendingClient(): { im: { message: { create: unknown } } } {
    return {
      im: {
        message: {
          create: vi.fn(async () => ({ code: 0, data: { message_id: `om_${Date.now()}` } })),
        },
      },
    };
  }

  it("re-arms the same card when the 10-minute streaming window nears expiry", async () => {
    const { deps, contentPuts, cardCreates, patches } = expiryFetch();
    const session = createStreamingSession(
      createSendingClient() as never,
      { appId: "app", appSecret: "secret" },
      undefined,
      deps,
    );
    await session.start("chat_id", "open_id");
    setSessionClock(session, Date.now() - (9 * 60_000 + 30_000));

    await session.update("work in progress");

    // No new card: the SAME card keeps streaming for the whole turn.
    expect(cardCreates.length).toBe(1);
    // One settings PATCH re-armed streaming_mode to true on the same card.
    expect(patches.length).toBe(1);
    const rearmBody = JSON.parse(patches[0] ?? "{}") as { settings?: string };
    const rearmConfig = JSON.parse(rearmBody.settings ?? "{}") as {
      config?: { streaming_mode?: boolean };
    };
    expect(rearmConfig.config?.streaming_mode).toBe(true);
    // The in-flight snapshot continues on the same card.
    expect(contentPuts.some((body) => body.includes("work in progress"))).toBe(true);
    expect(session.isActive()).toBe(true);

    // The activation clock restarted: further updates do not re-arm again.
    await session.update("more progress");
    expect(patches.length).toBe(1);
    await session.closeWithResult("done", { note: "final" });
  });

  it("re-arms automatically after the platform rejects a write", async () => {
    const { deps, contentPuts, patches } = expiryFetch({ failFirstPut: true });
    const session = createStreamingSession(
      createSendingClient() as never,
      { appId: "app", appSecret: "secret" },
      undefined,
      deps,
    );
    await session.start("chat_id", "open_id");

    // First write hits the platform-closed card and gets rejected.
    await session.update("rejected snapshot");
    expect(contentPuts.length).toBe(1);

    // The next update self-heals: re-arm, then deliver the snapshot on the same card.
    await session.update("recovered snapshot");
    expect(patches.length).toBe(1);
    expect(contentPuts.length).toBe(2);
    const internals = session as unknown as { state: { sentText: string } };
    expect(internals.state.sentText).toBe("recovered snapshot");
    await session.closeWithResult("done");
  });

  it("short-circuits when re-arm fails and finalizes with the rejected-final contract", async () => {
    const { deps, contentPuts, patches } = expiryFetch({ failRearm: true });
    const logs: string[] = [];
    const session = createStreamingSession(
      createSendingClient() as never,
      { appId: "app", appSecret: "secret" },
      (message) => logs.push(message),
      deps,
    );
    await session.start("chat_id", "open_id");
    setSessionClock(session, Date.now() - (9 * 60_000 + 30_000));

    await session.update("progress before failure");
    expect(contentPuts.length).toBe(0);

    // Later updates keep retrying the re-arm once each, never content writes.
    await session.update("another snapshot");
    expect(contentPuts.length).toBe(0);
    expect(patches.length).toBe(2);

    let finalizationError: unknown;
    let closeResult: Awaited<ReturnType<FeishuStreamingSession["closeWithResult"]>> | undefined;
    try {
      closeResult = await session.closeWithResult("the completed answer");
    } catch (error) {
      finalizationError = error;
    }
    expect(finalizationError).toBeInstanceOf(FeishuStreamingFinalizationError);
    const failure = finalizationError as FeishuStreamingFinalizationError;
    expect(failure.result.finalTextAccepted).toBe(false);
    expect(failure.result.visibleReplySent).toBe(false);
    expect(closeResult).toBeUndefined();
  });

  it("keeps retrying ordinary write failures the way it did before", async () => {
    const updateBodies: string[] = [];
    const deps = createMemoryFetch((url, body) => {
      if (url.pathname.includes("/auth/")) {
        return jsonResponse({ code: 0, msg: "ok", tenant_access_token: "token", expire: 7200 });
      }
      if (url.pathname.includes("/elements/content/content")) {
        updateBodies.push(body);
        return jsonResponse({ code: 500, msg: "internal hiccup" });
      }
      return jsonResponse({ code: 0, msg: "ok" });
    });
    const session = createActiveSession(deps, {
      cardId: "card-flaky",
      messageId: "om_flaky",
      text: "accepted",
    });
    setSessionClock(session, Date.now() - 1000);

    await session.update("snapshot one");
    // The second update lands inside the throttle window; wait for the
    // scheduled flush timer before asserting the retry went out.
    await new Promise((resolve) => setTimeout(resolve, 250));
    await session.update("snapshot two");
    await new Promise((resolve) => setTimeout(resolve, 250));
    expect(updateBodies.length).toBe(2);
  });
});
