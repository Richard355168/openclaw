/**
 * Wire-level helpers for the Feishu streaming card: CardKit token lifecycle,
 * API base/host policy resolution, response auditing, and streaming text
 * merging. Split from streaming-card.ts to respect the repo's max-lines
 * budget without a suppression.
 */

import {
  asDateTimestampMs,
  resolveDateTimestampMs,
  resolveExpiresAtMsFromDurationSeconds,
} from "openclaw/plugin-sdk/number-runtime";
import { fetchWithSsrFGuard, type LookupFn } from "openclaw/plugin-sdk/ssrf-runtime";
import { sliceUtf16Safe } from "openclaw/plugin-sdk/text-utility-runtime";
import { FEISHU_HTTP_TIMEOUT_MS } from "./client-timeout.js";
import { getFeishuUserAgent } from "./client.js";
import { readFeishuJsonResponse } from "./json-response.js";
import type { FeishuDomain } from "./types.js";

export type Credentials = {
  appId: string;
  appSecret: string;
  domain?: FeishuDomain;
  httpTimeoutMs?: number;
};

export type FeishuStreamingFetch = typeof fetch;

export type FeishuStreamingDeps = {
  /** Override fetch for tests while preserving the real SSRF guard path. */
  fetchImpl?: FeishuStreamingFetch;
  /** Override hostname lookup for hermetic SSRF-guard tests. */
  lookupFn?: LookupFn;
};

type CardKitResponse = { code?: number; msg?: string };

export const STREAMING_UPDATE_THROTTLE_MS = 160;
const STREAMING_SIGNIFICANT_DELTA_CHARS = 18;
const FEISHU_STREAMING_TOKEN_DEFAULT_LIFETIME_SECONDS = 7200;

// Token cache (keyed by domain + appId)
const tokenCache = new Map<string, { token: string; expiresAt: number }>();

function resolveStreamingTokenExpiresAt(value: unknown, nowMs = Date.now()): number {
  const now = resolveDateTimestampMs(nowMs);
  if (typeof value === "number" && Number.isFinite(value) && value <= 0) {
    return now;
  }
  return (
    resolveExpiresAtMsFromDurationSeconds(value, { nowMs: now }) ??
    resolveExpiresAtMsFromDurationSeconds(FEISHU_STREAMING_TOKEN_DEFAULT_LIFETIME_SECONDS, {
      nowMs: now,
    }) ??
    now
  );
}

export function resolveApiBase(domain?: FeishuDomain): string {
  if (domain === "lark") {
    return "https://open.larksuite.com/open-apis";
  }
  if (domain && domain !== "feishu" && domain.startsWith("http")) {
    return `${domain.replace(/\/+$/, "")}/open-apis`;
  }
  return "https://open.feishu.cn/open-apis";
}

export function resolveAllowedHostnames(domain?: FeishuDomain): string[] {
  if (domain === "lark") {
    return ["open.larksuite.com"];
  }
  if (domain && domain !== "feishu" && domain.startsWith("http")) {
    try {
      return [new URL(domain).hostname];
    } catch {
      return [];
    }
  }
  return ["open.feishu.cn"];
}

export function cancelUnreadResponseBody(response: Response): void {
  // A rejected response leaves its body unread; start cancellation before the
  // guarded dispatcher is released so the connection is not leaked. Do not
  // await: debug capture can tee the stream and deadlock a waiter.
  if (!response.bodyUsed) {
    void response.body?.cancel().catch(() => undefined);
  }
}

export async function assertSuccessfulCardKitResponse(
  response: Response,
  auditContext: string,
  action: string,
): Promise<void> {
  if (!response.ok) {
    cancelUnreadResponseBody(response);
    throw new Error(`${action} failed with HTTP ${response.status}`);
  }
  const data = await readFeishuJsonResponse<CardKitResponse>(response, auditContext);
  if (data.code !== 0) {
    throw new FeishuCardKitError(
      `${action} failed: ${data.msg ?? "unknown error"} (code=${String(data.code)})`,
      data.code ?? 0,
    );
  }
}

/** CardKit API rejection that carries the platform error code for callers that branch on it. */
export class FeishuCardKitError extends Error {
  readonly cardKitCode: number;

  constructor(message: string, cardKitCode: number) {
    super(message);
    this.name = "FeishuCardKitError";
    this.cardKitCode = cardKitCode;
  }
}

/**
 * Feishu closes a card's streaming mode 10 minutes after its last activation
 * regardless of update activity (CardKit streaming updates overview). Content
 * writes against the closed card are rejected with code 300309; the timeout
 * path reports the same condition as 200850. Both codes prescribe the same
 * remedy — PATCH settings streaming_mode=true re-arms the same card.
 */
export const FEISHU_CARDKIT_STREAMING_CLOSED_CODE = 300309;
const CARDKIT_STREAMING_CLOSED_CODES = new Set([FEISHU_CARDKIT_STREAMING_CLOSED_CODE, 200850]);

export function isCardKitStreamingClosedError(error: unknown): boolean {
  return (
    error instanceof FeishuCardKitError && CARDKIT_STREAMING_CLOSED_CODES.has(error.cardKitCode)
  );
}

export async function getToken(creds: Credentials, deps?: FeishuStreamingDeps): Promise<string> {
  const key = `${creds.domain ?? "feishu"}|${creds.appId}`;
  const cached = tokenCache.get(key);
  const rawNow = Date.now();
  const hasValidClock = asDateTimestampMs(rawNow) !== undefined;
  const now = resolveDateTimestampMs(rawNow);
  const minUsableExpiresAt = resolveExpiresAtMsFromDurationSeconds(60, { nowMs: now }) ?? now;
  if (cached && hasValidClock && cached.expiresAt > minUsableExpiresAt) {
    return cached.token;
  }

  const { response, release } = await fetchWithSsrFGuard({
    url: `${resolveApiBase(creds.domain)}/auth/v3/tenant_access_token/internal`,
    init: {
      method: "POST",
      headers: { "Content-Type": "application/json", "User-Agent": getFeishuUserAgent() },
      body: JSON.stringify({ app_id: creds.appId, app_secret: creds.appSecret }),
    },
    fetchImpl: deps?.fetchImpl,
    lookupFn: deps?.lookupFn,
    policy: { allowedHostnames: resolveAllowedHostnames(creds.domain) },
    auditContext: "feishu.streaming-card.token",
    timeoutMs: creds.httpTimeoutMs ?? FEISHU_HTTP_TIMEOUT_MS,
  });
  let data: {
    code: number;
    msg: string;
    tenant_access_token?: string;
    expire?: number;
  };
  try {
    if (!response.ok) {
      cancelUnreadResponseBody(response);
      throw new Error(`Token request failed with HTTP ${response.status}`);
    }
    data = await readFeishuJsonResponse(response, "feishu.streaming-card.token");
  } finally {
    await release();
  }
  if (data.code !== 0 || !data.tenant_access_token) {
    throw new Error(`Token error: ${data.msg}`);
  }
  tokenCache.set(key, {
    token: data.tenant_access_token,
    expiresAt: resolveStreamingTokenExpiresAt(data.expire, now),
  });
  return data.tenant_access_token;
}

export function truncateSummary(text: string, max = 50): string {
  if (!text) {
    return "";
  }
  const clean = text.replace(/\n/g, " ").trim();
  // Slice on a code-point boundary so CardKit never receives a lone surrogate at the limit.
  return clean.length <= max ? clean : sliceUtf16Safe(clean, 0, max - 3) + "...";
}

export function shouldPushStreamingUpdate(previousText: string, nextText: string): boolean {
  return (
    !previousText ||
    /[\n。！？!?；;：:]$/.test(nextText) ||
    nextText.length - previousText.length >= STREAMING_SIGNIFICANT_DELTA_CHARS
  );
}
