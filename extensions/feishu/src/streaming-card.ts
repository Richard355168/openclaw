/**
 * Feishu Streaming Card - Card Kit streaming API for real-time text output
 */

import type { Client } from "@larksuiteoapi/node-sdk";
import { readProviderJsonResponse } from "openclaw/plugin-sdk/provider-http";
import { fetchWithSsrFGuard } from "openclaw/plugin-sdk/ssrf-runtime";
import { FEISHU_HTTP_TIMEOUT_MS } from "./client-timeout.js";
import { getFeishuUserAgent } from "./client.js";
import { sendFeishuMessage } from "./message-send.js";
import { resolveFeishuCardTemplate } from "./native-card.js";
import {
  createFeishuPartialReplyDeliveryError,
  createFeishuReplyDeliveryResult,
  type FeishuReplyDeliveryResult,
} from "./reply-delivery-result.js";
import type { CardHeaderConfig } from "./send.js";
import {
  assertSuccessfulCardKitResponse,
  cancelUnreadResponseBody,
  getToken,
  resolveAllowedHostnames,
  resolveApiBase,
  type Credentials,
  type FeishuStreamingDeps,
  type FeishuStreamingFetch,
  shouldPushStreamingUpdate,
  STREAMING_UPDATE_THROTTLE_MS,
  truncateSummary,
} from "./streaming-card-wire.js";

type CardState = {
  cardId: string;
  messageId?: string;
  sequence: number;
  currentText: string;
  sentText: string;
  hasNote: boolean;
  /** Append-only process log (patch 0006 v2): sent vs desired text. */
  processLogSent: string;
  processLogDesired: string;
  processLogEntries: number;
  /** Any escalated failure row — a single-row log still keeps its panel. */
  processLogHasFailure: boolean;
};

type FeishuStreamingCloseResult = {
  visibleReplySent: boolean;
  content?: string;
  messageId?: string;
  /**
   * False when the card rejected the final text (for example after CardKit closed
   * streaming mode). Earlier accepted preview text may still be visible, but it is
   * not the completed answer, so the dispatcher must deliver that answer another way.
   */
  finalTextAccepted?: boolean;
};

/** Provider finalization failed after a streaming card may already be visible. */
export class FeishuStreamingFinalizationError extends Error {
  readonly result: FeishuStreamingCloseResult;

  constructor(cause: unknown, result: FeishuStreamingCloseResult) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = "FeishuStreamingFinalizationError";
    this.result = result;
  }
}

/** Options for customising the initial streaming card appearance. */
type StreamingStartOptions = {
  replyToMessageId?: string;
  replyInThread?: boolean;
  rootId?: string;
  header?: CardHeaderConfig;
  note?: string;
};

// CardKit closes streaming_mode ten minutes after it was last enabled, then rejects
// element writes with 200850 (timed out) or 300309 (closed).
const STREAMING_CLOSED_ERROR = /\(code=(200850|300309)\)$/;

/** Streaming card session manager */
export class FeishuStreamingSession {
  private client: Client;
  private creds: Credentials;
  private state: CardState | null = null;
  private queue: Promise<void> = Promise.resolve();
  private closed = false;
  private log?: (msg: string) => void;
  private lastUpdateTime = 0;
  private pendingText: string | null = null;
  // Resolves with confirmed transport acceptance once a flush cycle attempts
  // the exact text; publication caching upstream must never advance on a
  // write the card API rejected.
  private contentFlushWaiters: Array<{ text: string; resolve: (ok: boolean) => void }> = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private updateThrottleMs = STREAMING_UPDATE_THROTTLE_MS;
  private fetchImpl?: FeishuStreamingFetch;
  private lookupFn?: LookupFn;
  private deps?: FeishuStreamingDeps;
  // Process-log throttle state (patch 0006 v2): the timer mirrors the answer
  // channel's flushTimer; the desired text itself lives on the card state
  // (processLogDesired) and the write clock is shared with the answer channel
  // (lastUpdateTime — every card element PUT consumes one single-card budget).
  private processLogTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    client: Client,
    creds: Credentials,
    log?: (msg: string) => void,
    deps?: FeishuStreamingDeps,
  ) {
    this.client = client;
    this.creds = creds;
    this.log = log;
    this.deps = deps;
  }

  private async requestCardKit<T>(
    path: string,
    operation: string,
    method: "POST" | "PUT" | "PATCH" | "DELETE",
    body: () => Record<string, unknown>,
    readResponse: (response: Response, auditContext: string) => Promise<T>,
    token?: string,
  ): Promise<T> {
    const auditContext = `feishu.streaming-card.${operation}`;
    const { response, release } = await fetchWithSsrFGuard({
      url: `${resolveApiBase(this.creds.domain)}/cardkit/v1/cards${path}`,
      init: {
        method,
        headers: {
          Authorization: `Bearer ${token ?? (await getToken(this.creds, this.deps))}`,
          "Content-Type":
            method === "PATCH" ? "application/json; charset=utf-8" : "application/json",
          "User-Agent": getFeishuUserAgent(),
        },
        // Token renewal can await; read the current sequence only at dispatch.
        body: JSON.stringify(body()),
      },
      fetchImpl: this.deps?.fetchImpl,
      lookupFn: this.deps?.lookupFn,
      policy: { allowedHostnames: resolveAllowedHostnames(this.creds.domain) },
      auditContext,
      timeoutMs: this.creds.httpTimeoutMs ?? FEISHU_HTTP_TIMEOUT_MS,
    });
    try {
      return await readResponse(response, auditContext);
    } finally {
      await release();
    }
  }

  async start(
    receiveId: string,
    receiveIdType: "open_id" | "user_id" | "union_id" | "email" | "chat_id" = "chat_id",
    options?: StreamingStartOptions,
  ): Promise<void> {
    if (this.state) {
      return;
    }

    const elements: Record<string, unknown>[] = [
      { tag: "markdown", content: "", element_id: "content" },
      // Process lane (patch 0006 v2): append-only log rows inside a
      // collapsible panel. Content PUTs inside the panel stream fine
      // (QwenPaw #7570, live-verified), and prefix-only growth lets CardKit
      // type just the new rows instead of re-rendering on every tool call.
      // expanded defaults to FALSE and must stay false in the card JSON:
      // every content PUT produces a new card data version and the client
      // re-render resets the panel to the JSON value (live-verified 2026-11-07
      // — a user-collapsed panel popped back open with expanded: true). A
      // false baseline keeps the panel collapsed across updates; a manually
      // expanded view folds back on the next write (platform limitation —
      // the panel exposes no interaction callback to track user state).
      // The divider owns an element id so the terminal fold can remove it
      // together with the panel — a lone hr would survive on the closed card.
      { tag: "hr", element_id: "process_log_divider" },
      {
        tag: "collapsible_panel",
        element_id: "process_log",
        expanded: false,
        header: {
          title: { tag: "plain_text", content: "执行过程" },
          vertical_align: "center",
          icon: {
            tag: "standard_icon",
            token: "down-small-ccm_outlined",
            color: "",
            size: "16px 16px",
          },
          icon_position: "right",
          icon_expanded_angle: -180,
        },
        border: { color: "grey", corner_radius: "5px" },
        vertical_spacing: "8px",
        padding: "8px 8px 8px 8px",
        elements: [{ tag: "markdown", content: "", element_id: "process_log_content" }],
      },
    ];
    if (options?.note) {
      elements.push({ tag: "hr" });
      elements.push({
        tag: "markdown",
        content: `<font color='grey'>${options.note}</font>`,
        element_id: "note",
      });
    }
    const cardJson: Record<string, unknown> = {
      schema: "2.0",
      config: {
        streaming_mode: true,
        summary: { content: "[Generating...]" },
        streaming_config: { print_frequency_ms: { default: 50 }, print_step: { default: 1 } },
      },
      body: { elements },
    };
    if (options?.header) {
      cardJson.header = {
        title: { tag: "plain_text", content: options.header.title },
        template: resolveFeishuCardTemplate(options.header.template) ?? "blue",
      };
    }

    const createData = await this.requestCardKit(
      "",
      "create",
      "POST",
      () => ({ type: "card_json", data: JSON.stringify(cardJson) }),
      async (response, auditContext) => {
        if (!response.ok) {
          cancelUnreadResponseBody(response);
          throw new Error(`Create card request failed with HTTP ${response.status}`);
        }
        return await readProviderJsonResponse<{
          code: number;
          msg: string;
          data?: { card_id: string };
        }>(response, auditContext);
      },
    );
    if (createData.code !== 0 || !createData.data?.card_id) {
      throw new Error(`Create card failed: ${createData.msg}`);
    }
    const cardId = createData.data.card_id;
    const cardContent = JSON.stringify({ type: "card", data: { card_id: cardId } });

    // Prefer message.reply when we have a reply target — reply_in_thread
    // reliably routes streaming cards into Feishu topics, whereas
    // message.create with root_id may silently ignore root_id for card
    // references (card_id format).
    const sendRes = await sendFeishuMessage(
      { client: this.client, receiveId, receiveIdType },
      { ...options, content: cardContent, msgType: "interactive" },
      "Send card failed",
    );
    if (sendRes.code !== 0) {
      throw new Error(`Send card failed: ${sendRes.msg}`);
    }

    const messageId = sendRes.data?.message_id?.trim();
    this.state = {
      cardId,
      ...(messageId ? { messageId } : {}),
      sequence: 1,
      currentText: "",
      sentText: "",
      hasNote: Boolean(options?.note),
      processLogSent: "",
      processLogDesired: "",
      processLogEntries: 0,
      processLogHasFailure: false,
    };
    this.log?.(`Started streaming: cardId=${cardId}${messageId ? `, messageId=${messageId}` : ""}`);
  }

  // Re-enabling streaming_mode in card settings reopens a closed window on the same card.
  // Setting it while already on does not restart the window, so turn it off first.
  private async putElement(
    element: string,
    operation: string,
    action: string,
    body: (sequence: number) => Record<string, unknown>,
    token?: string,
  ): Promise<void> {
    const state = this.state!;
    const put = () => {
      state.sequence += 1;
      return this.requestCardKit(
        `/${state.cardId}/elements/${element}`,
        operation,
        "PUT",
        () => body(state.sequence),
        (response, auditContext) => assertSuccessfulCardKitResponse(response, auditContext, action),
        token,
      );
    };
    try {
      await put();
    } catch (error) {
      if (!STREAMING_CLOSED_ERROR.test(String(error))) {
        throw error;
      }
      for (const streaming_mode of [false, true]) {
        await this.updateSettings({ streaming_mode }, "reopen", "Reopen streaming card");
      }
      await put();
    }
  }

  private async updateSettings(
    config: Record<string, unknown>,
    operation: string,
    action: string,
  ): Promise<void> {
    const state = this.state!;
    state.sequence += 1;
    await this.requestCardKit(
      `/${state.cardId}/settings`,
      operation,
      "PATCH",
      () => ({
        settings: JSON.stringify({ config }),
        sequence: state.sequence,
        uuid: `c_${state.cardId}_${state.sequence}`,
      }),
      (response, auditContext) => assertSuccessfulCardKitResponse(response, auditContext, action),
    );
  }

  private async writeCardContent(
    text: string,
    onError?: (error: unknown) => void,
  ): Promise<boolean> {
    if (!this.state) {
      return false;
    }
    try {
      await this.putElement(
        "content/content",
        "update",
        "Update card content",
        (sequence) => ({
          content: text,
          sequence,
          uuid: `s_${this.state!.cardId}_${sequence}`,
        }),
      );
      return true;
    } catch (error) {
      onError?.(error);
      return false;
    }
  }

  private clearFlushTimer(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private schedulePendingFlush(): void {
    if (this.flushTimer || !this.pendingText || this.closed) {
      return;
    }
    const delayMs = Math.max(0, this.updateThrottleMs - (Date.now() - this.lastUpdateTime));
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      if (!this.pendingText || this.closed) {
        return;
      }
      this.lastUpdateTime = Date.now();
      void this.flushPendingUpdate().catch((error: unknown) =>
        this.log?.(`Scheduled flush update failed: ${String(error)}`),
      );
    }, delayMs);
  }

  private async flushPendingUpdate(): Promise<void> {
    this.queue = this.queue.then(async () => {
      try {
        if (!this.state || this.closed) {
          return;
        }
        const nextText = this.pendingText;
        if (!nextText) {
          return;
        }
        this.pendingText = null;
        if (nextText === this.state.sentText) {
          return;
        }
        const sent = await this.writeCardContent(nextText, (e) =>
          this.log?.(`Update failed: ${String(e)}`),
        );
        if (sent && this.state) {
          this.state.sentText = nextText;
        } else if (!sent && this.state && !this.closed) {
          // A rejected write must not lose the text: keep it pending so the
          // next update retries it. Retries stay caller-driven — a rejected
          // write self-rescheduling here would retry-loop against an outage.
          this.pendingText = this.pendingText ?? nextText;
        }
      } finally {
        this.settleContentFlushWaiters();
      }
    });
    await this.queue;
  }

  private settleContentFlushWaiters(): void {
    const sentText = this.state?.sentText ?? "";
    for (const waiter of this.contentFlushWaiters.splice(0)) {
      waiter.resolve(this.closed ? false : sentText === waiter.text);
    }
  }

  async update(text: string): Promise<void> {
    if (!this.state || this.closed || !text) {
      return;
    }
    // The caller supplies the complete current card text. CardKit derives its own
    // display delta, so merging snapshots here can duplicate divergent reasoning.
    this.state.currentText = text;
    this.pendingText = text;
    this.clearFlushTimer();

    const shouldForceUpdate = shouldPushStreamingUpdate(this.state.sentText, text);
    const now = Date.now();
    if (!shouldForceUpdate && now - this.lastUpdateTime < this.updateThrottleMs) {
      this.schedulePendingFlush();
      return;
    }
    this.lastUpdateTime = now;
    await this.flushPendingUpdate();
  }

  // Parks a waiter for an upcoming write of `text` and resolves with the
  // transport outcome of the flush cycle that attempts it: rejected writes
  // report false so publication caching upstream (the progress-draft
  // compositor) keeps the draft unacknowledged, superseded snapshots resolve
  // false for the same reason. Callers register BEFORE update() — an
  // immediate rejected attempt settles observers inside update() and never
  // schedules another cycle, so a later registration could never settle.
  registerContentObserver(text: string): Promise<boolean> {
    if (!this.state || this.closed) {
      return Promise.resolve(false);
    }
    return new Promise<boolean>((resolve) => {
      this.contentFlushWaiters.push({ text, resolve });
    });
  }

  private async updateNoteContent(note: string): Promise<void> {
    if (!this.state || !this.state.hasNote) {
      return;
    }
    // Token failures propagate; only the note request itself is best effort.
    const token = await getToken(this.creds, this.deps);
    await this.putElement(
      "note/content",
      "note-update",
      "Update card note",
      (sequence) => ({
        content: `<font color='grey'>${note}</font>`,
        sequence,
        uuid: `n_${this.state!.cardId}_${sequence}`,
      }),
      token,
    ).catch((e: unknown) => this.log?.(`Note update failed: ${String(e)}`));
  }

  async closeWithResult(
    finalText?: string,
    options?: { note?: string },
  ): Promise<FeishuReplyDeliveryResult> {
    if (!this.state || this.closed) {
      return { visibleReplySent: false };
    }
    this.closed = true;
    this.clearFlushTimer();
    this.settleProcessLogPending();
    await this.queue;
    this.settleContentFlushWaiters();

    const text = finalText ?? this.pendingText ?? this.state.currentText;
    // A failed final rewrite does not erase previously accepted visible content.
    // sentText advances only for an accepted write; the return value reports any visible content.
    let visibleContentSent = Boolean(this.state.sentText.trim());
    let finalWriteError: unknown;

    // Only send final update if content differs from what's already displayed.
    // An explicit empty final text clears a transient preview before closeout.
    if ((text || finalText !== undefined) && text !== this.state.sentText) {
      // Always publish through the content-level form: live CardKit accepts
      // element-level replaces with code 0 but never applies them while
      // streaming_mode is on, and a progress-draft final diverges from the
      // last streamed snapshot by construction, so the element form would
      // freeze the card on that snapshot (observed on production CardKit;
      // https://github.com/openclaw/openclaw/pull/148176#issuecomment-6377376895).
      const sent = await this.writeCardContent(text, (e) => {
        finalWriteError = e;
        this.log?.(`Final update failed: ${String(e)}`);
      });
      this.state.currentText = text;
      if (sent) {
        this.state.sentText = text;
        visibleContentSent = Boolean(text.trim());
      }
    }

    // Update note with final model/provider info
    if (options?.note) {
      await this.updateNoteContent(options.note);
    }

    // A rejected final write must not advertise content that CardKit never accepted.
    const acceptedText = this.state.sentText;
    let closeError: unknown;
    try {
      await this.updateSettings(
        { streaming_mode: false, summary: { content: truncateSummary(acceptedText) } },
        "close",
        "Close streaming card",
      );
    } catch (error: unknown) {
      closeError = error;
      this.log?.(`Close failed: ${String(error)}`);
    }

    // Terminal fold (patch 0006 v2): collapse the process panel after close,
    // or remove it when the turn produced no process rows. Element PATCH
    // post-close is the QwenPaw #7570 verified path; a failure here is logged
    // only — the streamed card (answer + panel) stays fully usable either way.
    if (closeError === undefined) {
      // Awaited (not fire-and-forget): the fold is part of the close contract
      // the tests assert on, and keeping it synchronous preserves the
      // "close returned ⇒ panel already folded" invariant.
      await this.finalizeProcessPanel().catch((error: unknown) =>
        this.log?.(`Process panel finalize skipped: ${String(error)}`),
      );
    }

    const finalState = this.state;
    this.state = null;
    this.pendingText = null;

    this.log?.(`Closed streaming: cardId=${finalState.cardId}`);
    const result: FeishuReplyDeliveryResult = {
      ...createFeishuReplyDeliveryResult({
        results: [finalState],
        visibleReplySent: visibleContentSent,
        ...(visibleContentSent ? { content: finalState.sentText } : {}),
        kind: "card",
      }),
      ...(finalWriteError === undefined ? {} : { finalTextAccepted: false }),
    };
    if (finalWriteError !== undefined || closeError !== undefined) {
      const cause =
        finalWriteError !== undefined && closeError !== undefined
          ? new AggregateError(
              [finalWriteError, closeError],
              "Feishu streaming card finalization failed",
            )
          : (finalWriteError ?? closeError);
      throw new FeishuStreamingFinalizationError(cause, result);
    }
    return result;
  }

  // Enqueues one append of the process log element. The text is the FULL
  // cumulative log and only ever grows (append-only builder), so CardKit's
  // typewriter renders just the new suffix; a rejected write keeps the text
  // desired so the next append retries it. Bursts coalesce: the queued task
  // always writes the latest desired text.
  async appendProcessLog(text: string, entryCount: number, hasFailure = false): Promise<void> {
    if (!this.state || this.closed || !text) {
      return;
    }
    if (text === this.state.processLogSent) {
      return;
    }
    this.state.processLogDesired = text;
    this.state.processLogEntries = Math.max(this.state.processLogEntries, entryCount);
    this.state.processLogHasFailure = hasFailure;
    // Same throttle cadence as the answer channel: narration deltas stream
    // per publish, so without this gate the log PUT rate tracks the publish
    // rate (a narration burst crosses the 10 req/s single-card budget and a
    // 429 would turn every later event into one rejected retry).
    this.scheduleProcessLogFlush();
  }

  private scheduleProcessLogFlush(): void {
    if (this.processLogTimer || this.closed) {
      return;
    }
    // Shared write clock with the answer channel: an answer PUT and a log PUT
    // draw on the same single-card rate budget, so both gates read one clock
    // and a burst on either side defers the other by at most one window.
    const delayMs = Math.max(0, this.updateThrottleMs - (Date.now() - this.lastUpdateTime));
    this.processLogTimer = setTimeout(() => {
      this.processLogTimer = null;
      if (this.closed) {
        return;
      }
      this.lastUpdateTime = Date.now();
      this.enqueueProcessLogWrite("process-log");
    }, delayMs);
  }

  // One queued process-log PUT against the confirmed Sent text, routed
  // through putElement so a closed streaming window reopens on the same card
  // before the one retry. During pure-tool phases the answer channel is
  // silent and this PUT is the only live writer, so it must be able to
  // re-arm the window itself (2026-11-26 lesson: the old direct
  // requestCardKit bypass left the panel frozen when the window expired).
  // Failures keep the text desired: the next append retries it.
  private enqueueProcessLogWrite(operation: string, options?: { allowClosed?: boolean }): void {
    this.queue = this.queue.then(async () => {
      // The settle variant runs inside closeWithResult/discard after the
      // `closed` flag is already set: streaming mode is still on, so this
      // write is legal (the flag gates the answer channel's update path, not
      // this settle — live-verified "Process panel collapsed" follows a
      // successful settings close).
      if (!this.state || (this.closed && !options?.allowClosed)) {
        return;
      }
      const desired = this.state.processLogDesired;
      if (!desired || desired === this.state.processLogSent) {
        return;
      }
      try {
        await this.putElement(
          "process_log_content/content",
          operation,
          "Append process log",
          (sequence) => ({
            content: desired,
            sequence,
            uuid: `p_${this.state!.cardId}_${sequence}`,
          }),
        );
        this.state.processLogSent = desired;
      } catch (error) {
        this.log?.(`Process log update failed: ${String(error)}`);
      }
    });
  }

  // Settles the latest desired log text before the close path drains the
  // queue, so the final rows reach the panel while the session is still
  // writable (the throttle timer is cancelled — it would see `closed`).
  private settleProcessLogPending(): void {
    if (this.processLogTimer) {
      clearTimeout(this.processLogTimer);
      this.processLogTimer = null;
    }
    if (!this.state) {
      return;
    }
    this.lastUpdateTime = Date.now();
    this.enqueueProcessLogWrite("process-log-final", { allowClosed: true });
  }

  // Terminal fold: collapse the process panel (log rows carry real content —
  // multiple rows or a failure escalation) or remove it otherwise, so one-step
  // turns like a compact action do not keep a "执行过程 · 1 步" divider block.
  // Both run after the settings close, where element PATCH applies freely
  // (QwenPaw #7570 verified post-close); failures never propagate — the
  // streamed card is already complete and usable either way.
  private async finalizeProcessPanel(): Promise<void> {
    if (!this.state) {
      return;
    }
    const state = this.state;
    if (state.processLogSent && (state.processLogEntries > 1 || state.processLogHasFailure)) {
      state.sequence += 1;
      await this.requestCardKit(
        `/${state.cardId}/elements/process_log`,
        "panel-collapse",
        "PATCH",
        () => ({
          partial_element: JSON.stringify({
            expanded: false,
            header: {
              title: { tag: "plain_text", content: `执行过程 · ${state.processLogEntries} 步` },
            },
          }),
          sequence: state.sequence,
          uuid: `c_${state.cardId}_${state.sequence}`,
        }),
        (response, auditContext) =>
          assertSuccessfulCardKitResponse(response, auditContext, "Collapse process panel"),
      );
      this.log?.("Process panel collapsed on the closed card");
      return;
    }
    state.sequence += 1;
    await this.requestCardKit(
      `/${state.cardId}/elements/process_log`,
      "panel-remove",
      "DELETE",
      () => ({
        sequence: state.sequence,
        uuid: `d_${state.cardId}_${state.sequence}`,
      }),
      (response, auditContext) =>
        assertSuccessfulCardKitResponse(response, auditContext, "Remove process panel"),
    );
    // The hr divider above the panel is ours (element_id at card build);
    // it must go with the panel or a lone line survives on the closed
    // card. Secondary cleanup: never blocks the fold.
    try {
      state.sequence += 1;
      await this.requestCardKit(
        `/${state.cardId}/elements/process_log_divider`,
        "panel-divider-remove",
        "DELETE",
        () => ({
          sequence: state.sequence,
          uuid: `d_${state.cardId}_${state.sequence}`,
        }),
        (response, auditContext) =>
          assertSuccessfulCardKitResponse(response, auditContext, "Remove process panel divider"),
      );
    } catch (error: unknown) {
      this.log?.(
        `Process panel divider removal skipped: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
    this.log?.("Process panel removed on the closed card");
  }

  async discard(): Promise<FeishuStreamingCloseResult> {
    if (!this.state || this.closed) {
      return { visibleReplySent: false };
    }
    const { cardId, messageId } = this.state;
    if (!messageId) {
      // Accepted cards without a message receipt can still be cleared by card id.
      return this.closeWithResult("");
    }
    this.closed = true;
    this.clearFlushTimer();
    // No settleProcessLogPending here: any pending rows are about to be
    // deleted together with the whole message; on delete failure the fallback
    // closeWithResult("") performs its own settle.
    await this.queue;
    this.settleContentFlushWaiters();

    try {
      const response = await this.client.im.message.delete({
        path: { message_id: messageId },
      });
      if (response.code !== undefined && response.code !== 0) {
        throw new Error(`Delete streaming card message failed: ${response.msg ?? response.code}`);
      }
      this.state = null;
      this.pendingText = null;
      this.log?.(`Discarded streaming card: cardId=${cardId}`);
      return { visibleReplySent: false };
    } catch (error) {
      this.log?.(`Discard failed: ${String(error)}`);
      this.closed = false;
      // A rejected clear leaves accepted text visible; preserve its receipt and failure.
      return this.closeWithResult("");
    }
  }

  isActive(): boolean {
    return this.state !== null && !this.closed;
  }
}
