import { isRecord } from "@openclaw/normalization-core/record-coerce";

type CompletionParams = {
  history: unknown;
  sessionKey: string;
  sessionId?: string;
  plannedToolCallId: string;
  plannedToolItemId: string;
  plannedCode: string;
  marker: string;
  deliveryStatus: "sent" | "partial_failed";
};

type CompletionObservation =
  | {
      status: "pending";
      reason: string;
      diagnostics: {
        messageCount: number;
        matchingAssistantCalls: number;
        matchingExecResults: number;
        recentRows: Array<{
          index: number;
          role: "assistant" | "toolResult" | "other";
          isError: boolean;
          contentBlockCount: number;
          currentExecResult: boolean;
        }>;
      };
    }
  | {
      status: "complete";
      sessionId: string;
      toolCallId: string;
      messages: unknown[];
      execResult: Record<string, unknown>;
    };

/** Observe the public history projection, never private result details or escaped JSON text. */
export function observeCodeModeTurnCompletion(params: CompletionParams): CompletionObservation {
  const history = params.history;
  const messages = isRecord(history) && Array.isArray(history.messages) ? history.messages : [];
  const transcriptToolCallId = `${params.plannedToolCallId}|${params.plannedToolItemId}`;
  let matchingAssistantCalls = 0;
  let matchingExecResults = 0;
  // Only bounded structural metadata leaves this poll; transcript content and identities stay private.
  const pending = (reason: string): CompletionObservation => ({
    status: "pending",
    reason,
    diagnostics: {
      messageCount: messages.length,
      matchingAssistantCalls,
      matchingExecResults,
      recentRows: messages.slice(-4).map((row, index) => {
        const message = isRecord(row) ? row : {};
        return {
          index: Math.max(0, messages.length - 4) + index,
          role:
            message.role === "assistant" || message.role === "toolResult" ? message.role : "other",
          isError: message.isError === true,
          contentBlockCount: Array.isArray(message.content) ? message.content.length : 0,
          currentExecResult:
            message.role === "toolResult" &&
            message.toolName === "exec" &&
            message.toolCallId === transcriptToolCallId,
        };
      }),
    },
  });
  if (!isRecord(history) || !Array.isArray(history.messages) || history.messages.length > 1_000) {
    return pending("history unavailable");
  }
  if (
    history.sessionKey !== params.sessionKey ||
    typeof history.sessionId !== "string" ||
    !history.sessionId ||
    (params.sessionId !== undefined && history.sessionId !== params.sessionId)
  ) {
    return pending("session identity mismatch");
  }
  if (
    !params.plannedToolCallId ||
    !params.plannedToolItemId ||
    !params.marker ||
    !params.plannedCode.includes(params.marker)
  ) {
    return pending("current provider plan unavailable");
  }
  const calls = history.messages.flatMap((message, index) =>
    isRecord(message) && message.role === "assistant" && Array.isArray(message.content)
      ? message.content.flatMap((block) =>
          isRecord(block) &&
          block.type === "toolCall" &&
          block.name === "exec" &&
          block.id === transcriptToolCallId &&
          isRecord(block.arguments) &&
          block.arguments.code === params.plannedCode
            ? [index]
            : [],
        )
      : [],
  );
  matchingAssistantCalls = calls.length;
  if (calls.length !== 1) {
    return pending("current assistant exec identity unavailable or ambiguous");
  }
  const results = history.messages.flatMap((message, index) =>
    isRecord(message) &&
    message.role === "toolResult" &&
    message.toolName === "exec" &&
    message.toolCallId === transcriptToolCallId
      ? [{ message, index }]
      : [],
  );
  matchingExecResults = results.length;
  if (results.length !== 1 || results[0]!.index <= calls[0]!) {
    return pending("current exec result unavailable or ambiguous");
  }
  const { message, index } = results[0]!;
  if (index !== history.messages.length - 1) {
    return pending("current exec is not the final transcript result");
  }
  if (message.isError === true) {
    return pending("current exec transcript result is an error");
  }
  const textBlocks = Array.isArray(message.content)
    ? message.content.filter((block) => isRecord(block) && block.type === "text")
    : [];
  const text = textBlocks.length === 1 ? textBlocks[0]?.text : undefined;
  if (typeof text !== "string" || text.length > 64_000) {
    return pending("current exec text unavailable or oversized");
  }
  let result: unknown;
  try {
    result = JSON.parse(text);
  } catch {
    return pending("current exec text is not complete JSON");
  }
  if (!isRecord(result) || result.status !== "completed") {
    return pending("current exec has not completed");
  }
  const value = result.value;
  const sent = isRecord(value) ? value.sent : undefined;
  if (
    !isRecord(value) ||
    value.observed !== true ||
    !isRecord(sent) ||
    sent.status !== params.deliveryStatus ||
    (params.deliveryStatus === "partial_failed" &&
      (sent.sentBeforeError !== true || typeof sent.error !== "string" || !sent.error.trim()))
  ) {
    return pending("current delivery outcome or following observation mismatch");
  }
  if (!isRecord(history.sessionInfo) || history.sessionInfo.hasActiveRun !== false) {
    return pending("session still active or activity unknown");
  }
  return {
    status: "complete",
    sessionId: history.sessionId,
    toolCallId: transcriptToolCallId,
    messages: history.messages,
    execResult: result,
  };
}
