// Feishu-side presentation helpers for the shared progress-draft compositor
// (patch 0006 v2). The process lane lives in its own card element inside a
// collapsible panel and is APPEND-ONLY: every published text is a prefix
// extension of the previous one, so CardKit's typewriter only types the new
// rows instead of re-rendering the card on every tool call. The compositor's
// window semantics (in-place updates, rolling eviction) never reach this
// element — new ids append, narration growth extends the last row, failures
// escalate as extra rows.
import {
  isChannelProgressAttentionLine,
  type AgentPlanStep,
  type ChannelProgressDraftLine,
} from "openclaw/plugin-sdk/channel-outbound";

export type ProcessLogBuilderOptions = {
  /** Head-trim beyond this many rows (breaks the prefix once, rarely). */
  maxEntries?: number;
  /** Head-trim beyond this many characters of log text. */
  maxChars?: number;
};

export type ProcessLogBuilder = {
  record: (lines: readonly (string | ChannelProgressDraftLine)[]) => void;
  recordPlan: (steps?: readonly AgentPlanStep[]) => void;
  renderText: () => string;
  entryCount: () => number;
  /** Any escalated ⚠️ row — a single-row log must still keep its panel. */
  hasFailureEntry: () => boolean;
  /** Keyed retraction for mid-turn line retirement (hidden/suppressed items). */
  retract: (keys: readonly string[]) => void;
  reset: () => void;
};

const COMMENTARY_LABEL = "Commentary";
const COMMENTARY_PREFIX_RE = /^💬\s*/u;
// Local approximation of core isShellToolDisplayName (not SDK-exported):
// normalized lowercase plus the "shell" alias the core predicate carries.
const SHELL_TOOL_NAMES = new Set(["bash", "exec", "shell"]);
const DEFAULT_MAX_ENTRIES = 120;
const DEFAULT_MAX_CHARS = 16_000;

type LogEntry = {
  row: string;
  /** Commentary bare text so far, for cumulative-snapshot growth. */
  bare?: string;
  /** Lifecycle status seen on the row, for failure escalation. */
  status?: string;
};

function isCommentary(line: ChannelProgressDraftLine): boolean {
  return line.label === COMMENTARY_LABEL;
}

function commentaryBody(line: ChannelProgressDraftLine): string {
  return line.text.replace(COMMENTARY_PREFIX_RE, "").trim();
}

/**
 * Mirror of core getProgressDraftLineText for log rows: same glyph/label/
 * detail composition. Escalation uses the SDK-exported core attention
 * predicate (covers approval rows and non-zero exit statuses); only the
 * shell-name predicate stays local until core exports it (upstream ask
 * logged in patches/README.md).
 */
function renderProcessLogRow(line: string | ChannelProgressDraftLine): string {
  if (typeof line === "string") {
    return line;
  }
  const icon = line.icon?.trim();
  const prefix = icon ? `${icon} ` : "";
  const label = line.label.trim();
  const detail = line.detail?.trim();
  const status = line.status?.trim();
  const displayStatus = status && status !== "completed" ? status : undefined;
  if (detail) {
    const shell =
      line.toolName !== undefined && SHELL_TOOL_NAMES.has(line.toolName.trim().toLowerCase());
    const showStatus =
      Boolean(displayStatus) &&
      detail !== displayStatus &&
      (line.kind === "command-output" ||
        (displayStatus !== undefined && isChannelProgressAttentionLine(line)));
    const text =
      showStatus && !detail.startsWith(`${displayStatus};`)
        ? `${displayStatus}; ${detail}`
        : detail;
    return label && !shell && (showStatus || line.kind !== "patch")
      ? `${prefix}${label}: ${text}`
      : `${prefix}${text}`;
  }
  if (displayStatus) {
    return label
      ? `${prefix}${label}: ${displayStatus}`
      : line.text.trim() || `${prefix}${displayStatus}`;
  }
  const text = line.text.trim();
  if (!icon && text && text !== label) {
    return text;
  }
  return `${prefix}${label}`.trim();
}

export function createProcessLogBuilder(options?: ProcessLogBuilderOptions): ProcessLogBuilder {
  const maxEntries = options?.maxEntries ?? DEFAULT_MAX_ENTRIES;
  const maxChars = options?.maxChars ?? DEFAULT_MAX_CHARS;
  const entries = new Map<string, LogEntry>();
  // renderText memoization: the compositor publishes several times a second
  // but most publishes change nothing, and each flush needs the full text
  // only to discover it is unchanged. Invalidate on every real mutation.
  let cachedRender: string | null = null;

  const trimOverflow = () => {
    while (entries.size > maxEntries) {
      entries.delete(entries.keys().next().value);
    }
    while (
      [...entries.values()].reduce((sum, entry) => sum + entry.row.length, 0) > maxChars &&
      entries.size > 1
    ) {
      entries.delete(entries.keys().next().value);
    }
  };

  let lastKey: string | undefined;
  const append = (key: string, row: string, extra?: Omit<LogEntry, "row">) => {
    entries.set(key, { row, ...extra });
    lastKey = key;
    cachedRender = null;
    trimOverflow();
  };

  return {
    record(lines) {
      for (const line of lines) {
        const key =
          typeof line === "string" ? `raw:${line}` : line.id?.trim() || `text:${line.text.trim()}`;
        const row = renderProcessLogRow(line);
        if (!row) {
          continue;
        }
        const existing = entries.get(key);
        if (!existing) {
          if (typeof line !== "string" && isCommentary(line)) {
            // Commentary rows stream as cumulative snapshots of one item:
            // recording the bare body (lane prefix stripped) lets growth
            // checks run on the narration text itself, which grows strictly
            // at the end and keeps the row prefix-safe.
            append(key, row, { bare: commentaryBody(line) });
            continue;
          }
          append(key, row, {
            ...(typeof line !== "string" ? { status: line.status?.trim() } : {}),
          });
          continue;
        }
        if (typeof line === "string") {
          continue;
        }
        // Cumulative narration snapshots grow their row, but only while the
        // row is still the LAST one — growing a middle row would rewrite
        // already-typed text and break the append-only prefix contract.
        if (isCommentary(line) && existing.bare !== undefined) {
          const newBody = commentaryBody(line);
          if (
            lastKey === key &&
            newBody.length > existing.bare.length &&
            newBody.startsWith(existing.bare) &&
            row.startsWith(existing.row)
          ) {
            entries.set(key, { ...existing, row, bare: newBody });
            cachedRender = null;
          }
        }
        // A known row that turned failed/blocked escalates as a NEW row:
        // in-place status rewrites are exactly the churn this element exists
        // to avoid.
        const status = line.status?.trim();
        if (status && isChannelProgressAttentionLine(line) && status !== existing.status) {
          append(`${key}:escalate:${status}`, `⚠️ ${row}`);
        }
        if (status && status !== existing.status) {
          existing.status = status;
          cachedRender = null;
        }
      }
    },
    recordPlan(steps) {
      for (const step of steps ?? []) {
        const title = step.step?.trim();
        if (!title) {
          continue;
        }
        const key = `plan:${title}`;
        if (!entries.has(key)) {
          // Plan rows are recorded once, without status glyphs: status
          // transitions would rewrite the row and break the append-only
          // prefix; the step sequence itself is the record.
          append(key, `▸ ${title}`);
        }
      }
    },
    renderText() {
      if (cachedRender === null) {
        cachedRender = [...entries.values()].map((entry) => entry.row).join("\n");
      }
      return cachedRender;
    },
    entryCount() {
      return entries.size;
    },
    hasFailureEntry() {
      for (const entry of entries.values()) {
        if (entry.row.startsWith("⚠️")) {
          return true;
        }
      }
      return false;
    },
    retract(keys) {
      for (const key of keys) {
        // Deleting an existing row invalidates the memo; a key that is not
        // present (never recorded, or already retracted) is a no-op.
        if (entries.delete(key)) {
          cachedRender = null;
        }
      }
    },
    reset() {
      entries.clear();
      lastKey = undefined;
      // cachedRender deliberately NOT cleared (fold/b semantics): an append
      // task already enqueued before this reset must still read the last
      // published text via the memo when it runs — the session-side
      // processLogSent dedupe guards against any duplicate write.
    },
  };
}
