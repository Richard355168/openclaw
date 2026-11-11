// Feishu process-log builder tests (patch 0006 v2): the log element is
// append-only — new ids append rows, cumulative narration extends the last
// row, failures escalate as new rows. renderText() must never shrink.
import { describe, expect, it } from "vitest";
import { createProcessLogBuilder } from "./progress-display-text.js";

describe("createProcessLogBuilder", () => {
  const line = (id: string, text: string, extra?: Partial<{ status: string; label: string }>) => ({
    id,
    kind: "tool" as const,
    text,
    label: extra?.label ?? "",
    ...(extra?.status ? { status: extra.status } : {}),
  });

  it("appends new lines in arrival order and renders the cumulative text", () => {
    const builder = createProcessLogBuilder();
    builder.record([line("a", "💬 _先读取配置文件。_"), line("b", "🔎 Web Search")]);
    expect(builder.renderText()).toBe("💬 _先读取配置文件。_\n🔎 Web Search");
    expect(builder.entryCount()).toBe(2);
  });

  it("extends the last narration row when a cumulative snapshot grows", () => {
    const builder = createProcessLogBuilder();
    const narration = (text: string) => ({
      id: "commentary:1",
      kind: "item" as const,
      text,
      label: "Commentary",
    });
    builder.record([narration("💬 先读取配置文件。")]);
    const first = builder.renderText();
    builder.record([narration("💬 先读取配置文件。然后检查模板引擎。")]);
    const second = builder.renderText();
    expect(first).toBe("💬 先读取配置文件。");
    expect(second.startsWith(first)).toBe(true);
    expect(second).toBe("💬 先读取配置文件。然后检查模板引擎。");
  });

  it("does not extend a narration row once it is no longer the last row", () => {
    const builder = createProcessLogBuilder();
    const narration = (text: string) => ({
      id: "commentary:1",
      kind: "item" as const,
      text,
      label: "Commentary",
    });
    builder.record([narration("💬 _先读取配置文件。_"), line("tool-1", "🛠️ Exec: ls")]);
    const before = builder.renderText();
    builder.record([
      line("tool-1", "🛠️ Exec: ls"),
      narration("💬 _先读取配置文件。然后补充了很多内容。_"),
    ]);
    expect(builder.renderText()).toBe(before);
  });

  it("escalates a failed status as a new warning row instead of rewriting", () => {
    const builder = createProcessLogBuilder();
    builder.record([line("cmd-1", "🛠️ Exec: pnpm test")]);
    const before = builder.renderText();
    builder.record([line("cmd-1", "🛠️ Exec: pnpm test", { status: "failed" })]);
    const after = builder.renderText();
    expect(after.startsWith(before)).toBe(true);
    expect(after).toContain("⚠️");
    expect(builder.entryCount()).toBe(2);
  });

  it("ignores ordinary in-place updates on known rows", () => {
    const builder = createProcessLogBuilder();
    builder.record([line("cmd-1", "🛠️ Exec: pnpm test", { status: "running" })]);
    const before = builder.renderText();
    builder.record([line("cmd-1", "🛠️ Exec: pnpm test", { status: "completed" })]);
    expect(builder.renderText()).toBe(before);
  });

  it("renders tool rows with icon, label and detail like the draft window", () => {
    const builder = createProcessLogBuilder();
    builder.record([
      {
        id: "cmd-1",
        kind: "tool",
        text: "🛠️ Exec: pnpm test",
        label: "Exec",
        icon: "🛠️",
        detail: "pnpm test",
        toolName: "exec",
      },
    ]);
    expect(builder.renderText()).toBe("🛠️ pnpm test");
  });

  it("renders label-only rows for tools without detail", () => {
    const builder = createProcessLogBuilder();
    builder.record([{ id: "ws-1", kind: "tool", text: "", label: "Web Search", icon: "🔎" }]);
    expect(builder.renderText()).toBe("🔎 Web Search");
  });

  it("handles plain string lines", () => {
    const builder = createProcessLogBuilder();
    builder.record(["raw line"]);
    expect(builder.renderText()).toBe("raw line");
  });

  it("head-trims beyond the entry cap and rebuilds the text", () => {
    const builder = createProcessLogBuilder({ maxEntries: 2 });
    builder.record([line("1", "one"), line("2", "two"), line("3", "three")]);
    expect(builder.renderText()).toBe("two\nthree");
    expect(builder.entryCount()).toBe(2);
  });

  it("resets cleanly", () => {
    const builder = createProcessLogBuilder();
    builder.record([line("a", "x")]);
    builder.reset();
    expect(builder.renderText()).toBe("");
    expect(builder.entryCount()).toBe(0);
  });
});
