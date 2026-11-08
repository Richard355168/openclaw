import { describe, expect, it } from "vitest";
import { buildFeishuCompactionProgressLine } from "./progress-draft.js";

describe("feishu progress draft helpers", () => {
  it("builds compaction progress lines with a stable id", () => {
    expect(buildFeishuCompactionProgressLine("start")).toMatchObject({
      id: "context-compaction",
      kind: "item",
    });
    expect(buildFeishuCompactionProgressLine("complete").label).toBe("Compaction complete");
  });
});
