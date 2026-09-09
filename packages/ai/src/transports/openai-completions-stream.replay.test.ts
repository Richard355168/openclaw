import { createServer } from "node:http";
import { afterAll, describe, expect, it } from "vitest";
import { resetDiagnosticRunActivityForTest } from "../../../../src/logging/diagnostic-run-activity.js";
import { streamOpenAICompletions } from "../providers/openai-completions.js";
import { createOpenAICompletionsTransportStreamFn } from "./openai-completions-transport.js";
import { makeCompletionsChunk, makeCompletionsModel } from "./openai-completions.test-support.js";

const TEXT_A = "The harbor lights flicker at dusk. ";
const TEXT_B = "Ferries cross until midnight. ";
const TEXT_C = "Then the tide takes over. ";

type ReplayChunk = ReturnType<typeof makeCompletionsChunk>;

type ReplayCase = {
  chunks: ReplayChunk[];
  compat?: Record<string, unknown>;
  expectedText: string;
};

async function runStream(
  createStream: typeof streamOpenAICompletions,
  caseInput: ReplayCase,
): Promise<string> {
  const server = createServer((req, res) => {
    req.setEncoding("utf8");
    req.on("data", () => {});
    req.on("end", () => {
      res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8" });
      for (const chunk of caseInput.chunks) {
        res.write(`data: ${JSON.stringify(chunk)}\n\n`);
      }
      res.end("data: [DONE]\n\n");
    });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("Missing loopback server address");
    }
    const model = makeCompletionsModel({
      provider: "compatible-proxy",
      baseUrl: `http://127.0.0.1:${address.port}/v1`,
      reasoning: false,
      ...(caseInput.compat ? { compat: caseInput.compat } : {}),
    });
    const stream = await createStream(
      model,
      { messages: [{ role: "user", content: "Stream the text.", timestamp: 1 }] },
      { apiKey: "synthetic-test-key" },
    );
    const result = await stream.result();
    return result.content
      .filter((block) => block.type === "text")
      .map((block) => (block as { type: "text"; text: string }).text)
      .join("");
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

const replayChunks = (): ReplayChunk[] => [
  makeCompletionsChunk({ role: "assistant", content: TEXT_A }),
  makeCompletionsChunk({ content: TEXT_B }),
  // Provider quirk under test: one frame restating everything so far.
  makeCompletionsChunk({ content: TEXT_A + TEXT_B }),
  makeCompletionsChunk({ content: TEXT_C }),
  makeCompletionsChunk({}, "stop"),
];

describe.each([
  { name: "direct", createStream: streamOpenAICompletions },
  { name: "managed", createStream: createOpenAICompletionsTransportStreamFn() },
])("$name cumulative text delta replays", ({ createStream }) => {
  afterAll(() => {
    resetDiagnosticRunActivityForTest();
  });

  it("drops a bare delta that restates the whole accumulated text when enabled", async () => {
    const text = await runStream(createStream, {
      chunks: replayChunks(),
      compat: { dropCumulativeTextDeltaReplays: true },
      expectedText: TEXT_A + TEXT_B + TEXT_C,
    });
    expect(text).toBe(TEXT_A + TEXT_B + TEXT_C);
  });

  it("keeps the historical append behavior when disabled", async () => {
    const text = await runStream(createStream, {
      chunks: replayChunks(),
      expectedText: TEXT_A + TEXT_B + (TEXT_A + TEXT_B) + TEXT_C,
    });
    expect(text).toBe(TEXT_A + TEXT_B + (TEXT_A + TEXT_B) + TEXT_C);
  });

  it("preserves short exact repeats while enabled", async () => {
    const text = await runStream(createStream, {
      chunks: [
        makeCompletionsChunk({ role: "assistant", content: "Ha" }),
        makeCompletionsChunk({ content: "Ha" }),
        makeCompletionsChunk({}, "stop"),
      ],
      compat: { dropCumulativeTextDeltaReplays: true },
      expectedText: "HaHa",
    });
    expect(text).toBe("HaHa");
  });

  it("preserves long repeats that do not restate the whole message while enabled", async () => {
    const text = await runStream(createStream, {
      chunks: [
        makeCompletionsChunk({ role: "assistant", content: TEXT_A + TEXT_B }),
        // Repeats the previous delta verbatim, but not the whole message text.
        makeCompletionsChunk({ content: TEXT_B }),
        makeCompletionsChunk({ content: TEXT_C }),
        makeCompletionsChunk({}, "stop"),
      ],
      compat: { dropCumulativeTextDeltaReplays: true },
      expectedText: TEXT_A + TEXT_B + TEXT_B + TEXT_C,
    });
    expect(text).toBe(TEXT_A + TEXT_B + TEXT_B + TEXT_C);
  });

  it("drops a message-shaped frame equal to the accumulated text while enabled", async () => {
    const text = await runStream(createStream, {
      chunks: [
        makeCompletionsChunk({ role: "assistant", content: TEXT_A }),
        makeCompletionsChunk({ content: TEXT_B }),
        makeCompletionsChunk({}, null, {
          choices: [
            {
              index: 0,
              delta: {},
              message: { role: "assistant", content: TEXT_A + TEXT_B },
              finish_reason: null,
            },
          ],
        }),
        makeCompletionsChunk({ content: TEXT_C }),
        makeCompletionsChunk({}, "stop"),
      ],
      compat: { dropCumulativeTextDeltaReplays: true },
      expectedText: TEXT_A + TEXT_B + TEXT_C,
    });
    expect(text).toBe(TEXT_A + TEXT_B + TEXT_C);
  });

  it("continues appending ordinary deltas after a dropped replay while enabled", async () => {
    const text = await runStream(createStream, {
      chunks: [
        makeCompletionsChunk({ role: "assistant", content: TEXT_A }),
        makeCompletionsChunk({ content: TEXT_A }),
        makeCompletionsChunk({ content: TEXT_A + TEXT_B }),
        makeCompletionsChunk({}, "stop"),
      ],
      compat: { dropCumulativeTextDeltaReplays: true },
      expectedText: TEXT_A + (TEXT_A + TEXT_B),
    });
    expect(text).toBe(TEXT_A + TEXT_A + TEXT_B);
  });
});
