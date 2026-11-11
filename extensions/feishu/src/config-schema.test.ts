import { validateJsonSchemaValue } from "openclaw/plugin-sdk/json-schema-runtime";
import { describe, expect, it } from "vitest";
import { FeishuChannelConfigSchema, FeishuConfigSchema } from "./config-schema.js";

// Separate negative fixture tokens from webhook literals to avoid the
// GHSA-G353-MGV3-8PCJ opengrep rule. Positive fixtures stay literal.
const topLevelWebhookBase = {
  connectionMode: "webhook",
  appId: "cli_top",
  appSecret: "secret_top", // pragma: allowlist secret
};
const accountWebhookBase = { ...topLevelWebhookBase, appId: "cli_main" };

function expectSchemaIssue(value: unknown, issuePath: string) {
  const result = FeishuConfigSchema.safeParse(value);
  expect(result.success).toBe(false);
  if (!result.success) {
    expect(result.error.issues.map((issue) => issue.path.join("."))).toContain(issuePath);
  }
}

describe("FeishuConfigSchema", () => {
  it("normalizes legacy groupPolicy allowall to open", () => {
    expect(FeishuConfigSchema.parse({ groupPolicy: "allowall" }).groupPolicy).toBe("open");
  });

  it.each([
    [topLevelWebhookBase, "verificationToken"],
    [{ ...topLevelWebhookBase, verificationToken: "token_top" }, "encryptKey"],
    [{ accounts: { main: accountWebhookBase } }, "accounts.main.verificationToken"],
    [
      { accounts: { main: { ...accountWebhookBase, verificationToken: "token_main" } } },
      "accounts.main.encryptKey",
    ],
  ] as const)("rejects webhook config %j without %s", (value, issuePath) => {
    expectSchemaIssue(value, issuePath);
  });

  it("accepts SecretRef encryptKey in webhook mode", () => {
    const result = FeishuConfigSchema.safeParse({
      connectionMode: "webhook",
      verificationToken: { source: "env", provider: "default", id: "FEISHU_VERIFICATION_TOKEN" },
      encryptKey: { source: "env", provider: "default", id: "FEISHU_ENCRYPT_KEY" },
      appId: "cli_top",
      appSecret: { source: "env", provider: "default", id: "FEISHU_APP_SECRET" },
    });

    expect(result.success).toBe(true);
  });
  const entry = { file_received: ["thumbs up", "赞", "👍"] };

  function expectCatalogValidation(value: Record<string, unknown>, accepted: boolean) {
    expect(FeishuConfigSchema.safeParse(value).success, "Zod validation").toBe(accepted);
    const result = validateJsonSchemaValue({
      schema: FeishuChannelConfigSchema.schema,
      cacheKey: "feishu-sticker-catalog-test",
      value,
      applyDefaults: true,
    });
    expect(result.ok, "exported JSON Schema validation").toBe(accepted);
    if (result.ok) {
      expect(result.value).toMatchObject(value);
    }
  }

  it("accepts bounded bot catalogs only at channel scope", () => {
    const stickerSets = {
      "bot-without-prefix": entry,
      ["a".repeat(128)]: Object.fromEntries(
        Array.from({ length: 256 }, (_, index) => [
          `file_${index}`.padEnd(512, "界"),
          Array.from({ length: 8 }, () => "赞".repeat(64)),
        ]),
      ),
      ...Object.fromEntries(Array.from({ length: 30 }, (_, index) => [`bot_${index}`, {}])),
    };
    expectCatalogValidation({ stickerSets }, true);
    expect(FeishuConfigSchema.parse({ stickerSets }).stickerSets).toEqual(stickerSets);
    expect(FeishuConfigSchema.parse({}).stickerSets).toBeUndefined();
    expectCatalogValidation({ accounts: { work: { stickerSets } } }, false);
  });
  it("rejects defaultAccount when it does not match an account key", () => {
    expectSchemaIssue(
      {
        defaultAccount: "router-d",
        accounts: {
          backup: { appId: "cli_backup", appSecret: "secret_backup" }, // pragma: allowlist secret
        },
      },
      "defaultAccount",
    );
  });
});

describe("FeishuConfigSchema streaming.finalize upgrade compatibility", () => {
  // A stored config shaped exactly like a pre-finalize build: the full legacy
  // streaming surface, no `finalize` key. It must keep loading unchanged so
  // existing stored settings survive the upgrade; the runtime resolves the
  // absent key to the documented "overwrite" default, which matches the
  // pre-PR final-card disposition (reply-dispatcher tests cover that half).
  const legacyStoredConfig = {
    connectionMode: "websocket",
    dmPolicy: "pairing",
    groupPolicy: "allowlist",
    streaming: {
      mode: "partial",
      chunkMode: "newline",
      block: { enabled: true, coalesce: { idleMs: 250 } },
    },
    accounts: {
      main: {
        streaming: { mode: "partial", block: { enabled: false } },
      },
    },
  };

  it("loads pre-finalize stored configs unchanged at both scopes", () => {
    const result = FeishuConfigSchema.parse(legacyStoredConfig);
    expect(result.streaming).toEqual(legacyStoredConfig.streaming);
    expect(result.accounts?.main?.streaming).toEqual(legacyStoredConfig.accounts.main.streaming);
    expect(result.streaming?.finalize).toBeUndefined();
    expect(result.accounts?.main?.streaming?.finalize).toBeUndefined();

    const exported = validateJsonSchemaValue({
      schema: FeishuChannelConfigSchema.schema,
      cacheKey: "feishu-finalize-upgrade-compat",
      value: legacyStoredConfig,
      applyDefaults: true,
    });
    expect(exported.ok, "exported JSON Schema validation").toBe(true);
    if (exported.ok) {
      expect(exported.value.streaming?.finalize).toBeUndefined();
      expect(exported.value.accounts?.main?.streaming?.finalize).toBeUndefined();
    }
  });

  it("adds finalize as an optional enum only at both scopes", () => {
    const result = FeishuConfigSchema.parse({
      streaming: { finalize: "append" },
      accounts: { main: { streaming: { finalize: "overwrite" } } },
    });
    expect(result.streaming?.finalize).toBe("append");
    expect(result.accounts?.main?.streaming?.finalize).toBe("overwrite");
    expect(FeishuConfigSchema.safeParse({ streaming: { finalize: "keep" } }).success).toBe(false);
    expect(FeishuChannelConfigSchema.schema).toMatchObject({
      properties: {
        streaming: {
          properties: { finalize: { enum: ["overwrite", "append"], type: "string" } },
        },
        accounts: {
          additionalProperties: {
            properties: {
              streaming: {
                properties: { finalize: { enum: ["overwrite", "append"], type: "string" } },
              },
            },
          },
        },
      },
    });
  });
});
