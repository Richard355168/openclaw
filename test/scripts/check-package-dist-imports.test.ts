import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  collectPackageDistImportErrors,
  collectPackageDistImports,
} from "../../scripts/lib/package-dist-imports.mjs";
import { cleanupTempDirs, makeTempDir } from "../helpers/temp-dir.js";

const CHECK_SCRIPT = "scripts/check-package-dist-imports.mjs";
const tempDirs: string[] = [];

afterEach(() => {
  cleanupTempDirs(tempDirs);
});

describe("collectPackageDistImports", () => {
  it("preserves ordered duplicate edges and the narrower import.meta.url filter", () => {
    const imports = collectPackageDistImports({
      files: ["dist/index.js"],
      readText: () =>
        [
          'import "./chunk.js?first";',
          'export * from "./chunk.js#second";',
          'import("./data.json");',
          'require("../outside.cjs");',
          'new URL("./chunk.js?third", import.meta.url);',
          'new URL("./asset.png", import.meta.url);',
          'new URL("../outside.cjs", import.meta.url);',
          'require("./chunk.js");',
          'import "node:fs";',
          'import "/absolute.js";',
        ].join("\n"),
    });

    expect(imports).toEqual(
      [
        "dist/chunk.js",
        "dist/chunk.js",
        "dist/data.json",
        "outside.cjs",
        "dist/chunk.js",
        "dist/chunk.js",
      ].map((importedPath) => ({ importerPath: "dist/index.js", importedPath })),
    );
    expect(
      collectPackageDistImportErrors({
        files: ["dist/index.js"],
        imports,
        readText: () => {
          throw new Error("provided edges must not reread the source");
        },
      }),
    ).toEqual(imports.map(({ importedPath }) => `dist/index.js imports missing ${importedPath}`));
  });

  it("normalizes a single file and reuses its only source buffer", () => {
    const readText = vi.fn((relativePath: string): string => {
      expect(relativePath).toBe("dist/index.mjs");
      if (readText.mock.calls.length > 1) {
        throw new Error("source buffer must be read only once");
      }
      return 'import "./chunk.mjs";';
    });
    const imports = collectPackageDistImports({
      files: ["package\\dist\\index.mjs"],
      readText,
    });

    expect(readText).toHaveBeenCalledTimes(1);
    expect(imports).toEqual([{ importerPath: "dist/index.mjs", importedPath: "dist/chunk.mjs" }]);
  });

  it.each([
    { files: [] },
    { files: ["README.md"] },
    { files: ["package/dist/node_modules/dependency/index.js"] },
  ])("does not read skipped input $files", ({ files }) => {
    const readText = vi.fn(() => {
      throw new Error("skipped files must not be read");
    });
    expect(collectPackageDistImports({ files, readText })).toEqual([]);
    expect(readText).not.toHaveBeenCalled();
  });

  it("normalizes, deduplicates and orders multiple files without reordering their edges", () => {
    const readText = vi.fn(() => 'require("./second.cjs"); import("./first.js");');
    const imports = collectPackageDistImports({
      files: [
        "dist/z.cjs",
        "package\\dist\\a.js",
        "dist/a.js",
        "package/dist/z.cjs",
        "dist/node_modules/dependency/index.js",
        "README.md",
      ],
      readText,
    });

    expect(readText.mock.calls).toEqual([["dist/a.js"], ["dist/z.cjs"]]);
    expect(imports).toEqual(
      ["dist/a.js", "dist/z.cjs"].flatMap((importerPath) =>
        ["dist/second.cjs", "dist/first.js"].map((importedPath) => ({
          importerPath,
          importedPath,
        })),
      ),
    );
  });

  it.each([
    {
      name: "nested import",
      source: 'function f() { import "./a.mjs"; }',
      paths: ["dist/a.mjs"],
    },
    {
      name: "TypeScript import equals",
      source: 'import x = require("./a.cjs");',
      paths: ["dist/a.cjs"],
    },
    {
      name: "syntax error between imports",
      source: 'import "./before.mjs"; const = ; import "./after.mjs";',
      paths: ["dist/before.mjs", "dist/after.mjs"],
    },
    {
      name: "unterminated string after require",
      source: 'require("./before.cjs"); const s = "unterminated',
      paths: ["dist/before.cjs"],
    },
    {
      name: "syntax error before require",
      source: 'const = ; require("./after.cjs");',
      paths: ["dist/after.cjs"],
    },
  ])("preserves TypeScript recovery for $name", ({ source, paths }) => {
    expect(collectPackageDistImports({ files: ["dist/index.js"], readText: () => source })).toEqual(
      paths.map((importedPath) => ({ importerPath: "dist/index.js", importedPath })),
    );
  });
});

describe("check-package-dist-imports", () => {
  it("prints help before reading package state", () => {
    const result = spawnSync("node", [CHECK_SCRIPT, "--help"], { encoding: "utf8" });

    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain(
      "Usage: node scripts/check-package-dist-imports.mjs [package-root]",
    );
    expect(result.stderr).toBe("");
  });

  it("rejects option-like and extra arguments before dist scanning", () => {
    const unknown = spawnSync("node", [CHECK_SCRIPT, "--tag"], { encoding: "utf8" });

    expect(unknown.status).not.toBe(0);
    expect(unknown.stderr).toContain("Unknown package dist import check option: --tag");
    expect(unknown.stderr).not.toContain("missing dist directory");

    const extra = spawnSync("node", [CHECK_SCRIPT, ".", "extra"], { encoding: "utf8" });

    expect(extra.status).not.toBe(0);
    expect(extra.stderr).toContain("Unexpected package dist import check argument: extra");
    expect(extra.stderr).not.toContain("missing dist directory");
  });

  it("accepts a minimal package dist root", () => {
    const root = makeTempDir(tempDirs, "openclaw-package-dist-imports-");
    mkdirSync(join(root, "dist"), { recursive: true });
    writeFileSync(join(root, "dist", "index.js"), "export {};\n", "utf8");

    const result = spawnSync("node", [CHECK_SCRIPT, root], { encoding: "utf8" });

    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain("OpenClaw package dist import closure passed.");
  });

  it("rejects missing chunks across ESM import, re-export, and CommonJS forms", () => {
    const root = makeTempDir(tempDirs, "openclaw-package-dist-imports-");
    mkdirSync(join(root, "dist"), { recursive: true });
    const sources = {
      "named-import.js": 'import { value } from "./missing.js";\n',
      "multiline-import.js": 'import {\n  value,\n} from "./missing.js";\n',
      "named-export.js": 'export { value } from "./missing.js";\n',
      "multiline-export.js": 'export {\n  value,\n} from "./missing.js";\n',
      "index.cjs": 'module.exports = require("./chunk.cjs");\n',
    };
    for (const [file, source] of Object.entries(sources)) {
      writeFileSync(join(root, "dist", file), source, "utf8");
    }

    const result = spawnSync("node", [CHECK_SCRIPT, root], { encoding: "utf8" });

    expect(result.status).not.toBe(0);
    for (const file of Object.keys(sources)) {
      const target = file.endsWith(".cjs") ? "chunk.cjs" : "missing.js";
      expect(result.stderr).toContain(`dist/${file} imports missing dist/${target}`);
    }
  });

  it("ignores import-like text inside multiline template literals", () => {
    const root = makeTempDir(tempDirs, "openclaw-package-dist-imports-");
    mkdirSync(join(root, "dist"), { recursive: true });
    writeFileSync(
      join(root, "dist", "index.js"),
      'const example = `\nimport "./phantom.js"\n`;\n',
      "utf8",
    );

    const result = spawnSync("node", [CHECK_SCRIPT, root], { encoding: "utf8" });

    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain("OpenClaw package dist import closure passed.");
  });

  it("ignores import.meta.url probes outside packaged dist", () => {
    const root = makeTempDir(tempDirs, "openclaw-package-dist-imports-");
    mkdirSync(join(root, "dist"), { recursive: true });
    const probes = [
      "../../openclaw.mjs",
      "../../scripts/run-node.mjs",
      "../../dist/entry.js",
      "../../dist/entry.mjs",
    ];
    writeFileSync(
      join(root, "dist", "index.js"),
      probes
        .map(
          (specifier, index) =>
            `const candidate${index} = new URL(${JSON.stringify(specifier)}, import.meta.url);`,
        )
        .join("\n"),
      "utf8",
    );

    const result = spawnSync("node", [CHECK_SCRIPT, root], { encoding: "utf8" });

    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain("OpenClaw package dist import closure passed.");
  });
});
