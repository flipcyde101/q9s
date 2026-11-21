import { defineConfig } from "tsup";

export default defineConfig([
  {
    entry: { cli: "src/cli.ts" },
    format: ["esm"],
    target: "node24",
    platform: "node",
    dts: false,
    clean: false,
    sourcemap: false,
    splitting: false,
    removeNodeProtocol: false,
    banner: {
      js: "#!/usr/bin/env node",
    },
  },
  {
    entry: { index: "src/index.ts" },
    format: ["esm"],
    target: "node24",
    platform: "node",
    dts: true,
    clean: false,
    sourcemap: false,
    splitting: false,
    removeNodeProtocol: false,
  },
]);
