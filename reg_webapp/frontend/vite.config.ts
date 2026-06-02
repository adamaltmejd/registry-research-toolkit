/// <reference types="vitest/config" />
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [svelte()],
  server: {
    // Dev proxy: the SPA fetches /api/* from the FastAPI backend on :8000.
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  // Vitest reuses this config so it can resolve `.svelte` imports. jsdom gives
  // the unit tests a `window`/`fetch`-shaped global (router.svelte.ts reads
  // `window` at module load; api.ts mocks `fetch`).
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.ts"],
  },
});
