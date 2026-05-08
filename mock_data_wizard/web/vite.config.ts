import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

const PY_API_PORT = 8765;

export default defineConfig({
  plugins: [svelte()],
  server: {
    proxy: {
      // Forward API + (eventually) WebSocket calls to the Python server
      // running locally. The Python server is the source of truth in dev
      // and prod; Vite only serves the SPA shell during development.
      "/api": `http://127.0.0.1:${PY_API_PORT}`,
    },
  },
  build: {
    // Vite writes the built SPA next to the Python package so hatchling
    // bundles it into the wheel. emptyOutDir replaces the placeholder
    // index.html the package ships when the frontend hasn't been built.
    outDir: "../src/mock_data_wizard/static",
    emptyOutDir: true,
    // Sourcemaps would bloat the wheel by ~10x for marginal debugging
    // benefit; rebuild locally with `bun run build -- --sourcemap` to
    // get them when investigating a specific bug.
    sourcemap: false,
  },
});
