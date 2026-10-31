import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backend = "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": {
        target: backend,
        changeOrigin: true,
      },
      "/healthz": backend,
      "/ws": {
        target: backend,
        ws: true,
        changeOrigin: true,
        configure(proxy) {
          proxy.on("proxyReqWs", (proxyReq) => {
            proxyReq.setHeader("Origin", backend);
          });
        },
      },
    },
  },
});
