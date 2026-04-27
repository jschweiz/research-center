import { resolve } from "node:path";

import react from "@vitejs/plugin-react-swc";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import { VitePWA } from "vite-plugin-pwa";

export default defineConfig(({ mode }) => {
  const isPublished =
    mode === "published" || process.env.BUILD_TARGET === "published" || process.env.npm_lifecycle_event === "build:published";
  const assetBase = isPublished ? "./" : "/";

  return {
    base: assetBase,
    build: {
      outDir: isPublished ? "dist-published" : "dist",
      rollupOptions: {
        input: resolve(__dirname, "index.html"),
      },
    },
    define: {
      __PUBLISHED_ONLY__: JSON.stringify(isPublished),
    },
    plugins: [
      react(),
      tailwindcss(),
      VitePWA({
        registerType: "autoUpdate",
        includeAssets: ["icon.svg", "icon-192.png", "icon-512.png", "apple-touch-icon.png"],
        workbox: {
          runtimeCaching: isPublished
            ? [
                {
                  urlPattern: ({ url }) => url.pathname.endsWith("/archive.json"),
                  handler: "NetworkFirst",
                  options: {
                    cacheName: "published-archive",
                    expiration: {
                      maxEntries: 2,
                    },
                  },
                },
                {
                  urlPattern: ({ url }) => url.pathname.endsWith("/manifest.json"),
                  handler: "NetworkFirst",
                  options: {
                    cacheName: "published-manifests",
                    expiration: {
                      maxEntries: 12,
                    },
                  },
                },
              ]
            : [],
        },
        manifest: {
          name: "Research Center",
          short_name: "Research Center",
          description: "Editorial daily research briefing for mobile and tablet review.",
          theme_color: "#ece4d3",
          background_color: "#ece4d3",
          display: "standalone",
          start_url: assetBase,
          scope: assetBase,
          icons: [
            {
              src: `${assetBase}icon-192.png`,
              sizes: "192x192",
              type: "image/png",
            },
            {
              src: `${assetBase}icon-512.png`,
              sizes: "512x512",
              type: "image/png",
            },
            {
              src: `${assetBase}icon.svg`,
              sizes: "512x512",
              type: "image/svg+xml",
              purpose: "any maskable",
            },
          ],
        },
      }),
    ],
    server: {
      port: 5173,
    },
  };
});
